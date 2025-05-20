use crate::db::FuguDB;
use crate::object::ObjectRecord;
use crate::tokeinze::{Token, TokenPosition, TokenType, tokenize_query};
use crate::tracing_utils;
use anyhow::{Context, Result, anyhow};
use futures_util::future::FutureExt; // For now_or_never
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tracing::{Instrument, debug, error, info, warn};

// Struct definition for completeness
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub terms: Vec<Token>,
    pub logical_operator: LogicalOperator,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}
/// Query configuration options
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Default number of results to return
    pub default_limit: usize,
    /// Whether to include snippets with highlights
    pub highlight_snippets: bool,
    /// Minimum threshold for matching
    pub min_score_threshold: f64,
    /// Maximum number of results to fetch
    pub max_results: usize,
    /// Context window size for snippets
    pub snippet_context_size: usize,
    /// BM25 k1 parameter (kept for compatibility)
    pub bm25_k1: f64,
    /// BM25 b parameter (kept for compatibility)
    pub bm25_b: f64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            default_limit: 1000,
            highlight_snippets: true,
            min_score_threshold: 0.1,
            max_results: 1000,
            snippet_context_size: 50,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        }
    }
}

/// Represents a query to search objects
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Query {
    pub terms: Vec<QueryTerm>,
    pub logical_operator: LogicalOperator,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            terms: Vec::new(),
            logical_operator: LogicalOperator::And,
            limit: None,
            offset: None,
        }
    }
}

/// Individual query term with modifiers
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryTerm {
    pub text: String,
    pub weight: Option<f64>,
    pub fuzzy: Option<bool>,
    pub prefix: Option<bool>,
}

impl QueryTerm {
    pub fn new(text: &str) -> Self {
        Self {
            text: text.to_string(),
            weight: None,
            fuzzy: None,
            prefix: None,
        }
    }
}

/// Logical operators for combining query terms
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogicalOperator {
    And,
    Or,
}

impl FromStr for LogicalOperator {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "and" => Ok(LogicalOperator::And),
            "or" => Ok(LogicalOperator::Or),
            _ => Err(format!("Unknown logical operator: {}", s)),
        }
    }
}

/// A position of a term in a document
#[derive(Clone, Debug)]
pub struct TermPosition {
    pub position: TokenPosition,
    pub document_id: String,
}

impl fmt::Display for TermPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.document_id, self.position.start)
    }
}

/// Represents an index mapping terms to their positions in documents
#[derive(Clone, Debug)]
pub struct TermIndex {
    terms: HashMap<String, Vec<TermPosition>>,
}

impl TermIndex {
    /// Create a new empty term index
    pub fn new() -> Self {
        Self {
            terms: HashMap::new(),
        }
    }

    /// Create a term index from an existing HashMap
    pub fn from_hashmap(terms: HashMap<String, Vec<TermPosition>>) -> Self {
        Self { terms }
    }

    /// Get positions for a specific term
    pub fn get(&self, term: &str) -> Option<&Vec<TermPosition>> {
        self.terms.get(term)
    }

    /// Add a term position to the index
    pub fn add(&mut self, term: String, position: TermPosition) {
        self.terms
            .entry(term)
            .or_insert_with(Vec::new)
            .push(position);
    }

    /// Get a reference to the internal HashMap
    pub fn as_hashmap(&self) -> &HashMap<String, Vec<TermPosition>> {
        &self.terms
    }

    /// Get a mutable reference to the internal HashMap
    pub fn as_hashmap_mut(&mut self) -> &mut HashMap<String, Vec<TermPosition>> {
        &mut self.terms
    }
}

impl fmt::Display for TermIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sorted_keys: Vec<&String> = self.terms.keys().collect();
        sorted_keys.sort();

        for (i, term) in sorted_keys.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }

            write!(f, "{}: [", term)?;

            if let Some(positions) = self.terms.get(*term) {
                for (j, pos) in positions.iter().enumerate() {
                    if j > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", pos)?;
                }
            }

            write!(f, "]")?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct QueryTerms(pub Vec<String>);

impl fmt::Display for QueryTerms {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;

        for (i, term) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "\"{}\"", term)?;
        }

        write!(f, "]")
    }
}

/// A matching document with score and optional highlights
#[derive(Clone, Debug, Serialize)]
pub struct ScoredDocument {
    pub id: String,
    pub score: f64,
    pub metadata: Option<Value>,
    pub highlights: Option<Vec<String>>,
}

impl fmt::Display for ScoredDocument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display doc ID and score
        writeln!(f, "Document: {} (Score: {:.4})", self.id, self.score)?;

        // Display metadata if available
        if let Some(meta) = &self.metadata {
            writeln!(f, "Metadata:")?;

            // Try to find common important fields
            let important_fields = [
                "title", "name", "subject", "author", "date", "source", "url",
            ];
            let mut found_fields = false;

            // First display important fields if present
            for field in important_fields.iter() {
                if let Some(value) = meta.get(*field) {
                    let value_str = match value {
                        Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    writeln!(f, "  {}: {}", field, value_str)?;
                    found_fields = true;
                }
            }

            // Then display other fields
            if let Value::Object(obj) = meta {
                for (key, value) in obj.iter() {
                    // Skip fields we've already displayed
                    if important_fields.contains(&key.as_str()) {
                        continue;
                    }

                    let value_str = match value {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        Value::Bool(b) => b.to_string(),
                        Value::Array(a) => {
                            if a.len() <= 3 {
                                format!(
                                    "[{}]",
                                    a.iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )
                            } else {
                                format!(
                                    "[{}, ... and {} more]",
                                    a.iter()
                                        .take(2)
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(", "),
                                    a.len() - 2
                                )
                            }
                        }
                        Value::Object(_) => "{...}".to_string(),
                        Value::Null => "null".to_string(),
                    };

                    // Truncate very long values
                    let display_value = if value_str.len() > 80 {
                        format!("{}...", &value_str[0..77])
                    } else {
                        value_str
                    };

                    writeln!(f, "  {}: {}", key, display_value)?;
                    found_fields = true;
                }
            } else {
                // If metadata is not an object, just display it
                if !found_fields {
                    writeln!(f, "  {}", meta)?;
                }
            }
        } else {
            writeln!(f, "Metadata: None")?;
        }

        // Display highlights if available
        if let Some(highlights) = &self.highlights {
            writeln!(f, "Highlights:")?;
            for (i, highlight) in highlights.iter().enumerate() {
                if i >= 3 && highlights.len() > 4 {
                    writeln!(f, "  ... and {} more snippets", highlights.len() - i)?;
                    break;
                }

                let display_highlight = if highlight.len() > 100 {
                    format!("{}...", &highlight[0..97])
                } else {
                    highlight.clone()
                };

                writeln!(f, "  {}: \"{}\"", i + 1, display_highlight)?;
            }
        } else {
            writeln!(f, "Highlights: None")?;
        }

        Ok(())
    }
}

/// Alias for backward compatibility with old API
pub type QueryHit = ScoredDocument;

/// Represents search results returned to the client
#[derive(Clone, Debug, Serialize)]
pub struct QueryResults {
    pub took_ms: u64,
    pub total_hits: usize,
    pub hits: Vec<QueryHit>,
}

/// Highlight in search results
#[derive(Clone, Debug, Serialize)]
pub struct Highlight {
    pub term: String,
    pub context: String,
}

/// A wrapper for a collection of scored documents that implements Display
#[derive(Clone, Debug)]
pub struct SearchResults(pub Vec<ScoredDocument>);

impl SearchResults {
    /// Create a new SearchResults from a vector of ScoredDocument
    pub fn new(docs: Vec<ScoredDocument>) -> Self {
        Self(docs)
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get a reference to the underlying vector
    pub fn as_vec(&self) -> &Vec<ScoredDocument> {
        &self.0
    }

    /// Get a mutable reference to the underlying vector
    pub fn as_vec_mut(&mut self) -> &mut Vec<ScoredDocument> {
        &mut self.0
    }

    /// Take ownership of the underlying vector
    pub fn into_vec(self) -> Vec<ScoredDocument> {
        self.0
    }
}

impl fmt::Display for SearchResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return write!(f, "No search results found");
        }

        writeln!(f, "Search Results ({} documents):", self.0.len())?;

        // Calculate the max width for ID field for alignment
        let max_id_width = self
            .0
            .iter()
            .map(|doc| doc.id.len())
            .max()
            .unwrap_or(10)
            .min(40); // Cap at 40 chars to avoid excessive width

        // Table header
        writeln!(
            f,
            "{:<width$} | {:<10} | {}",
            "Document ID",
            "Score",
            "Metadata",
            width = max_id_width
        )?;

        writeln!(
            f,
            "{:-<width$}-+-{:-<10}-+-{:-<20}",
            "",
            "",
            "",
            width = max_id_width
        )?;

        // Table rows
        for doc in &self.0 {
            let id_display = if doc.id.len() > max_id_width {
                format!("{}...", &doc.id[0..(max_id_width - 3)])
            } else {
                doc.id.clone()
            };

            // Format metadata as a short preview
            let metadata_display = match &doc.metadata {
                Some(meta) => {
                    // Check for title-like fields in the JSON metadata
                    let title = meta
                        .get("title")
                        .or_else(|| meta.get("name"))
                        .or_else(|| meta.get("subject"));

                    match title {
                        Some(title_val) => {
                            // Convert JSON Value to string for display
                            let title_str = match title_val {
                                Value::String(s) => s.clone(),
                                _ => title_val.to_string(),
                            };

                            if title_str.len() > 50 {
                                format!("\"{}...\"", &title_str[0..47])
                            } else {
                                format!("\"{}\"", title_str)
                            }
                        }
                        None => {
                            // For JSON objects, get some keys
                            if let Value::Object(obj) = meta {
                                let keys: Vec<String> = obj
                                    .keys()
                                    .take(3)
                                    .map(|k| k.to_string()) // Convert &str to String
                                    .collect();

                                if keys.is_empty() {
                                    "{...}".to_string()
                                } else {
                                    // Now we can join strings
                                    format!("{{{}...}}", keys.join(", "))
                                }
                            } else {
                                // For non-objects, convert to string
                                let meta_str = meta.to_string();
                                if meta_str.len() > 50 {
                                    format!("{}...", &meta_str[0..47])
                                } else {
                                    meta_str
                                }
                            }
                        }
                    }
                }
                None => "No metadata".to_string(),
            };

            writeln!(
                f,
                "{:<width$} | {:<10.4} | {}",
                id_display,
                doc.score,
                metadata_display,
                width = max_id_width
            )?;

            // Add snippet preview if available
            if let Some(highlights) = &doc.highlights {
                if !highlights.is_empty() {
                    let snippet = &highlights[0];
                    let preview = if snippet.len() > 70 {
                        format!("\"{}...\"", &snippet[0..67])
                    } else {
                        format!("\"{}\"", snippet)
                    };

                    writeln!(f, "{:<width$}   └─ {}", "", preview, width = max_id_width)?;
                }
            }
        }

        Ok(())
    }
}

/// Main query engine that works with our database backend
pub struct QueryEngine {
    db: Arc<FuguDB>,
    config: QueryConfig,
}

impl QueryEngine {
    /// Create a new query engine with the given database and config
    pub fn new(db: Arc<FuguDB>, config: QueryConfig) -> Self {
        Self { db, config }
    }

    pub async fn execute(&self, query: Query) -> anyhow::Result<Vec<ScoredDocument>> {
        let span = tracing_utils::db_span("query_execute");
        let _enter = span.enter();

        let start_time = Instant::now();
        info!("Executing query: {:?}", query);

        // Parse the query using StandardTokenizer
        let parsed_query = self
            .parse_query(query)
            .await
            .context("Failed to parse query")?;

        // Log the parsed query for debugging
        info!("Parsed query: {:?}", parsed_query);

        // IMPORTANT: If we have no terms, return early
        if parsed_query.terms.is_empty() {
            info!("Query has no terms, returning empty result");
            return Ok(Vec::new());
        }

        // Collect document statistics for scoring
        let (doc_count, avg_doc_length, term_freq) = self
            .collect_document_statistics()
            .context("Failed to collect document statistics")?;
        info!(
            "Document stats: count={}, avg_length={:.2}",
            doc_count, avg_doc_length
        );

        // Get query terms from the parsed tokens with proper normalization
        let query_terms: Vec<String> = parsed_query
            .terms
            .iter()
            .map(|t| t.text.to_lowercase()) // Ensure consistent lowercase for searching
            .collect();

        // Get term positions from the inverted index
        let mut term_positions = HashMap::new();
        for term in &query_terms {
            let positions_result = self
                .get_term_positions(term)
                .with_context(|| format!("Failed to get positions for term '{}'", term));

            match positions_result {
                Ok(positions_map) => {
                    let positions = TermIndex::from_hashmap(positions_map);
                    info!(
                        "Term '{}': found in {} documents with {} total positions",
                        term,
                        positions.terms.len(),
                        positions.terms.values().map(|v| v.len()).sum::<usize>()
                    );

                    term_positions.insert(term.clone(), positions.terms);
                }
                Err(e) => {
                    // Log the error but continue with other terms
                    warn!("Error getting positions for term '{}': {:?}", term, e);
                }
            }
        }

        // Debug: Check if we have any terms with positions
        if term_positions.is_empty() {
            info!("No terms found in index, returning empty result");
            return Ok(Vec::new());
        }

        // Score documents based on term positions and document statistics
        let scored_docs = self.score_documents(
            &query_terms,
            &term_positions,
            doc_count,
            avg_doc_length,
            &parsed_query.logical_operator,
        );

        // Debug: Check if we have any scored documents
        info!("Scored {} documents", scored_docs.len());

        // Sort by score descending
        let mut results: Vec<_> = scored_docs.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

        // Debug: Log score distribution
        if !results.is_empty() {
            let max_score = results.first().map(|(_, s)| *s).unwrap_or(0.0);
            let min_score = results.last().map(|(_, s)| *s).unwrap_or(0.0);
            info!(
                "Score range: {:.4} to {:.4}, threshold: {:.4}",
                max_score, min_score, self.config.min_score_threshold
            );
        }

        // Apply limit
        let limit = parsed_query.limit.unwrap_or(self.config.default_limit);
        let offset = parsed_query.offset.unwrap_or(0);

        // Debug collection before filtering
        let results_before_filter = results.iter().skip(offset).take(limit).collect::<Vec<_>>();

        info!(
            "Before threshold filter: {} results",
            results_before_filter.len()
        );

        if !results_before_filter.is_empty() {
            info!(
                "Sample scores before filter: {:?}",
                results_before_filter.iter().take(5).collect::<Vec<_>>()
            );
        }

        let results = results
            .into_iter()
            .skip(offset)
            .take(limit)
            .filter(|(_, score)| {
                let passes = *score >= self.config.min_score_threshold;
                if !passes {
                    debug!(
                        "Filtering out doc with score {:.4} < threshold {:.4}",
                        *score, self.config.min_score_threshold
                    );
                }
                passes
            })
            .collect::<Vec<_>>();

        info!(
            "Found {} results in {:.2?}",
            results.len(),
            start_time.elapsed()
        );

        // Build the final result objects
        let mut documents = Vec::new();
        for (doc_id, score) in results {
            let mut highlights = None;
            let mut metadata = None;

            // Fetch full document record
            if let Some(record) = self.db.get(&doc_id) {
                metadata = Some(record.metadata.clone());

                // Add highlights if enabled and score is above threshold
                if self.config.highlight_snippets && score >= self.config.min_score_threshold {
                    // Extract positions for this document across all terms
                    let mut doc_term_positions = HashMap::new();
                    for (term, positions_map) in &term_positions {
                        if let Some(positions) = positions_map.get(&doc_id) {
                            doc_term_positions.insert(term.clone(), positions.clone());
                        }
                    }

                    let snippets =
                        self.create_highlights(&record.text, &query_terms, &doc_term_positions);

                    if !snippets.is_empty() {
                        highlights = Some(snippets);
                    }
                }
            } else {
                warn!("Document with ID {} not found in database", doc_id);
            }

            documents.push(ScoredDocument {
                id: doc_id,
                score,
                metadata,
                highlights,
            });
        }

        let search_results = SearchResults(documents.clone());
        info!("Search results:\n{}", search_results);

        Ok(documents)
    }

    pub async fn search_text(
        &self,
        query_text: &str,
        limit: Option<usize>,
    ) -> anyhow::Result<QueryResults> {
        let start_time = Instant::now();
        info!("Executing text search: {}", query_text);

        // Create a query from the text using our StandardTokenizer
        let mut tokens = Vec::new();

        // Use the tokenizer to properly tokenize the input
        tokenize_query(query_text, &mut tokens)
            .await
            .context("Failed to tokenize query text")?;

        // Convert tokens to QueryTerms, filtering out non-searchable tokens
        let query_terms: Vec<QueryTerm> = tokens
            .into_iter()
            .filter(|token| {
                !matches!(
                    token.token_type,
                    TokenType::Punctuation | TokenType::PageHeader
                )
            })
            .map(|token| QueryTerm::new(&token.text))
            .collect();

        // Create the query
        let query = Query {
            terms: query_terms,
            logical_operator: LogicalOperator::And,
            limit,
            offset: None,
        };

        info!(
            "Search terms: {:?}, operator: {:?}",
            query.terms.iter().map(|t| &t.text).collect::<Vec<_>>(),
            query.logical_operator
        );

        // Execute the query
        let scored_docs = self
            .execute(query)
            .await
            .context("Failed to execute query")?;

        info!("Found {} documents matching query", scored_docs.len());

        // Convert to QueryResults format
        Ok(QueryResults {
            took_ms: start_time.elapsed().as_millis() as u64,
            total_hits: scored_docs.len(),
            hits: scored_docs,
        })
    }

    // Update search_json to be async
    pub async fn search_json(
        &self,
        query_json: Value,
        limit: Option<usize>,
    ) -> Result<QueryResults> {
        let start_time = Instant::now();

        // Extract query text from JSON
        let query_text = match query_json.get("query") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(anyhow!("Invalid query: missing 'query' field")),
        };

        // Get top_k from JSON if available
        let json_limit = match query_json.get("top_k") {
            Some(Value::Number(n)) => n.as_u64().map(|v| v as usize),
            _ => None,
        };

        // Use provided limit or the one from JSON
        let final_limit = limit.or(json_limit);

        // Just delegate to search_text for the actual search
        let mut results = self.search_text(&query_text, final_limit).await?;

        // Apply any filters from the JSON
        if let Some(Value::Array(filters)) = query_json.get("filters") {
            debug!("Filters not yet implemented in unified API");
            // TODO: When implementing filters, apply them to results.hits here
        }

        // Update timing to include filter application
        results.took_ms = start_time.elapsed().as_millis() as u64;

        Ok(results)
    }
    pub async fn parse_query(&self, query: Query) -> anyhow::Result<ParsedQuery> {
        let span = tracing_utils::db_span("parse_query");
        let _enter = span.enter();

        info!("Parsing query with {} terms", query.terms.len());

        // If there are no terms, return an empty parsed query
        if query.terms.is_empty() {
            info!("Query has no terms, returning empty parsed query");
            return Ok(ParsedQuery {
                terms: Vec::new(),
                logical_operator: query.logical_operator,
                limit: query.limit,
                offset: query.offset,
            });
        }

        // Convert each QueryTerm to a full Token
        let mut all_tokens = Vec::new();

        let mut token_tasks = Vec::new();

        for query_term in &query.terms {
            let term_text = query_term.text.clone();

            // Spawn a task for each term tokenization
            let task = tokio::spawn(async move {
                let mut tokens = Vec::new();
                match tokenize_query(&term_text, &mut tokens).await {
                    Ok(_) => Ok(tokens),
                    Err(e) => Err(anyhow!("Failed to tokenize term '{}': {}", term_text, e)),
                }
            });

            token_tasks.push(task);
        }

        // Await all tokenization tasks
        for task in token_tasks {
            match task.await {
                Ok(Ok(tokens)) => all_tokens.extend(tokens),
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(anyhow!("Task join error: {}", e)),
            }
        }

        // Filter out any tokens that shouldn't be used for search
        let terms: Vec<Token> = all_tokens
            .into_iter()
            .filter(|token| {
                !matches!(
                    token.token_type,
                    TokenType::Punctuation | TokenType::PageHeader
                )
            })
            .collect();

        info!("Parsed {} tokens from query terms", terms.len());

        if !terms.is_empty() {
            let term_debug = terms
                .iter()
                .map(|t| format!("'{}' ({:?})", t.text, t.token_type))
                .collect::<Vec<_>>()
                .join(", ");
            debug!("Query tokens: {}", term_debug);
        }

        // Construct the parsed query
        Ok(ParsedQuery {
            terms,
            logical_operator: query.logical_operator,
            limit: query.limit,
            offset: query.offset,
        })
    }

    /// Collect document statistics for scoring
    /// Made public for testing purposes
    pub fn collect_document_statistics(&self) -> Result<(usize, f64, HashMap<String, usize>)> {
        let span = tracing_utils::db_span("collect_document_statistics");
        let _enter = span.enter();

        // We need:
        // 1. Total document count
        // 2. Average document length
        // 3. Term frequency across all documents

        let mut doc_count = 0;
        let mut total_tokens = 0;
        let mut term_doc_freqs = HashMap::new();

        // Get the RECORDS tree handle
        let records_handle = match self.db.open_tree(crate::db::TREE_RECORDS) {
            Ok(handle) => handle,
            Err(e) => {
                error!("Failed to open RECORDS collection: {}", e);
                return Err(anyhow!("Failed to open RECORDS collection: {}", e));
            }
        };

        // Scan all records using our abstracted iterator
        let records_iter = match records_handle.iter() {
            Ok(iter) => iter,
            Err(e) => {
                error!("Failed to create iterator for RECORDS collection: {}", e);
                return Err(anyhow!(
                    "Failed to create iterator for RECORDS collection: {}",
                    e
                ));
            }
        };

        for item in records_iter {
            match item {
                Ok((key_vec, value_vec)) => {
                    doc_count += 1;

                    // Try to deserialize the record
                    if let Ok(archivable) = rkyv::from_bytes::<
                        crate::object::ArchivableObjectRecord,
                        rkyv::rancor::Error,
                    >(&value_vec)
                    {
                        let record = ObjectRecord::from(archivable);

                        // Tokenize text (simple whitespace tokenization for now)
                        let tokens: Vec<&str> = record.text.split_whitespace().collect();
                        total_tokens += tokens.len();

                        // Count document frequency for each unique term
                        let mut seen_terms = std::collections::HashSet::new();
                        for token in tokens {
                            let term = token.to_lowercase();
                            if seen_terms.insert(term.clone()) {
                                *term_doc_freqs.entry(term).or_insert(0) += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error iterating record: {}", e);
                }
            }
        }

        let avg_doc_length = if doc_count > 0 {
            total_tokens as f64 / doc_count as f64
        } else {
            0.0
        };

        debug!(
            "Document statistics: count={}, avg_length={:.2}, unique_terms={}",
            doc_count,
            avg_doc_length,
            term_doc_freqs.len()
        );

        Ok((doc_count, avg_doc_length, term_doc_freqs))
    }

    /// Get term positions from the index
    fn get_term_positions(&self, term: &str) -> Result<HashMap<String, Vec<TermPosition>>> {
        let mut result_map = HashMap::new();
        let prefix = crate::db::PREFIX_RECORD_INDEX_TREE;

        // Define a function to process a tree/partition
        let process_tree = |name_str: &str, result: &mut HashMap<String, Vec<TermPosition>>| {
            // Skip trees that don't match our prefix pattern
            if !name_str.starts_with(&prefix) {
                return;
            }

            // Extract the object ID from the tree name
            let object_id = name_str[prefix.len()..].to_string();

            // Open the object's index tree using our unified API
            if let Ok(index_handle) = self.db.open_tree(name_str) {
                // Check if this object contains the term
                if let Ok(Some(positions_bytes)) = index_handle.get(term) {
                    // Deserialize the positions
                    if let Ok(positions) = crate::db::deserialize_positions(&positions_bytes) {
                        // Convert to term positions
                        let term_positions: Vec<TermPosition> = positions
                            .iter()
                            .map(|&pos| TermPosition {
                                position: pos, // Convert u64 to usize
                                document_id: object_id.clone(),
                            })
                            .collect();

                        if !term_positions.is_empty() {
                            result.insert(object_id.clone(), term_positions);
                        }
                    } else {
                        error!(
                            "Failed to deserialize positions for term '{}' in document '{}'",
                            term, object_id
                        );
                    }
                }
            }
        };

        // Get tree/partition names using a unified approach
        let tree_names = self.get_storage_names()?;
        for name_str in tree_names {
            process_tree(&name_str, &mut result_map);
        }

        debug!("Term '{}' found in {} documents", term, result_map.len());
        Ok(result_map)
    }

    /// Get storage names (tree names for sled, partition names for fjall)
    fn get_storage_names(&self) -> Result<Vec<String>> {
        let partitions = self.db.partition_names();
        Ok(partitions)
    }

    /// Score documents based on term positions
    fn score_documents(
        &self,
        terms: &[String],
        term_positions: &HashMap<String, HashMap<String, Vec<TermPosition>>>,
        doc_count: usize,
        avg_doc_length: f64,
        operator: &LogicalOperator,
    ) -> HashMap<String, f64> {
        let mut document_scores = HashMap::new();

        // For AND logic, pre-filter documents to those containing all terms
        let docs_to_process = if let LogicalOperator::And = operator {
            // Find documents containing all terms
            let mut docs_with_all_terms: Option<HashSet<String>> = None;

            for term in terms {
                if let Some(positions) = term_positions.get(term) {
                    let term_docs: HashSet<String> = positions.keys().cloned().collect();

                    match &docs_with_all_terms {
                        None => docs_with_all_terms = Some(term_docs),
                        Some(current_docs) => {
                            // Intersection with previous terms
                            let intersection: HashSet<String> =
                                current_docs.intersection(&term_docs).cloned().collect();

                            docs_with_all_terms = Some(intersection);

                            // Early exit if intersection is empty
                            if docs_with_all_terms.as_ref().unwrap().is_empty() {
                                return HashMap::new();
                            }
                        }
                    }
                } else {
                    // Term not found, no documents match all terms
                    return HashMap::new();
                }
            }

            docs_with_all_terms
        } else {
            // For OR, include all documents with any term
            None
        };

        // Calculate document lengths once

        // Cache IDF values for each term
        let mut idf_cache = HashMap::new();

        // Score only relevant documents (all for OR, intersection for AND)
        for term in terms {
            if let Some(positions) = term_positions.get(term) {
                // Calculate IDF once per term
                let idf = {
                    let doc_freq = positions.len();
                    (1.0 + (doc_count as f64 - doc_freq as f64 + 0.5) / (doc_freq as f64 + 0.5))
                        .ln()
                };
                idf_cache.insert(term, idf);

                for (doc_id, term_positions_in_doc) in positions {
                    // Skip if we're doing AND and this doc isn't in all terms
                    if let Some(ref docs_with_all_terms) = docs_to_process {
                        if !docs_with_all_terms.contains(doc_id) {
                            continue;
                        }
                    }

                    // Calculate position score once
                    let tf = term_positions_in_doc.len() as f64;

                    // Position bias calculation
                    let position_sum: f64 = term_positions_in_doc
                        .iter()
                        .map(|pos| 1.0 / (1.0 + pos.position.start as f64 * 0.01))
                        .sum();

                    let position_score = if term_positions_in_doc.is_empty() {
                        0.0
                    } else {
                        position_sum / term_positions_in_doc.len() as f64
                    };

                    // Combine scores more efficiently
                    let score = tf * idf * position_score;

                    match operator {
                        LogicalOperator::Or => {
                            *document_scores.entry(doc_id.clone()).or_insert(0.0) += score;
                        }
                        LogicalOperator::And => {
                            *document_scores.entry(doc_id.clone()).or_insert(1.0) *= score;
                        }
                    }
                }
            }
        }

        document_scores
    }
    /// Create highlighted snippets for a matched document
    fn create_highlights(
        &self,
        text: &str,
        terms: &[String],
        term_positions: &HashMap<String, Vec<TermPosition>>,
    ) -> Vec<String> {
        const MAX_SNIPPETS: usize = 3; // Make this configurable if needed
        let context_size = self.config.snippet_context_size;

        let mut snippets = Vec::new();
        let mut used_ranges = HashSet::new(); // To avoid overlapping snippets

        // Split text into words for snippets
        let words: Vec<&str> = text.split_whitespace().collect();
        if words.is_empty() {
            return Vec::new(); // Early return for empty text
        }

        // Get all terms with their positions
        let mut all_positions: Vec<(&String, &TermPosition)> = Vec::new();
        for term in terms {
            if let Some(positions) = term_positions.get(term) {
                for position in positions {
                    all_positions.push((term, position));
                }
            }
        }

        // Sort positions by their location in the text (for natural reading order)
        all_positions.sort_by_key(|(_, pos)| pos.position.start);

        // Process positions to create snippets
        for (term, position) in all_positions {
            let pos = position.position;

            // Validate position is in bounds
            if pos.start >= words.len() || pos.end > words.len() || pos.start == pos.end {
                continue; // Skip invalid positions
            }

            // Calculate snippet boundaries with context
            let start = if pos.start > context_size {
                pos.start - context_size
            } else {
                0
            };

            let end = std::cmp::min(pos.end + context_size, words.len());

            // Check for overlap with existing snippets
            let current_range = (start, end);
            let overlaps = used_ranges.iter().any(|&(s, e)| {
                (s <= start && start < e) || // Start within existing range
            (s < end && end <= e) ||     // End within existing range
            (start <= s && e <= end) // Existing range entirely within current range
            });

            if overlaps {
                continue; // Skip overlapping snippets
            }

            used_ranges.insert(current_range);

            // Create snippet parts
            let before = if start < pos.start {
                words[start..pos.start].join(" ")
            } else {
                String::new()
            };

            // Get the actual matched text
            let matched = words[pos.start..pos.end].join(" ");

            let after = if pos.end < end {
                words[pos.end..end].join(" ")
            } else {
                String::new()
            };

            // Construct the snippet with highlighting
            let mut snippet = String::with_capacity(
                before.len() + matched.len() + after.len() + 10, // Additional space for formatting
            );

            if !before.is_empty() {
                snippet.push_str(&before);
                snippet.push_str(" ");
            }

            snippet.push_str("<b>");
            snippet.push_str(&matched);
            snippet.push_str("</b>");

            if !after.is_empty() {
                snippet.push_str(" ");
                snippet.push_str(&after);
            }

            snippets.push(snippet);

            // Check if we've hit the maximum number of snippets
            if snippets.len() >= MAX_SNIPPETS {
                break;
            }
        }

        snippets
    }
}
