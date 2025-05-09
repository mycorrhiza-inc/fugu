use crate::db::FuguDB;
use crate::object::ObjectRecord;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use futures_util::future::FutureExt; // For now_or_never
use tracing::{debug, error, info, Instrument};
use crate::tracing_utils;

/// Query configuration options
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Default number of results to return
    pub default_limit: usize,

    /// Whether to include snippet highlights in results
    pub highlight_snippets: bool,

    /// Maximum frequency of a term to consider for scoring
    pub max_term_frequency: usize,

    /// Minimum score threshold for results (0.0 - 1.0)
    pub min_score_threshold: f64,

    /// BM25 k1 parameter
    pub bm25_k1: f64,

    /// BM25 b parameter
    pub bm25_b: f64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            default_limit: 10,
            highlight_snippets: true,
            max_term_frequency: 1000,
            min_score_threshold: 0.01,
            bm25_k1: 1.2,
            bm25_b: 0.75,
        }
    }
}

/// A search hit in query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryHit {
    /// Document identifier
    pub document_id: String,

    /// Relevance score (higher is better)
    pub score: f64,

    /// Optional text snippets with highlighted matches
    pub highlights: Option<Vec<String>>,

    /// Optional document metadata
    pub metadata: Option<Value>,
}

/// Results from a search query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResults {
    /// Matching documents
    pub hits: Vec<QueryHit>,

    /// Total number of matching documents (may be more than returned)
    pub total_hits: usize,

    /// Time taken to execute the query in milliseconds
    pub took_ms: u64,
}

/// Operators that can be used in queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryOperator {
    /// Logical AND (intersection)
    And,

    /// Logical OR (union)
    Or,

    /// Logical NOT (negation)
    Not,
}

impl std::str::FromStr for QueryOperator {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "AND" => Ok(QueryOperator::And),
            "OR" => Ok(QueryOperator::Or),
            "NOT" => Ok(QueryOperator::Not),
            _ => Err(anyhow!("Invalid operator: {}", s)),
        }
    }
}

/// A term in a parsed query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTerm {
    /// The text of the term
    pub text: String,

    /// Optional operator preceding this term
    pub operator: Option<QueryOperator>,

    /// Whether this term is a phrase (surrounded by quotes)
    pub is_phrase: bool,

    /// Whether this term contains wildcards
    pub is_wildcard: bool,

    /// Optional boost factor for this term
    pub boost: Option<f64>,
}

impl QueryTerm {
    /// Create a new query term
    pub fn new(text: String) -> Self {
        let is_wildcard = text.contains('*');

        Self {
            text,
            operator: None,
            is_phrase: false,
            is_wildcard,
            boost: None,
        }
    }

    /// Create a new phrase term
    pub fn new_phrase(text: String) -> Self {
        Self {
            text,
            operator: None,
            is_phrase: true,
            is_wildcard: false,
            boost: None,
        }
    }

    /// Set the operator for this term
    pub fn with_operator(mut self, operator: QueryOperator) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Set the boost factor for this term
    pub fn with_boost(mut self, boost: f64) -> Self {
        self.boost = Some(boost);
        self
    }
}

/// A parsed query ready for processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedQuery {
    /// The terms in the query
    pub terms: Vec<QueryTerm>,

    /// Default operator to use between terms
    pub default_operator: QueryOperator,
}

impl ParsedQuery {
    /// Create a new empty parsed query
    pub fn new() -> Self {
        Self {
            terms: Vec::new(),
            default_operator: QueryOperator::And,
        }
    }

    /// Add a term to this query
    pub fn add_term(&mut self, term: QueryTerm) {
        self.terms.push(term);
    }

    /// Set the default operator
    pub fn with_default_operator(mut self, operator: QueryOperator) -> Self {
        self.default_operator = operator;
        self
    }
}

/// Parse a text query string into a structured query
pub fn parse_text_query(query: &str) -> Result<ParsedQuery> {
    let mut parsed_query = ParsedQuery::new();
    let mut current_operator: Option<QueryOperator> = None;

    // Simple tokenization by splitting on whitespace
    let tokens: Vec<&str> = query.split_whitespace().collect();

    for i in 0..tokens.len() {
        let token = tokens[i];

        // Check if this token is an operator
        if let Ok(op) = QueryOperator::from_str(token) {
            current_operator = Some(op);
            continue;
        }

        // Process the token as a term
        let mut term_text = token.to_string();

        // Basic preprocessing
        term_text = term_text.to_lowercase();

        // Check for phrase (assuming simple implementation for now)
        let is_phrase = term_text.starts_with('"') && term_text.ends_with('"');

        if is_phrase {
            // Remove the quotes
            term_text = term_text[1..term_text.len() - 1].to_string();
            let term = QueryTerm::new_phrase(term_text);
            parsed_query.add_term(if let Some(op) = current_operator {
                term.with_operator(op)
            } else {
                term
            });
        } else {
            // Check for boost factor (e.g., term^2.0)
            let mut boost = None;
            if let Some(boost_idx) = term_text.find('^') {
                if boost_idx < term_text.len() - 1 {
                    if let Ok(factor) = term_text[boost_idx + 1..].parse::<f64>() {
                        boost = Some(factor);
                        term_text = term_text[0..boost_idx].to_string();
                    }
                }
            }

            // Create a normal term
            let mut term = QueryTerm::new(term_text);
            if let Some(op) = current_operator {
                term = term.with_operator(op);
            }
            if let Some(b) = boost {
                term = term.with_boost(b);
            }

            parsed_query.add_term(term);
        }

        // Reset the operator after it's been applied
        current_operator = None;
    }

    Ok(parsed_query)
}

/// Parse a query that includes quoted phrases
pub fn parse_query_with_phrases(query: &str) -> Result<ParsedQuery> {
    let mut parsed_query = ParsedQuery::new();
    let mut current_pos = 0;
    let mut current_operator: Option<QueryOperator> = None;

    let query_chars: Vec<char> = query.chars().collect();

    while current_pos < query_chars.len() {
        // Skip whitespace
        while current_pos < query_chars.len() && query_chars[current_pos].is_whitespace() {
            current_pos += 1;
        }

        if current_pos >= query_chars.len() {
            break;
        }

        // Check for quoted phrases
        if query_chars[current_pos] == '"' {
            current_pos += 1; // Skip the opening quote
            let start_pos = current_pos;

            // Find the closing quote
            while current_pos < query_chars.len() && query_chars[current_pos] != '"' {
                current_pos += 1;
            }

            if current_pos < query_chars.len() {
                let phrase = query_chars[start_pos..current_pos]
                    .iter()
                    .collect::<String>();
                let term = QueryTerm::new_phrase(phrase);

                parsed_query.add_term(if let Some(op) = current_operator {
                    term.with_operator(op)
                } else {
                    term
                });

                current_pos += 1; // Skip the closing quote
                current_operator = None;
            } else {
                // Unclosed quote, treat the rest as a normal term
                let term_text = query_chars[start_pos..].iter().collect::<String>();
                parsed_query.add_term(QueryTerm::new(term_text));
                break;
            }
        } else {
            // Check if this is an operator
            let remaining = query_chars[current_pos..].iter().collect::<String>();
            let next_space = remaining
                .find(|c: char| c.is_whitespace())
                .unwrap_or(remaining.len());
            let word = &remaining[0..next_space];

            if let Ok(op) = QueryOperator::from_str(word) {
                current_operator = Some(op);
                current_pos += word.len();
                continue;
            }

            // Normal term
            let start_pos = current_pos;

            while current_pos < query_chars.len() && !query_chars[current_pos].is_whitespace() {
                current_pos += 1;
            }

            let term_text = query_chars[start_pos..current_pos]
                .iter()
                .collect::<String>();

            // Check for boost factor
            let mut term_text_final = term_text.clone();
            let mut boost = None;

            if let Some(boost_idx) = term_text.find('^') {
                if boost_idx < term_text.len() - 1 {
                    if let Ok(factor) = term_text[boost_idx + 1..].parse::<f64>() {
                        boost = Some(factor);
                        term_text_final = term_text[0..boost_idx].to_string();
                    }
                }
            }

            // Create and add the term
            let mut term = QueryTerm::new(term_text_final);
            if let Some(op) = current_operator {
                term = term.with_operator(op);
            }
            if let Some(b) = boost {
                term = term.with_boost(b);
            }

            parsed_query.add_term(term);
            current_operator = None;
        }
    }

    Ok(parsed_query)
}

/// Filter expression types for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FilterExpression {
    /// Exact term match
    #[serde(rename = "term")]
    Term { field: String, value: String },

    /// Range filter for numeric values
    #[serde(rename = "range")]
    Range {
        field: String,
        min: Option<Value>,
        max: Option<Value>,
    },

    /// Logical AND of multiple filters
    #[serde(rename = "and")]
    And { filters: Vec<FilterExpression> },

    /// Logical OR of multiple filters
    #[serde(rename = "or")]
    Or { filters: Vec<FilterExpression> },

    /// Logical NOT of a filter
    #[serde(rename = "not")]
    Not { filter: Box<FilterExpression> },
}

/// JSON query structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonQuery {
    /// Text query string
    pub query: String,

    /// Maximum number of results to return
    #[serde(rename = "top_k")]
    pub top_k: Option<usize>,

    /// Optional filter expressions
    pub filters: Option<Vec<FilterExpression>>,

    /// Optional field boosts
    pub boost: Option<Vec<BoostCriteria>>,

    /// Result offset for pagination
    pub offset: Option<usize>,
}

/// Boost criteria for terms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoostCriteria {
    /// Field to boost
    pub field: String,

    /// Boost factor (multiplier)
    pub factor: f64,
}

/// Parse a JSON query string
pub fn parse_json_query(json_str: &str) -> Result<JsonQuery> {
    serde_json::from_str(json_str).context("Failed to parse JSON query")
}

/// Simple BM25 scoring implementation
pub struct BM25 {
    /// K1 parameter (term frequency saturation)
    pub k1: f64,

    /// B parameter (length normalization)
    pub b: f64,
}

impl Default for BM25 {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

impl BM25 {
    /// Create a new BM25 scoring instance with specified parameters
    pub fn new(k1: f64, b: f64) -> Self {
        Self { k1, b }
    }

    /// Calculate BM25 score for a single term in a document
    pub fn score(
        &self,
        term_freq: usize,
        doc_length: usize,
        avg_doc_length: f64,
        doc_count: usize,
        term_doc_freq: usize,
    ) -> f64 {
        if term_doc_freq == 0 || doc_count == 0 {
            return 0.0;
        }

        // Calculate the inverse document frequency (IDF)
        let idf = (doc_count as f64 - term_doc_freq as f64 + 0.5) / (term_doc_freq as f64 + 0.5);
        let idf = (1.0 + idf).ln();

        // If IDF is negative or zero, this term isn't useful for scoring
        if idf <= 0.0 {
            return 0.0;
        }

        // Calculate the term frequency component
        let tf_component = term_freq as f64 * (self.k1 + 1.0)
            / (term_freq as f64
                + self.k1 * (1.0 - self.b + self.b * doc_length as f64 / avg_doc_length));

        // Return the final score
        idf * tf_component
    }
}

/// Term positions for query scoring
#[derive(Debug, Clone)]
pub struct TermPosition {
    /// Position in the document
    pub position: usize,

    /// Document ID
    pub document_id: String,
}

/// Main query engine for the Fugu database
pub struct QueryEngine {
    /// Database connection
    db: Arc<FuguDB>,

    /// Query configuration
    config: QueryConfig,

    /// BM25 scoring algorithm
    bm25: BM25,
}

impl QueryEngine {
    /// Create a new query engine instance
    pub fn new(db: Arc<FuguDB>, config: QueryConfig) -> Self {
        let bm25 = BM25::new(config.bm25_k1, config.bm25_b);

        Self { db, config, bm25 }
    }

    /// Execute a simple text search query
    pub fn search_text(&self, query_text: &str, limit: Option<usize>) -> Result<QueryResults> {
        // Create a query span for this operation
        let mut additional_fields = HashMap::new();
        additional_fields.insert("limit", limit.unwrap_or(self.config.default_limit));
        let span = tracing_utils::query_span("search_text", query_text, Some(additional_fields));

        async move {
            let start_time = Instant::now();
            let limit = limit.unwrap_or(self.config.default_limit);

            debug!("Executing text search query: {}", query_text);

            // Parse the query
            let parsed_query = match parse_query_with_phrases(query_text) {
                Ok(query) => query,
                Err(e) => {
                    error!("Failed to parse text query: {}", e);
                    return Err(e.context("Failed to parse text query"));
                }
            };

            // Get document statistics for scoring
            let (doc_count, avg_doc_length, term_doc_freqs) = match self.get_document_statistics() {
                Ok(stats) => {
                    let (count, avg_len, _) = stats;
                    debug!("Retrieved document statistics: {} docs, {:.2} avg length", count, avg_len);
                    stats
                },
                Err(e) => {
                    error!("Failed to get document statistics: {}", e);
                    return Err(e.context("Failed to get document statistics"));
                }
            };

            // Retrieve term positions for all query terms
            let mut term_positions = HashMap::new();
            for term in &parsed_query.terms {
                match self.get_term_positions(&term.text) {
                    Ok(positions) => {
                        if !positions.is_empty() {
                            debug!("Term '{}' found in {} documents", term.text, positions.len());
                            term_positions.insert(term.text.clone(), positions);
                        } else {
                            debug!("Term '{}' not found in any documents", term.text);
                        }
                    },
                    Err(e) => {
                        error!("Error retrieving positions for term '{}': {}", term.text, e);
                        // Continue with other terms rather than failing the whole query
                    }
                }
            }

            // Score the documents
            let doc_scores = self.score_documents(
                &parsed_query
                    .terms
                    .iter()
                    .map(|t| t.text.clone())
                    .collect::<Vec<_>>(),
                &term_positions,
                doc_count,
                avg_doc_length,
                &term_doc_freqs,
            );

            debug!("Scored {} documents", doc_scores.len());

            // Sort by score and apply limit
            let mut scored_docs: Vec<_> = doc_scores.into_iter().collect();
            scored_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
            let total_hits = scored_docs.len();

            // Keep only top-k results
            if scored_docs.len() > limit {
                scored_docs.truncate(limit);
            }

            // Create query hits with highlights if requested
            let mut hits = Vec::with_capacity(scored_docs.len());
            for (doc_id, score) in scored_docs {
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

                        let snippets = self.create_highlights(
                            &record.text,
                            &parsed_query
                                .terms
                                .iter()
                                .map(|t| t.text.clone())
                                .collect::<Vec<_>>(),
                            &doc_term_positions,
                        );

                        if !snippets.is_empty() {
                            highlights = Some(snippets);
                        }
                    }
                }

                hits.push(QueryHit {
                    document_id: doc_id,
                    score,
                    highlights,
                    metadata,
                });
            }

            let took_ms = start_time.elapsed().as_millis() as u64;

            let results = QueryResults {
                hits,
                total_hits,
                took_ms,
            };

            info!(
                total_hits = total_hits,
                took_ms = took_ms,
                "Query returned {} results in {}ms",
                total_hits, took_ms
            );

            Ok(results)
        }
        .instrument(span)
        // Run the async block synchronously since the API is sync
        .now_or_never()
        .expect("Query execution should never be pending")
    }

    /// Execute a JSON-formatted search query
    pub fn search_json(&self, json_query: &str) -> Result<QueryResults> {
        // Create a query span for this operation
        let span = tracing_utils::query_span::<String>("search_json", json_query, None);

        async move {
            let start_time = Instant::now();

            debug!("Executing JSON search query");

            // Parse the JSON query
            let parsed_json_query = match parse_json_query(json_query) {
                Ok(query) => {
                    debug!("Successfully parsed JSON query: {}", query.query);
                    query
                },
                Err(e) => {
                    error!("Failed to parse JSON query: {}", e);
                    return Err(e.context("Failed to parse JSON query"));
                }
            };

            // Record additional fields in the current span
            let span = tracing::Span::current();
            span.record("query_text", &tracing::field::debug(&parsed_json_query.query));
            span.record("top_k", &tracing::field::debug(&parsed_json_query.top_k));

            if let Some(filters) = &parsed_json_query.filters {
                span.record("has_filters", &tracing::field::debug(true));
                span.record("filter_count", &tracing::field::debug(filters.len()));
            }

            // Convert to a text query and execute
            let result = self.search_text(&parsed_json_query.query, parsed_json_query.top_k);

            // TODO: Apply filters from the JSON query
            if let Some(filters) = &parsed_json_query.filters {
                debug!("JSON query has {} filters (not yet implemented)", filters.len());
            }

            let took_ms = start_time.elapsed().as_millis() as u64;

            info!(
                took_ms = took_ms,
                "JSON query processed in {}ms",
                took_ms
            );

            result
        }
        .instrument(span)
        // Run the async block synchronously since the API is sync
        .now_or_never()
        .expect("Query execution should never be pending")
    }

    /// Get document statistics needed for scoring
    fn get_document_statistics(&self) -> Result<(usize, f64, HashMap<String, usize>)> {
        // Access the database to get term and document statistics
        // This is a simplified implementation that scans all records

        // Open the RECORDS tree
        let records_tree = match self.db.db().open_tree(crate::db::TREE_RECORDS) {
            Ok(tree) => tree,
            Err(e) => {
                error!("Failed to open RECORDS tree: {}", e);
                return Err(anyhow!("Failed to open RECORDS tree: {}", e));
            }
        };

        let mut doc_count = 0;
        let mut total_tokens = 0;
        let mut term_doc_freqs = HashMap::new();

        // Scan all records
        for item in records_tree.iter() {
            if let Ok((key, value)) = item {
                doc_count += 1;

                // Try to deserialize the record
                if let Ok(archivable) = crate::rkyv_adapter::deserialize::<
                    crate::object::ArchivableObjectRecord,
                >(&value)
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
        let mut result = HashMap::new();

        // Query the inverted index for this term
        // This is a simplified implementation that scans object index trees

        // Get a list of all object index trees
        let db = self.db.db();
        let trees = db.tree_names();

        let prefix = format!("{}:", crate::db::PREFIX_RECORD_INDEX_TREE);

        for tree_name in trees {
            let tree_name_str = std::str::from_utf8(&tree_name)?;

            // Skip trees that don't match our prefix pattern
            if !tree_name_str.starts_with(&prefix) {
                continue;
            }

            // Extract the object ID from the tree name
            let object_id = tree_name_str[prefix.len()..].to_string();

            // Open the object's index tree
            if let Ok(index_tree) = db.open_tree(tree_name) {
                // Check if this object contains the term
                if let Ok(Some(positions_bytes)) = index_tree.get(term.as_bytes()) {
                    // Deserialize the positions
                    let positions = crate::db::deserialize_positions(&positions_bytes);
                    // Convert to term positions
                    let term_positions: Vec<TermPosition> = positions
                        .iter()
                        .map(|&pos| TermPosition {
                            position: pos,
                            document_id: object_id.clone(),
                        })
                        .collect();

                    if !term_positions.is_empty() {
                        result.insert(object_id, term_positions);
                    }
                }
            }
        }

        debug!("Term '{}' found in {} documents", term, result.len());

        Ok(result)
    }

    /// Score documents based on term positions
    fn score_documents(
        &self,
        terms: &[String],
        term_positions: &HashMap<String, HashMap<String, Vec<TermPosition>>>,
        doc_count: usize,
        avg_doc_length: f64,
        term_doc_frequencies: &HashMap<String, usize>,
    ) -> HashMap<String, f64> {
        let mut document_scores = HashMap::new();
        let mut document_lengths = HashMap::new();

        // First pass: calculate document lengths
        for term in terms {
            if let Some(positions_map) = term_positions.get(term) {
                for (doc_id, positions) in positions_map {
                    *document_lengths.entry(doc_id.clone()).or_insert(0) += positions.len();
                }
            }
        }

        // Second pass: score each document for each term
        for term in terms {
            if let Some(positions_map) = term_positions.get(term) {
                let term_freqs = calculate_term_frequencies(positions_map);

                for (doc_id, freq) in term_freqs {
                    let doc_length = *document_lengths.get(&doc_id).unwrap_or(&0);
                    let term_doc_freq = *term_doc_frequencies.get(term).unwrap_or(&0);

                    if doc_length > 0 {
                        let score = self.bm25.score(
                            freq,
                            doc_length,
                            avg_doc_length,
                            doc_count,
                            term_doc_freq,
                        );

                        *document_scores.entry(doc_id).or_insert(0.0) += score;
                    }
                }
            }
        }

        document_scores
    }

    /// Create highlighted snippets for search results
    fn create_highlights(
        &self,
        content: &str,
        terms: &[String],
        positions: &HashMap<String, Vec<TermPosition>>,
    ) -> Vec<String> {
        // Implementation of a simple highlighting algorithm
        if content.is_empty() || terms.is_empty() || positions.is_empty() {
            return Vec::new();
        }

        // Split content into words
        let words: Vec<&str> = content.split_whitespace().collect();

        // Collect all positions for all terms
        let mut all_positions = Vec::new();
        for term in terms {
            if let Some(term_positions) = positions.get(term) {
                for pos in term_positions {
                    all_positions.push(pos.position);
                }
            }
        }

        // No positions found
        if all_positions.is_empty() {
            return Vec::new();
        }

        // Sort positions
        all_positions.sort();

        // Extract snippets around positions with some context
        let context_size = 5; // Words before/after match
        let mut snippets = Vec::new();

        // Track already covered positions to avoid duplicate snippets
        let mut covered_positions = std::collections::HashSet::new();

        for &pos in &all_positions {
            if covered_positions.contains(&pos) {
                continue;
            }

            // Calculate snippet range
            let start = pos.saturating_sub(context_size);
            let end = std::cmp::min(pos + context_size + 1, words.len());

            // Mark this and nearby positions as covered
            for p in start..end {
                covered_positions.insert(p);
            }

            // Create the snippet
            let mut snippet = String::new();

            // Add prefix if not starting from the beginning
            if start > 0 {
                snippet.push_str("... ");
            }

            // Add words with highlighting
            for i in start..end {
                // Check if this position is a match
                let is_match = all_positions.contains(&i);

                if is_match {
                    snippet.push_str("**"); // Markdown style highlighting
                }

                if i < words.len() {
                    snippet.push_str(words[i]);
                }

                if is_match {
                    snippet.push_str("**");
                }

                snippet.push(' ');
            }

            // Add suffix if not ending at the end
            if end < words.len() {
                snippet.push_str("...");
            }

            snippets.push(snippet);

            // Limit the number of snippets
            if snippets.len() >= 3 {
                break;
            }
        }

        snippets
    }
}

/// Calculate term frequencies from positions
fn calculate_term_frequencies(
    positions_map: &HashMap<String, Vec<TermPosition>>,
) -> HashMap<String, usize> {
    let mut term_freqs = HashMap::new();

    for (doc_id, positions) in positions_map {
        term_freqs.insert(doc_id.clone(), positions.len());
    }

    term_freqs
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_parse_simple_query() {
        let query = "hello world";
        let parsed = parse_text_query(query).unwrap();

        assert_eq!(parsed.terms.len(), 2);
        assert_eq!(parsed.terms[0].text, "hello");
        assert_eq!(parsed.terms[1].text, "world");
        assert_eq!(parsed.terms[0].operator, None);
        assert_eq!(parsed.terms[1].operator, None);
    }

    #[test]
    fn test_parse_query_with_operators() {
        let query = "hello AND world OR fugu";
        let parsed = parse_text_query(query).unwrap();

        assert_eq!(parsed.terms.len(), 3);
        assert_eq!(parsed.terms[0].text, "hello");
        assert_eq!(parsed.terms[1].text, "world");
        assert_eq!(parsed.terms[2].text, "fugu");
        assert_eq!(parsed.terms[0].operator, None);
        assert_eq!(parsed.terms[1].operator, Some(QueryOperator::And));
        assert_eq!(parsed.terms[2].operator, Some(QueryOperator::Or));
    }

    #[test]
    fn test_parse_query_with_phrase() {
        let query = "hello \"world fugu\"";
        let parsed = parse_query_with_phrases(query).unwrap();

        assert_eq!(parsed.terms.len(), 2);
        assert_eq!(parsed.terms[0].text, "hello");
        assert_eq!(parsed.terms[1].text, "world fugu");
        assert!(!parsed.terms[0].is_phrase);
        assert!(parsed.terms[1].is_phrase);
    }

    #[test]
    fn test_parse_json_query() {
        let json = r#"
        {
            "query": "hello world",
            "top_k": 5,
            "filters": [
                {
                    "type": "term",
                    "field": "category",
                    "value": "docs"
                }
            ]
        }
        "#;

        let result = parse_json_query(json);
        assert!(result.is_ok());

        let query = result.unwrap();
        assert_eq!(query.query, "hello world");
        assert_eq!(query.top_k, Some(5));
        assert!(query.filters.is_some());
    }

    #[test]
    fn test_bm25_scoring() {
        let bm25 = BM25::default();

        // Test with typical values
        let doc_length = 100;
        let avg_doc_length = 120.0;
        let doc_count = 1000;
        let term_doc_freq = 10;

        // Common term (appears in many docs)
        let common_score = bm25.score(5, doc_length, avg_doc_length, doc_count, 500);

        // Rare term (appears in few docs)
        let rare_score = bm25.score(5, doc_length, avg_doc_length, doc_count, 5);

        // Rare terms should score higher than common terms with same frequency
        assert!(rare_score > common_score);
    }
}
