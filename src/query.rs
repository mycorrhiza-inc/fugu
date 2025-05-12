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
            default_limit: 10,
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
    pub position: usize,
    pub document_id: String,
}

/// A matching document with score and optional highlights
#[derive(Clone, Debug, Serialize)]
pub struct ScoredDocument {
    pub id: String,
    pub score: f64,
    pub metadata: Option<Value>,
    pub highlights: Option<Vec<String>>,
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

    /// Execute a query and return scored results
    pub fn execute(&self, query: Query) -> Result<Vec<ScoredDocument>> {
        let span = tracing_utils::db_span("query_execute");
        let _enter = span.enter();

        let start_time = Instant::now();
        debug!("Executing query: {:?}", query);

        // Parse the query
        let parsed_query = self.parse_query(query)?;

        // Collect document statistics for scoring
        let (doc_count, avg_doc_length, term_freq) = self.collect_document_statistics()?;

        // Get term positions from the inverted index
        let mut term_positions = HashMap::new();
        for term in &parsed_query.terms {
            let positions = self.get_term_positions(&term.text.to_lowercase())?;
            term_positions.insert(term.text.clone(), positions);
        }

        // Score documents based on term positions and document statistics
        let scored_docs = self.score_documents(
            &parsed_query
                .terms
                .iter()
                .map(|t| t.text.clone())
                .collect::<Vec<_>>(),
            &term_positions,
            doc_count,
            avg_doc_length,
            &parsed_query.logical_operator,
        );

        // Sort by score descending
        let mut results: Vec<_> = scored_docs.into_iter().collect();
        results.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(Ordering::Equal)
        });

        // Apply limit
        let limit = parsed_query.limit.unwrap_or(self.config.default_limit);
        let offset = parsed_query.offset.unwrap_or(0);
        let results = results
            .into_iter()
            .skip(offset)
            .take(limit)
            .filter(|(_, score)| *score >= self.config.min_score_threshold)
            .collect::<Vec<_>>();

        debug!(
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

            documents.push(ScoredDocument {
                id: doc_id,
                score,
                metadata,
                highlights,
            });
        }

        Ok(documents)
    }
    
    /// Compatibility method for the old API - search using text query
    pub fn search_text(&self, query_text: &str, limit: Option<usize>) -> Result<QueryResults> {
        let start_time = Instant::now();
        
        // Create a simple query from the text
        let mut query = Query::default();
        query.terms.push(QueryTerm::new(query_text));
        query.limit = limit;
        
        // Execute the query
        let scored_docs = self.execute(query)?;
        
        // Convert to QueryResults format
        Ok(QueryResults {
            took_ms: start_time.elapsed().as_millis() as u64,
            total_hits: scored_docs.len(),
            hits: scored_docs,
        })
    }
    
    /// Compatibility method for the old API - search using JSON
    pub fn search_json(&self, query_json: Value, limit: Option<usize>) -> Result<QueryResults> {
        let start_time = Instant::now();
        
        // Extract query text from JSON
        let query_text = match query_json.get("query") {
            Some(Value::String(s)) => s.clone(),
            _ => return Err(anyhow!("Invalid query: missing 'query' field")),
        };
        
        // Create a simple query
        let mut query = Query::default();
        query.terms.push(QueryTerm::new(&query_text));
        query.limit = limit;
        
        // Apply any filters from the JSON
        if let Some(Value::Array(filters)) = query_json.get("filters") {
            // Implementation would go here
            debug!("Filters not yet implemented in unified API");
        }
        
        // Execute the query
        let scored_docs = self.execute(query)?;
        
        // Convert to QueryResults format
        Ok(QueryResults {
            took_ms: start_time.elapsed().as_millis() as u64,
            total_hits: scored_docs.len(),
            hits: scored_docs,
        })
    }

    /// Parse a query into terms
    fn parse_query(&self, query: Query) -> Result<Query> {
        // For now we just do minimal validation
        if query.terms.is_empty() {
            return Err(anyhow!("Query must have at least one term"));
        }

        Ok(query)
    }

    /// Collect document statistics for scoring
    fn collect_document_statistics(&self) -> Result<(usize, f64, HashMap<String, usize>)> {
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
                return Err(anyhow!("Failed to create iterator for RECORDS collection: {}", e));
            }
        };

        for item in records_iter {
            match item {
                Ok((key_vec, value_vec)) => {
                    doc_count += 1;

                    // Try to deserialize the record
                    if let Ok(archivable) = crate::rkyv_adapter::deserialize::<
                        crate::object::ArchivableObjectRecord,
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
                },
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
        let prefix = format!("{}:", crate::db::PREFIX_RECORD_INDEX_TREE);

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
                                position: pos as usize, // Convert u64 to usize
                                document_id: object_id.clone(),
                            })
                            .collect();

                        if !term_positions.is_empty() {
                            result.insert(object_id.clone(), term_positions);
                        }
                    } else {
                        error!("Failed to deserialize positions for term '{}' in document '{}'",
                               term, object_id);
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
        #[cfg(feature = "use-sled")]
        {
            // tree_names() returns Vec<IVec> directly, not a Result
            let trees = self.db.db().tree_names();
            let mut result = Vec::new();
            for tree_name in trees {
                if let Ok(name_str) = std::str::from_utf8(tree_name.as_ref()) {
                    result.push(name_str.to_string());
                } else {
                    error!("Invalid UTF-8 in tree name");
                }
            }
            Ok(result)
        }

        #[cfg(feature = "use-fjall")]
        {
            match self.db.partition_names() {
                Ok(partitions) => Ok(partitions),
                Err(e) => {
                    error!("Failed to get partition names: {:?}", e);
                    Err(anyhow!("Failed to get partition names"))
                }
            }
        }
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

        // Simple BM25 scoring with position bias
        // For each term, score documents containing it
        for term in terms {
            if let Some(positions) = term_positions.get(term) {
                for (doc_id, term_positions) in positions {
                    // For simplicity, calculate score as term frequency with position bias
                    // (positions closer to the start of the document get higher weight)
                    let tf = term_positions.len() as f64;
                    
                    // Position bias - positions closer to the start get higher weight
                    let position_sum: f64 = term_positions.iter()
                        .map(|pos| 1.0 / (1.0 + pos.position as f64 * 0.01))
                        .sum();
                    
                    let position_score = position_sum / term_positions.len() as f64;
                    
                    // TF-IDF component
                    let idf = (doc_count as f64 / positions.len() as f64).ln();
                    
                    // Combined score
                    let score = tf * idf * position_score;
                    
                    // Accumulate scores based on the logical operator
                    match operator {
                        LogicalOperator::And => {
                            // For AND, we multiply scores 
                            let entry = document_scores.entry(doc_id.clone()).or_insert(1.0);
                            *entry *= score;
                        }
                        LogicalOperator::Or => {
                            // For OR, we sum scores
                            let entry = document_scores.entry(doc_id.clone()).or_insert(0.0);
                            *entry += score;
                        }
                    }
                }
            }
        }

        // For AND logic, filter out documents that don't contain all terms
        if let LogicalOperator::And = operator {
            let all_docs: std::collections::HashSet<&String> = term_positions.values()
                .flat_map(|positions| positions.keys())
                .collect();
                
            for term in terms {
                if let Some(positions) = term_positions.get(term) {
                    let term_docs: std::collections::HashSet<&String> = positions.keys().collect();
                    
                    let missing_docs: Vec<String> = all_docs.difference(&term_docs)
                        .map(|s| (*s).clone())
                        .collect();
                        
                    for doc_id in missing_docs {
                        document_scores.remove(&doc_id);
                    }
                } else {
                    // This term has no matching documents, so AND logic would return empty
                    document_scores.clear();
                    break;
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
        let mut snippets = Vec::new();
        let context_size = self.config.snippet_context_size;

        let words: Vec<&str> = text.split_whitespace().collect();

        // For each matched term, create snippets
        for term in terms {
            if let Some(positions) = term_positions.get(term) {
                for position in positions {
                    let pos = position.position;
                    
                    // Calculate snippet boundaries
                    let start = if pos > context_size { pos - context_size } else { 0 };
                    let end = std::cmp::min(pos + context_size + 1, words.len());
                    
                    if end > start {
                        // Create the snippet with highlighting
                        let prefix = words[start..pos].join(" ");
                        let matched_term = words[pos];
                        let suffix = if pos + 1 < end {
                            words[pos+1..end].join(" ")
                        } else {
                            String::new()
                        };
                        
                        // Create prefixed and suffixed strings once to avoid temporary value issues
                        let formatted_prefix = if prefix.is_empty() { String::new() } else { format!("{} ", prefix) };
                        let formatted_suffix = if suffix.is_empty() { String::new() } else { format!(" {}", suffix) };
                        
                        // Now use the stored strings
                        let snippet = format!("{}**{}**{}",
                            formatted_prefix,
                            matched_term,
                            formatted_suffix
                        );
                        
                        snippets.push(snippet);
                    }
                    
                    // Limit the number of snippets
                    if snippets.len() >= 3 {
                        break;
                    }
                }
            }
        }

        snippets
    }
}