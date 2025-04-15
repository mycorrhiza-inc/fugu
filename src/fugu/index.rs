/// Index module provides text search capabilities through BM25 and inverted index
///
/// This module implements:
/// - A persistent inverted index for fast text search
/// - Token-based indexing and retrieval
/// - Text tokenization and relevancy ranking
/// - BM25 scoring for search results
/// - Performance metrics collection for search operations
use crate::fugu::wal::WALCMD;
use rkyv;
use rkyv::{rancor::Error, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use sled;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use bm25::{SearchEngine, SearchEngineBuilder, Embedder, EmbedderBuilder, Language};
use std::path::Path;

/// Represents an indexed term with all its document occurrences
///
/// This structure maintains:
/// - The term being indexed
/// - A mapping of document IDs to position lists
/// - The overall frequency of the term
#[derive(Debug, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize, Clone)]
// #[rkyv(compare(PartialEq), derive(Debug))]
pub struct TermIndex {
    /// The term text being indexed
    pub term: String,
    /// Mapping from document IDs to positions where the term occurs
    pub doc_ids: HashMap<String, Vec<u64>>, // doc_id -> all positions
    /// Overall frequency of this term across all documents
    pub term_frequency: u32,
}

impl TermIndex {
    /// Returns a list of all document IDs containing this term
    ///
    /// # Returns
    ///
    /// Vector of document IDs
    fn get_docs(&self) -> Vec<String> {
        let d = self.doc_ids.keys();
        let mut o = vec![];
        for i in d {
            o.push(i.clone());
        }
        o
    }
    
    /// Sets the term frequency
    ///
    /// # Arguments
    ///
    /// * `n` - The new frequency value
    pub fn set_frequency(&mut self, n: u32) {
        self.term_frequency = n;
    }
}

/// Display formatting for TermIndex
impl std::fmt::Display for TermIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let t = self.term.as_str();
        let n = self.term_frequency;
        let text = format!("{t} found in  {n} documents");
        write!(f, "{}", text)
    }
}

/// Represents a single term occurrence in a document
///
/// A token contains:
/// - The term text
/// - The document it belongs to
/// - Its position within the document
pub struct Token {
    /// The term text
    pub term: String,
    /// The document identifier
    pub doc_id: String,
    /// The position within the document (0-based)
    pub position: u64,
}

/// Internal structure for document entry processing
struct DocEntry {
    /// Document identifier
    id: String,
    /// Position within the document
    position: u64,
}

/// Trait for text tokenization strategies
///
/// Allows for different tokenization approaches to be implemented
pub trait Tokenizer {
    /// Converts text into a sequence of tokens
    ///
    /// # Arguments
    ///
    /// * `text` - The text to tokenize
    /// * `doc_id` - The document identifier
    ///
    /// # Returns
    ///
    /// A vector of tokens from the text
    fn tokenize(&self, text: &str, doc_id: &str) -> Vec<Token>;
}

/// A simple tokenizer that splits text on whitespace
#[derive(Clone, Copy)]
pub struct WhitespaceTokenizer;

impl Tokenizer for WhitespaceTokenizer {
    /// Splits text on whitespace and creates tokens
    ///
    /// This tokenizer:
    /// - Splits on whitespace
    /// - Converts to lowercase
    /// - Trims whitespace from terms
    /// - Assigns sequential positions
    ///
    /// # Arguments
    ///
    /// * `text` - The text to tokenize
    /// * `doc_id` - The document identifier
    ///
    /// # Returns
    ///
    /// A vector of tokens from the text
    fn tokenize(&self, text: &str, doc_id: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position: u64 = 0;

        for word in text.split_whitespace() {
            let term = word.trim().to_lowercase();
            if !term.is_empty() {
                tokens.push(Token {
                    term,
                    doc_id: doc_id.to_string(),
                    position,
                });
                position += 1;
            }
        }

        tokens
    }
}

/// Collection of performance metrics for search operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchMetrics {
    /// Time taken to parse and prepare query
    pub query_parsing_time: Duration,
    /// Time taken to retrieve matching documents
    pub retrieval_time: Duration,
    /// Time taken to score and rank results
    pub scoring_time: Duration,
    /// Total search time
    pub total_time: Duration,
    /// Number of documents searched
    pub documents_searched: usize,
    /// Number of documents matched (before limiting)
    pub documents_matched: usize,
}

impl SearchMetrics {
    /// Create a new empty metrics object
    pub fn new() -> Self {
        Self {
            query_parsing_time: Duration::default(),
            retrieval_time: Duration::default(),
            scoring_time: Duration::default(),
            total_time: Duration::default(),
            documents_searched: 0,
            documents_matched: 0,
        }
    }
}

/// Represents a search result with relevance scoring
///
/// Contains:
/// - The matching document ID
/// - A relevance score based on BM25
/// - Term match positions for highlighting
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// The document identifier
    pub doc_id: String,
    /// Relevance score (higher is more relevant)
    pub relevance_score: f64,
    /// Map of terms to their positions in the document
    pub term_matches: HashMap<String, Vec<u64>>,
}

/// The main inverted index implementation
///
/// This structure:
/// - Provides persistent storage of index data using sled
/// - Supports concurrent read/write access
/// - Uses WAL for durability
/// - Implements BM25 relevance scoring
/// - Tracks search performance metrics
/// - Persistently stores document contents in a dedicated sled DB
#[derive(Clone, Debug)]
pub struct InvertedIndex {
    /// Main index database for term-to-document mappings
    db: Arc<RwLock<sled::Db>>,
    /// Channel for WAL operations
    wal_chan: mpsc::Sender<WALCMD>,
    /// Path to the index storage
    path: String,
    /// BM25 search engine for ranking search results
    search_engine: Arc<RwLock<Option<SearchEngine<String>>>>,
    /// Embedder for converting text to sparse embeddings
    embedder: Arc<RwLock<Option<Embedder>>>,
    /// Document content database (separate from index)
    doc_db: Arc<RwLock<sled::Db>>,
    /// Latest search metrics
    last_metrics: Arc<RwLock<SearchMetrics>>,
    /// Total documents in the index
    total_docs: Arc<RwLock<usize>>,
}

impl InvertedIndex {
    /// Creates a new InvertedIndex instance
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the index storage directory
    /// * `wal_chan` - Channel for WAL operations
    ///
    /// # Returns
    ///
    /// A new InvertedIndex instance
    pub async fn new(path: &str, wal_chan: mpsc::Sender<WALCMD>) -> Self {
        // Create the base path if it doesn't exist
        if !Path::new(path).exists() {
            std::fs::create_dir_all(path).expect("Failed to create index directory");
        }
        
        // Main index database
        let index_path = format!("{}/index", path);
        let db = sled::open(&index_path).expect("Failed to open index database");
        
        // Document content database
        let docs_path = format!("{}/docs", path);
        let doc_db = sled::open(&docs_path).expect("Failed to open documents database");
        
        // Initialize an empty search engine with default parameters
        // We're using String type for document IDs
        let empty_docs: Vec<bm25::Document<String>> = Vec::new();
        let search_engine = SearchEngineBuilder::<String>::with_documents(
                Language::English, 
                empty_docs
            )
            .k1(1.2)  // Term frequency saturation parameter
            .b(0.75)  // Document length normalization parameter
            .build();
            
        // Create a basic embedder with reasonable defaults
        let embedder = EmbedderBuilder::with_avgdl(200.0).build();
        
        InvertedIndex {
            db: Arc::new(RwLock::new(db)),
            wal_chan,
            path: path.to_string(),
            search_engine: Arc::new(RwLock::new(Some(search_engine))),
            embedder: Arc::new(RwLock::new(Some(embedder))),
            doc_db: Arc::new(RwLock::new(doc_db)),
            last_metrics: Arc::new(RwLock::new(SearchMetrics::new())),
            total_docs: Arc::new(RwLock::new(0)),
        }
    }
    
    /// Retrieves the latest search performance metrics
    pub async fn get_last_metrics(&self) -> SearchMetrics {
        self.last_metrics.read().await.clone()
    }
    
    /// Flushes all pending changes to disk
    ///
    /// Ensures that all index changes are written to persistent storage
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Flush the main index database
        let db = self.db.read().await;
        db.flush()?;
        
        // Flush the document database
        let doc_db = self.doc_db.read().await;
        doc_db.flush()?;
        
        println!("Flushed index and document data to {}", self.path);
        Ok(())
    }

    pub async fn add_term(&self, token: Token) -> Result<(), Box<dyn std::error::Error>> {
        let db = self.db.write().await;
        let term = token.term;
        let doc_id = token.doc_id;
        let position = token.position;

        // Create or update index entry
        let entry = match db.get(term.as_bytes())? {
            Some(existing) => {
                let mut entry: TermIndex = rkyv::from_bytes::<TermIndex, Error>(&existing)?;
                // let did = doc_id.clone();
                match entry.doc_ids.get(&doc_id.to_string()) {
                    // if there is an existing instance of this term in this document,
                    // append the new one
                    Some(positions) => {
                        let mut p = positions.clone();
                        p.push(position);
                        let k = doc_id.to_string().clone();
                        entry.doc_ids.insert(k, p);
                    }
                    // else, create one
                    None => {
                        entry.doc_ids.insert(doc_id.to_string(), vec![position]);
                    }
                }
                entry.set_frequency(entry.term_frequency + 1);
                entry
            }
            None => {
                let mut doc_ids: HashMap<String, Vec<u64>> = HashMap::new();
                doc_ids.insert(doc_id.to_string(), vec![position]);
                TermIndex {
                    term: term.to_string(),
                    doc_ids,
                    term_frequency: 1,
                }
            }
        };

        let serialized = rkyv::to_bytes::<Error>(&entry)?;
        // Log to WAL before making the change
        self.wal_chan
            .send(WALCMD::Put {
                key: term.to_string(),
                value: serialized.to_vec(),
            })
            .await?;

        // Update the index
        db.insert(term.as_bytes(), serialized.as_slice())?;

        Ok(())
    }
    
    /// Adds a term with multiple positions at once
    ///
    /// This is an optimized version of add_term that can add a term with multiple positions
    /// in a single operation, which is much more efficient for parallel indexing.
    ///
    /// # Arguments
    ///
    /// * `token` - The token to add
    /// * `positions` - List of all positions for this term
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn add_term_with_positions(
        &self, 
        token: Token, 
        positions: Vec<u64>
    ) -> Result<(), Box<dyn std::error::Error>> {
        if positions.is_empty() {
            return Ok(());
        }
        
        let db = self.db.write().await;
        let term = token.term;
        let doc_id = token.doc_id;

        // Create or update index entry
        let entry = match db.get(term.as_bytes())? {
            Some(existing) => {
                let mut entry: TermIndex = rkyv::from_bytes::<TermIndex, Error>(&existing)?;
                match entry.doc_ids.get(&doc_id.to_string()) {
                    // If there's already positions for this term in this document,
                    // merge them with the new positions
                    Some(existing_positions) => {
                        let mut merged_positions = existing_positions.clone();
                        
                        // Add the new positions
                        merged_positions.extend_from_slice(&positions);
                        
                        // Deduplicate and sort
                        merged_positions.sort();
                        merged_positions.dedup();
                        
                        let k = doc_id.to_string().clone();
                        entry.doc_ids.insert(k, merged_positions);
                    }
                    // Otherwise, create a new entry
                    None => {
                        // Sort positions for efficiency in later operations
                        let mut sorted_positions = positions.clone();
                        sorted_positions.sort();
                        sorted_positions.dedup();
                        
                        entry.doc_ids.insert(doc_id.to_string(), sorted_positions);
                    }
                }
                // Update frequency - we don't just add positions.len() as we might be deduplicating
                let new_term_freq = entry.doc_ids.values()
                    .fold(0, |acc, pos_vec| acc + pos_vec.len() as u32);
                
                entry.set_frequency(new_term_freq);
                entry
            }
            None => {
                // Create a new entry for this term
                let mut doc_ids: HashMap<String, Vec<u64>> = HashMap::new();
                
                // Sort positions for efficiency
                let mut sorted_positions = positions.clone();
                sorted_positions.sort();
                sorted_positions.dedup();
                
                doc_ids.insert(doc_id.to_string(), sorted_positions.clone());
                
                TermIndex {
                    term: term.to_string(),
                    doc_ids,
                    term_frequency: sorted_positions.len() as u32,
                }
            }
        };

        let serialized = rkyv::to_bytes::<rkyv::rancor::Panic>(&entry)?;
        
        // Log to WAL before making the change
        self.wal_chan
            .send(WALCMD::Put {
                key: term.to_string(),
                value: serialized.to_vec(),
            })
            .await?;

        // Update the index
        db.insert(term.as_bytes(), serialized.as_slice())?;

        Ok(())
    }

    pub async fn index_document<T: Tokenizer>(
        &self,
        doc_id: &str,
        text: &str,
        tokenizer: &T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        
        // Store the document content in the document database
        let doc_db = self.doc_db.write().await;
        doc_db.insert(doc_id.as_bytes(), text.as_bytes())?;
        
        // Tokenize the document for the inverted index
        let tokens = tokenizer.tokenize(text, doc_id);
        
        // Add document to search engine
        {
            // Get search engine
            let mut search_engine_guard = self.search_engine.write().await;
            
            if let Some(search_engine) = search_engine_guard.as_mut() {
                // Create a document with ID and content
                // Note: We normalize the text to lowercase for consistent case-insensitive indexing
                let normalized_text = text.to_lowercase();
                
                // Create a document with ID and normalized (lowercase) content for consistent search
                let document = bm25::Document::new(doc_id.to_string(), &normalized_text);
                
                // Add or update document in the search engine
                search_engine.upsert(document);
            }
        }
        
        // Increment total document count
        {
            let mut total_docs = self.total_docs.write().await;
            *total_docs += 1;
        }
        
        // Use the optimized batch indexing method for all tokens
        // Group tokens by term and document ID for batch insertion
        let mut term_positions_map: std::collections::HashMap<(String, String), Vec<u64>> = std::collections::HashMap::new();
        
        for token in tokens {
            let key = (token.term.clone(), token.doc_id.clone());
            term_positions_map.entry(key).or_default().push(token.position);
        }
        
        // Index terms with their positions
        for ((term, doc_id), positions) in term_positions_map {
            let token = Token {
                term,
                doc_id,
                position: 0, // This value doesn't matter for batch insertion
            };
            self.add_term_with_positions(token, positions).await?;
        }
        
        // Record indexing performance
        let indexing_time = start_time.elapsed();
        println!("Indexed document '{}' in {:?}", doc_id, indexing_time);
        
        Ok(())
    }

    pub async fn remove_term(
        &self,
        term: &str,
        doc_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let db = self.db.write().await;

        if let Some(existing) = db.get(term.as_bytes())? {
            let mut entry: TermIndex = rkyv::from_bytes::<TermIndex, Error>(&existing)?;
            entry.doc_ids.remove(doc_id);

            if entry.doc_ids.is_empty() {
                // Log deletion to WAL
                self.wal_chan
                    .send(WALCMD::Delete {
                        key: term.to_string(),
                    })
                    .await?;

                // Remove the term if no documents reference it
                db.remove(term.as_bytes())?;
            } else {
                let serialized = rkyv::to_bytes::<rkyv::rancor::Panic>(&entry)?;

                // Log update to WAL
                self.wal_chan
                    .send(WALCMD::Put {
                        key: term.to_string(),
                        value: serialized.to_vec(),
                    })
                    .await?;

                // Update the index
                db.insert(term.as_bytes(), serialized.as_slice())?;
            }
        }

        Ok(())
    }

    pub async fn search(
        &self,
        term: &str,
    ) -> Result<Option<TermIndex>, Box<dyn std::error::Error>> {
        let db = self.db.read().await;

        if let Some(entry_bytes) = db.get(term.as_bytes())? {
            let entry: TermIndex = rkyv::from_bytes::<TermIndex, Error>(&entry_bytes)?;
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }
    
    /// Deletes a document from the index
    ///
    /// # Arguments
    ///
    /// * `doc_id` - ID of the document to delete
    ///
    /// # Returns
    ///
    /// Result indicating success or error with deletion time
    pub async fn delete_document(&self, doc_id: &str) -> Result<Duration, Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        
        // Find all terms that reference this document
        let db = self.db.read().await;
        let mut terms_to_update = Vec::new();
        
        // Scan the index for terms referencing this document
        for item in db.iter() {
            if let Ok((key, value)) = item {
                let term_index: TermIndex = rkyv::from_bytes::<TermIndex, Error>(&value)?;
                if term_index.doc_ids.contains_key(doc_id) {
                    let term = String::from_utf8(key.to_vec())?;
                    terms_to_update.push(term);
                }
            }
        }
        
        // Check if we need to decrement the document count
        let has_terms = !terms_to_update.is_empty();
        
        // Remove the document from each term's index
        for term in &terms_to_update {
            self.remove_term(term, doc_id).await?;
        }
        
        // Remove from BM25 search engine
        let mut search_engine_guard = self.search_engine.write().await;
        if let Some(search_engine) = search_engine_guard.as_mut() {
            search_engine.remove(&doc_id.to_string());
        }
        
        // Remove from document database
        let doc_db = self.doc_db.write().await;
        doc_db.remove(doc_id.as_bytes())?;
        
        // Decrement document count if we had any terms
        if has_terms {
            let mut total_docs = self.total_docs.write().await;
            if *total_docs > 0 {
                *total_docs -= 1;
            }
        }
        
        let elapsed = start_time.elapsed();
        println!("Deleted document '{}' from index in {:?}", doc_id, elapsed);
        
        Ok(elapsed)
    }

    pub async fn search_text<T: Tokenizer>(
        &self,
        query: &str,
        tokenizer: &T,
    ) -> Result<Vec<SearchResult>, Box<dyn std::error::Error>> {
        // Start timing the entire search operation
        let search_start = Instant::now();
        
        // Initialize metrics
        let mut metrics = SearchMetrics::new();
        metrics.total_time = Duration::default();
        
        // Parsing phase timing
        let parsing_start = Instant::now();
        
        // Tokenize the query for position-based retrieval
        let tokens = tokenizer.tokenize(query, "query");

        if tokens.is_empty() {
            return Ok(vec![]);
        }
        
        metrics.query_parsing_time = parsing_start.elapsed();
        
        // Retrieval phase timing
        let retrieval_start = Instant::now();
        
        // Get the inverted index results for position data (needed for highlighting)
        let mut all_docs = HashSet::new();
        let mut term_results = HashMap::new();

        // Get matching docs for each term
        for token in &tokens {
            let term = token.term.clone();
            if let Some(term_index) = self.search(&term).await? {
                term_results.insert(term, term_index.clone()); // Store reference
                
                // Collect all matching document IDs
                for doc_id in term_index.doc_ids.keys() {
                    all_docs.insert(doc_id.clone());
                }
            }
        }
        
        // Update retrieval metrics
        metrics.retrieval_time = retrieval_start.elapsed();
        metrics.documents_searched = term_results.values()
            .map(|term_index| term_index.doc_ids.len())
            .sum();
        metrics.documents_matched = all_docs.len();

        // Early return if no matches in the inverted index
        if all_docs.is_empty() {
            metrics.total_time = search_start.elapsed();
            *self.last_metrics.write().await = metrics;
            return Ok(vec![]);
        }
        
        // Scoring phase timing
        let scoring_start = Instant::now();
        
        // Use BM25 search engine for scoring
        let search_engine_guard = self.search_engine.read().await;
        let mut search_results = Vec::new();
        
        // We use our TF-IDF implementation for consistent case-insensitive search
        // This allows us to control the term normalization process
        let use_tf_idf_scoring = true;
        
        // The BM25 search engine implementation has been disabled due to case sensitivity issues
        // This may be revisited later if the BM25 engine can be further customized
        if !use_tf_idf_scoring && search_engine_guard.as_ref().is_some() {
            let search_engine = search_engine_guard.as_ref().unwrap();
            
            // Normalize query to lowercase for case-insensitive search
            let normalized_query = query.to_lowercase();
            
            // Perform the BM25 search with a limit of 100 results
            let top_results = search_engine.search(&normalized_query, 100);
            
            // Build search results with scores and term positions
            for result in top_results {
                let doc_id = result.document.id.to_string();
                let score = result.score;
                
                // Skip documents with very low scores
                if score < 0.00001 {
                    continue;
                }
                
                let mut doc_term_matches = HashMap::new();
                
                // Collect term matches for highlighting
                for (term, term_index) in &term_results {
                    if let Some(positions) = term_index.doc_ids.get(&doc_id) {
                        doc_term_matches.insert(term.clone(), positions.clone());
                    }
                }
                
                // Include results even if we don't have position data for highlighting
                // This allows semantic matches to still appear in results
                search_results.push(SearchResult {
                    doc_id: doc_id.clone(),
                    relevance_score: score as f64,
                    term_matches: doc_term_matches,
                });
            }
        } else {
            // Use TF-IDF scoring for case-insensitive search
            // This is our primary search method for reliable case insensitivity
            let total_docs = *self.total_docs.read().await;
            let total_docs_f64 = total_docs as f64;
            
            for doc_id in all_docs {
                let mut relevance_score = 0.0;
                let mut doc_term_matches = HashMap::new();
                let mut total_terms = 0.0;
                
                for (term, term_index) in &term_results {
                    if let Some(positions) = term_index.doc_ids.get(&doc_id) {
                        // Calculate TF-IDF score components:
                        // TF (term frequency) = number of times term appears in document
                        let term_freq = positions.len() as f64;
                        
                        // DF (document frequency) = number of documents containing this term
                        let doc_freq = term_index.doc_ids.len() as f64;
                        
                        // IDF (inverse document frequency) = log(total_docs / doc_freq)
                        let idf = if doc_freq > 0.0 {
                            (total_docs_f64 / doc_freq).ln()
                        } else {
                            0.0
                        };
                        
                        // Accumulate stats
                        total_terms += term_freq;
                        
                        // Compute term score and add to document relevance
                        let term_score = term_freq * idf;
                        relevance_score += term_score;
                        
                        // Save term positions for highlighting
                        doc_term_matches.insert(term.clone(), positions.clone());
                    }
                }
                
                // Normalize relevance score by total terms in document
                // This helps make scores comparable across documents of different lengths
                if total_terms > 0.0 {
                    relevance_score /= total_terms;
                }

                if !doc_term_matches.is_empty() {
                    search_results.push(SearchResult {
                        doc_id,
                        relevance_score,
                        term_matches: doc_term_matches,
                    });
                }
            }
        }
        
        // Update scoring metrics
        metrics.scoring_time = scoring_start.elapsed();
        
        // Sort by relevance score (descending)
        search_results.sort_by(|a, b| {
            b.relevance_score
                .partial_cmp(&a.relevance_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        
        // Complete performance metrics
        metrics.total_time = search_start.elapsed();
        
        // Log search performance
        println!(
            "Search for '{}' found {} results in {:?} (parsing: {:?}, retrieval: {:?}, scoring: {:?})",
            query,
            search_results.len(),
            metrics.total_time,
            metrics.query_parsing_time,
            metrics.retrieval_time,
            metrics.scoring_time
        );
        
        // Save metrics for later retrieval
        *self.last_metrics.write().await = metrics;

        Ok(search_results)
    }
}
