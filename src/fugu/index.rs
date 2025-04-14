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
        println!("just inserted {entry}");

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
                let document = bm25::Document::new(doc_id.to_string(), text);
                
                // Add or update document in the search engine
                search_engine.upsert(document);
            }
        }
        
        // Increment total document count
        {
            let mut total_docs = self.total_docs.write().await;
            *total_docs += 1;
        }
        
        // Index all tokens in the inverted index for position-based retrieval
        for token in tokens {
            self.add_term(token).await?;
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
                let serialized = rkyv::to_bytes::<Error>(&entry)?;

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
        
        if let Some(search_engine) = search_engine_guard.as_ref() {
            // Perform the BM25 search with a limit of 100 results
            let top_results = search_engine.search(query, 100);
            
            // Build search results with scores and term positions
            for result in top_results {
                let doc_id = result.document.id.to_string();
                let score = result.score;
                
                // Skip documents with very low scores
                if score < 0.001 {
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
            // Fallback to TF-IDF scoring if BM25 engine is not available
            let total_docs = *self.total_docs.read().await;
            let total_docs_f64 = total_docs as f64;
            
            for doc_id in all_docs {
                let mut relevance_score = 0.0;
                let mut doc_term_matches = HashMap::new();
                let mut total_terms = 0.0;
                
                for (term, term_index) in &term_results {
                    if let Some(positions) = term_index.doc_ids.get(&doc_id) {
                        // TF-IDF scoring
                        let term_freq = positions.len() as f64;
                        let doc_freq = term_index.doc_ids.len() as f64;
                        let idf = if doc_freq > 0.0 {
                            (total_docs_f64 / doc_freq).ln()
                        } else {
                            0.0
                        };
                        
                        total_terms += term_freq;
                        let term_score = term_freq * idf;
                        relevance_score += term_score;
                        doc_term_matches.insert(term.clone(), positions.clone());
                    }
                }
                
                // Normalize by total terms in document
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
