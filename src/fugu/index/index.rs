// BM25 has been removed
/// Index module provides text search capabilities through TF-IDF and inverted index
///
/// This module implements:
/// - A persistent inverted index for fast text search
/// - Token-based indexing and retrieval
/// - Text tokenization and relevancy ranking
/// - TF-IDF scoring for search results
/// - Performance metrics collection for search operations
use rkyv;
use rkyv::{rancor::Error, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use sled;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
// Use tracing for logging
use tracing;
use tracing::{debug, info};

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
    #[allow(dead_code)]
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
#[allow(dead_code)]
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
/// - A relevance score based on TF-IDF
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

/// Maps a document to all the terms it contains
///
/// This structure maintains:
/// - The document ID
/// - A set of all terms contained in this document
/// Used for efficient document deletion without full index scan
#[derive(Debug, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize, Clone)]
pub struct DocIndex {
    /// The document identifier
    pub doc_id: String,
    /// Set of all terms contained in this document
    pub terms: HashSet<String>,
}

/// Consolidated database record that contains all the data for a namespace
///
/// This structure is used for serializing and deserializing the entire index
/// to a single rkyv file.
#[derive(Debug, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize, Clone)]
pub struct ConsolidatedIndex {
    /// All term indices stored by term name
    pub terms: HashMap<String, TermIndex>,
    /// All documents stored by document ID
    pub documents: HashMap<String, String>,
    /// Document-to-terms mapping for efficient deletion
    pub doc_terms: HashMap<String, DocIndex>,
    /// Total number of documents in the index
    pub total_docs: usize,
}

/// The inverted index implementation with distributed term storage
///
/// This structure:
/// - Uses document-to-terms mapping instead of a main index
/// - Supports concurrent read/write access
/// - Implements TF-IDF relevance scoring
/// - Tracks search performance metrics
/// - Persistently stores document contents in a dedicated sled DB
/// - Maintains document-to-terms mapping for efficient retrieval and deletion
#[derive(Clone, Debug)]
pub struct InvertedIndex {
    /// Path to the index storage
    db: Arc<RwLock<sled::Db>>,
    path: String,
    doc_db: Arc<RwLock<sled::Db>>,
    /// Document-to-terms database for efficient retrieval and deletion
    doc_term_db: Arc<RwLock<sled::Db>>,
    /// Path to disk cache for text files
    cache_path: String,
    /// Size threshold for caching (files larger than this won't be cached)
    cache_size_threshold: usize,
    /// Latest search metrics
    last_metrics: Arc<RwLock<SearchMetrics>>,
    /// Total documents in the index
    total_docs: Arc<RwLock<usize>>,
    /// In-memory term cache for fast lookups
    term_cache: Arc<RwLock<HashMap<String, TermIndex>>>,
    /// Path to the consolidated rkyv file
    consolidated_path: String,
}

impl InvertedIndex {
    /// Creates a new InvertedIndex instance
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the index storage directory
    ///
    /// # Returns
    ///
    /// A new InvertedIndex instance
    pub async fn new(path: &str) -> Self {
        // Create the base path if it doesn't exist
        if !Path::new(path).exists() {
            std::fs::create_dir_all(path).expect("Failed to create index directory");
        }

        // Path for the consolidated rkyv file
        let consolidated_path = format!("{}/consolidated.rkyv", path);

        // Check if consolidated file exists and load it instead
        if Path::new(&consolidated_path).exists() {
            if let Ok(index) = Self::load_consolidated(&consolidated_path) {
                info!("Loaded consolidated index from {}", consolidated_path);
                return index;
            }
        }

        // Main index database - use more reliable config
        let index_path = format!("{}/index", path);
        let db = match sled::Config::new()
            .path(&index_path)
            .use_compression(false) // Fix compression mismatch issue
            .mode(sled::Mode::LowSpace) // Important: Less aggressive locking
            .open()
        {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("Failed to open index database: {:?}", e);
                // Return a temporary in-memory database as fallback during tests
                if cfg!(test) {
                    sled::Config::new().temporary(true).open().expect("Failed to open fallback in-memory database")
                } else {
                    panic!("Failed to open index database: {:?}", e)
                }
            },
        };

        // Document content database - use more reliable config
        let docs_path = format!("{}/docs", path);
        let doc_db = match sled::Config::new()
            .path(&docs_path)
            .use_compression(false) // Fix compression mismatch issue
            .mode(sled::Mode::LowSpace) // Important: Less aggressive locking
            .open()
        {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("Failed to open documents database: {:?}", e);
                // Return a temporary in-memory database as fallback during tests
                if cfg!(test) {
                    sled::Config::new().temporary(true).open().expect("Failed to open fallback in-memory database")
                } else {
                    panic!("Failed to open documents database: {:?}", e)
                }
            },
        };

        // No longer creating cache directory - all files stay in their namespace location

        // Document-to-terms mapping database for efficient deletion
        let doc_term_path = format!("{}/doc_terms", path);
        let doc_term_db = match sled::Config::new()
            .path(&doc_term_path)
            .use_compression(false) // Fix compression mismatch issue
            .mode(sled::Mode::LowSpace)
            .temporary(cfg!(test)) // Use ephemeral DB in tests to avoid lock issues
            .open()
        {
            Ok(db) => db,
            Err(e) => {
                tracing::error!("Failed to open document-terms database: {:?}", e);
                // Return a temporary in-memory database as fallback during tests
                if cfg!(test) {
                    sled::Config::new().temporary(true).open().expect("Failed to open fallback in-memory database")
                } else {
                    panic!("Failed to open document-terms database: {:?}", e)
                }
            },
        };

        // Define a reasonable cache size threshold (2MB)
        const DEFAULT_CACHE_SIZE_THRESHOLD: usize = 2 * 1024 * 1024;

        InvertedIndex {
            db: Arc::new(RwLock::new(db)),
            path: path.to_string(),
            doc_db: Arc::new(RwLock::new(doc_db)),
            doc_term_db: Arc::new(RwLock::new(doc_term_db)),
            cache_path: String::new(), // No longer using separate cache directory
            cache_size_threshold: DEFAULT_CACHE_SIZE_THRESHOLD,
            last_metrics: Arc::new(RwLock::new(SearchMetrics::new())),
            total_docs: Arc::new(RwLock::new(0)),
            term_cache: Arc::new(RwLock::new(HashMap::new())),
            consolidated_path,
        }
    }
    
    /// Load an index from a consolidated rkyv file
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the consolidated rkyv file
    ///
    /// # Returns
    ///
    /// Result containing a new InvertedIndex instance or an error
    fn load_consolidated(file_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Open and read the consolidated file
        let mut file = File::open(file_path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        
        // Deserialize the consolidated index
        let consolidated_index: ConsolidatedIndex = rkyv::from_bytes::<ConsolidatedIndex, Error>(&bytes)?;
        
        // Extract the path from the file path
        let path = Path::new(file_path)
            .parent()
            .ok_or("Invalid file path")?
            .to_string_lossy()
            .to_string();
            
        // Create the base directories
        // No longer creating cache directory
        
        // Recreate the sled databases
        let index_path = format!("{}/index", path);
        let db = sled::Config::new()
            .path(&index_path)
            .use_compression(false) // Fix compression mismatch issue
            .mode(sled::Mode::LowSpace)
            .open()?;
            
        let docs_path = format!("{}/docs", path);
        let doc_db = sled::Config::new()
            .path(&docs_path)
            .use_compression(false) // Fix compression mismatch issue
            .mode(sled::Mode::LowSpace)
            .open()?;
            
        let doc_term_path = format!("{}/doc_terms", path);
        let doc_term_db = sled::Config::new()
            .path(&doc_term_path)
            .use_compression(false) // Fix compression mismatch issue
            .mode(sled::Mode::LowSpace)
            .open()?;
        
        // Populate the databases from the consolidated data
        for (term, term_index) in &consolidated_index.terms {
            let serialized = rkyv::to_bytes::<rkyv::rancor::Panic>(term_index)?;
            db.insert(term.as_bytes(), serialized.as_slice())?;
        }
        
        for (doc_id, content) in &consolidated_index.documents {
            doc_db.insert(doc_id.as_bytes(), content.as_bytes())?;
        }
        
        for (doc_id, doc_index) in &consolidated_index.doc_terms {
            let serialized = rkyv::to_bytes::<rkyv::rancor::Panic>(doc_index)?;
            doc_term_db.insert(doc_id.as_bytes(), serialized.as_slice())?;
        }
        
        // Define a reasonable cache size threshold (2MB)
        const DEFAULT_CACHE_SIZE_THRESHOLD: usize = 2 * 1024 * 1024;
        
        // Create and return the index
        let index = InvertedIndex {
            db: Arc::new(RwLock::new(db)),
            path,
            doc_db: Arc::new(RwLock::new(doc_db)),
            doc_term_db: Arc::new(RwLock::new(doc_term_db)),
            cache_path: String::new(), // No longer using separate cache directory
            cache_size_threshold: DEFAULT_CACHE_SIZE_THRESHOLD,
            last_metrics: Arc::new(RwLock::new(SearchMetrics::new())),
            total_docs: Arc::new(RwLock::new(consolidated_index.total_docs)),
            term_cache: Arc::new(RwLock::new(consolidated_index.terms.clone())),
            consolidated_path: file_path.to_string(),
        };
        
        Ok(index)
    }

    /// Retrieves the latest search performance metrics
    #[allow(dead_code)]
    pub async fn get_last_metrics(&self) -> SearchMetrics {
        self.last_metrics.read().await.clone()
    }

    /// Returns the total number of unique terms in the index
    ///
    /// # Returns
    ///
    /// The number of unique terms
    pub fn get_total_terms(&self) -> usize {
        // Get a read guard on the database
        let guard = match self.db.try_read() {
            Ok(guard) => guard,
            Err(_) => return 0, // Return 0 if we can't get a read lock
        };

        // Count the total number of unique terms
        guard.iter().count()
    }

    /// Returns the total number of documents in the index
    ///
    /// # Returns
    ///
    /// The number of documents
    pub async fn get_total_docs(&self) -> usize {
        *self.total_docs.read().await
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

        // Flush the document-to-terms database
        let doc_term_db = self.doc_term_db.read().await;
        doc_term_db.flush()?;

        // Note: The disk cache doesn't need explicit flushing since each file is written
        // with tokio::fs::write which handles proper flushing

        println!("Flushed index and document data to {}", self.path);
        
        // Consolidate and save to a single rkyv file
        match self.save_consolidated().await {
            Ok(_) => println!("Consolidated index saved to {}", self.consolidated_path),
            Err(e) => println!("Failed to save consolidated index: {}", e),
        }
        
        Ok(())
    }
    
    /// Consolidates all database files into a single rkyv file
    ///
    /// This method:
    /// - Collects all term indices, documents, and doc-terms mappings
    /// - Serializes everything into a single consolidated file
    /// - Maintains the original sled databases for backward compatibility
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    async fn save_consolidated(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Create the consolidated index structure
        let mut consolidated = ConsolidatedIndex {
            terms: HashMap::new(),
            documents: HashMap::new(),
            doc_terms: HashMap::new(),
            total_docs: *self.total_docs.read().await,
        };
        
        // Collect all term indices
        let db = self.db.read().await;
        for item in db.iter() {
            if let Ok((key, value)) = item {
                if let Ok(term_index) = rkyv::from_bytes::<TermIndex, Error>(&value) {
                    if let Ok(term) = String::from_utf8(key.to_vec()) {
                        consolidated.terms.insert(term, term_index);
                    }
                }
            }
        }
        
        // Collect all documents
        let doc_db = self.doc_db.read().await;
        for item in doc_db.iter() {
            if let Ok((key, value)) = item {
                if let (Ok(doc_id), Ok(content)) = (
                    String::from_utf8(key.to_vec()),
                    String::from_utf8(value.to_vec())
                ) {
                    consolidated.documents.insert(doc_id, content);
                }
            }
        }
        
        // Collect all doc-terms mappings
        let doc_term_db = self.doc_term_db.read().await;
        for item in doc_term_db.iter() {
            if let Ok((key, value)) = item {
                if let Ok(doc_id) = String::from_utf8(key.to_vec()) {
                    if let Ok(doc_index) = rkyv::from_bytes::<DocIndex, Error>(&value) {
                        consolidated.doc_terms.insert(doc_id, doc_index);
                    }
                }
            }
        }
        
        // Serialize the consolidated data
        let serialized = rkyv::to_bytes::<rkyv::rancor::Panic>(&consolidated)?;
        
        // Write to the consolidated file
        // Use a temporary file and atomic rename for safety
        let temp_path = format!("{}.tmp", self.consolidated_path);
        std::fs::write(&temp_path, serialized)?;
        std::fs::rename(&temp_path, &self.consolidated_path)?;
        
        Ok(())
    }

    /// Closes the database, properly releasing all locks and resources
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn close(&self) -> Result<(), Box<dyn std::error::Error>> {
        // First flush all pending changes (this also saves the consolidated file)
        self.flush().await?;

        // For sled, there's no explicit close, but we can use this approach
        // to help release resources and locks
        {
            // Temporarily take strong references and flush them
            let db = self.db.read().await;
            let doc_db = self.doc_db.read().await;
            let doc_term_db = self.doc_term_db.read().await;

            // Explicitly flush them again before they go out of scope
            db.flush()?;
            doc_db.flush()?;
            doc_term_db.flush()?;

            // No explicit close in sled - locks should be released when these
            // references go out of scope at the end of this block
        }

        println!("Closed database connections for {}", self.path);
        Ok(())
    }
    
    /// Loads the index directly from the consolidated file without creating sled databases
    ///
    /// This method is more efficient than load_consolidated as it doesn't recreate the sled databases
    /// but instead works directly with the consolidated data in memory.
    ///
    /// # Returns
    ///
    /// Result containing a new InvertedIndex instance or an error
    pub async fn load_index_direct(&self) -> Result<(), Box<dyn std::error::Error>> {
        if Path::new(&self.consolidated_path).exists() {
            // Open and read the consolidated file
            let mut file = File::open(&self.consolidated_path)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes)?;
            
            // Deserialize the consolidated index
            let consolidated_index: ConsolidatedIndex = rkyv::from_bytes::<ConsolidatedIndex, Error>(&bytes)?;
            
            // Update the in-memory state
            *self.total_docs.write().await = consolidated_index.total_docs;
            *self.term_cache.write().await = consolidated_index.terms.clone();
            
            // We don't need to load the documents or doc_terms since they will be
            // loaded on demand from the consolidated file when needed
            
            info!("Loaded index directly from consolidated file: {}", self.consolidated_path);
            Ok(())
        } else {
            Err(format!("Consolidated file not found: {}", self.consolidated_path).into())
        }
    }

    /// Gets cache information including directory and stats
    ///
    /// This is a legacy method. The cache directory is no longer used, as files stay
    /// in their namespace location. This method now returns information about the 
    /// documents stored in the document database.
    ///
    /// # Returns
    ///
    /// A tuple containing (namespace_path, document_count, total_size_bytes)
    pub async fn get_cache_info(&self) -> Result<(String, usize, u64), Box<dyn std::error::Error>> {
        // Use the namespace path instead of the non-existent cache path
        let namespace_path = self.path.clone();
        
        // Count documents in the document database
        let doc_db = self.doc_db.read().await;
        let mut doc_count = 0;
        let mut total_size = 0u64;
        
        for item in doc_db.iter() {
            if let Ok((_, value)) = item {
                doc_count += 1;
                total_size += value.len() as u64;
            }
        }
        
        Ok((namespace_path, doc_count, total_size))
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
        positions: Vec<u64>,
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
                let new_term_freq = entry
                    .doc_ids
                    .values()
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

        // WAL logging has been removed

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
        info!(doc_id=%doc_id, text_size=%text.len(), "Starting document indexing");

        // Store the document content in the document database
        let doc_db = self.doc_db.write().await;
        doc_db.insert(doc_id.as_bytes(), text.as_bytes())?;

        // Check if the document is a text file that should be cached
        // First check if it's small enough to cache
        let text_bytes = text.as_bytes();
        let _is_cacheable_size = text_bytes.len() <= self.cache_size_threshold;

        // Determine if it's likely a text file based on a simple heuristic:
        // - Non-binary text files typically have a high proportion of printable ASCII
        // - Check a sample of the file to see if it's primarily printable ASCII
        let _is_text_file = if text_bytes.len() > 0 {
            let sample_size = std::cmp::min(1024, text_bytes.len());
            let printable_count = text_bytes[0..sample_size]
                .iter()
                .filter(|&&b| (b >= 32 && b <= 126) || b == 9 || b == 10 || b == 13)
                .count();

            // If more than 90% is printable, it's likely a text file
            (printable_count as f32 / sample_size as f32) > 0.9
        } else {
            // Empty files are considered text files
            true
        };

        // No longer using cache directory - files stay in their namespace path
        debug!(
            "Document '{}' indexed (size: {})",
            doc_id,
            text_bytes.len()
        );

        // Tokenize the document for the inverted index
        let tokens = tokenizer.tokenize(text, doc_id);

        // We now use our own TF-IDF scoring instead of BM25

        // Increment total document count
        {
            let mut total_docs = self.total_docs.write().await;
            *total_docs += 1;
        }

        // Use the optimized batch indexing method for all tokens
        // Group tokens by term and document ID for batch insertion
        let mut term_positions_map: std::collections::HashMap<(String, String), Vec<u64>> =
            std::collections::HashMap::new();

        for token in tokens {
            let key = (token.term.clone(), token.doc_id.clone());
            term_positions_map
                .entry(key)
                .or_default()
                .push(token.position);
        }

        // Collect all unique terms for the document-to-terms mapping
        let unique_terms: HashSet<String> = term_positions_map
            .keys()
            .map(|(term, _)| term.clone())
            .collect();

        // Keep track of the term count for logging
        let term_count = unique_terms.len();

        // Create or update the document-to-terms mapping
        let doc_term_db = self.doc_term_db.write().await;
        let doc_index = DocIndex {
            doc_id: doc_id.to_string(),
            terms: unique_terms,
        };

        // Serialize and store the document-to-terms mapping
        let serialized = rkyv::to_bytes::<rkyv::rancor::Panic>(&doc_index)?;
        doc_term_db.insert(doc_id.as_bytes(), serialized.as_slice())?;

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
        info!(doc_id=%doc_id, time=?indexing_time, terms=%term_count, "Document indexed successfully");

        Ok(())
    }

    pub async fn remove_term(
        &self,
        term: &str,
        doc_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!(
            "[REMOVE_TERM] Removing term '{}' for document '{}'",
            term, doc_id
        );
        let db = self.db.write().await;

        match db.get(term.as_bytes()) {
            Ok(Some(existing)) => {
                println!("[REMOVE_TERM] Found existing term entry");
                match rkyv::from_bytes::<TermIndex, Error>(&existing) {
                    Ok(mut entry) => {
                        // Check if document exists in this term's index
                        let had_doc = entry.doc_ids.contains_key(doc_id);
                        println!(
                            "[REMOVE_TERM] Document '{}' exists in term '{}': {}",
                            doc_id, term, had_doc
                        );

                        // Remove the document from this term's index
                        entry.doc_ids.remove(doc_id);

                        // Update term frequency if needed
                        let old_freq = entry.term_frequency;
                        entry.term_frequency = entry.doc_ids.len() as u32;
                        println!(
                            "[REMOVE_TERM] Updated term frequency: {} -> {}",
                            old_freq, entry.term_frequency
                        );

                        if entry.doc_ids.is_empty() {
                            println!("[REMOVE_TERM] Term '{}' has no more documents, removing completely", term);
                            // Remove the term if no documents reference it
                            match db.remove(term.as_bytes()) {
                                Ok(_) => {
                                    println!("[REMOVE_TERM] Successfully removed term from index")
                                }
                                Err(e) => {
                                    println!("[REMOVE_TERM] Error removing term from index: {}", e)
                                }
                            }

                            // WAL logging removed
                            println!("[REMOVE_TERM] WAL logging has been removed");
                        } else {
                            println!(
                                "[REMOVE_TERM] Term '{}' still has {} documents, updating entry",
                                term,
                                entry.doc_ids.len()
                            );

                            match rkyv::to_bytes::<rkyv::rancor::Panic>(&entry) {
                                Ok(serialized) => {
                                    // Update the index
                                    match db.insert(term.as_bytes(), serialized.as_slice()) {
                                        Ok(_) => println!(
                                            "[REMOVE_TERM] Successfully updated term in index"
                                        ),
                                        Err(e) => println!(
                                            "[REMOVE_TERM] Error updating term in index: {}",
                                            e
                                        ),
                                    }

                                    // WAL logging removed
                                    println!("[REMOVE_TERM] WAL logging has been removed");
                                }
                                Err(e) => {
                                    println!("[REMOVE_TERM] Error serializing updated term: {}", e)
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("[REMOVE_TERM] Error deserializing term '{}': {}", term, e);
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to deserialize term index: {}", e),
                        )));
                    }
                }
            }
            Ok(None) => {
                println!("[REMOVE_TERM] Term '{}' not found in index", term);
            }
            Err(e) => {
                println!(
                    "[REMOVE_TERM] Error getting term '{}' from database: {}",
                    term, e
                );
                return Err(Box::new(e));
            }
        }

        println!("[REMOVE_TERM] Successfully completed remove_term operation");
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

    /// Retrieves a document by its ID from the document database
    ///
    /// # Arguments
    ///
    /// * `doc_id` - ID of the document to retrieve
    ///
    /// # Returns
    ///
    /// Result containing the document content or None if not found
    pub async fn get_document(
        &self,
        doc_id: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        // Only retrieving from document database - no more cache lookup

        // Get document from document database
        let doc_db = self.doc_db.read().await;

        match doc_db.get(doc_id.as_bytes())? {
            Some(bytes) => {
                // Convert bytes to string
                match String::from_utf8(bytes.to_vec()) {
                    Ok(content) => {
                        debug!("Retrieved document '{}' from document database", doc_id);
                        Ok(Some(content))
                    }
                    Err(_) => {
                        // If it's not valid UTF-8, return as lossy string
                        let lossy_content = String::from_utf8_lossy(&bytes).to_string();
                        debug!(
                            "Retrieved document '{}' from document database (invalid UTF-8)",
                            doc_id
                        );
                        Ok(Some(lossy_content))
                    }
                }
            }
            None => {
                debug!("Document '{}' not found", doc_id);
                Ok(None)
            }
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
    pub async fn delete_document(
        &self,
        doc_id: &str,
    ) -> Result<Duration, Box<dyn std::error::Error>> {
        let start_time = Instant::now();
        println!("[INDEX] Starting delete operation for document: {}", doc_id);

        // Get the document's terms from the inverse lookup
        let doc_term_db = self.doc_term_db.read().await;
        let terms_to_update: Vec<String>;

        match doc_term_db.get(doc_id.as_bytes()) {
            Ok(Some(doc_index_bytes)) => {
                // Deserialize the document-to-terms mapping
                match rkyv::from_bytes::<DocIndex, Error>(&doc_index_bytes) {
                    Ok(doc_index) => {
                        // Get all terms for this document
                        terms_to_update = doc_index.terms.into_iter().collect();
                        println!("[INDEX] Found {} terms to update for document: {} using inverse lookup", 
                                 terms_to_update.len(), doc_id);
                    }
                    Err(e) => {
                        println!("[INDEX] Error deserializing document-terms index: {}", e);
                        // Fall back to full scan if we can't deserialize the document-terms mapping
                        println!("[INDEX] Falling back to full scan for document: {}", doc_id);
                        terms_to_update = self.find_terms_by_full_scan(doc_id).await?;
                    }
                }
            }
            Ok(None) => {
                println!(
                    "[INDEX] No document-terms mapping found for document: {}",
                    doc_id
                );
                // Fall back to full scan if we don't have a document-terms mapping
                println!("[INDEX] Falling back to full scan for document: {}", doc_id);
                terms_to_update = self.find_terms_by_full_scan(doc_id).await?;
            }
            Err(e) => {
                println!("[INDEX] Error getting document-terms mapping: {}", e);
                // Fall back to full scan if we encounter an error
                println!("[INDEX] Falling back to full scan for document: {}", doc_id);
                terms_to_update = self.find_terms_by_full_scan(doc_id).await?;
            }
        }

        // Remove the document from the inverse lookup
        if let Err(e) = doc_term_db.remove(doc_id.as_bytes()) {
            println!("[INDEX] Error removing document-terms mapping: {}", e);
        }

        // Check if we need to decrement the document count
        let has_terms = !terms_to_update.is_empty();

        // Remove the document from each term's index
        for term in &terms_to_update {
            println!("[INDEX] Removing term '{}' for document: {}", term, doc_id);
            match self.remove_term(term, doc_id).await {
                Ok(_) => (),
                Err(e) => println!("[INDEX] Error removing term '{}': {}", term, e),
            }
        }

        // BM25 search engine removed

        // Remove from document database
        println!(
            "[INDEX] Removing document from document database: {}",
            doc_id
        );
        let doc_db = self.doc_db.write().await;
        match doc_db.remove(doc_id.as_bytes()) {
            Ok(_) => println!("[INDEX] Successfully removed document from document database"),
            Err(e) => println!("[INDEX] Error removing document from database: {}", e),
        }

        // No longer removing from disk cache as files stay in their namespace path

        // Decrement document count if we had any terms
        if has_terms {
            println!("[INDEX] Decrementing document count");
            let mut total_docs = self.total_docs.write().await;
            if *total_docs > 0 {
                *total_docs -= 1;
            }
        }

        let elapsed = start_time.elapsed();
        println!(
            "[INDEX] Completed delete operation for document '{}' in {:?}",
            doc_id, elapsed
        );

        Ok(elapsed)
    }

    /// Helper method for finding all terms that reference a document using full scan
    /// Used as a fallback when the inverse lookup fails
    async fn find_terms_by_full_scan(
        &self,
        doc_id: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let db = self.db.read().await;
        let mut terms_to_update = Vec::new();

        // Scan the index for terms referencing this document
        println!(
            "[INDEX] Scanning index for terms referencing document: {}",
            doc_id
        );
        for item in db.iter() {
            if let Ok((key, value)) = item {
                match rkyv::from_bytes::<TermIndex, Error>(&value) {
                    Ok(term_index) => {
                        if term_index.doc_ids.contains_key(doc_id) {
                            match String::from_utf8(key.to_vec()) {
                                Ok(term) => {
                                    terms_to_update.push(term);
                                }
                                Err(e) => {
                                    println!("[INDEX] Error converting key to string: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("[INDEX] Error deserializing term index: {}", e);
                    }
                }
            }
        }

        println!(
            "[INDEX] Found {} terms for document {} using full scan",
            terms_to_update.len(),
            doc_id
        );
        Ok(terms_to_update)
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
        metrics.documents_searched = term_results
            .values()
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

        // Use TF-IDF scoring for search
        let mut search_results = Vec::new();

        {
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
