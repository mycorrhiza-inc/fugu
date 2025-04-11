/// Index module provides text search capabilities through an inverted index
///
/// This module implements:
/// - A persistent inverted index for fast text search
/// - Token-based indexing and retrieval
/// - Text tokenization and relevancy ranking
/// - TF-IDF scoring for search results
use crate::fugu::wal::WALCMD;
use rkyv;
use rkyv::{rancor::Error, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use sled;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

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

/// The main inverted index implementation
///
/// This structure:
/// - Provides persistent storage of index data
/// - Supports concurrent read/write access
/// - Uses WAL for durability
/// - Implements TF-IDF relevance scoring
#[derive(Clone, Debug)]
pub struct InvertedIndex {
    /// Underlying database for persistence
    db: Arc<RwLock<sled::Db>>,
    /// Channel for WAL operations
    wal_chan: mpsc::Sender<WALCMD>,
    /// Path to the index storage
    path: String,
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
        let db = sled::open(path).expect("Failed to open sled database");
        InvertedIndex {
            db: Arc::new(RwLock::new(db)),
            wal_chan,
            path: path.to_string(),
        }
    }
    
    /// Flushes all pending changes to disk
    ///
    /// Ensures that all index changes are written to persistent storage
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        let db = self.db.read().await;
        db.flush()?;
        println!("Flushed index data to {}", self.path);
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
        let tokens = tokenizer.tokenize(text, doc_id);

        for token in tokens {
            self.add_term(token).await?;
        }

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

    pub async fn search_text<T: Tokenizer>(
        &self,
        query: &str,
        tokenizer: &T,
    ) -> Result<Vec<SearchResult>, Box<dyn std::error::Error>> {
        // Tokenize the query
        let tokens = tokenizer.tokenize(query, "query");

        if tokens.is_empty() {
            return Ok(vec![]);
        }

        let mut all_docs = HashSet::new();
        let mut term_results = HashMap::new();
        let total_terms = tokens.len() as f64;

        // Get matching docs for each term
        for token in tokens {
            let term = token.term;
            if let Some(term_index) = self.search(&term).await? {
                term_results.insert(term.clone(), term_index.clone()); // Store reference
                let keys = term_index.get_docs();
                for doc_id in keys {
                    all_docs.insert(doc_id.clone());
                }
            }
        }

        // Early return if no matches
        if all_docs.is_empty() {
            return Ok(vec![]);
        }

        // Calculate relevance for each doc
        let mut search_results = Vec::new();
        let total_docs = all_docs.len() as f64;

        for doc_id in all_docs {
            let mut relevance_score = 0.0;
            let mut doc_term_matches = HashMap::new();

            for (term, term_index) in &term_results {
                if let Some(positions) = term_index.doc_ids.get(&doc_id) {
                    // TF-IDF scoring (simplified)
                    let term_freq = positions.len() as f64;
                    let doc_freq = term_index.doc_ids.len() as f64;
                    let idf = if doc_freq > 0.0 {
                        (total_docs / doc_freq).ln()
                    } else {
                        0.0
                    };
                    let term_score = (term_freq * idf) / total_terms;

                    relevance_score += term_score;
                    doc_term_matches.insert(term.clone(), positions.clone());
                }
            }

            if !doc_term_matches.is_empty() {
                search_results.push(SearchResult {
                    doc_id,
                    relevance_score,
                    term_matches: doc_term_matches,
                });
            }
        }

        // Sort by relevance score (descending)
        search_results.sort_by(|a, b| {
            b.relevance_score
                .partial_cmp(&a.relevance_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(search_results)
    }
}
