use crate::fugu::wal::{WALCMD, WALOP};
use rkyv;
use rkyv::{rancor::Error, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use sled;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

#[derive(Debug, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize, Clone)]
// #[rkyv(compare(PartialEq), derive(Debug))]
pub struct TermIndex {
    pub term: String,
    pub doc_ids: HashMap<String, Vec<u64>>, // doc_id -> all positions
    pub term_frequency: u32,
}

impl TermIndex {
    fn get_docs(&self) -> Vec<String> {
        let d = self.doc_ids.keys();
        let mut o = vec![];
        for i in d {
            o.push(i.clone());
        }
        o
    }
    pub fn set_frequency(&mut self, n: u32) {
        self.term_frequency = n;
    }
}

impl std::fmt::Display for TermIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let t = self.term.as_str();
        let n = self.term_frequency;
        let text = format!("{t} found in  {n} documents");
        write!(f, "{}", text)
    }
}
pub struct Token {
    pub term: String,
    pub doc_id: String,
    pub position: u64,
}

struct DocEntry {
    id: String,
    position: u64,
}

pub trait Tokenizer {
    fn tokenize(&self, text: &str, doc_id: &str) -> Vec<Token>;
}

pub struct WhitespaceTokenizer;

impl Tokenizer for WhitespaceTokenizer {
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

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub doc_id: String,
    pub relevance_score: f64,
    pub term_matches: HashMap<String, Vec<u64>>,
}

pub struct InvertedIndex {
    db: Arc<RwLock<sled::Db>>,
    wal_chan: mpsc::Sender<WALCMD>,
}

impl InvertedIndex {
    pub async fn new(path: &str, wal_chan: mpsc::Sender<WALCMD>) -> Self {
        let db = sled::open(path).expect("Failed to open sled database");
        InvertedIndex {
            db: Arc::new(RwLock::new(db)),
            wal_chan,
        }
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
