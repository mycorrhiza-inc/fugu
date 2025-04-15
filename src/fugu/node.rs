/// Node module provides namespace-specific indexing and search functionality
///
/// A Node represents a single namespace in the Fugu search engine and:
/// - Manages an inverted index for fast text search
/// - Handles write-ahead logging for data durability
/// - Provides persistence of indexes to disk
/// - Supports concurrent operations
use crate::fugu::index::{InvertedIndex, Token, WhitespaceTokenizer};
use crate::fugu::wal::{WALCMD, WALOP};
use crate::fugu::config::{ConfigManager, new_config_manager};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::io::SeekFrom;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::io::{AsyncSeekExt, AsyncReadExt};
use tracing::{trace, info, warn, error, debug};

/// Job types that a Node can process
/// 
/// Currently a placeholder for future job processing functionality
#[derive(Clone, Debug)]
pub enum NodeJob {}

/// A Node represents a single namespace in the Fugu search engine
///
/// Each Node:
/// - Has its own namespace identifier
/// - Manages a dedicated inverted index
/// - Communicates with the WAL for durability
/// - Can be loaded/unloaded independently
#[derive(Clone, Debug)]
pub struct Node {
    /// Unique namespace identifier for this node
    namespace: String,
    /// Configuration manager for file paths
    config: ConfigManager,
    /// How often to check for jobs (in milliseconds)
    frequency: u16,
    /// Channel for sending WAL commands
    wal_chan: tokio::sync::mpsc::Sender<WALCMD>,
    /// Flag to signal node shutdown
    // Channel endpoints can't be cloned, so we use a bool to track shutdown state instead
    shutdown: bool,
    /// Queue of pending jobs
    job_queue: Vec<NodeJob>,
    /// Optional inverted index (loaded on demand)
    inverted_index: Option<InvertedIndex>,
}

impl Node {
    /// Creates a new Node for the specified namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - Unique namespace identifier
    /// * `config_path` - Optional path for configuration and index storage
    /// * `wal_chan` - Channel for sending WAL commands
    ///
    /// # Returns
    ///
    /// A new Node instance
    pub fn new(namespace: String, config_path: Option<PathBuf>, wal_chan: mpsc::Sender<WALCMD>) -> Self {
        // Create config manager with optional custom path
        let config = new_config_manager(config_path);
        
        // Ensure namespace directory exists
        let _ = config.ensure_namespace_dir(&namespace);
        
        Node {
            namespace,
            config,
            wal_chan,
            frequency: 1000, //check every second
            shutdown: false,
            job_queue: vec![],
            inverted_index: None,
        }
    }
    
    /// Returns the path where this node's index is stored
    ///
    /// # Returns
    ///
    /// Path to the index directory
    fn get_index_path(&self) -> PathBuf {
        self.config.namespace_index_path(&self.namespace)
    }
    
    /// Returns the index path as a string
    ///
    /// # Returns
    ///
    /// String representation of the index path
    fn get_index_path_str(&self) -> String {
        self.get_index_path().to_str().unwrap_or_default().to_string()
    }
    
    /// Initializes the inverted index for this node
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    async fn init_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let index_path_str = self.get_index_path_str();
        let index = InvertedIndex::new(&index_path_str, self.wal_chan.clone()).await;
        self.inverted_index = Some(index);
        Ok(())
    }

    /// Adds a term to the inverted index
    ///
    /// # Arguments
    ///
    /// * `term_token` - Token to add to the index
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    async fn index_term(&self, term_token: Token) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(index) = &self.inverted_index {
            index.add_term(term_token).await?;
        }
        Ok(())
    }
    
    // fn new_file(&self) {}
    
    /// Indexes a file using the BM25 scoring algorithm
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to index
    ///
    /// # Returns
    ///
    /// Result indicating success or error with indexing time
    pub async fn index_file(&self, path: PathBuf) -> Result<Duration, Box<dyn std::error::Error>> {
        // Start timing
        let start_time = Instant::now();
        
        // Get the document ID from the filename
        let doc_id = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("Invalid file path")?
            .to_string();
        
        // Index the content if the index is loaded
        if let Some(index) = &self.inverted_index {
            // Open the file for streaming
            let file = tokio::fs::File::open(&path).await?;
            
            // Create a buffered reader with a 8KB buffer size
            // This allows us to incrementally read large files without loading them entirely in memory
            let mut reader = tokio::io::BufReader::with_capacity(8192, file);
            
            // Create a tokenizer
            let tokenizer = WhitespaceTokenizer;
            
            // Buffer to accumulate file content as we read it
            let mut content = String::new();
            
            // Stream the file content
            // For extremely large files, let's process chunks instead of loading everything in memory
            let file_size = path.metadata()?.len();
            const MAX_MEMORY_SIZE: u64 = 100 * 1024 * 1024; // 100 MB threshold
            
            if file_size > MAX_MEMORY_SIZE {
                // Process large file in chunks
                info!("Processing large file in chunks for indexing (size: {} bytes)", file_size);
                
                let mut buffer = [0; 8192]; // 8KB buffer
                let mut chunk = String::new();
                const CHUNK_SIZE: usize = 10 * 1024 * 1024; // 10MB chunks for processing
                let mut processed_any = false;
                
                loop {
                    match reader.read(&mut buffer).await {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            // Append bytes to content, handling potential non-UTF8 content
                            match std::str::from_utf8(&buffer[..n]) {
                                Ok(s) => {
                                    chunk.push_str(s);
                                    processed_any = true;
                                },
                                Err(_) => {
                                    // Try to decode the bytes using a more lenient approach
                                    let lossy_str = String::from_utf8_lossy(&buffer[..n]);
                                    chunk.push_str(&lossy_str);
                                    processed_any = true;
                                    warn!("Using lossy UTF-8 conversion for non-UTF8 data at position {}", chunk.len());
                                }
                            }
                            
                            // Process chunk if it's large enough
                            if chunk.len() >= CHUNK_SIZE {
                                // Index this chunk
                                index.index_document(&doc_id, &chunk, &tokenizer).await?;
                                
                                // Clear chunk but keep some overlap for term continuity
                                let overlap = chunk.split_whitespace().take(10).collect::<Vec<_>>().join(" ");
                                chunk.clear();
                                chunk.push_str(&overlap);
                            }
                        }
                        Err(e) => return Err(Box::new(e)),
                    }
                }
                
                // Index final chunk if not empty
                if !chunk.is_empty() {
                    index.index_document(&doc_id, &chunk, &tokenizer).await?;
                }
                
                // If we weren't able to process any data (e.g., completely binary file)
                // create a minimal document record so it's at least searchable by filename
                if !processed_any {
                    warn!("File appears to be binary or non-text. Creating minimal document record.");
                    let minimal_text = format!("Binary file {}", doc_id);
                    index.index_document(&doc_id, &minimal_text, &tokenizer).await?;
                }
            } else {
                // For smaller files, try to read the entire content at once
                match reader.read_to_string(&mut content).await {
                    Ok(_) => {
                        // Successfully read as string, index it
                        index.index_document(&doc_id, &content, &tokenizer).await?;
                    },
                    Err(_) => {
                        // If it fails (likely binary content), use a different approach
                        warn!("Couldn't read file as text, using lossy UTF-8 conversion");
                        
                        // Reset the file position to the beginning
                        reader.seek(SeekFrom::Start(0)).await?;
                        
                        // Use a buffer and lossy conversion for binary content
                        let mut buffer = Vec::with_capacity(file_size as usize);
                        reader.read_to_end(&mut buffer).await?;
                        
                        // Convert bytes to string with lossy conversion
                        let lossy_content = String::from_utf8_lossy(&buffer).to_string();
                        
                        if !lossy_content.trim().is_empty() {
                            // If we got some usable text, index it
                            index.index_document(&doc_id, &lossy_content, &tokenizer).await?;
                        } else {
                            // If the file is truly binary with no usable text, create a minimal record
                            warn!("File appears to be binary. Creating minimal document record.");
                            let minimal_text = format!("Binary file {}", doc_id);
                            index.index_document(&doc_id, &minimal_text, &tokenizer).await?;
                        }
                    }
                }
            }
            
            // Get the elapsed time
            let elapsed = start_time.elapsed();
            info!(path=%path.display(), elapsed=?elapsed, "Indexed file");
            
            Ok(elapsed)
        } else {
            Err("Index not loaded".into())
        }
    }
    
    /// Logs a WAL operation
    ///
    /// # Arguments
    ///
    /// * `msg` - WAL operation to log
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn walog(&self, msg: WALOP) -> Result<(), mpsc::error::SendError<WALCMD>> {
        let msg_clone = msg.clone();
        match self.wal_chan.send(msg.into()).await {
            Ok(_) => {
                debug!("{:?}", msg_clone);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    
    /// Deletes a file from the index
    ///
    /// # Arguments
    ///
    /// * `doc_id` - ID of the document to delete
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn delete_file(&self, doc_id: &str) -> Result<Duration, Box<dyn std::error::Error>> {
        if let Some(index) = &self.inverted_index {
            // Use the new delete_document method from InvertedIndex
            let elapsed = index.delete_document(doc_id).await?;
            Ok(elapsed)
        } else {
            Err("Index not loaded".into())
        }
    }
    
    /// Searches the index for the given query
    ///
    /// # Arguments
    ///
    /// * `query` - The search query string
    /// * `limit` - Maximum number of results to return
    /// * `offset` - Starting position of results
    ///
    /// # Returns
    ///
    /// Result containing search results and performance metrics
    pub async fn search_text(&self, query: &str, limit: usize, offset: usize) 
        -> Result<(Vec<crate::fugu::index::SearchResult>, Duration), Box<dyn std::error::Error>> {
        
        // Start timing
        let start_time = Instant::now();
        
        if let Some(index) = &self.inverted_index {
            // Create a tokenizer
            let tokenizer = WhitespaceTokenizer;
            
            // Perform the search
            let mut results = index.search_text(query, &tokenizer).await?;
            
            // Apply pagination
            if offset < results.len() {
                results = results.into_iter()
                    .skip(offset)
                    .take(limit)
                    .collect();
            } else {
                results = Vec::new();
            }
            
            // Get the elapsed time
            let elapsed = start_time.elapsed();
            
            // Return both the results and the timing
            Ok((results, elapsed))
        } else {
            Err("Index not loaded".into())
        }
    }
    
    /// Loads the index from the configured path
    ///
    /// This method:
    /// - Ensures the index directory exists
    /// - Creates a new inverted index or loads an existing one
    /// - Makes the index available for operations
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn load_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let index_path = self.get_index_path();
        let index_path_str = self.get_index_path_str();
        
        // Ensure parent directories exist
        if let Some(parent) = index_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }
        
        let index = InvertedIndex::new(
            &index_path_str,
            self.wal_chan.clone()
        ).await;
        
        self.inverted_index = Some(index);
        info!(path=%index_path.display(), "Loaded index");
        Ok(())
    }
    
    /// Unloads the index from memory, ensuring all data is flushed to disk
    ///
    /// This method:
    /// - Flushes any pending changes to disk
    /// - Releases memory resources
    /// - Ensures data durability
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn unload_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(index) = self.inverted_index.take() {
            // Flush any pending changes to disk
            index.flush().await?;
            info!(path=%self.get_index_path().display(), "Unloaded index");
        }
        Ok(())
    }
    
    /// Checks if the index is loaded
    ///
    /// # Returns
    ///
    /// Boolean indicating if the index is loaded
    pub fn has_index(&self) -> bool {
        self.inverted_index.is_some()
    }
    
    /// Provides access to the inverted index if loaded
    ///
    /// # Returns
    ///
    /// Option containing a reference to the inverted index if loaded
    pub fn get_index(&self) -> Option<&InvertedIndex> {
        self.inverted_index.as_ref()
    }
}

/// Creates a new Node for the specified namespace
///
/// This is a convenience function that calls `Node::new()`
///
/// # Arguments
///
/// * `namespace` - Unique namespace identifier
/// * `config_path` - Optional path for configuration and index storage
/// * `wal_chan` - Channel for sending WAL commands
///
/// # Returns
///
/// A new Node instance
pub fn new(namespace: String, config_path: Option<PathBuf>, wal_chan: mpsc::Sender<WALCMD>) -> Node {
    Node::new(namespace, config_path, wal_chan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fugu::index::{InvertedIndex, Token, WhitespaceTokenizer};
    use std::fs;
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio::time::sleep;
    
    // Helper function to create a test token
    fn create_token(term: &str, doc_id: &str, position: u64) -> Token {
        Token {
            term: term.to_string(),
            doc_id: doc_id.to_string(),
            position,
        }
    }
    
    // Helper function to setup test environment with a Node
    async fn setup_test_node() -> (Node, tempfile::TempDir) {
        // Create temporary directory
        let temp_dir = tempdir().unwrap();
        
        // Create a WAL channel
        let (tx, _rx) = mpsc::channel::<WALCMD>(100);
        
        // Create a node with the temp dir as config path
        let node = Node::new(
            "test_namespace".to_string(),
            Some(temp_dir.path().to_path_buf()),
            tx
        );
        
        (node, temp_dir)
    }
    
    #[tokio::test]
    async fn test_node_load_unload_index() {
        // Setup test environment
        let (mut node, temp_dir) = setup_test_node().await;
        
        // Ensure index directory doesn't exist yet
        let index_path = node.get_index_path();
        assert!(!index_path.exists());
        
        // Load the index (this should create it)
        node.load_index().await.unwrap();
        
        // Verify index directory exists
        assert!(index_path.exists());
        assert!(node.inverted_index.is_some());
        
        // Unload the index
        node.unload_index().await.unwrap();
        
        // Verify index is unloaded but directory still exists
        assert!(node.inverted_index.is_none());
        assert!(index_path.exists());
        
        // Clean up
        temp_dir.close().unwrap();
    }
    
    #[tokio::test]
    async fn test_index_persistence() {
        // Setup test environment
        let (mut node1, temp_dir) = setup_test_node().await;
        
        // Load the index - this should create the directory
        node1.load_index().await.unwrap();
        
        // Check that the index directory exists
        let index_path = node1.get_index_path();
        assert!(index_path.exists(), "Index directory should exist after loading");
        
        // Manually write a file to the index directory to test persistence
        let test_file = index_path.join("test_persistence.txt");
        std::fs::write(&test_file, "Test content for persistence").unwrap();
        
        // Unload the index
        node1.unload_index().await.unwrap();
        
        // Create a new node with the same config path
        let (tx, _rx) = mpsc::channel::<WALCMD>(100);
        let mut node2 = Node::new(
            "test_namespace".to_string(),
            Some(temp_dir.path().to_path_buf()),
            tx
        );
        
        // Load the index
        node2.load_index().await.unwrap();
        
        // Verify the directory and test file still exist
        let index_path2 = node2.get_index_path();
        assert!(index_path2.exists(), "Index directory should persist after reload");
        
        let test_file2 = index_path2.join("test_persistence.txt");
        assert!(test_file2.exists(), "Test file should persist after reload");
        
        let content = std::fs::read_to_string(&test_file2).unwrap();
        assert_eq!(content, "Test content for persistence", "File content should be preserved");
        
        // Clean up
        temp_dir.close().unwrap();
    }
    
    #[tokio::test]
    async fn test_multiple_node_load_unload() {
        // Setup test environment with multiple nodes
        let temp_dir = tempdir().unwrap();
        let (tx, _rx) = mpsc::channel::<WALCMD>(100);
        
        // Create multiple nodes with different namespaces but same config path
        let mut node1 = Node::new(
            "namespace1".to_string(),
            Some(temp_dir.path().to_path_buf()),
            tx.clone()
        );
        
        let mut node2 = Node::new(
            "namespace2".to_string(),
            Some(temp_dir.path().to_path_buf()),
            tx.clone()
        );
        
        let mut node3 = Node::new(
            "namespace3".to_string(),
            Some(temp_dir.path().to_path_buf()),
            tx.clone()
        );
        
        // Load all indices
        node1.load_index().await.unwrap();
        node2.load_index().await.unwrap();
        node3.load_index().await.unwrap();
        
        // Check that each node has its own index path
        let path1 = node1.get_index_path();
        let path2 = node2.get_index_path();
        let path3 = node3.get_index_path();
        
        assert!(path1.exists());
        assert!(path2.exists());
        assert!(path3.exists());
        assert_ne!(path1, path2);
        assert_ne!(path2, path3);
        assert_ne!(path1, path3);
        
        // Add some data to each index
        node1.index_term(create_token("apple", "doc1", 0)).await.unwrap();
        node2.index_term(create_token("banana", "doc1", 0)).await.unwrap();
        node3.index_term(create_token("cherry", "doc1", 0)).await.unwrap();
        
        // Unload all indices
        node1.unload_index().await.unwrap();
        node2.unload_index().await.unwrap();
        node3.unload_index().await.unwrap();
        
        // Reload node1 and check its data
        node1.load_index().await.unwrap();
        if let Some(index) = &node1.inverted_index {
            let result = index.search("apple").await.unwrap();
            assert!(result.is_some());
            
            // Verify it doesn't have data from other nodes
            let result = index.search("banana").await.unwrap();
            assert!(result.is_none());
            let result = index.search("cherry").await.unwrap();
            assert!(result.is_none());
        } else {
            panic!("Index was not loaded correctly");
        }
        
        // Clean up
        temp_dir.close().unwrap();
    }
    
    #[tokio::test]
    async fn test_concurrent_operations() {
        // Setup test environment
        let (mut node, temp_dir) = setup_test_node().await;
        
        // Load the index
        node.load_index().await.unwrap();
        
        // Create multiple files concurrently in the index directory
        let index_path = node.get_index_path();
        
        // Clone the path for concurrent operations
        let path1 = index_path.clone();
        let path2 = index_path.clone();
        let path3 = index_path.clone();
        
        // Spawn multiple tasks that create files concurrently
        let task1 = tokio::spawn(async move {
            for i in 0..10 {
                let file_path = path1.join(format!("task1_file_{}.txt", i));
                let content = format!("Content from task1 file {}", i);
                std::fs::write(file_path, content).unwrap();
                sleep(Duration::from_millis(1)).await;
            }
        });
        
        let task2 = tokio::spawn(async move {
            for i in 0..10 {
                let file_path = path2.join(format!("task2_file_{}.txt", i));
                let content = format!("Content from task2 file {}", i);
                std::fs::write(file_path, content).unwrap();
                sleep(Duration::from_millis(1)).await;
            }
        });
        
        let task3 = tokio::spawn(async move {
            for i in 0..10 {
                let file_path = path3.join(format!("task3_file_{}.txt", i));
                let content = format!("Content from task3 file {}", i);
                std::fs::write(file_path, content).unwrap();
                sleep(Duration::from_millis(1)).await;
            }
        });
        
        // Wait for all tasks to complete
        task1.await.unwrap();
        task2.await.unwrap();
        task3.await.unwrap();
        
        // Unload the index
        node.unload_index().await.unwrap();
        
        // Reload the index
        node.load_index().await.unwrap();
        
        // Verify files were created
        let mut file_count = 0;
        for entry in std::fs::read_dir(&index_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "txt") {
                file_count += 1;
                let filename = path.file_name().unwrap().to_string_lossy();
                debug!(filename=%filename, "Found file");
            }
        }
        
        // Ensure we found the expected files (30 total)
        assert_eq!(file_count, 30, "Should have created 30 files concurrently");
        
        // Clean up
        temp_dir.close().unwrap();
    }
}
