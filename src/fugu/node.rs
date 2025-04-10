use crate::fugu::index::{InvertedIndex, Token};
use crate::fugu::wal::{WALCMD, WALOP};
use serde_json::json;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

#[derive(Clone, Debug)]
pub enum NodeJob {}

#[derive(Clone, Debug)]
pub struct Node {
    namespace: String,
    config_path: PathBuf,
    frequency: u16, // how often to check
    wal_chan: tokio::sync::mpsc::Sender<WALCMD>,
    // Channel endpoints can't be cloned, so we use a bool to track shutdown state instead
    shutdown: bool,
    job_queue: Vec<NodeJob>,
    inverted_index: Option<InvertedIndex>,
}

impl Node {
    pub fn new(namespace: String, config_path: Option<PathBuf>, wal_chan: mpsc::Sender<WALCMD>) -> Self {
        // Set default config path if not provided
        let config_path = config_path.unwrap_or_else(|| {
            let home_dir = std::env::var("HOME").unwrap_or_else(|_| String::from("/tmp"));
            PathBuf::from(home_dir).join(".fugu")
        });
        
        Node {
            namespace,
            config_path,
            wal_chan,
            frequency: 1000, //check every second
            shutdown: false,
            job_queue: vec![],
            inverted_index: None,
        }
    }
    
    fn get_index_path(&self) -> PathBuf {
        self.config_path.join(format!("{}_index", self.namespace))
    }
    
    fn get_index_path_str(&self) -> String {
        self.get_index_path().to_str().unwrap_or_default().to_string()
    }
    
    async fn init_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let index_path_str = self.get_index_path_str();
        let index = InvertedIndex::new(&index_path_str, self.wal_chan.clone()).await;
        self.inverted_index = Some(index);
        Ok(())
    }

    async fn index_term(&self, term_token: Token) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(index) = &self.inverted_index {
            index.add_term(term_token).await?;
        }
        Ok(())
    }
    
    // fn new_file(&self) {}
    async fn index_file(&self, _path: PathBuf) {
        let _ = json!({});
    }
    
    pub async fn walog(&self, msg: WALOP) -> Result<(), mpsc::error::SendError<WALCMD>> {
        let msg_clone = msg.clone();
        match self.wal_chan.send(msg.into()).await {
            Ok(_) => {
                println!("{:?}", msg_clone);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    
    fn delete_file(&self) {}
    
    /// Loads the index from the configured path
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
        println!("Loaded index from {}", index_path.display());
        Ok(())
    }
    
    /// Unloads the index from memory, ensuring all data is flushed to disk
    pub async fn unload_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(index) = self.inverted_index.take() {
            // Flush any pending changes to disk
            index.flush().await?;
            println!("Unloaded index from {}", self.get_index_path().display());
        }
        Ok(())
    }
}

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
                println!("Found file: {}", filename);
            }
        }
        
        // Ensure we found the expected files (30 total)
        assert_eq!(file_count, 30, "Should have created 30 files concurrently");
        
        // Clean up
        temp_dir.close().unwrap();
    }
}
