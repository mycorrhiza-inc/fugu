#[cfg(test)]
mod tests {
    use crate::fugu::index::{InvertedIndex, Token, WhitespaceTokenizer};
    use crate::fugu::node::Node;
    use tempfile::tempdir;

    // Helper function to create a test token
    fn test_token(term: &str, doc_id: &str, position: u64) -> Token {
        Token {
            term: term.to_string(),
            doc_id: doc_id.to_string(),
            position,
        }
    }

    // Helper function to create test documents with known search terms
    async fn setup_test_index(index: &InvertedIndex) {
        // Document 1: simple document with "hello world"
        index.add_term(test_token("hello", "doc1", 1)).await.unwrap();
        index.add_term(test_token("world", "doc1", 2)).await.unwrap();
        
        // Document 2: only has "hello" but not "world"
        index.add_term(test_token("hello", "doc2", 1)).await.unwrap();
        index.add_term(test_token("test", "doc2", 2)).await.unwrap();
        
        // Document 3: has both terms but separated
        index.add_term(test_token("hello", "doc3", 1)).await.unwrap();
        index.add_term(test_token("test", "doc3", 2)).await.unwrap();
        index.add_term(test_token("world", "doc3", 3)).await.unwrap();
        
        // Document 4: complex document with multiple instances
        index.add_term(test_token("hello", "doc4", 1)).await.unwrap();
        index.add_term(test_token("hello", "doc4", 3)).await.unwrap();
        index.add_term(test_token("world", "doc4", 2)).await.unwrap();
        index.add_term(test_token("world", "doc4", 4)).await.unwrap();
        index.add_term(test_token("test", "doc4", 5)).await.unwrap();
    }

    #[tokio::test]
    async fn test_individual_term_search() {
        // Create a temporary directory for the index
        let index_dir = tempdir().unwrap();
        let index_path = index_dir.path().join("test_index").to_str().unwrap().to_string();
        
        // Create the inverted index
        let index = InvertedIndex::new(&index_path).await;
        
        // Set up test data
        setup_test_index(&index).await;
        
        // Test search for "hello"
        let result = index.search("hello").await.unwrap();
        assert!(result.is_some());
        let term_index = result.unwrap();
        assert_eq!(term_index.term, "hello");
        assert_eq!(term_index.doc_ids.len(), 4);  // Should be in all 4 docs
        
        // Test search for "world"
        let result = index.search("world").await.unwrap();
        assert!(result.is_some());
        let term_index = result.unwrap();
        assert_eq!(term_index.term, "world");
        assert_eq!(term_index.doc_ids.len(), 3);  // Should be in docs 1, 3, 4
        assert!(term_index.doc_ids.contains_key("doc1"));
        assert!(!term_index.doc_ids.contains_key("doc2"));
        assert!(term_index.doc_ids.contains_key("doc3"));
        assert!(term_index.doc_ids.contains_key("doc4"));
        
        // Test search for "test"
        let result = index.search("test").await.unwrap();
        assert!(result.is_some());
        let term_index = result.unwrap();
        assert_eq!(term_index.term, "test");
        assert_eq!(term_index.doc_ids.len(), 3);  // Should be in docs 2, 3, 4
        assert!(!term_index.doc_ids.contains_key("doc1"));
        assert!(term_index.doc_ids.contains_key("doc2"));
        assert!(term_index.doc_ids.contains_key("doc3"));
        assert!(term_index.doc_ids.contains_key("doc4"));
        
        // Test search for non-existent term
        let result = index.search("nonexistent").await.unwrap();
        assert!(result.is_none());
        
        // Clean up
        index_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_multi_term_search_text() {
        // Create a temporary directory for the index
        let index_dir = tempdir().unwrap();
        let index_path = index_dir.path().join("test_index").to_str().unwrap().to_string();
        
        // Create the inverted index
        let index = InvertedIndex::new(&index_path).await;
        
        // Create a tokenizer
        let tokenizer = WhitespaceTokenizer;
        
        // Create test documents with specific content
        let test_docs = [
            ("doc1", "hello world"),
            ("doc2", "hello test"),
            ("doc3", "hello test world"),
            ("doc4", "hello world hello world test"),
        ];
        
        // Index all test documents
        for (doc_id, content) in &test_docs {
            index.index_document(doc_id, content, &tokenizer).await.unwrap();
        }
        
        // Test search for "hello" (single term)
        let results = index.search_text("hello", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 4, "All docs should match 'hello'");
        
        // Test search for "world" (single term)
        let results = index.search_text("world", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 3, "3 docs should match 'world'");
        
        // Test search for "test" (single term)
        let results = index.search_text("test", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 3, "3 docs should match 'test'");
        
        // Test search for "nonexistent" (single term)
        let results = index.search_text("nonexistent", &tokenizer).await.unwrap();
        assert_eq!(results.len(), 0, "No docs should match 'nonexistent'");
        
        // Clean up
        index_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_node_search_functionality() {
        // Create temporary directory for node
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().to_path_buf();
        
        // Create a timestamp-based namespace
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let namespace = format!("tests/{}/", timestamp);
        
        // Create a node
        let mut node = Node::new(namespace, Some(config_path.clone()));
        
        // Load the index
        node.load_index().await.unwrap();
        
        // Add test documents using the test_content function
        let test_docs = [
            ("doc1.txt", "hello world this is a test document"),
            ("doc2.txt", "another document without hello"),
            ("doc3.txt", "hello and world are separate in this one"),
            ("doc4.txt", "this document has hello world twice - hello world"),
            ("doc5.txt", "completely different terms here"),
        ];
        
        // Create test files and index them
        for (filename, content) in &test_docs {
            let file_path = temp_dir.path().join(filename);
            std::fs::write(&file_path, content).unwrap();
            node.index_file(file_path).await.unwrap();
        }
        
        // Test search for "hello"
        let (results, _duration) = node.search_text("hello", 10, 0).await.unwrap();
        assert_eq!(results.len(), 4);  // Should match docs 1, 2, 3, 4 because doc2 has "hello" in "without hello"
        
        // Extract doc_ids from results for verification, handling namespaced IDs
        let result_docs: Vec<&str> = results.iter()
            .map(|r| r.doc_id.split('/').last().unwrap_or(r.doc_id.as_str()))
            .collect();
        
        assert!(result_docs.contains(&"doc1.txt"));
        assert!(result_docs.contains(&"doc2.txt"));
        assert!(result_docs.contains(&"doc3.txt"));
        assert!(result_docs.contains(&"doc4.txt"));
        
        // Test pagination with limit and offset
        let (results, _duration) = node.search_text("hello", 1, 0).await.unwrap();
        assert_eq!(results.len(), 1);  // Should only return top result
        
        // Clean up
        node.unload_index().await.unwrap();
        temp_dir.close().unwrap();
    }

    #[tokio::test]
    async fn test_bm25_search_ranking() {
        // Create a temporary directory for the index
        let index_dir = tempdir().unwrap();
        let index_path = index_dir.path().join("bm25_test").to_str().unwrap().to_string();
        
        // Create the inverted index
        let index = InvertedIndex::new(&index_path).await;
        
        // Create a tokenizer
        let tokenizer = WhitespaceTokenizer;
        
        // Create test documents with varying term frequencies
        let test_docs = [
            // Simple document with hello or world
            ("doc1", "hello test"),
            ("doc2", "world test"),
            ("doc3", "test document"),
            ("doc4", "hello hello document"),
            ("doc5", "hello world hello world"),
        ];
        
        // Index all test documents
        for (doc_id, content) in &test_docs {
            index.index_document(doc_id, content, &tokenizer).await.unwrap();
        }
        
        // Test search for "hello" (single term)
        let results = index.search_text("hello", &tokenizer).await.unwrap();
        assert!(results.len() >= 3, "Expected at least 3 results for 'hello', got {}", results.len());
        
        // Test search for "test" (single term)
        let results = index.search_text("test", &tokenizer).await.unwrap();
        assert!(results.len() >= 3, "Expected at least 3 results for 'test', got {}", results.len());
        
        // Clean up
        index_dir.close().unwrap();
    }
}