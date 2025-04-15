use fugu::fugu::node;
use std::path::PathBuf;
use tempfile::TempDir;
// Removed unused import
// No need for tokio::sync::mpsc

#[tokio::test]
async fn test_document_disk_cache() {
    // Set up a temporary directory for the test
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();
    
    // Create a node
    let mut test_node = node::new("test_cache".to_string(), Some(path.clone()));
    
    // Load the index
    test_node.load_index().await.unwrap();
    
    // Create a test text file
    let text_file_path = path.join("text_file.txt");
    let text_content = "This is a test text file for caching.\nIt should be cached.";
    tokio::fs::write(&text_file_path, text_content).await.unwrap();
    
    // Create a test binary file (simulated with bytes)
    let binary_file_path = path.join("binary_file.bin");
    let binary_content = vec![0u8, 1u8, 2u8, 3u8, 255u8, 254u8, 253u8, 252u8];
    tokio::fs::write(&binary_file_path, &binary_content).await.unwrap();
    
    // Create a large text file (> threshold)
    let large_file_path = path.join("large_file.txt");
    let large_content = "A".repeat(3 * 1024 * 1024); // 3MB
    tokio::fs::write(&large_file_path, &large_content).await.unwrap();
    
    // Index the files
    test_node.index_file(text_file_path.clone()).await.unwrap();
    test_node.index_file(binary_file_path.clone()).await.unwrap();
    test_node.index_file(large_file_path.clone()).await.unwrap();
    
    // Get the index instance
    let index = test_node.get_index();
    
    if let Some(idx) = index {
        // Test retrieving a document via get_document
        let doc_content = idx.get_document("text_file.txt").await.unwrap();
        assert!(doc_content.is_some(), "Should retrieve the document content");
        assert_eq!(doc_content.unwrap(), text_content, "Retrieved content should match original");
        
        // Test retrieval of binary and large files
        let binary_content_retrieved = idx.get_document("binary_file.bin").await.unwrap();
        assert!(binary_content_retrieved.is_some(), "Should retrieve binary content");
        
        let large_content_retrieved = idx.get_document("large_file.txt").await.unwrap();
        assert!(large_content_retrieved.is_some(), "Should retrieve large file content");
        
        // Test deleting a document
        test_node.delete_file("text_file.txt").await.unwrap();
        
        // After deletion, the document should no longer be retrievable
        let deleted_content = idx.get_document("text_file.txt").await.unwrap();
        assert!(deleted_content.is_none(), "Deleted document should not be retrievable");
    } else {
        panic!("Failed to get index instance");
    }
    
    // IMPORTANT: Unload index to release locks after test completes
    test_node.unload_index().await.unwrap();
}