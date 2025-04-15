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
        // Check cache status
        let (cache_path, file_count, _) = idx.get_cache_info().await.unwrap();
        
        // The text file should be in the cache, but not the binary or large files
        assert!(file_count >= 1, "At least one file should be in the cache");
        
        // Check if the text file is in cache
        let text_cache_path = PathBuf::from(&cache_path).join("text_file.txt");
        assert!(text_cache_path.exists(), "Text file should be in the disk cache");
        
        // Check text file contents
        let cached_content = tokio::fs::read_to_string(&text_cache_path).await.unwrap();
        assert_eq!(cached_content, text_content, "Cached content should match original");
        
        // The binary file should not be in cache
        let binary_cache_path = PathBuf::from(&cache_path).join("binary_file.bin");
        assert!(!binary_cache_path.exists(), "Binary file should not be in the disk cache");
        
        // The large file should not be in cache
        let large_cache_path = PathBuf::from(&cache_path).join("large_file.txt");
        assert!(!large_cache_path.exists(), "Large file should not be in the disk cache");
        
        // Test retrieving a document via get_document
        let doc_content = idx.get_document("text_file.txt").await.unwrap();
        assert!(doc_content.is_some(), "Should retrieve the document content");
        assert_eq!(doc_content.unwrap(), text_content, "Retrieved content should match original");
        
        // Test deleting a document
        test_node.delete_file("text_file.txt").await.unwrap();
        
        // After deletion, the file should not be in the cache
        assert!(!text_cache_path.exists(), "Text file should be removed from cache after deletion");
    } else {
        panic!("Failed to get index instance");
    }
}