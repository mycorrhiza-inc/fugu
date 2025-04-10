use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time;
use std::sync::Arc;
use std::fs;
use tempfile::tempdir;
use pretty_assertions::assert_eq;

use crate::fugu::grpc::{
    start_grpc_server, NamespaceClient,
};

#[tokio::test]
async fn test_grpc_server_client() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the server
    let temp_dir = tempdir()?;
    let server_path = temp_dir.path().to_path_buf();
    
    // Create a oneshot channel to signal when the server is ready
    let (tx, rx) = oneshot::channel();
    
    // Start the server in a separate task
    let server_addr = Arc::new(std::sync::Mutex::new(String::new()));
    let server_addr_clone = server_addr.clone();
    
    tokio::spawn(async move {
        // Find an available port
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port();
        
        // We need to close the listener before tonic tries to bind to the same port
        drop(listener);
        
        // Use the port we found
        let server_addr_str = format!("127.0.0.1:{}", port);
        
        // Store the actual bound address with http:// prefix for the client
        *server_addr_clone.lock().unwrap() = format!("http://{}", server_addr_str);
        
        // Start the server - give it time for the port to be released
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        if let Err(e) = start_grpc_server(server_path, server_addr_str, Some(tx), None).await {
            eprintln!("Server error: {}", e);
        }
    });
    
    // Wait for the server to start
    rx.await?;
    
    // Wait a moment to ensure the server is fully ready
    time::sleep(Duration::from_millis(500)).await;
    
    // Create a test file to index
    let test_file_path = temp_dir.path().join("test_file.txt");
    let test_content = b"This is a test file for indexing.";
    fs::write(&test_file_path, test_content)?;
    
    // Get the bound server address
    let server_addr = server_addr.lock().unwrap().clone();
    println!("Server started at {}", server_addr);
    
    // Create a client
    let mut client = NamespaceClient::connect(server_addr).await?;
    
    // Test the index operation
    let file_name = "test_file.txt".to_string();
    let index_response = client.index(file_name.clone(), test_content.to_vec()).await?;
    assert_eq!(index_response.success, true);
    assert_eq!(index_response.location, format!("/{}", file_name));
    
    // Test the search operation
    let search_response = client.search("test".to_string(), 10, 0).await?;
    assert_eq!(search_response.total, 1);
    assert!(!search_response.results.is_empty());
    
    // Test vector search operation
    let test_vector = vec![0.1, 0.2, 0.3, 0.4, 0.5];
    let vector_search_response = client.vector_search(test_vector.clone(), 5, 10, 0, 0.5).await?;
    assert_eq!(vector_search_response.total, 1);
    assert!(!vector_search_response.results.is_empty());
    
    // Test the delete operation
    let delete_response = client.delete(format!("/{}", file_name)).await?;
    assert_eq!(delete_response.success, true);
    
    // Clean up
    temp_dir.close()?;
    
    Ok(())
}

#[tokio::test]
async fn test_client_connection_error() {
    // Try to connect to a non-existent server
    let result = NamespaceClient::connect("http://127.0.0.1:1".to_string()).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_server_startup_error() {
    // Try to start a server on a port that's already in use
    let temp_dir = tempdir().unwrap();
    let server_path = temp_dir.path().to_path_buf();
    
    // Start a TCP listener to occupy a port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_addr = format!("{}:{}", addr.ip(), addr.port());
    
    // Try to start the server on the same port
    let result = start_grpc_server(server_path, server_addr, None, None).await;
    assert!(result.is_err());
    
    temp_dir.close().unwrap();
}