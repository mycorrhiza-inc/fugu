use std::fs;
use std::path::Path;
use tempfile::tempdir;
use tokio::process::Command;
use tokio::time::{sleep, Duration};

// This is an integration test script that tests the gRPC client and server
// by running actual binaries rather than just testing the library code

#[tokio::test]
async fn test_grpc_client_server_integration() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the server data
    let temp_dir = tempdir()?;
    let _server_dir = temp_dir.path().to_string_lossy().to_string();

    // Pick a random port
    let port = 50051; // You can make this random if needed
    let server_addr = format!("127.0.0.1:{}", port);
    let server_url = format!("http://{}", server_addr);

    // Create a test file
    let test_file_path = temp_dir.path().join("test_document.txt");
    let test_content = "This is a test document for the Fugu search engine.";
    fs::write(&test_file_path, test_content)?;

    // Start the server process in the background
    println!("Starting server on {}", server_addr);
    // Get the path to the binary - first try environment variable, then use project root path
    let binary_path = std::env::var("CARGO_BIN_EXE_fugu").unwrap_or_else(|_| {
        let current_dir = std::env::current_dir().unwrap();
        // Navigate to project root (from tests directory if needed)
        let project_root = if current_dir.ends_with("tests") {
            current_dir.parent().unwrap().to_path_buf()
        } else {
            current_dir
        };
        project_root
            .join("target/debug/fugu")
            .to_string_lossy()
            .to_string()
    });
    println!("Using binary at: {}", binary_path);
    // Create a data directory for the server
    let data_dir = temp_dir.path().join("data");
    fs::create_dir_all(&data_dir)?;
    
    // Use the server's default directory location
    let mut server_process = Command::new(&binary_path)
        .args(["up", "--port", &port.to_string()])
        .current_dir(&temp_dir.path()) // Set working directory to temp dir
        .spawn()?;

    // Give the server time to start
    sleep(Duration::from_secs(2)).await;

    // Index the test file
    let index_status = Command::new(&binary_path)
        .args([
            "index",
            "--file",
            test_file_path.to_string_lossy().as_ref(),
            "--addr",
            &server_url,
        ])
        .status()
        .await?;

    assert!(index_status.success(), "Failed to index test file");

    // Search for a term in the indexed document
    let search_output = Command::new(&binary_path)
        .args([
            "search",
            "--namespace",
            "default",
            "--limit",
            "10",
            "--addr",
            &server_url,
            "test", // Query as the last argument
        ])
        .output()
        .await?;

    assert!(search_output.status.success(), "Search failed");

    let search_stdout = String::from_utf8(search_output.stdout)?;
    println!("Search output: {}", search_stdout);

    // Verify search results contain expected information
    assert!(
        search_stdout.contains("Search response"),
        "Search response not found in output"
    );

    // Delete the document
    let delete_status = Command::new(&binary_path)
        .args([
            "delete",
            "--location",
            &format!(
                "/{}",
                Path::new(&test_file_path)
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
            ),
            "--addr",
            &server_url,
        ])
        .status()
        .await?;

    assert!(delete_status.success(), "Failed to delete document");

    // Clean up
    server_process.kill().await?;
    temp_dir.close()?;

    Ok(())
}
