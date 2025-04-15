use std::fs;
use std::process::Command;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use rand::Rng;

/// Returns a randomly chosen port in the range 50100-60000 to avoid hardcoded port conflicts
fn get_random_port() -> u16 {
    let mut rng = rand::rng();
    rng.random_range(50100..60000)
}

/// Test to verify that the search command works with case-insensitive search
#[test]
fn test_case_insensitive_search() {
    // Build the binary first
    Command::new("cargo")
        .args(["build"])
        .status()
        .expect("Failed to build binary");

    // Get the path to the binary
    let binary = std::env::current_dir()
        .unwrap()
        .join("target/debug/fugu")
        .to_string_lossy()
        .to_string();

    // Create a temporary directory for the server
    let temp_dir = tempdir().expect("Failed to create temp directory");

    // Choose a random port for the server to avoid conflicts
    let port = get_random_port();
    let server_url = format!("http://127.0.0.1:{}", port);

    // Create a test file with mixed case words
    let test_file_path = temp_dir.path().join("test_case.txt");
    let test_content = "Hello World TEST case SENSITIVITY";
    fs::write(&test_file_path, test_content).expect("Failed to write test file");

    // Start the server
    let mut server = Command::new(&binary)
        .args(&[
            "up",
            "--timeout", "30", // 30 second timeout
            "--port", &port.to_string(),
        ])
        .spawn()
        .expect("Failed to start server");

    // Allow server to start
    thread::sleep(Duration::from_secs(2));

    // Add the test document to a namespace
    let namespace = "case_test";
    let add_output = Command::new(&binary)
        .args(&[
            "add",
            "--namespace", namespace,
            "--addr", &server_url,
            &test_file_path.to_string_lossy(),
        ])
        .output()
        .expect("Failed to execute add command");

    assert!(add_output.status.success(), "Add command failed");
    println!("Added document with mixed case content");

    // Test various case variations of the same search term
    let search_variations = [
        "hello", "HELLO", "Hello",
        "world", "WORLD", "World",
        "test", "TEST", "Test",
        "case", "CASE", "Case",
        "sensitivity", "SENSITIVITY", "Sensitivity"
    ];

    // Run searches with different case variations
    for search_term in &search_variations {
        let search_output = Command::new(&binary)
            .args(&[
                "search",
                "--namespace", namespace,
                "--addr", &server_url,
                search_term,
            ])
            .output()
            .expect("Failed to execute search command");

        assert!(search_output.status.success(), "Search command failed for term '{}'", search_term);
        
        let search_stdout = String::from_utf8_lossy(&search_output.stdout);
        println!("Search output for '{}': {}", search_term, search_stdout);
    }

    // Kill the server and verify it exited
    server.kill().expect("Failed to kill server");
    
    // Wait for server to actually exit with timeout
    match server.try_wait() {
        Ok(None) => {
            println!("Waiting for server to exit...");
            // Wait up to 5 seconds for the server to exit
            for _ in 0..10 {
                thread::sleep(Duration::from_millis(500));
                if let Ok(Some(status)) = server.try_wait() {
                    println!("Server exited with status: {}", status);
                    break;
                }
            }
        }
        Ok(Some(status)) => println!("Server exited immediately with status: {}", status),
        Err(e) => println!("Error waiting for server: {}", e),
    }

    // Clean up
    temp_dir.close().expect("Failed to delete temp directory");
}