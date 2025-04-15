use std::env;
use std::fs;
use std::path::PathBuf;
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

/// Test to verify that the search command works correctly with the server
#[test]
fn test_search_command() {
    // Build the binary first
    Command::new("cargo")
        .args(["build"])
        .status()
        .expect("Failed to build binary");

    // Get the path to the binary
    let binary = env::var("CARGO_BIN_EXE_fugu").unwrap_or_else(|_| {
        env::current_dir()
            .unwrap()
            .join("target/debug/fugu")
            .to_string_lossy()
            .to_string()
    });

    // Create a temporary directory for the server
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let _server_dir = temp_dir.path().to_string_lossy().to_string();

    // Choose a random port for the server to avoid conflicts
    let port = get_random_port();
    let server_addr = format!("127.0.0.1:{}", port);
    let server_url = format!("http://{}", server_addr);

    // Create test documents with known content
    let test_docs = [
        ("doc1.txt", "Hello world from document one"),
        ("doc2.txt", "Another document without the search term"),
        ("doc3.txt", "Hello again from the third document with world keyword"),
    ];
    
    let file_paths: Vec<PathBuf> = test_docs
        .iter()
        .map(|(name, content)| {
            let path = temp_dir.path().join(name);
            fs::write(&path, content).expect("Failed to write test file");
            path
        })
        .collect();

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

    // Test namespace
    let namespace = "test";

    // Add documents to the test namespace
    for path in &file_paths {
        let add_output = Command::new(&binary)
            .args(&[
                "add",
                "--namespace", namespace,
                "--addr", &server_url,
                &path.to_string_lossy(),
            ])
            .output()
            .expect("Failed to execute add command");

        assert!(add_output.status.success(), "Add command failed");
        println!("Added document: {}", path.display());
    }

    // Now test searching for a single keyword that exists in the documents
    let search_term = "Hello";
    let search_output = Command::new(&binary)
        .args(&[
            "search",
            "--namespace", namespace,
            "--addr", &server_url,
            search_term,
        ])
        .output()
        .expect("Failed to execute search command");

    assert!(search_output.status.success(), "Search command failed");
    let search_stdout = String::from_utf8_lossy(&search_output.stdout);
    println!("Search output for 'Hello': {}", search_stdout);

    // Verify search output contains expected messaging
    assert!(
        search_stdout.contains(&format!("Searching in namespace `{}`", namespace)),
        "Search output doesn't contain expected namespace"
    );
    assert!(
        search_stdout.contains(&format!("query \"{}\"", search_term)),
        "Search output doesn't contain expected query"
    );

    // Test searching for "world" (should find docs 1 and 3)
    let search_term = "world";
    let search_output = Command::new(&binary)
        .args(&[
            "search",
            "--namespace", namespace,
            "--addr", &server_url,
            search_term,
        ])
        .output()
        .expect("Failed to execute search command");

    assert!(search_output.status.success(), "Search command failed");
    let search_stdout = String::from_utf8_lossy(&search_output.stdout);
    println!("Search output for 'world': {}", search_stdout);

    // Test searching for a term that doesn't exist
    let search_term = "nonexistent";
    let search_output = Command::new(&binary)
        .args(&[
            "search",
            "--namespace", namespace,
            "--addr", &server_url,
            search_term,
        ])
        .output()
        .expect("Failed to execute search command");

    assert!(search_output.status.success(), "Search command failed");
    let search_stdout = String::from_utf8_lossy(&search_output.stdout);
    println!("Search output for 'nonexistent': {}", search_stdout);

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

/// Test to verify that search works across different namespaces correctly
#[test]
fn test_namespace_isolation_in_search() {
    // Build the binary first
    Command::new("cargo")
        .args(["build"])
        .status()
        .expect("Failed to build binary");

    // Get the path to the binary
    let binary = env::var("CARGO_BIN_EXE_fugu").unwrap_or_else(|_| {
        env::current_dir()
            .unwrap()
            .join("target/debug/fugu")
            .to_string_lossy()
            .to_string()
    });

    // Create a temporary directory for the server
    let temp_dir = tempdir().expect("Failed to create temp directory");
    
    // Choose a random port for the server to avoid conflicts
    let port = get_random_port();
    let server_addr = format!("127.0.0.1:{}", port);
    let server_url = format!("http://{}", server_addr);

    // Create test documents for different namespaces with some content overlap
    let namespace_docs = [
        ("namespace1", "doc1.txt", "Hello world in namespace1"),
        ("namespace2", "doc2.txt", "Hello world in namespace2"),
        ("namespace1", "doc3.txt", "Additional content in namespace1"),
    ];
    
    // Create files for each namespace/document
    for (namespace, filename, content) in &namespace_docs {
        let dir = temp_dir.path().join(namespace);
        fs::create_dir_all(&dir).expect("Failed to create namespace directory");
        
        let path = dir.join(filename);
        fs::write(&path, content).expect("Failed to write test file");
    }

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

    // Add documents to their respective namespaces
    for (namespace, filename, _) in &namespace_docs {
        let file_path = temp_dir.path().join(namespace).join(filename);
        
        let add_output = Command::new(&binary)
            .args(&[
                "add",
                "--namespace", namespace,
                "--addr", &server_url,
                &file_path.to_string_lossy(),
            ])
            .output()
            .expect("Failed to execute add command");

        assert!(add_output.status.success(), "Add command failed for namespace {}", namespace);
        println!("Added document: {} to namespace {}", filename, namespace);
    }

    // Test searching in namespace1 for a single keyword
    let search_term = "Hello";
    let search_output = Command::new(&binary)
        .args(&[
            "search",
            "--namespace", "namespace1",
            "--addr", &server_url,
            search_term,
        ])
        .output()
        .expect("Failed to execute search command for namespace1");

    assert!(search_output.status.success(), "Search command failed for namespace1");
    let search_stdout = String::from_utf8_lossy(&search_output.stdout);
    println!("Search output for 'Hello' in namespace1: {}", search_stdout);

    // Test searching in namespace2 for the same keyword
    let search_term = "Hello";
    let search_output = Command::new(&binary)
        .args(&[
            "search",
            "--namespace", "namespace2",
            "--addr", &server_url,
            search_term,
        ])
        .output()
        .expect("Failed to execute search command for namespace2");

    assert!(search_output.status.success(), "Search command failed for namespace2");
    let search_stdout = String::from_utf8_lossy(&search_output.stdout);
    println!("Search output for 'Hello' in namespace2: {}", search_stdout);

    // Test searching for a term that's only in namespace1
    let search_term = "Additional";
    let search_output = Command::new(&binary)
        .args(&[
            "search",
            "--namespace", "namespace1",
            "--addr", &server_url,
            search_term,
        ])
        .output()
        .expect("Failed to execute search command");

    assert!(search_output.status.success(), "Search command failed");
    let search_stdout = String::from_utf8_lossy(&search_output.stdout);
    println!("Search output for 'Additional' in namespace1: {}", search_stdout);

    // Test searching for the same term in namespace2 (should have no results)
    let search_term = "Additional";
    let search_output = Command::new(&binary)
        .args(&[
            "search",
            "--namespace", "namespace2",
            "--addr", &server_url,
            search_term,
        ])
        .output()
        .expect("Failed to execute search command");

    assert!(search_output.status.success(), "Search command failed");
    let search_stdout = String::from_utf8_lossy(&search_output.stdout);
    println!("Search output for 'Additional' in namespace2: {}", search_stdout);

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