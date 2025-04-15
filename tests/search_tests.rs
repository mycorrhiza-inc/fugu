use std::fs;
use std::env;
use std::path::PathBuf;
use tempfile::tempdir;

// Import the shared test fixture
mod shared_test_fixture;
use shared_test_fixture::{ensure_server_running, shutdown_server};

/// Test to verify that the search command works correctly with the server
#[test]
fn test_search_command() {
    // Get the server URL and test directory path
    let (server_url, _) = ensure_server_running();

    // Get the path to the binary
    let binary = env::var("CARGO_BIN_EXE_fugu").unwrap_or_else(|_| {
        env::current_dir()
            .unwrap()
            .join("target/debug/fugu")
            .to_string_lossy()
            .to_string()
    });

    // Create a temporary directory for test files
    let temp_dir = tempdir().expect("Failed to create temp directory");

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

    // Test namespace
    let namespace = "search_test";

    // Add documents to the test namespace
    for path in &file_paths {
        let add_output = std::process::Command::new(&binary)
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
    let search_output = std::process::Command::new(&binary)
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
    let search_output = std::process::Command::new(&binary)
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
    let search_output = std::process::Command::new(&binary)
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

    // Clean up
    temp_dir.close().expect("Failed to delete temp directory");
}

// The common server cleanup will happen at the end of all tests by calling shutdown_server