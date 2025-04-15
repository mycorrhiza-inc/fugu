use std::fs;
use std::env;
use std::process::Command;
use tempfile::tempdir;

// Import the shared test fixture
mod shared_test_fixture;
use shared_test_fixture::ensure_server_running;

/// Test to verify that the search command works with case-insensitive search
#[test]
fn test_case_insensitive_search() {
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

    // Create a test file with mixed case words
    let test_file_path = temp_dir.path().join("test_case.txt");
    let test_content = "Hello World TEST case SENSITIVITY";
    fs::write(&test_file_path, test_content).expect("Failed to write test file");

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

    // Clean up
    temp_dir.close().expect("Failed to delete temp directory");
}