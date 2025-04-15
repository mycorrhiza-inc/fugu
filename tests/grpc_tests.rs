use std::env;
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::tempdir;

// Import the shared test fixture
mod shared_test_fixture;
use shared_test_fixture::ensure_server_running;

// End-to-end tests for the GRPC client and server

// Function to save performance data to CSV
fn save_performance_csv(test_name: &str, durations: &[Duration]) -> std::io::Result<()> {
    // Create the data directory if it doesn't exist
    let data_dir = Path::new("tests/data");
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)?;
    }

    // Create CSV file
    let file_path = data_dir.join(format!("integration_{}.csv", test_name));
    let mut file = File::create(file_path)?;

    // Write header
    writeln!(file, "operation,duration_us")?;

    // Write each measurement
    for (i, duration) in durations.iter().enumerate() {
        writeln!(file, "{},{}", i, duration.as_micros())?;
    }

    Ok(())
}

#[test]
fn test_grpc_e2e() {
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

    // Create a test file
    let test_file_path = temp_dir.path().join("sample.txt");
    let test_content = "This is a sample document for testing the fugu search engine";
    fs::write(&test_file_path, test_content).expect("Failed to write test file");

    // Performance measurement arrays
    let mut index_durations = Vec::new();
    let mut search_durations = Vec::new();
    let mut delete_durations = Vec::new();

    // Run multiple operations for performance measurement
    let num_operations = 50; // Less than unit tests since these are slower

    // Test namespace
    let namespace = "grpc_e2e_test";

    for i in 0..num_operations {
        // Create a unique test file for each operation
        let test_file_path_i = temp_dir.path().join(format!("sample_{}.txt", i));
        let test_content_i = format!(
            "This is sample document {} for testing the fugu search engine",
            i
        );
        fs::write(&test_file_path_i, &test_content_i).expect("Failed to write test file");

        // Use the client to index a file - measure time
        let start = Instant::now();
        let index_output = std::process::Command::new(&binary)
            .args(&[
                "index",
                "--file",
                &test_file_path_i.to_string_lossy(),
                "--namespace", namespace,
                "--addr",
                &server_url,
            ])
            .output()
            .expect("Failed to execute index command");
        let duration = start.elapsed();
        index_durations.push(duration);

        assert!(index_output.status.success(), "Index command failed");
        let index_stdout = String::from_utf8_lossy(&index_output.stdout);
        if i == 0 {
            println!("Index output: {}", index_stdout);
            assert!(
                index_stdout.contains("Indexing file in namespace"),
                "Index was not successful"
            );
        }
    }

    // Save index performance data
    if let Err(e) = save_performance_csv("index", &index_durations) {
        eprintln!("Failed to save integration index performance data: {}", e);
    }

    // Test search performance with different queries
    let search_queries = ["sample", "document", "testing", "engine", "fugu"];

    for i in 0..num_operations {
        let query = search_queries[i % search_queries.len()];

        // Use the client to search - measure time
        let start = Instant::now();
        let search_output = std::process::Command::new(&binary)
            .args(&[
                "search",
                "--limit",
                "10",
                "--namespace", namespace,
                "--addr",
                &server_url,
                query, // query is a positional argument
            ])
            .output()
            .expect("Failed to execute search command");
        let duration = start.elapsed();
        search_durations.push(duration);

        assert!(search_output.status.success(), "Search command failed");
        let search_stdout = String::from_utf8_lossy(&search_output.stdout);
        if i == 0 {
            println!("Search output: {}", search_stdout);
            assert!(
                search_stdout.contains("Searching in namespace"),
                "Search response not found"
            );
        }
    }

    // Save search performance data
    if let Err(e) = save_performance_csv("search", &search_durations) {
        eprintln!("Failed to save integration search performance data: {}", e);
    }

    // Test delete performance
    for i in 0..num_operations {
        let file_name = format!("sample_{}.txt", i);

        // Use the client to delete - measure time
        let start = Instant::now();
        let delete_output = std::process::Command::new(&binary)
            .args(&[
                "delete",
                &format!("/{}", file_name),
                "--namespace", namespace,
                "--addr",
                &server_url,
            ])
            .output()
            .expect("Failed to execute delete command");
        let duration = start.elapsed();
        delete_durations.push(duration);

        assert!(delete_output.status.success(), "Delete command failed");
        let delete_stdout = String::from_utf8_lossy(&delete_output.stdout);
        if i == 0 {
            println!("Delete output: {}", delete_stdout);
            assert!(
                delete_stdout.contains("Deleting from namespace"),
                "Delete was not successful"
            );
        }
    }

    // Save delete performance data
    if let Err(e) = save_performance_csv("delete", &delete_durations) {
        eprintln!("Failed to save integration delete performance data: {}", e);
    }

    // Clean up
    temp_dir.close().expect("Failed to delete temp directory");
}

#[test]
fn test_grpc_client_error_handling() {
    // Reference the test in src/fugu/test_grpc.rs that handles this case
    println!("Skipping test_grpc_client_error_handling as it's covered by test_client_connection_error in src/fugu/test_grpc.rs");
}

// Integration test for index persistence across namespaces
#[test]
fn test_multiple_namespaces() {
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

    // Create test files for different namespaces
    let namespace_files = vec![
        (
            "namespace1",
            "file1.txt",
            "Document for namespace 1 with unique keywords",
        ),
        (
            "namespace2",
            "file2.txt",
            "Document for namespace 2 with different terms",
        ),
        (
            "namespace3",
            "file3.txt",
            "Document for namespace 3 with its own content",
        ),
    ];

    for (namespace, filename, content) in &namespace_files {
        let dir = temp_dir.path().join(namespace);
        fs::create_dir_all(&dir).expect("Failed to create namespace directory");

        let path = dir.join(filename);
        fs::write(&path, content).expect("Failed to write test file");
    }

    // Index a file in each namespace
    for (namespace, filename, _) in &namespace_files {
        let file_path = temp_dir.path().join(namespace).join(filename);

        let index_output = std::process::Command::new(&binary)
            .args(&[
                "index",
                "--file",
                &file_path.to_string_lossy(),
                "--namespace",
                namespace,
                "--addr",
                &server_url,
            ])
            .output()
            .expect("Failed to execute index command");

        assert!(
            index_output.status.success(),
            "Index command failed for namespace {}",
            namespace
        );

        println!("Indexed file for namespace {}", namespace);
    }

    // Search in each namespace for unique and shared terms
    for (namespace, _, _) in &namespace_files {
        // Search for namespace-specific term
        let search_term = namespace;

        let search_output = std::process::Command::new(&binary)
            .args(&[
                "search",
                "--namespace",
                namespace,
                "--addr",
                &server_url,
                search_term, // query as positional argument, not using --query flag
            ])
            .output()
            .expect("Failed to execute search command");

        assert!(
            search_output.status.success(),
            "Search command failed for namespace {}",
            namespace
        );

        let search_stdout = String::from_utf8_lossy(&search_output.stdout);
        println!(
            "Search output for namespace {}: {}",
            namespace, search_stdout
        );

        // Search for common term "document" which should be in all namespaces
        let common_search_output = std::process::Command::new(&binary)
            .args(&[
                "search",
                "--namespace",
                namespace,
                "--addr",
                &server_url,
                "document", // query as positional argument, not using --query flag
            ])
            .output()
            .expect("Failed to execute common search command");

        assert!(
            common_search_output.status.success(),
            "Common search command failed for namespace {}",
            namespace
        );
    }

    // Clean up
    temp_dir.close().expect("Failed to delete temp directory");
}

// Test for the add command
#[test]
fn test_add_command() {
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

    // Create test files with different content in different namespaces
    let namespace_files = vec![
        (
            "docs",
            "article.txt",
            "This is an article about Fugu search",
        ),
        (
            "code",
            "sample.rs",
            "fn main() { println!(\"Hello Fugu!\"); }",
        ),
        (
            "data",
            "users.json",
            "{\"users\": [{\"name\": \"test\", \"role\": \"admin\"}]}",
        ),
    ];

    for (namespace, filename, content) in &namespace_files {
        let dir = temp_dir.path().join(namespace);
        fs::create_dir_all(&dir).expect("Failed to create namespace directory");

        let path = dir.join(filename);
        fs::write(&path, content).expect("Failed to write test file");
    }

    // Test the add command for each namespace/file
    for (namespace, filename, _) in &namespace_files {
        let file_path = temp_dir.path().join(namespace).join(filename);

        // Use the add command to index the file
        let add_output = std::process::Command::new(&binary)
            .args(&[
                "add",
                "--namespace",
                namespace,
                "--addr",
                &server_url,
                &file_path.to_string_lossy(),
            ])
            .output()
            .expect("Failed to execute add command");

        assert!(
            add_output.status.success(),
            "Add command failed for namespace {}",
            namespace
        );

        let stdout = String::from_utf8_lossy(&add_output.stdout);
        println!("Add output for namespace {}: {}", namespace, stdout);

        // Verify output contains expected text
        assert!(
            stdout.contains(&format!("Adding file to namespace `{}`", namespace)),
            "Add command output missing expected text for {}",
            namespace
        );
    }

    // Clean up
    temp_dir.close().expect("Failed to delete temp directory");
}