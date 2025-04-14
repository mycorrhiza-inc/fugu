use rand::Rng;
use std::env;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

// Returns a randomly chosen port in the range 50100-60000 to avoid hardcoded port conflicts
fn get_random_port() -> u16 {
    let mut rng = rand::thread_rng();
    rng.gen_range(50100..60000)
}

// Function to save performance data to CSV
fn save_performance_csv(test_name: &str, durations: &[Duration]) -> std::io::Result<()> {
    // Create the data directory if it doesn't exist
    let data_dir = Path::new("tests/data");
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)?;
    }

    // Create CSV file
    let file_path = data_dir.join(format!("{}.csv", test_name));
    let mut file = File::create(file_path)?;

    // Write header
    writeln!(file, "operation,duration_us")?;

    // Write each measurement
    for (i, duration) in durations.iter().enumerate() {
        writeln!(file, "{},{}", i, duration.as_micros())?;
    }

    Ok(())
}

/// Test to measure the performance difference between cold and hot loading
///
/// This test measures:
/// 1. Server startup time with existing data (cold start)
/// 2. Initial query latency after cold start
/// 3. Query latency after multiple requests (hot)
#[cfg(feature = "performance-tests")]
#[test]
fn test_hot_cold_loading_performance() {
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
    let server_dir = temp_dir.path().to_string_lossy().to_string();

    // Choose a random port for the server to avoid conflicts
    let port = get_random_port();
    let server_addr = format!("127.0.0.1:{}", port);
    let server_url = format!("http://{}", server_addr);

    // Create test files with varying content
    let num_files = 100; // Higher number to create significant data volume
    let mut file_paths = Vec::new();

    for i in 0..num_files {
        // Create file with varied content to exercise index
        let filename = format!("file_{}.txt", i);
        let path = temp_dir.path().join(&filename);

        // Vary content to create a realistic index
        let content = if i % 3 == 0 {
            format!("Document {} contains technical terms like algorithm, database, and performance testing", i)
        } else if i % 3 == 1 {
            format!(
                "File {} discusses user interfaces, web applications and design patterns",
                i
            )
        } else {
            format!("Content {} relates to system architecture, networking protocols and security measures", i)
        };

        fs::write(&path, &content).expect("Failed to write test file");
        file_paths.push(path);
    }

    println!("Created {} test files", num_files);

    // ======== PHASE 1: INITIAL SERVER STARTUP AND INDEXING ========
    println!("PHASE 1: Initial server startup and indexing");

    // Start the server with a timeout
    let mut server = Command::new(&binary)
        .args(&[
            "up",
            "--timeout",
            "30", // 30 second timeout
        ])
        .spawn()
        .expect("Failed to start server");

    // Allow server to start
    thread::sleep(Duration::from_secs(2));

    // Index all files - this is the initial data load
    for (i, path) in file_paths.iter().enumerate() {
        let index_output = Command::new(&binary)
            .args(&[
                "index",
                "--file",
                &path.to_string_lossy(),
                "--addr",
                &server_url,
            ])
            .output()
            .expect("Failed to execute index command");

        assert!(index_output.status.success(), "Index command failed");

        // Print progress only occasionally
        if i % 20 == 0 {
            println!("Indexed {}/{} files", i, num_files);
        }
    }

    println!("All files indexed");

    // Check that search works
    let search_output = Command::new(&binary)
        .args(&["search", "--query", "document", "--addr", &server_url])
        .output()
        .expect("Failed to execute search command");

    assert!(search_output.status.success(), "Initial search failed");

    // Gracefully shut down the server
    println!("Shutting down server after initial indexing");
    server.kill().expect("Failed to kill server");

    // Wait for server to exit
    for _ in 0..20 {
        if server.try_wait().map(|s| s.is_some()).unwrap_or(false) {
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }

    thread::sleep(Duration::from_secs(2)); // Ensure full shutdown

    // ======== PHASE 2: MEASURE COLD START PERFORMANCE ========
    println!("PHASE 2: Measuring cold start performance");

    // Measure cold start time
    let cold_start_time = Instant::now();

    let mut server2 = Command::new(&binary)
        .args(&[
            "up",
            "--timeout",
            "30", // 30 second timeout
        ])
        .spawn()
        .expect("Failed to start server for cold startup measurement");

    // Wait for the server to start and be ready
    thread::sleep(Duration::from_secs(2));

    let cold_start_duration = cold_start_time.elapsed();
    println!("Cold server startup took: {:?}", cold_start_duration);

    // Track the durations for different query types in cold state
    let num_operations = 50; // Number of operations to test
    let mut cold_search_durations = Vec::with_capacity(num_operations);

    // Define a variety of search terms to test different aspects of the index
    let search_terms = [
        "document",
        "technical",
        "algorithm",
        "database",
        "performance",
        "interface",
        "application",
        "design",
        "pattern",
        "architecture",
        "network",
        "protocol",
        "security",
    ];

    // Measure cold search performance
    println!("Measuring cold search performance");
    for i in 0..num_operations {
        // Use different search terms
        let term_index = i % search_terms.len();
        let query = search_terms[term_index];

        // Measure search time
        let start = Instant::now();
        let search_output = Command::new(&binary)
            .args(&["search", "--query", query, "--addr", &server_url])
            .output()
            .expect("Failed to execute search command");
        let duration = start.elapsed();

        // Store the duration
        cold_search_durations.push(duration);

        // Validate the search worked
        assert!(search_output.status.success(), "Cold search failed");

        // Print progress only occasionally
        if i % 10 == 0 {
            println!("Completed {}/{} cold searches", i, num_operations);
        }
    }

    // Save cold search performance data
    if let Err(e) = save_performance_csv("cold_search_performance", &cold_search_durations) {
        eprintln!("Failed to save cold search performance data: {}", e);
    }

    // ======== PHASE 3: MEASURE HOT PERFORMANCE ========
    println!("PHASE 3: Measuring hot performance");

    // Perform the warm-up phase - execute a series of queries to warm up the server
    println!("Warming up the server");
    for _ in 0..20 {
        for query in &search_terms {
            let _ = Command::new(&binary)
                .args(&["search", "--query", query, "--addr", &server_url])
                .output()
                .expect("Failed to execute warm-up search");
        }
    }

    // Measure hot search performance
    let mut hot_search_durations = Vec::with_capacity(num_operations);

    println!("Measuring hot search performance");
    for i in 0..num_operations {
        // Use different search terms
        let term_index = i % search_terms.len();
        let query = search_terms[term_index];

        // Measure search time
        let start = Instant::now();
        let search_output = Command::new(&binary)
            .args(&["search", "--query", query, "--addr", &server_url])
            .output()
            .expect("Failed to execute search command");
        let duration = start.elapsed();

        // Store the duration
        hot_search_durations.push(duration);

        // Validate the search worked
        assert!(search_output.status.success(), "Hot search failed");

        // Print progress only occasionally
        if i % 10 == 0 {
            println!("Completed {}/{} hot searches", i, num_operations);
        }
    }

    // Save hot search performance data
    if let Err(e) = save_performance_csv("hot_search_performance", &hot_search_durations) {
        eprintln!("Failed to save hot search performance data: {}", e);
    }

    // ======== PHASE 4: MEASURE SERVER RELOAD TIME WITH MIXED OPERATIONS ========
    println!("PHASE 4: Measuring server reload with mixed operations");

    // Shut down the server
    println!("Shutting down server for reload test");
    server2.kill().expect("Failed to kill server");

    // Wait for server to exit
    for _ in 0..20 {
        if server2.try_wait().map(|s| s.is_some()).unwrap_or(false) {
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }

    thread::sleep(Duration::from_secs(2)); // Ensure full shutdown

    // Measure server reload time
    let reload_start_time = Instant::now();

    let mut server3 = Command::new(&binary)
        .args(&[
            "up",
            "--timeout",
            "30", // 30 second timeout
        ])
        .spawn()
        .expect("Failed to reload server");

    // Wait for the server to start and be ready
    thread::sleep(Duration::from_secs(2));

    let reload_duration = reload_start_time.elapsed();
    println!("Server reload took: {:?}", reload_duration);

    // Record reload duration in a format compatible with our CSV
    let reload_durations = vec![reload_duration];
    if let Err(e) = save_performance_csv("server_reload_performance", &reload_durations) {
        eprintln!("Failed to save server reload performance data: {}", e);
    }

    // Clean up the server
    server3.kill().expect("Failed to kill server");

    // Wait for server to exit
    for _ in 0..20 {
        if server3.try_wait().map(|s| s.is_some()).unwrap_or(false) {
            break;
        }
        thread::sleep(Duration::from_millis(500));
    }

    // Clean up the temporary directory
    temp_dir.close().expect("Failed to delete temp directory");

    // Print summary
    println!("Hot/Cold Loading Test Complete");
    println!("Cold startup time: {:?}", cold_start_duration);
    println!(
        "Cold search median: {:?}",
        median_duration(&cold_search_durations)
    );
    println!(
        "Hot search median: {:?}",
        median_duration(&hot_search_durations)
    );
    println!("Server reload time: {:?}", reload_duration);
}

// Helper function to calculate median duration
fn median_duration(durations: &[Duration]) -> Duration {
    if durations.is_empty() {
        return Duration::from_secs(0);
    }

    let mut values: Vec<u128> = durations.iter().map(|d| d.as_micros()).collect();
    values.sort_unstable();

    let mid = values.len() / 2;
    let median_micros = if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2
    } else {
        values[mid]
    };

    Duration::from_micros(median_micros as u64)
}
