use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use lazy_static::lazy_static;
use rand::Rng;
use tempfile::TempDir;

static INIT: Once = Once::new();

lazy_static! {
    static ref SERVER_PROCESS: Mutex<Option<std::process::Child>> = Mutex::new(None);
    static ref SERVER_PORT: Mutex<u16> = Mutex::new(0);
    static ref TEST_DIR: Mutex<Option<TempDir>> = Mutex::new(None);
}

/// Returns a randomly chosen port in the range 50100-60000 to avoid hardcoded port conflicts
pub fn get_random_port() -> u16 {
    let mut rng = rand::thread_rng();
    rng.gen_range(50100..60000)
}

/// Initialize the test server if it's not already running
pub fn ensure_server_running() -> (String, PathBuf) {
    INIT.call_once(|| {
        // Create a temporary directory for the server
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_dir_path = temp_dir.path().to_path_buf();
        
        // Choose a random port for the server
        let port = get_random_port();
        
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

        // Start the server with a timeout
        println!("Starting shared test server on port {}...", port);
        let server = Command::new(&binary)
            .args(&[
                "up",
                "--timeout", "300", // 5 minute timeout for all tests
                "--port", &port.to_string(),
            ])
            .spawn()
            .expect("Failed to start server");

        // Allow server to start
        thread::sleep(Duration::from_secs(2));
        
        // Store server details in static variables
        *SERVER_PROCESS.lock().unwrap() = Some(server);
        *SERVER_PORT.lock().unwrap() = port;
        *TEST_DIR.lock().unwrap() = Some(temp_dir);
    });
    
    // Get the port for the URL
    let port = *SERVER_PORT.lock().unwrap();
    let server_url = format!("http://127.0.0.1:{}", port);
    
    // Get the temp dir path
    let temp_dir_path = match &*TEST_DIR.lock().unwrap() {
        Some(dir) => dir.path().to_path_buf(),
        None => panic!("Test directory not initialized")
    };
    
    (server_url, temp_dir_path)
}

/// Cleanup the server when tests are done
pub fn shutdown_server() {
    if let Some(mut server) = SERVER_PROCESS.lock().unwrap().take() {
        println!("Shutting down shared test server...");
        server.kill().expect("Failed to kill server");
        
        // Wait for server to exit
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
    }
}