//! Main test runner that initializes and cleans up the shared test server

mod shared_test_fixture;
use shared_test_fixture::{ensure_server_running, shutdown_server};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Once;
    
    static INIT: Once = Once::new();
    static CLEANUP: Once = Once::new();
    
    #[test]
    fn test_01_init_shared_server() {
        // Initialize the shared server on the first run
        INIT.call_once(|| {
            println!("Initializing shared test server...");
            let (url, _) = ensure_server_running();
            println!("Shared test server running at: {}", url);
        });
    }
    
    // This should run last
    #[test]
    fn test_99_cleanup_shared_server() {
        // Register an atexit handler to ensure the server is shut down
        CLEANUP.call_once(|| {
            println!("Registering cleanup for shared test server");
            std::thread::spawn(|| {
                // Try to delay shutdown until all tests have run
                std::thread::sleep(std::time::Duration::from_secs(1));
                shutdown_server();
            });
        });
    }
}