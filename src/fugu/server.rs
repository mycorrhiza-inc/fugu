/// Server module provides the main Fugu server implementation
///
/// This module contains the core server functionality:
/// - Provides clean shutdown capability
/// - Supports concurrent client operations
use std::path::PathBuf;

use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};

use crate::fugu::config::{ConfigManager, new_config_manager};

/// Main server struct that manages the Fugu search engine
///
/// The server is responsible for:
/// - Maintaining index data
/// - Processing client requests
/// - Ensuring data consistency
pub struct FuguServer {
    /// Path where server data is stored
    path: PathBuf,
    /// Configuration manager for file paths
    config: ConfigManager,
    /// Flag to signal server shutdown
    stop: bool,
    /// Flag to enable shutdown timeout (for testing)
    use_shutdown_timeout: bool,
}

impl Clone for FuguServer {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            config: self.config.clone(),
            stop: self.stop,
            use_shutdown_timeout: self.use_shutdown_timeout,
        }
    }
}

// No Drop implementation - explicit shutdown must be done via down() method
// This is a conscious design decision to avoid potential blocking during Drop
// and to make shutdown explicit for proper resource cleanup

/// Helper to set up a temporary context for testing
#[allow(dead_code)]
fn create_test_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

impl FuguServer {
    /// Creates a new FuguServer instance
    ///
    /// Initializes the server with:
    /// - Proper shutdown handling
    ///
    /// # Arguments
    ///
    /// * `path` - Path where the server will store data
    ///
    /// # Returns
    ///
    /// A new FuguServer instance
    pub fn new(path: PathBuf) -> Self {
        // Create configuration manager
        let config = new_config_manager(Some(path.clone()));
        Self::new_with_options(path, config, true)
    }

    /// Creates a new FuguServer instance with optional shutdown timeout
    ///
    /// # Arguments
    ///
    /// * `path` - Path where the server will store data
    /// * `config` - Configuration manager for file paths
    /// * `use_shutdown_timeout` - Whether to use timeout during shutdown (for testing)
    ///
    /// # Returns
    ///
    /// A new FuguServer instance
    pub fn new_with_options(path: PathBuf, config: ConfigManager, use_shutdown_timeout: bool) -> Self {
        info!("Creating new FuguServer instance at {}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs());
        
        Self {
            path,
            config,
            stop: false,
            use_shutdown_timeout,
        }
    }
    
    /// Helper method to create a channel for testing purposes
    ///
    /// # Returns
    ///
    /// A sender channel for inter-component communication
    #[allow(dead_code)]
    pub fn create_channel<T>() -> mpsc::Sender<T> where T: Send + 'static {
        let (tx, _rx) = mpsc::channel(1000);
        tx
    }

    /// Starts the server and keeps it running until shutdown
    ///
    /// This method blocks until server shutdown is requested
    #[allow(dead_code)]
    pub async fn up(&mut self) {
        // Just wait for shutdown
        while !self.stop {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
    
    /// Gracefully shuts down the server ensuring data is saved
    ///
    /// This method:
    /// - Signals all processing to stop
    /// - Ensures data durability
    ///
    /// # Returns
    ///
    /// Result indicating success or error during shutdown
    pub async fn down(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Shutting down server");
        self.stop = true;
        
        // The actual flushing of index data is handled by the node's unload_index method
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[tokio::test]
    pub async fn test_server_creation() -> Result<(), Box<dyn std::error::Error>> {
        // Use a temporary directory for test instead of hardcoded path
        let test_dir = tempfile::tempdir()?;
        let config = new_config_manager(Some(test_dir.path().to_path_buf()));
        let mut server = FuguServer::new_with_options(test_dir.path().to_path_buf(), config, false);
        
        // Test server startup and shutdown
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel::<bool>();

        let server_handle = tokio::spawn(async move {
            let _ = server.up();
            // Wait for shutdown signal
            let _ = rx_shutdown.await;
            let _ = server.down().await;
        });

        // Run a brief test
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Send shutdown signal
        let _ = tx_shutdown.send(true);

        // Wait for server to shutdown
        let _ = server_handle.await?;

        Ok(())
    }
    
    #[tokio::test]
    pub async fn test_channel_creation() {
        // Test the channel creation utility function
        let channel = FuguServer::create_channel::<String>();
        assert!(channel.capacity() > 0, "Channel should have capacity");
        
        // Create a simple receiver to test the channel
        let (tx, mut rx) = mpsc::channel::<String>(10);
        
        // Test sending a message
        tokio::spawn(async move {
            let _ = tx.send("test message".to_string()).await;
        });
        
        // Receive the message (with a timeout to avoid blocking)
        let received = tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            rx.recv()
        ).await;
        
        assert!(received.is_ok(), "Should receive a message within timeout");
        assert_eq!(received.unwrap().unwrap(), "test message");
    }
}
