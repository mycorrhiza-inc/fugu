/// Server module provides the main Fugu server implementation
///
/// This module contains the core server functionality:
/// - Handles the write-ahead log (WAL) for durability
/// - Manages a background task for processing WAL commands
/// - Provides clean shutdown capability
/// - Supports concurrent client operations
use std::path::PathBuf;

use tokio::sync::mpsc;
use tracing::{info, warn, error, debug};

use crate::fugu::wal::{WAL, WALCMD};
use crate::fugu::config::{ConfigManager, new_config_manager};

/// Main server struct that manages the Fugu search engine
///
/// The server is responsible for:
/// - Managing the write-ahead log (WAL) for durability
/// - Maintaining index data
/// - Processing client requests
/// - Ensuring data consistency
pub struct FuguServer {
    /// Path where server data and WAL are stored
    path: PathBuf,
    /// Configuration manager for file paths
    config: ConfigManager,
    /// Write-ahead log for durability
    wal: WAL,
    /// Flag to signal server shutdown
    stop: bool,
    /// Channel sender for WAL commands
    wal_sender: tokio::sync::mpsc::Sender<WALCMD>,
    /// Tracks the number of WAL receivers in use
    #[allow(dead_code)]
    wal_receiver_count: usize, // We can't clone the receiver, so just track the count
    /// Flag to enable shutdown timeout (for testing)
    use_shutdown_timeout: bool,
    /// Channel to signal WAL processor task to shut down
    shutdown_tx: Option<tokio::sync::mpsc::Sender<bool>>,
}

impl Clone for FuguServer {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            config: self.config.clone(),
            wal: self.wal.clone(),
            stop: self.stop,
            wal_sender: self.wal_sender.clone(),
            wal_receiver_count: self.wal_receiver_count,
            use_shutdown_timeout: self.use_shutdown_timeout,
            shutdown_tx: self.shutdown_tx.clone(),
        }
    }
}

impl Drop for FuguServer {
    fn drop(&mut self) {
        info!("FuguServer instance being dropped at {}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs());
        
        // Send shutdown signal to WAL processor task
        if let Some(shutdown_tx) = &self.shutdown_tx {
            let _ = shutdown_tx.try_send(true);
            info!("Attempted to send shutdown signal to WAL processor");
        }
        
        // The WAL shutdown is now handled automatically via Drop on the WAL struct
        // The new WAL implementation handles namespaces and ensures all data is flushed
    }
}

/// Helper to set up a temporary context for testing
#[allow(dead_code)]
fn create_test_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

impl FuguServer {
    /// Creates a new FuguServer instance
    ///
    /// Initializes the server with:
    /// - A new Write-Ahead Log (WAL)
    /// - A background task to process WAL commands
    /// - Proper shutdown handling
    ///
    /// # Arguments
    ///
    /// * `path` - Path where the server will store data and WAL
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
    /// * `path` - Path where the server will store data and WAL
    /// * `config` - Configuration manager for file paths
    /// * `use_shutdown_timeout` - Whether to use timeout during shutdown (for testing)
    ///
    /// # Returns
    ///
    /// A new FuguServer instance
    pub fn new_with_options(path: PathBuf, config: ConfigManager, use_shutdown_timeout: bool) -> Self {
        // Create channel for WAL commands
        let (tx, mut rx): (mpsc::Sender<WALCMD>, mpsc::Receiver<WALCMD>) = mpsc::channel(1000);
        info!("Created WAL channel with capacity 1000 at {}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs());
        
        // Create WAL base directory path
        let wal_dir = config.base_dir().join("wal");
        
        // Create an instance of our namespace-aware WAL
        let wal = WAL::open(wal_dir.clone());
        
        // Create a clone for the processor task
        let wal_processor = wal.clone();
        
        // Spawn a task to process WAL messages with proper shutdown handling
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<bool>(1);
        // Store reference to shutdown_tx for the Drop impl to use
        let shutdown_tx_clone = shutdown_tx.clone();
        
        // Handle WAL commands by routing them to the appropriate namespace
        tokio::spawn(async move {
            info!("WAL processor started");
            
            loop {
                tokio::select! {
                    // Process WAL message
                    Some(msg) = rx.recv() => {
                        // Process the command based on the namespace
                        if let Err(e) = wal_processor.process_command(msg).await {
                            error!("WAL process_command error: {:?}", e);
                        }
                    },
                    // Shutdown message
                    Some(_) = shutdown_rx.recv() => {
                        info!("WAL processor received shutdown signal");
                        
                        // Ensure all namespaces are flushed on shutdown
                        wal_processor.shutdown().await;
                        
                        break;
                    },
                    // Channel closed (server dropped)
                    else => {
                        debug!("WAL processor channel closed (normal during shutdown)");
                        
                        // Ensure all namespaces are flushed on shutdown
                        wal_processor.shutdown().await;
                        
                        break;
                    }
                }
            }
            
            // The WAL will automatically handle proper shutdown via Drop
            info!("WAL processor shutdown complete");
        });
        
        Self {
            path,
            config,
            wal,
            stop: false,
            wal_sender: tx,
            wal_receiver_count: 1,
            use_shutdown_timeout,
            shutdown_tx: Some(shutdown_tx_clone),
        }
    }
    
    /// Returns a clone of the WAL sender channel
    ///
    /// This allows components like Node to send WAL commands
    ///
    /// # Returns
    ///
    /// A sender channel for WAL commands
    pub fn get_wal_sender(&self) -> mpsc::Sender<WALCMD> {
        self.wal_sender.clone()
    }
    
    /// Dumps the WAL contents for a specific namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to dump, or None for all namespaces
    ///
    /// # Returns
    ///
    /// A string representation of the WAL or an error
    #[allow(dead_code)]
    async fn dump_wal(&self, namespace: Option<String>) -> Result<String, Box<dyn std::error::Error>> {
        // Create a oneshot channel for the response
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        // Create and send the dump command
        let cmd = WALCMD::DumpWAL {
            response: tx,
            namespace,
        };
        
        self.wal_sender.send(cmd).await?;
        
        // Wait for the response
        match rx.await {
            Ok(dump) => Ok(dump),
            Err(e) => Err(format!("Failed to receive WAL dump: {:?}", e).into()),
        }
    }

    // The wal_listen logic is now handled by the tokio task spawned in new()
    
    /// Starts the server and keeps it running until shutdown
    ///
    /// This method blocks until server shutdown is requested
    #[allow(dead_code)]
    pub async fn up(&mut self) {
        // Just wait for shutdown, real processing is done in the spawned task
        while !self.stop {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
    
    /// Gracefully shuts down the server ensuring data is saved
    ///
    /// This method:
    /// - Signals all processing to stop
    /// - Attempts to flush final WAL commands
    /// - Ensures data durability
    ///
    /// # Returns
    ///
    /// Result indicating success or error during shutdown
    #[allow(dead_code)]
    pub async fn down(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Shutting down server and saving all data");
        self.stop = true;
        
        // First send shutdown signal to WAL processor task
        if self.use_shutdown_timeout {
            // With timeout (default behavior)
            // Send a dump command to ensure all namespaces are flushed
            if let Err(_) = tokio::time::timeout(
                tokio::time::Duration::from_secs(5),
                self.dump_wal(None)
            ).await {
                warn!("Timeout sending final WAL command");
            }
        } else {
            // Without timeout (for testing)
            let _ = self.dump_wal(None).await;
        }
        
        // Send individual flush commands to all namespaces
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        // Create a flush command for the default namespace (others will be handled by the WAL system)
        let cmd = WALCMD::FlushWAL { 
            namespace: "default".to_string(),
            response: Some(tx),
        };
        
        // Send the flush command
        self.wal_sender.send(cmd).await?;
        
        // Wait for the flush to complete
        if let Err(e) = rx.await {
            warn!("Error waiting for WAL flush response: {:?}", e);
        }
        
        // The actual flushing of index data is handled by the node's unload_index method
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fugu::{node, node::Node};
    use tempfile;

    #[tokio::test]
    pub async fn run_wal_text() -> Result<(), Box<dyn std::error::Error>> {
        // Use a temporary directory for test instead of hardcoded path
        let test_dir = tempfile::tempdir()?;
        let config = new_config_manager(Some(test_dir.path().to_path_buf()));
        let mut server = FuguServer::new_with_options(test_dir.path().to_path_buf(), config, false);
        let sender = server.get_wal_sender();
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel::<bool>();

        let server_handle = tokio::spawn(async move {
            let _ = server.up();
            // Wait for shutdown signal
            let _ = rx_shutdown.await;
            let _ = server.down().await;
        });

        let mut nodes: Vec<Node> = Vec::new();
        let mut handles = Vec::new();

        // Create 5 nodes with different namespaces
        for i in 0..5 {
            let namespace = format!("node_{}", i);
            let sender_clone = sender.clone();
            let node = node::new(namespace, None, sender_clone);
            nodes.push(node);
        }

        // Spawn concurrent tasks for each node
        for (i, _node) in nodes.into_iter().enumerate() {
            // Clone the sender for this task
            let task_sender = sender.clone();
            
            let handle = tokio::spawn(async move {
                // Each node does random operations
                for j in 1..10 {
                    let random = rand::random::<u64>() % 500;
                    // Random delay between operations (0-500ms)
                    tokio::time::sleep(tokio::time::Duration::from_millis(random)).await;

                    // Randomly choose between put and delete
                    if rand::random::<f64>() < 0.7 {
                        // 70% chance of put, 30% chance of delete
                        let key = format!("node_{}_key_{}", i, j);
                        let value = format!("value_from_node_{}_op_{}", i, j).into_bytes();
                        
                        // Create a namespace-aware WAL command
                        let cmd = WALCMD::Put {
                            key: key.clone(),
                            value: value.clone(),
                            namespace: format!("node_{}", i),
                        };
                        
                        // Send the command directly
                        if let Err(e) = task_sender.send(cmd).await {
                            eprintln!("Error sending command: {}", e);
                        }
                    } else {
                        // Delete a random previous key
                        let prev_key = format!("node_{}_key_{}", i, rand::random::<u32>() % j.max(1));
                        
                        // Create a namespace-aware WAL command
                        let cmd = WALCMD::Delete {
                            key: prev_key.clone(),
                            namespace: format!("node_{}", i),
                        };
                        
                        // Send the command directly
                        if let Err(e) = task_sender.send(cmd).await {
                            eprintln!("Error sending command: {}", e);
                        }
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all node operations to complete
        for handle in handles {
            if let Err(e) = handle.await {
                eprintln!("Error in join handle: {}", e);
            }
        }

        // Sleep briefly to ensure all operations are processed
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        
        // Send dump commands to verify operations for each namespace
        for i in 0..5 {
            let namespace = format!("node_{}", i);
            
            // Create a channel for the dump response
            let (tx, rx) = tokio::sync::oneshot::channel();
            
            // Create dump command for this namespace
            let cmd = WALCMD::DumpWAL {
                response: tx,
                namespace: Some(namespace.clone()),
            };
            
            // Send the dump command
            sender.send(cmd).await?;
            
            // Wait for the response
            let dump = rx.await?;
            
            // Verify the namespace was properly processed
            println!("Namespace {}: {}", namespace, dump);
            assert!(dump.contains(&namespace), "Dump should contain namespace name");
        }
        
        // Send shutdown signal
        let _ = tx_shutdown.send(true);

        // Wait for server to shutdown
        let _ = server_handle.await?;

        Ok(())
    }
}
