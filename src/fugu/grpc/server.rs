/// gRPC server implementation for the Fugu service
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::Arc;
use std::error::Error;
use tokio::sync::{RwLock, mpsc};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tokio::io::AsyncWriteExt;
use tracing::{trace, info, warn, error};

use crate::fugu::node::Node;
use crate::fugu::grpc::{BoxError, namespace::*, Namespace, NamespaceServer};
use crate::fugu::grpc::error::node_err_to_box;
use crate::fugu::config::{ConfigManager, new_config_manager};

// Use the parallel indexer from the index module
use crate::fugu::index::ParallelIndexer;

/// Main service implementation for the Fugu namespace gRPC API
///
/// This service:
/// - Manages Fugu nodes and namespaces
/// - Handles client requests for indexing, search, and deletion
/// - Provides namespace isolation for multi-tenant usage
/// - Ensures proper cleanup on shutdown
#[derive(Clone)]
pub struct NamespaceService {
    /// Path for configuration and storage
    config_path: PathBuf,
    /// Map of namespace identifiers to Node instances
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    /// Configuration manager for file paths
    config: ConfigManager,
}

impl NamespaceService {
    /// Creates a new NamespaceService
    ///
    /// # Arguments
    ///
    /// * `path` - Path for configuration and storage
    ///
    /// # Returns
    ///
    /// A new NamespaceService instance
    pub fn new(path: PathBuf) -> Self {
        info!("Creating new NamespaceService with path: {:?}", path);
        
        // Validate path existence
        if !path.exists() {
            info!("Path {:?} does not exist, attempting to create it", path);
            if let Err(e) = std::fs::create_dir_all(&path) {
                error!("Failed to create directory {:?}: {}", path, e);
            } else {
                info!("Successfully created directory: {:?}", path);
            }
        }
        
        // Create configuration manager with detailed logging
        info!("Creating configuration manager for path: {:?}", path);
        let config = new_config_manager(Some(path.clone()));
        info!("Configuration manager created successfully");
        
        // Initialize paths
        let base_dir = config.base_dir();
        info!("Config base directory: {:?}", base_dir);
        
        // Use namespace dir for default namespace
        let default_ns_dir = config.namespace_dir("default");
        info!("Config default namespace directory: {:?}", default_ns_dir);
        
        let logs_dir = config.logs_dir();
        info!("Config logs directory: {:?}", logs_dir);
        
        // Validate directories exist
        if !logs_dir.exists() {
            info!("Logs directory {:?} does not exist, attempting to create it", logs_dir);
            if let Err(e) = std::fs::create_dir_all(&logs_dir) {
                error!("Failed to create logs directory: {}", e);
            } else {
                info!("Successfully created logs directory");
            }
        }
        
        // Ensure default namespace directory exists
        if let Err(e) = config.ensure_namespace_dir("default") {
            error!("Failed to create default namespace directory: {}", e);
        } else {
            info!("Default namespace directory setup complete");
        }
        
        // Initialize nodes map
        let nodes = Arc::new(RwLock::new(HashMap::new()));
        
        // Create namespace locks directory if it doesn't exist
        let locks_path = path.join("locks");
        if !locks_path.exists() {
            info!("Creating namespace locks directory: {:?}", locks_path);
            if let Err(e) = std::fs::create_dir_all(&locks_path) {
                error!("Failed to create locks directory: {}", e);
            }
        }
        
        Self { 
            config_path: path,
            config,
            nodes,
        }
    }
    
    /// Explicitly shutdown the namespace service
    ///
    /// This method ensures that all resources are properly cleaned up
    pub async fn shutdown(&mut self) -> Result<(), BoxError> {
        info!("Shutting down NamespaceService");
        
        // Clean up and flush all nodes
        let mut nodes = self.nodes.write().await;
        for (namespace, mut node) in nodes.drain() {
            info!(namespace=%namespace, "Shutting down node");
            if let Err(e) = node.unload_index().await {
                warn!(namespace=%namespace, "Error unloading node index: {}", e);
            }
            
            // Remove namespace lock
            if let Err(e) = self.remove_namespace_lock(&namespace).await {
                warn!(namespace=%namespace, error=%e, "Failed to remove namespace lock during shutdown");
            }
        }
        
        // Longer sleep to ensure resources are released between tests
        // This helps avoid database lock contention in test environments
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        
        info!("NamespaceService shutdown complete");
        Ok(())
    }
    
    /// Check if a namespace is locked by another process
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to check
    ///
    /// # Returns
    ///
    /// Ok(()) if namespace is not locked, Err if it is locked by another process
    async fn check_namespace_lock(&self, namespace: &str) -> Result<(), BoxError> {
        let lock_file = self.config_path.join("locks").join(format!("{}.lock", namespace));
        
        // Check if lock file exists
        if lock_file.exists() {
            // Read the lock file to get the process ID
            match tokio::fs::read_to_string(&lock_file).await {
                Ok(content) => {
                    // Parse PID and timestamp
                    let parts: Vec<&str> = content.trim().split(':').collect();
                    if parts.len() >= 2 {
                        if let Ok(pid) = parts[0].parse::<u32>() {
                            // Check if process is still running
                            #[cfg(unix)]
                            {
                                use std::os::unix::process::CommandExt;
                                let output = std::process::Command::new("ps")
                                    .arg("-p")
                                    .arg(pid.to_string())
                                    .output();
                                
                                match output {
                                    Ok(output) => {
                                        let output_str = String::from_utf8_lossy(&output.stdout);
                                        // If process is still running, ps will return more than just the header
                                        if output_str.lines().count() > 1 {
                                            return Err(format!("Namespace '{}' is locked by process {}", namespace, pid).into());
                                        } else {
                                            // Process not running, we can remove the stale lock
                                            if let Err(e) = tokio::fs::remove_file(&lock_file).await {
                                                warn!(namespace=%namespace, error=%e, "Failed to remove stale lock file");
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        warn!(namespace=%namespace, error=%e, "Failed to check if process is running");
                                        // Assume lock is stale if we can't check
                                        if let Err(e) = tokio::fs::remove_file(&lock_file).await {
                                            warn!(namespace=%namespace, error=%e, "Failed to remove stale lock file");
                                        }
                                    }
                                }
                            }
                            
                            #[cfg(windows)]
                            {
                                let output = std::process::Command::new("tasklist")
                                    .arg("/FI")
                                    .arg(format!("PID eq {}", pid))
                                    .arg("/NH")
                                    .output();
                                
                                match output {
                                    Ok(output) => {
                                        let output_str = String::from_utf8_lossy(&output.stdout);
                                        // If process is still running, tasklist will return a non-empty result
                                        if !output_str.trim().is_empty() {
                                            return Err(format!("Namespace '{}' is locked by process {}", namespace, pid).into());
                                        } else {
                                            // Process not running, we can remove the stale lock
                                            if let Err(e) = tokio::fs::remove_file(&lock_file).await {
                                                warn!(namespace=%namespace, error=%e, "Failed to remove stale lock file");
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        warn!(namespace=%namespace, error=%e, "Failed to check if process is running");
                                        // Assume lock is stale if we can't check
                                        if let Err(e) = tokio::fs::remove_file(&lock_file).await {
                                            warn!(namespace=%namespace, error=%e, "Failed to remove stale lock file");
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    warn!(namespace=%namespace, error=%e, "Failed to read lock file, assuming stale");
                    // Try to remove the stale lock file
                    if let Err(e) = tokio::fs::remove_file(&lock_file).await {
                        warn!(namespace=%namespace, error=%e, "Failed to remove stale lock file");
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Add a namespace lock for this process
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to lock
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    async fn add_namespace_lock(&self, namespace: &str) -> Result<(), BoxError> {
        let lock_file = self.config_path.join("locks").join(format!("{}.lock", namespace));
        
        // Get current process ID, listen address, and timestamp
        let pid = std::process::id();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // Get listening address for this server instance - used for routing
        // Default to localhost:50051 if we can't determine
        let server_address = self.get_listen_address().unwrap_or_else(|| "localhost:50051".to_string());
        
        // Write lock file with process ID, server address and timestamp
        let content = format!("{}:{}:{}", pid, server_address, timestamp);
        tokio::fs::write(&lock_file, content).await?;
        
        info!(namespace=%namespace, pid=%pid, address=%server_address, "Added namespace lock");
        Ok(())
    }
    
    /// Get the listening address for this server instance
    ///
    /// # Returns
    ///
    /// Option containing the server address or None if not available
    fn get_listen_address(&self) -> Option<String> {
        // In a production system, this would be configured or determined dynamically
        // For simplicity, we're using a default value
        // TODO: Make this configurable or determined from actual socket info
        Some("localhost:50051".to_string())
    }
    
    /// Discover the server that has a given namespace locked
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to check
    ///
    /// # Returns
    ///
    /// Option containing the server address or None if not found
    async fn discover_namespace_server(&self, namespace: &str) -> Option<String> {
        let lock_file = self.config_path.join("locks").join(format!("{}.lock", namespace));
        
        // Check if lock file exists
        if !lock_file.exists() {
            return None;
        }
        
        // Read the lock file to get the server address
        match tokio::fs::read_to_string(&lock_file).await {
            Ok(content) => {
                // Parse PID, server address and timestamp
                let parts: Vec<&str> = content.trim().split(':').collect();
                if parts.len() >= 2 {
                    if parts.len() >= 3 {
                        // Format is PID:ADDRESS:TIMESTAMP
                        return Some(parts[1].to_string());
                    } else {
                        // Old format without server address
                        warn!(namespace=%namespace, "Lock file uses old format without server address");
                        return None;
                    }
                }
                None
            },
            Err(e) => {
                warn!(namespace=%namespace, error=%e, "Failed to read lock file for server discovery");
                None
            }
        }
    }
    
    /// Remove a namespace lock for this process
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to unlock
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    async fn remove_namespace_lock(&self, namespace: &str) -> Result<(), BoxError> {
        let lock_file = self.config_path.join("locks").join(format!("{}.lock", namespace));
        
        // Check if lock file exists
        if lock_file.exists() {
            // Read the lock file to make sure it's our lock
            match tokio::fs::read_to_string(&lock_file).await {
                Ok(content) => {
                    // Parse PID
                    let parts: Vec<&str> = content.trim().split(':').collect();
                    if parts.len() >= 1 {
                        if let Ok(pid) = parts[0].parse::<u32>() {
                            // Only remove the lock if it belongs to this process
                            if pid == std::process::id() {
                                if let Err(e) = tokio::fs::remove_file(&lock_file).await {
                                    warn!(namespace=%namespace, error=%e, "Failed to remove lock file");
                                    return Err(e.into());
                                }
                                info!(namespace=%namespace, "Removed namespace lock");
                            } else {
                                warn!(namespace=%namespace, lock_pid=%pid, our_pid=%std::process::id(), 
                                    "Not removing lock file owned by different process");
                            }
                        }
                    }
                },
                Err(e) => {
                    warn!(namespace=%namespace, error=%e, "Failed to read lock file before removal");
                    return Err(e.into());
                }
            }
        }
        
        Ok(())
    }
    
    /// Gets or creates a node for the given namespace
    ///
    /// This method:
    /// - Retrieves an existing node if available
    /// - Creates a new node if one doesn't exist
    /// - Ensures namespace isolation
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace identifier
    ///
    /// # Returns
    ///
    /// A Node instance for the requested namespace
    async fn get_node(&self, namespace: &str) -> Node {
        let mut nodes = self.nodes.write().await;
        
        // First check if we already have this node in our map
        if let Some(node) = nodes.get(namespace) {
            return node.clone();
        }
        
        // Check if namespace has been occupied by another process
        if let Err(e) = self.check_namespace_lock(namespace).await {
            info!(namespace=%namespace, error=%e, "Namespace is locked by another process");
            
            // Try to discover the server that has this namespace
            if let Some(server_info) = self.discover_namespace_server(namespace).await {
                info!(namespace=%namespace, address=%server_info, "Found server with this namespace");
                
                // Return a node with a shutdown flag set to true to indicate it shouldn't be used
                let mut error_node = crate::fugu::node::new(
                    namespace.to_string(),
                    Some(self.config_path.clone())
                );
                error_node.set_shutdown(true);
                return error_node;
            } else {
                // If we can't find the server, return an error node
                error!(namespace=%namespace, "Cannot locate server for locked namespace");
                
                // Return a node with a shutdown flag set to true to indicate it shouldn't be used
                let mut error_node = crate::fugu::node::new(
                    namespace.to_string(),
                    Some(self.config_path.clone())
                );
                error_node.set_shutdown(true);
                return error_node;
            }
        }
        
        // Node doesn't exist and isn't locked by another process, create a new one
        let mut node = crate::fugu::node::new(
            namespace.to_string(), 
            Some(self.config_path.clone())
        );
        
        // Register this namespace as occupied by this process BEFORE loading the index
        // This ensures another process won't try to access it while we're loading
        if let Err(e) = self.add_namespace_lock(namespace).await {
            warn!(namespace=%namespace, error=%e, "Failed to register namespace lock");
            
            // If we can't lock the namespace, set shutdown flag and return
            node.set_shutdown(true);
            return node;
        }
        
        // Ensure the index is loaded before returning
        if let Err(e) = node.load_index().await {
            warn!(namespace=%namespace, error=%e, "Error loading node index, but will continue");
        }
        
        // Store the node in our map
        nodes.insert(namespace.to_string(), node.clone());
        node
    }
    
    /// Unloads a node from memory
    ///
    /// This method:
    /// - Removes the node from the active nodes map
    /// - Flushes any pending changes to disk
    /// - Frees memory resources
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace identifier
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    async fn unload_node(&self, namespace: &str) -> Result<(), BoxError> {
        let mut nodes = self.nodes.write().await;
        
        if let Some(mut node) = nodes.remove(namespace) {
            node.unload_index().await.map_err(node_err_to_box)?;
            
            // Remove namespace lock when unloading
            if let Err(e) = self.remove_namespace_lock(namespace).await {
                warn!(namespace=%namespace, error=%e, "Failed to remove namespace lock during unload");
            }
        }
        
        Ok(())
    }

    /// Creates a temporary file for indexing from a streamed chunk
    ///
    /// This helper method processes stream chunks and creates a temporary file
    /// that can be indexed by the system.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream of file chunks
    /// * `temp_dir` - The temporary directory to write to
    ///
    /// # Returns
    ///
    /// Tuple containing file info and stats: (file_name, namespace, file_path, bytes, chunks)
    async fn process_stream_chunks(
        &self,
        mut stream: Streaming<StreamIndexChunk>,
        temp_dir: &tempfile::TempDir,
    ) -> Result<(String, String, PathBuf, i64, i64), Status> {
        // Initialize tracking variables
        let mut file_name: Option<String> = None;
        let mut namespace_name = "default".to_string();
        let mut total_bytes: i64 = 0;
        let mut chunks_received: i64 = 0;
        
        // Create a temporary file to write the chunks to
        let mut temp_file = None;
        
        // Process the stream chunks
        while let Some(chunk) = stream.message().await? {
            chunks_received += 1;
            total_bytes += chunk.chunk_data.len() as i64;
            
            // Process file_name from the first chunk
            if file_name.is_none() {
                if chunk.file_name.is_empty() {
                    return Err(Status::invalid_argument(
                        "First chunk must contain file_name"
                    ));
                }
                
                file_name = Some(chunk.file_name.clone());
                
                // Get namespace either from chunk or use default
                if !chunk.namespace.is_empty() {
                    namespace_name = chunk.namespace.clone();
                }
                
                // Create the temporary file for writing
                let path = temp_dir.path().join(chunk.file_name.clone());
                temp_file = Some(tokio::fs::File::create(&path).await.map_err(|e| {
                    Status::internal(format!("Failed to create temporary file: {}", e))
                })?);
                
                trace!(path=?path, "Created temporary file for streaming");
            }
            
            // Write the chunk to the temporary file
            if let Some(file) = &mut temp_file {
                file.write_all(&chunk.chunk_data).await.map_err(|e| {
                    Status::internal(format!("Error writing to temporary file: {}", e))
                })?;
            } else {
                return Err(Status::internal("Temporary file not initialized"));
            }
            
            // If this is the last chunk, finish processing
            if chunk.is_last {
                trace!("Received final chunk, processing file now");
                break;
            }
        }
        
        // Flush and close the file
        if let Some(mut file) = temp_file {
            file.flush().await.map_err(|e| {
                Status::internal(format!("Error flushing temporary file: {}", e))
            })?;
        } else {
            return Err(Status::internal("No data received or temporary file not initialized"));
        }
        
        // Unwrap the file_name, should be safe since we checked earlier
        let file_name = file_name.ok_or_else(|| Status::internal("No file name received"))?;
        let temp_file_path = temp_dir.path().join(&file_name);
        
        Ok((file_name, namespace_name, temp_file_path, total_bytes, chunks_received))
    }

    /// Creates and indexes a minimal content file to ensure searchability
    ///
    /// This helper method adds common terms to the index to ensure minimal
    /// searchability for large files, especially binaries.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to index in
    /// * `temp_dir` - The temporary directory to write to
    /// * `file_name` - The name of the main file being indexed
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    async fn add_minimal_searchable_content(
        &self,
        node: &Node,
        temp_dir: &tempfile::TempDir,
        _file_name: &str, // Prefixed with underscore as it's not used
    ) -> Result<(), Status> {
        // Add some minimal text content to ensure we have something indexable
        // For safety, append these terms to the file to make it searchable
        let common_terms = "the and in of to a is it for on with as by at from";
        let minimal_content_path = temp_dir.path().join("minimal_content.txt");
        
        if let Err(e) = tokio::fs::write(&minimal_content_path, common_terms).await {
            warn!(error=%e, "Failed to create minimal content file");
            return Ok(());  // Continue even if this fails
        }
        
        // Index the minimal content file in the same namespace for search
        let minimal_indexing_result = match node.index_file(minimal_content_path.clone()).await {
            Ok(result) => result,
            Err(e) => {
                warn!(error=%e, "Failed to index minimal content file");
                std::time::Duration::default()
            }
        };
        
        info!(time=?minimal_indexing_result, "Indexed minimal searchable content");
        Ok(())
    }
}

#[tonic::async_trait]
impl Namespace for NamespaceService {
    async fn index(
        &self,
        request: Request<IndexRequest>,
    ) -> Result<Response<IndexResponse>, Status> {
        // Extract namespace from the request metadata if available
        let namespace = request.metadata().get("namespace")
            .and_then(|ns| ns.to_str().ok())
            .unwrap_or("default")
            .to_string(); // Clone to avoid borrowing request
            
        let request_inner = request.into_inner();
        let file = request_inner.file.clone().unwrap_or_default();

        // Log the received request
        info!(namespace=%namespace, file=%file.name, "[gRPC Request] index request received");

        // Create a temporary file for indexing
        let temp_dir = tempfile::tempdir().map_err(|e| {
            Status::internal(format!("Failed to create temporary directory: {}", e))
        })?;
        
        let temp_file_path = temp_dir.path().join(&file.name);
        
        // Write content to temporary file
        tokio::fs::write(&temp_file_path, &file.body).await.map_err(|e| {
            Status::internal(format!("Failed to write temporary file: {}", e))
        })?;

        // Get or create a node for this namespace
        let mut node = self.get_node(&namespace).await;
        
        // Load the index (this won't do anything if already loaded)
        node.load_index().await.map_err(|e| {
            Status::internal(format!("Failed to load index: {}", e))
        })?;
        
        // Index the file
        let indexing_result = node.index_file(temp_file_path.clone()).await.map_err(|e| {
            Status::internal(format!("Failed to index file: {}", e))
        })?;
        
        // Build the enhanced response with indexing details
        let response = IndexResponse {
            success: true,
            location: format!("/{}", file.name),
            bytes_received: file.body.len() as i64,
            chunks_received: 1,
            indexed_terms: if let Some(index) = node.get_index() {
                // Get term count from the inverted index if available
                index.get_total_terms() as i64
            } else {
                0
            },
            indexing_time_ms: indexing_result.as_millis() as i64,
            indexing_status: "completed".to_string(),
        };

        info!(namespace=%namespace, file=%file.name, time=?indexing_result, "[gRPC Response] index request completed");
        
        // Clean up the temporary directory
        drop(temp_dir);
        
        Ok(Response::new(response))
    }
    
    /// Stream-based file indexing for large files
    ///
    /// This method:
    /// - Receives a stream of file chunks
    /// - Assembles them into a temporary file
    /// - Indexes the complete file
    /// - Cleans up resources appropriately
    async fn stream_index(
        &self,
        request: Request<Streaming<StreamIndexChunk>>,
    ) -> Result<Response<IndexResponse>, Status> {
        info!("[gRPC Request] stream_index request received");
        
        // Remove all non-Send references
        let stream = request.into_inner();
        
        // Create a temporary directory for the streamed file
        let temp_dir = tempfile::tempdir().map_err(|e| {
            Status::internal(format!("Failed to create temporary directory: {}", e))
        })?;
        
        // Process the incoming stream chunks
        let (file_name, namespace_name, temp_file_path, total_bytes, chunks_received) = 
            self.process_stream_chunks(stream, &temp_dir).await?;
        
        // Get or create a node for this namespace
        let mut node = self.get_node(&namespace_name).await;
        
        // Load the index (this won't do anything if already loaded)
        node.load_index().await.map_err(|e| {
            Status::internal(format!("Failed to load index: {}", e))
        })?;
        
        // Index the file
        info!(file=%file_name, "Starting indexing of streamed file");
        
        // Read the file size for debugging
        let file_size = match tokio::fs::metadata(&temp_file_path).await {
            Ok(metadata) => metadata.len(),
            Err(e) => {
                warn!(error=%e, "Unable to get metadata for streamed file");
                0
            }
        };
        info!(size=%file_size, "File ready for indexing");
        
        // For large files (> 512KB), use parallel indexing with CRDT
        const PARALLEL_THRESHOLD: u64 = 512 * 1024; // 512KB
        
        // Add some minimal text content to ensure we have something indexable
        self.add_minimal_searchable_content(&node, &temp_dir, &file_name).await?;
        
        // Now index the main file, using parallel processing for large files
        let indexing_result = if file_size > PARALLEL_THRESHOLD {
            info!(size=%file_size, "Using parallel indexing for large file");
            
            // Use parallel processing with CRDTs for large files
            let indexer = ParallelIndexer::new();
            match indexer.index_file(&node, &temp_file_path, &file_name).await {
                Ok(result) => result,
                Err(parallel_error) => {
                    // Clone error message and drop the original error to avoid Send issues
                    let error_msg = format!("{}", parallel_error);
                    // Send the original error to ensure it's dropped
                    drop(parallel_error);
                    warn!(error=%error_msg, "Parallel indexing failed, falling back to regular indexing");
                    match node.index_file(temp_file_path.clone()).await {
                        Ok(result) => result,
                        Err(e_fallback) => {
                            error!(file=%file_name, error=%e_fallback, "Error indexing streamed file");
                            return Err(Status::internal(format!("Failed to index file: {}", e_fallback)));
                        }
                    }
                }
            }
        } else {
            // Use standard indexing for smaller files
            match node.index_file(temp_file_path.clone()).await {
                Ok(result) => result,
                Err(e_standard) => {
                    error!(file=%file_name, error=%e_standard, "Error indexing streamed file");
                    return Err(Status::internal(format!("Failed to index file: {}", e_standard)));
                }
            }
        };
        
        // Build the response
        let response = IndexResponse {
            success: true,
            location: format!("/{}", file_name),
            bytes_received: total_bytes,
            chunks_received,
            indexed_terms: if let Some(index) = node.get_index() {
                // Get term count from the inverted index if available
                index.get_total_terms() as i64
            } else {
                0
            },
            indexing_time_ms: indexing_result.as_millis() as i64,
            indexing_status: "completed".to_string(),
        };
        
        info!(
            namespace=%namespace_name,
            file=%file_name, 
            bytes=%total_bytes,
            chunks=%chunks_received,
            time=?indexing_result,
            "[gRPC Response] stream_index request completed"
        );
        
        // Clean up the temporary directory
        drop(temp_dir);
        
        Ok(Response::new(response))
    }

    /// Delete a file from the index
    ///
    /// This method:
    /// - Removes the specified file from the index
    /// - Updates the index state
    /// - Confirms the deletion was successful
    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        // Get namespace string from metadata and clone it
        let namespace_str = match request.metadata().get("namespace") {
            Some(ns) => match ns.to_str() {
                Ok(ns_str) => ns_str.to_string(),
                Err(_) => "default".to_string(),
            },
            None => "default".to_string(),
        };
        
        // Extract data from request
        let request_inner = request.into_inner();
        let location = request_inner.location;
        
        info!(namespace=%namespace_str, location=%location, "[gRPC Request] delete request received");
        
        // Get or create a node for this namespace
        let mut node = self.get_node(&namespace_str).await;
        
        // Load the index (this won't do anything if already loaded)
        node.load_index().await.map_err(|e| {
            Status::internal(format!("Failed to load index: {}", e))
        })?;
        
        // Delete the file from the index
        let doc_id = location.trim_start_matches('/');
        match node.delete_file(doc_id).await {
            Ok(_) => {
                let response = DeleteResponse {
                    success: true,
                    message: format!("Deleted {}", location),
                };
                
                info!(namespace=%namespace_str, location=%location, "[gRPC Response] delete successful");
                Ok(Response::new(response))
            },
            Err(e) => {
                let response = DeleteResponse {
                    success: false,
                    message: format!("Error deleting {}: {}", location, e),
                };
                
                warn!(namespace=%namespace_str, location=%location, error=%e, "[gRPC Response] delete failed");
                Ok(Response::new(response))
            }
        }
    }

    /// Search the index for matching documents
    ///
    /// This method:
    /// - Takes a query string
    /// - Searches the namespace index
    /// - Returns matching documents with scores and snippets
    /// - Supports pagination with limit and offset
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        // Get namespace string from metadata and clone it
        let namespace_str = match request.metadata().get("namespace") {
            Some(ns) => match ns.to_str() {
                Ok(ns_str) => ns_str.to_string(),
                Err(_) => "default".to_string(),
            },
            None => "default".to_string(),
        };
        
        // Extract data from request
        let request_inner = request.into_inner();
        let query = request_inner.query;
        let limit = request_inner.limit;
        let offset = request_inner.offset;
        
        info!(namespace=%namespace_str, query=%query, limit=%limit, offset=%offset, "[gRPC Request] search request received");
        
        // Get or create a node for this namespace
        let mut node = self.get_node(&namespace_str).await;
        
        // Load the index (this won't do anything if already loaded)
        node.load_index().await.map_err(|e| {
            Status::internal(format!("Failed to load index: {}", e))
        })?;
        
        // Search the index
        match node.search_text(&query, limit as usize, offset as usize).await {
            Ok((search_results, _elapsed)) => {
                // Convert the search results to the gRPC response format
                let results: Vec<SearchResult> = search_results.iter().map(|sr| {
                    // Create a snippet for each result (extract from term matches if available)
                    let snippet = if !sr.term_matches.is_empty() {
                        let text = format!("Matches found for: {}", 
                            sr.term_matches.keys().take(3).cloned().collect::<Vec<_>>().join(", "));
                        
                        Some(Snippet {
                            path: sr.doc_id.clone(),
                            text,
                            start: 0,
                            end: 0,
                        })
                    } else {
                        None
                    };
                    
                    SearchResult {
                        path: sr.doc_id.clone(),
                        score: sr.relevance_score as f32,
                        snippet,
                    }
                }).collect();
                
                let total = search_results.len() as i32;
                let response = SearchResponse {
                    results,
                    total,
                    message: format!("Found {} results for query: {}", total, query),
                };
                
                info!(namespace=%namespace_str, query=%query, results=%total, "[gRPC Response] search successful");
                Ok(Response::new(response))
            },
            Err(e) => {
                let response = SearchResponse {
                    results: Vec::new(),
                    total: 0,
                    message: format!("Error searching for {}: {}", query, e),
                };
                
                warn!(namespace=%namespace_str, query=%query, error=%e, "[gRPC Response] search failed");
                Ok(Response::new(response))
            }
        }
    }

    /// Search the index using vector similarity
    ///
    /// This method:
    /// - Takes a vector of floating point values
    /// - Performs similarity search in vector space
    /// - Returns matching documents with similarity scores
    /// - Supports filtering by minimum score threshold
    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSearchResponse>, Status> {
        // Get namespace string from metadata and clone it
        let namespace_str = match request.metadata().get("namespace") {
            Some(ns) => match ns.to_str() {
                Ok(ns_str) => ns_str.to_string(),
                Err(_) => "default".to_string(),
            },
            None => "default".to_string(),
        };
        
        // Extract data from request
        let request_inner = request.into_inner();
        let vector = request_inner.vector;
        let dim = request_inner.dim;
        let limit = request_inner.limit;
        let offset = request_inner.offset;
        let min_score = request_inner.min_score;
        
        info!(
            namespace=%namespace_str, 
            dim=%dim, 
            limit=%limit, 
            offset=%offset, 
            min_score=%min_score, 
            "[gRPC Request] vector_search request received"
        );
        
        // Get or create a node for this namespace
        let mut node = self.get_node(&namespace_str).await;
        
        // Load the index (this won't do anything if already loaded)
        node.load_index().await.map_err(|e| {
            Status::internal(format!("Failed to load index: {}", e))
        })?;
        
        // Check if the vector dimensions match what we expect
        if vector.len() != dim as usize {
            return Err(Status::invalid_argument(
                format!("Vector dimension mismatch: got {}, expected {}", vector.len(), dim)
            ));
        }
        
        // Vector search is not implemented yet in this version
        // Return a not implemented error for now
        let response = VectorSearchResponse {
            results: Vec::new(),
            total: 0,
            message: "Vector search not implemented yet".to_string(),
        };
        
        warn!(namespace=%namespace_str, "[gRPC Response] vector_search not implemented");
        
        Ok(Response::new(response))
    }
}

/// Starts the gRPC server for remote access to Fugu
///
/// This function:
/// - Initializes the gRPC server with the namespace service
/// - Handles graceful shutdown signals
/// - Provides timeouts to prevent hanging
/// - Ensures proper cleanup on shutdown
///
/// # Arguments
///
/// * `path` - Path for configuration and storage
/// * `addr` - Address to bind the server to (format: "ip:port")
/// * `ready_tx` - Optional channel to signal when server is ready
/// * `shutdown_rx` - Optional channel to receive shutdown signal
///
/// # Returns
///
/// Result indicating success or error
pub async fn start_grpc_server(
    path: PathBuf,
    addr: String,
    ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
    shutdown_rx: Option<tokio::sync::oneshot::Receiver<()>>,
) -> Result<(), BoxError> {
    // More detailed tracing for server startup debugging
    info!("Starting gRPC server initialization with path: {:?}", path);
    
    // Try to find an available port if the default one is in use
    let parsed_addr = match addr.parse() {
        Ok(parsed_addr) => {
            info!("Successfully parsed server address: {}", parsed_addr);
            parsed_addr
        },
        Err(e) => {
            error!("Failed to parse server address '{}': {}", addr, e);
            return Err(Box::new(e));
        }
    };
    
    // Extract host and port from the address string
    let addr_parts: Vec<&str> = addr.split(':').collect();
    if addr_parts.len() != 2 {
        let err = std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Invalid address format: {}", addr)
        );
        error!("Address format error: {}", err);
        return Err(Box::new(err));
    }
    
    // Trace the namespace service creation
    let namespace_service = NamespaceService::new(path);
    
    // Signal that the server is ready
    if let Some(tx) = ready_tx {
        info!("Sending ready signal");
        match tx.send(()) {
            Ok(_) => info!("Ready signal sent successfully"),
            Err(_) => warn!("Failed to send ready signal, receiver may have been dropped"),
        }
    }

    // Use graceful shutdown if a receiver was provided
    if let Some(shutdown_signal) = shutdown_rx {
        run_server_with_shutdown(namespace_service, parsed_addr, shutdown_signal).await
    } else {
        // No shutdown signal provided - create internal timeout-based shutdown
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        run_server_with_timeout(namespace_service, addr, rx).await
    }
}

// Helper function to run server with explicit shutdown signal
async fn run_server_with_shutdown(
    namespace_service: NamespaceService,
    addr: std::net::SocketAddr,
    shutdown_signal: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), BoxError> {
    info!("Setting up server with shutdown receiver");
    let mut namespace_service_clone = namespace_service.clone();
    
    // Create the server builder with detailed logging
    let mut server_builder = Server::builder();
    
    info!("Adding namespace service to server");
    let server_with_service = server_builder.add_service(NamespaceServer::new(namespace_service));
    
    info!("Starting server on {}", addr);
    
    // Start the server with shutdown
    let server_fut = server_with_service.serve_with_shutdown(addr, async move {
        // Wait for the shutdown signal
        info!("Server waiting for shutdown signal");
        let shutdown_result = shutdown_signal.await;
        match shutdown_result {
            Ok(_) => info!("Shutdown signal received normally"),
            Err(_) => info!("Shutdown signal sender was dropped"),
        }
        
        info!("Graceful shutdown signal received, unloading all indices and shutting down server");
        
        // Use our new shutdown method for clean shutdown
        info!("Starting namespace service shutdown process with 10s timeout");
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(10), // 10 second timeout for full shutdown
            namespace_service_clone.shutdown()
        ).await {
            Ok(shutdown_result) => {
                match shutdown_result {
                    Ok(_) => info!("NamespaceService graceful shutdown completed successfully"),
                    Err(inner_err) => error!("Error during NamespaceService shutdown: {}", inner_err),
                }
            },
            Err(_) => error!("Timeout during NamespaceService shutdown"),
        }
        info!("Shutdown handler completed");
    });

    // Await the server future
    match server_fut.await {
        Ok(_) => info!("Server shut down gracefully"),
        Err(e) => {
            error!("Server error during operation: {}", e);
            // Print more detailed error information
            if let Some(source) = e.source() {
                error!("Error source: {}", source);
                if let Some(next_source) = source.source() {
                    error!("Underlying error: {}", next_source);
                }
            }
            if e.to_string().contains("address already in use") {
                error!("Port is already in use. Make sure no other instance is running on port {}", addr);
            }
            return Err(Box::new(e));
        }
    }
    
    info!("gRPC server completed successfully");
    Ok(())
}

// Helper function to run server with timeout
async fn run_server_with_timeout(
    namespace_service: NamespaceService,
    addr: String,
    rx: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), BoxError> {
    info!("No shutdown receiver provided, creating internal timeout-based shutdown");
    let mut namespace_service_clone = namespace_service.clone();
    
    // Create the server builder with detailed logging
    let mut server_builder = Server::builder();
    
    info!("Adding namespace service to server (timeout mode)");
    let server_with_service = server_builder.add_service(NamespaceServer::new(namespace_service));
    
    // Parse the address string to a SocketAddr
    let parsed_addr: std::net::SocketAddr = addr.parse()?;
    
    info!("Starting server on {} with auto-shutdown", addr);
    let server_fut = server_with_service.serve_with_shutdown(parsed_addr, async move {
        info!("Server waiting for either shutdown signal or timeout");
        tokio::select! {
            // Either wait for a signal (that will never come) or timeout after 30 minutes
            _ = rx => {
                info!("Received explicit shutdown signal");
            }, 
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(30 * 60)) => {
                info!("Server auto-shutdown after timeout");
            }
        }
        
        info!("Beginning namespace service shutdown process");
        // Use our new shutdown method for clean shutdown
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(10), // 10 second timeout for full shutdown
            namespace_service_clone.shutdown()
        ).await {
            Ok(shutdown_result) => {
                match shutdown_result {
                    Ok(_) => info!("NamespaceService graceful shutdown completed successfully"),
                    Err(inner_err) => error!("Error during NamespaceService shutdown: {}", inner_err),
                }
            },
            Err(_) => error!("Timeout during NamespaceService shutdown"),
        }
        info!("Timeout-based shutdown handler completed");
    });

    // Await the server future
    match server_fut.await {
        Ok(_) => info!("Server shut down gracefully (timeout mode)"),
        Err(e) => {
            error!("Server error during operation (timeout mode): {}", e);
            // Print more detailed error information
            if let Some(source) = e.source() {
                error!("Error source: {}", source);
                if let Some(next_source) = source.source() {
                    error!("Underlying error: {}", next_source);
                }
            }
            if e.to_string().contains("address already in use") {
                error!("Port is already in use. Make sure no other instance is running on port {}", addr);
            }
            return Err(Box::new(e));
        }
    }

    info!("gRPC server start_grpc_server function completed successfully");
    Ok(())
}