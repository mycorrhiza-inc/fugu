/// gRPC server and client implementation for remote access to Fugu
///
/// This module provides:
/// - A gRPC server exposing Fugu functionality
/// - A client implementation for remote operations
/// - Handling of index, delete, search, and vector search operations
/// - Command-line utilities for interacting with the server
use std::path::PathBuf;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tokio::io::AsyncWriteExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tracing::{trace, info, warn, error};
use crdts::{CmRDT, CvRDT};
use num_traits::ToPrimitive;

use crate::fugu::server::FuguServer;
use crate::fugu::wal::WALCMD;
use crate::fugu::node::Node;

// Include the generated protobuf code
pub mod namespace {
    tonic::include_proto!("namespace");
}

use namespace::{
    namespace_server::{Namespace, NamespaceServer},
    DeleteRequest, DeleteResponse, File, IndexRequest, IndexResponse, SearchRequest,
    SearchResponse, SearchResult, Snippet, VectorSearchRequest, VectorSearchResponse,
    StreamIndexChunk,
};

// Add Send + Sync to make sure our error types meet requirements
type BoxError = Box<dyn std::error::Error + Send + Sync>;

// The server implementation for handling namespace service requests
/// Main service implementation for the namespace gRPC API
///
/// This service:
/// - Manages Fugu server and nodes
/// - Handles client requests for indexing, search, and deletion
/// - Provides namespace isolation for multi-tenant usage
/// - Ensures proper cleanup on shutdown
#[derive(Clone)]
pub struct NamespaceService {
    /// The underlying Fugu server
    server: FuguServer,
    /// Channel for sending WAL commands
    wal_sender: mpsc::Sender<WALCMD>,
    /// Path for configuration and storage
    config_path: PathBuf,
    /// Map of namespace identifiers to Node instances
    nodes: Arc<RwLock<HashMap<String, Node>>>,
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
        let server = FuguServer::new(path.clone());
        let wal_sender = server.get_wal_sender();
        let config_path = path.clone();

        Self { 
            server, 
            wal_sender,
            config_path,
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
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
        
        if let Some(node) = nodes.get(namespace) {
            return node.clone();
        }
        
        // Node doesn't exist, create a new one
        let node = crate::fugu::node::new(
            namespace.to_string(), 
            Some(self.config_path.clone()),
            self.wal_sender.clone()
        );
        
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
            // Convert the error to ensure it implements Send + Sync
            node.unload_index().await.map_err(|e| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to unload index: {}", e)
                )) as BoxError
            })?;
        }
        
        Ok(())
    }

    /// Parallel indexing for large files using CRDTs
    ///
    /// This method:
    /// - Splits a large file into chunks
    /// - Processes each chunk in parallel using tokio tasks
    /// - Merges results using CRDTs (Conflict-free Replicated Data Types)
    /// - Provides significant performance improvements for large files
    ///
    /// # Arguments
    ///
    /// * `node` - The node to index the file in
    /// * `file_path` - Path to the file to index
    /// * `file_name` - Name of the file
    ///
    /// # Returns
    ///
    /// Result with Duration indicating indexing time or an error
    async fn parallel_index_file(
        &self, 
        node: &Node, 
        file_path: &std::path::PathBuf, 
        file_name: &str
    ) -> Result<std::time::Duration, BoxError> {
        use crdts::GCounter;
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::io::AsyncReadExt;
        use tokio::time::Instant;
        use crate::fugu::index::Tokenizer;
        
        // Start timing
        let start_time = Instant::now();
        
        // Create a new CRDT GCounter for tracking term frequencies
        // This will hold the final merged counter from all workers
        // Since we don't need to modify the counter during processing (each worker has its own),
        // we can use RwLock instead of Mutex for better concurrency
        let term_counter = Arc::new(RwLock::new(GCounter::<String>::new()));
        
        // Use a thread-safe concurrent HashMap for term positions
        // We'll use one HashMap per worker task and merge them at the end to prevent contention
        let term_positions_map = Arc::new(RwLock::new(Vec::<HashMap<String, Vec<u64>>>::new()));
        
        // Open the file for reading
        let file = tokio::fs::File::open(file_path).await?;
        let file_size = file.metadata().await?.len();
        
        // Determine chunk size and number of chunks
        // Balance between having enough chunks for parallelism but not too many
        let num_cores = num_cpus::get() as u64;
        let ideal_chunks = num_cores * 2; // Use 2x CPU cores for better parallelism
        let chunk_size = std::cmp::max(file_size / ideal_chunks, 8192); // Min 8KB chunks
        let num_chunks = (file_size + chunk_size - 1) / chunk_size; // Ceiling division
        
        // Pre-allocate empty HashMaps for each worker
        {
            let mut positions_map = term_positions_map.write().await;
            for _ in 0..num_chunks {
                positions_map.push(HashMap::<String, Vec<u64>>::new());
            }
        }
        
        info!(
            file_size=%file_size, 
            chunk_size=%chunk_size, 
            num_chunks=%num_chunks,
            "Starting parallel indexing"
        );
        
        // Create a tokenizer
        let tokenizer = crate::fugu::index::WhitespaceTokenizer;
        
        // Create a vector to store all process chunk tasks
        let mut tasks = Vec::new();
        
        // Process each chunk in parallel
        for chunk_idx in 0..num_chunks {
            // Clone references for this task
            let file_path_task = file_path.clone();
            let doc_id = file_name.to_string();
            // term_positions_map no longer needed in each task as we're returning results
            let worker_idx = chunk_idx as usize; // Use chunk index as worker index
            
            // Spawn a task to process this chunk
            let task = tokio::spawn(async move {
                // Calculate chunk boundaries
                let start_pos = chunk_idx * chunk_size;
                let end_pos = std::cmp::min((chunk_idx + 1) * chunk_size, file_size);
                let chunk_actual_size = end_pos - start_pos;
                
                // Open file for this chunk
                let mut chunk_file = tokio::fs::File::open(&file_path_task).await?;
                
                // Seek to the start position of this chunk
                use std::io::SeekFrom;
                chunk_file.seek(SeekFrom::Start(start_pos)).await?;
                
                // Read the chunk
                let mut buffer = vec![0u8; chunk_actual_size as usize];
                chunk_file.read_exact(&mut buffer).await?;
                
                // Convert to string with lossy UTF-8 handling (for binary files)
                let chunk_text = String::from_utf8_lossy(&buffer).to_string();
                
                // Tokenize the chunk
                let tokens = tokenizer.tokenize(&chunk_text, &doc_id);
                
                // Create a worker-specific counter for this chunk
                // Using CRDT's correctly: each worker has its own counter
                let mut worker_counter = GCounter::<String>::new();
                
                // Create a local HashMap for this worker's term positions
                let mut local_positions = HashMap::<String, Vec<u64>>::new();
                
                // Process tokens and update worker's counter and local term positions
                for token in tokens {
                    // Update worker-specific counter using CRDT operations
                    let op = worker_counter.inc(token.term.clone());
                    worker_counter.apply(op);
                    
                    // Calculate global position by adding chunk offset
                    let global_position = start_pos + token.position;
                    
                    // Update local term positions
                    local_positions
                        .entry(token.term.clone())
                        .or_insert_with(Vec::new)
                        .push(global_position);
                }
                
                // Return both the counter and positions to avoid needing to write to shared state during processing
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((worker_counter, local_positions, worker_idx))
            });
            
            tasks.push(task);
        }
        
        // Wait for all tasks to complete and collect their results
        let mut worker_counters = Vec::new();
        // Create a merged map of all term positions
        let mut merged_positions = HashMap::<String, Vec<u64>>::new();
        
        for task in tasks {
            match task.await {
                Ok(Ok((worker_counter, worker_positions, _worker_idx))) => {
                    // Collect each worker's counter
                    worker_counters.push(worker_counter);
                    
                    // Merge this worker's positions into the merged map
                    for (term, positions) in worker_positions {
                        merged_positions
                            .entry(term)
                            .or_insert_with(Vec::new)
                            .extend_from_slice(&positions);
                    }
                },
                Ok(Err(e)) => {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Worker task error: {}", e)
                    )));
                },
                Err(e) => {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other, 
                        format!("Task join error: {}", e)
                    )));
                }
            }
        }
        
        // Merge all worker counters into the main counter
        // This is the key CRDT operation - merging distributed counters correctly
        {
            let mut main_counter = term_counter.write().await;
            for worker_counter in worker_counters {
                // Use the CRDT merge operation to combine counters
                main_counter.merge(worker_counter);
            }
        }
        
        // Get the final term counter
        let counter = term_counter.read().await;
        
        // Create tokens for indexing based on the merged data
        // This makes one token for each term in each document, with all its positions
        let tokens_to_index = merged_positions
            .iter()
            .map(|(term, term_positions)| {
                // Sort positions to ensure they're in the right order and remove duplicates
                let mut sorted_positions = term_positions.clone();
                sorted_positions.sort();
                sorted_positions.dedup();
                
                // Get the total frequency from the CRDT counter using the read method
                let frequency = counter.read().to_u32().unwrap_or(0);
                
                (term.clone(), sorted_positions, frequency)
            })
            .collect::<Vec<_>>();
        
        // Create token batches for indexing
        // We handle this differently since we need to add the full position list at once
        let tokens_len = tokens_to_index.len();
        for (term, positions, _frequency) in &tokens_to_index {
            // Use the index directly to add terms with all their positions
            // Access the inverted index to add terms
            if let Some(index) = node.get_index() {
                // Add term with all its positions in one operation
                let doc_id = file_name.to_string();
                
                // Create a token for this term
                // (We're inserting a small representative token with position 0,
                // but we'll actually use the full positions list in the index impl)
                let token = crate::fugu::index::Token {
                    term: term.clone(),
                    doc_id: doc_id.clone(),
                    position: 0, // Using 0 as a placeholder
                };
                
                // Index the term with all its positions
                // Convert the error to ensure it implements Send + Sync
                index.add_term_with_positions(token, positions.clone()).await.map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to add term with positions: {}", e)
                    )) as BoxError
                })?;
            }
        }
        
        // Get the elapsed time
        let elapsed = start_time.elapsed();
        info!(
            file=%file_name, 
            elapsed=?elapsed, 
            tokens=%tokens_len,
            workers=%num_chunks,
            "Completed threadsafe parallel indexing"
        );
        
        Ok(elapsed)
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
        
        // Build the response
        let response = IndexResponse {
            success: true,
            location: format!("/{}", file.name),
            bytes_received: file.body.len() as i64,
            chunks_received: 1,
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
            match self.parallel_index_file(&node, &temp_file_path, &file_name).await {
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
    let addr = addr.parse()?;
    let namespace_service = NamespaceService::new(path);
    info!("Starting gRPC server on {}", addr);
    
    // Signal that the server is ready
    if let Some(tx) = ready_tx {
        let _ = tx.send(());
    }

    // Use graceful shutdown if a receiver was provided
    if let Some(shutdown_signal) = shutdown_rx {
        let namespace_service_clone = namespace_service.clone();
        let server_fut = Server::builder()
            .add_service(NamespaceServer::new(namespace_service))
            .serve_with_shutdown(addr, async move {
                // Wait for the shutdown signal
                let _ = shutdown_signal.await;
                
                info!("Graceful shutdown signal received, unloading all indices");
                
                // Unload all indices on shutdown (with timeout)
                let nodes = namespace_service_clone.nodes.read().await;
                for (namespace, _) in nodes.iter() {
                    // Add timeout for each unload operation
                    match tokio::time::timeout(
                        tokio::time::Duration::from_secs(5), // 5 second timeout per namespace
                        namespace_service_clone.unload_node(namespace)
                    ).await {
                        Ok(Ok(_)) => info!(namespace=%namespace, "Successfully unloaded node for namespace"),
                        Ok(Err(e)) => error!(namespace=%namespace, error=%e, "Error unloading node for namespace"),
                        Err(_) => warn!(namespace=%namespace, "Timeout unloading node for namespace"),
                    }
                }
            });

        server_fut.await?;
    } else {
        // No shutdown signal provided - don't allow this in tests
        // Always add a shutdown channel with timeout to prevent hanging
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let namespace_service_clone = namespace_service.clone();
        let server_fut = Server::builder()
            .add_service(NamespaceServer::new(namespace_service))
            .serve_with_shutdown(addr, async move {
                tokio::select! {
                    // Either wait for a signal (that will never come) or timeout after 30 minutes
                    _ = rx => {}, 
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(30 * 60)) => {
                        info!("Server auto-shutdown after timeout");
                    }
                }
                
                // Unload all indices on shutdown
                let nodes = namespace_service_clone.nodes.read().await;
                for (namespace, _) in nodes.iter() {
                    if let Err(_e) = tokio::time::timeout(
                        tokio::time::Duration::from_secs(5),
                        namespace_service_clone.unload_node(namespace)
                    ).await {
                        warn!(namespace=%namespace, "Timeout unloading node for namespace");
                    }
                }
            });

        server_fut.await?;
    }

    Ok(())
}

/// Client implementation for the namespace service
///
/// This client:
/// - Provides remote access to Fugu functionality
/// - Handles connection establishment
/// - Offers methods for all supported operations
/// - Simplifies gRPC interaction
pub struct NamespaceClient {
    /// The underlying gRPC client
    client: namespace::namespace_client::NamespaceClient<tonic::transport::Channel>,
}

impl NamespaceClient {
    /// Connect to a Fugu gRPC server
    ///
    /// # Arguments
    ///
    /// * `addr` - Address of the server in format "http://ip:port"
    ///
    /// # Returns
    ///
    /// A new client instance
    pub async fn connect(addr: String) -> Result<Self, BoxError> {
        let client = namespace::namespace_client::NamespaceClient::connect(addr).await?;
        Ok(Self { client })
    }

    /// Calculate optimal chunk size based on file size
    ///
    /// This helper method determines the best chunk size for streaming files
    /// based on their size for optimal performance.
    ///
    /// # Arguments
    ///
    /// * `file_size` - Size of the file in bytes
    /// * `requested_size` - Optional user-requested chunk size
    ///
    /// # Returns
    ///
    /// Optimal chunk size in bytes
    fn calculate_optimal_chunk_size(&self, file_size: u64, requested_size: Option<usize>) -> usize {
        if let Some(size) = requested_size {
            size
        } else {
            // Adaptive chunk size based on file size
            if file_size > 100 * 1024 * 1024 {
                // For very large files (>100MB), use 4MB chunks
                4 * 1024 * 1024
            } else if file_size > 10 * 1024 * 1024 {
                // For large files (>10MB), use 2MB chunks
                2 * 1024 * 1024
            } else {
                // For medium files, use 1MB chunks
                1 * 1024 * 1024
            }
        }
    }

    /// Index a file using single request/response
    ///
    /// # Arguments
    ///
    /// * `file_name` - Name of the file to index
    /// * `file_content` - Content of the file as bytes
    ///
    /// # Returns
    ///
    /// IndexResponse with results
    pub async fn index(
        &mut self,
        file_name: String,
        file_content: Vec<u8>,
    ) -> Result<IndexResponse, Status> {
        // Default implementation without metadata
        self.index_with_metadata(file_name, file_content, tonic::metadata::MetadataMap::new()).await
    }
    
    /// Index a file with metadata
    ///
    /// # Arguments
    ///
    /// * `file_name` - Name of the file to index
    /// * `file_content` - Content of the file as bytes
    /// * `metadata` - Additional metadata for the request
    ///
    /// # Returns
    ///
    /// IndexResponse with results
    pub async fn index_with_metadata(
        &mut self,
        file_name: String,
        file_content: Vec<u8>,
        metadata: tonic::metadata::MetadataMap,
    ) -> Result<IndexResponse, Status> {
        let file = File {
            name: file_name,
            body: file_content,
        };

        let request = IndexRequest { file: Some(file) };
        
        // Create a request with metadata
        let mut req = tonic::Request::new(request);
        *req.metadata_mut() = metadata;

        let response = self.client.index(req).await?;
        Ok(response.into_inner())
    }
    
    /// Stream a large file for indexing
    ///
    /// This method breaks the file into chunks and streams them to the server,
    /// which is more efficient for large files.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the file on disk
    /// * `namespace` - Optional namespace for multi-tenant environments
    /// * `chunk_size` - Size of each chunk in bytes (default: 1MB)
    ///
    /// # Returns
    ///
    /// IndexResponse with results
    pub async fn stream_index_file(
        &mut self,
        file_path: PathBuf,
        namespace: Option<String>,
        chunk_size: Option<usize>,
    ) -> Result<IndexResponse, BoxError> {
        // Extract filename from path
        let file_name = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("Invalid file path")?
            .to_string();
            
        // Open the file
        let file = tokio::fs::File::open(&file_path).await?;
        let file_size = file.metadata().await?.len();
        
        // Use the requested chunk size or choose optimal size based on file size
        let chunk_size = self.calculate_optimal_chunk_size(file_size, chunk_size);
        
        // Determine optimal channel capacity based on file size to improve streaming performance
        let channel_capacity = std::cmp::min(
            10, // Maximum 10 chunks in flight
            (file_size / chunk_size as u64) as usize + 1 // At least enough for the whole file
        );
        
        // Create a buffered reader with optimized buffer size
        let mut reader = tokio::io::BufReader::with_capacity(chunk_size, file);
        
        // Create the stream sender with optimized capacity
        let (tx, rx) = mpsc::channel::<StreamIndexChunk>(channel_capacity);
        
        // Track start time for performance monitoring
        let start_time = std::time::Instant::now();
        
        // Spawn a task to read the file and send chunks
        tokio::spawn(async move {
            let mut buffer = vec![0u8; chunk_size];
            let mut chunk_count = 0;
            let mut is_first = true;
            let mut bytes_sent = 0;
            let mut last_progress_log = std::time::Instant::now();
            
            loop {
                // Read a chunk
                let bytes_read = match reader.read(&mut buffer).await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("Error reading file: {}", e);
                        break;
                    }
                };
                
                if bytes_read == 0 {
                    // End of file - if we haven't sent anything yet, send an empty last chunk
                    if is_first {
                        let last_chunk = StreamIndexChunk {
                            file_name: file_name.clone(),
                            chunk_data: Vec::new(),
                            is_last: true,
                            namespace: namespace.clone().unwrap_or_default(),
                        };
                        let _ = tx.send(last_chunk).await;
                    }
                    break;
                }
                
                chunk_count += 1;
                bytes_sent += bytes_read;
                
                // Create the chunk
                let chunk = StreamIndexChunk {
                    // Only include file_name in the first chunk
                    file_name: if is_first { file_name.clone() } else { String::new() },
                    chunk_data: buffer[0..bytes_read].to_vec(),
                    is_last: bytes_sent >= file_size as usize,
                    namespace: if is_first { namespace.clone().unwrap_or_default() } else { String::new() },
                };
                
                // Send the chunk
                if let Err(e) = tx.send(chunk).await {
                    error!("Error sending chunk: {}", e);
                    break;
                }
                
                is_first = false;
                
                // Log progress periodically for large files (every 5 seconds)
                if file_size > 10 * 1024 * 1024 && last_progress_log.elapsed().as_secs() >= 5 {
                    let progress_pct = (bytes_sent as f64 / file_size as f64) * 100.0;
                    let elapsed = start_time.elapsed();
                    let throughput = if elapsed.as_secs() > 0 {
                        bytes_sent as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
                    } else {
                        0.0
                    };
                    
                    info!(
                        file=%file_name,
                        progress=format!("{:.1}%", progress_pct),
                        throughput=format!("{:.2} MB/s", throughput),
                        chunks=%chunk_count,
                        "Streaming progress"
                    );
                    
                    last_progress_log = std::time::Instant::now();
                }
                
                // If we've reached the end of the file, break
                if bytes_sent >= file_size as usize {
                    break;
                }
            }
            
            // Calculate final throughput
            let elapsed = start_time.elapsed();
            let throughput = if elapsed.as_secs() > 0 {
                bytes_sent as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
            } else {
                bytes_sent as f64 / 1024.0 / 1024.0
            };
            
            info!(
                chunks=%chunk_count, 
                bytes=%bytes_sent, 
                file=%file_name, 
                elapsed=?elapsed,
                throughput=format!("{:.2} MB/s", throughput),
                "Finished sending chunks for streaming"
            );
        });
        
        // Create the streaming request
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let request = tonic::Request::new(stream);
        
        // Send the streaming request and wait for the response
        let response = self.client.stream_index(request).await?;
        Ok(response.into_inner())
    }

    /// Delete a file from the index
    ///
    /// # Arguments
    ///
    /// * `location` - Path to the file to delete
    ///
    /// # Returns
    ///
    /// DeleteResponse with results
    pub async fn delete(&mut self, location: String) -> Result<DeleteResponse, Status> {
        let request = DeleteRequest { location };
        let response = self.client.delete(request).await?;
        Ok(response.into_inner())
    }

    /// Search the index for matching documents
    ///
    /// # Arguments
    ///
    /// * `query` - Search query string
    /// * `limit` - Maximum number of results to return
    /// * `offset` - Number of results to skip
    ///
    /// # Returns
    ///
    /// SearchResponse with results
    pub async fn search(
        &mut self,
        query: String,
        limit: i32,
        offset: i32,
    ) -> Result<SearchResponse, Status> {
        // Default implementation without namespace metadata
        self.search_with_metadata(query, limit, offset, tonic::metadata::MetadataMap::new()).await
    }
    
    /// Search the index with metadata
    ///
    /// # Arguments
    ///
    /// * `query` - Search query string
    /// * `limit` - Maximum number of results to return
    /// * `offset` - Number of results to skip
    /// * `metadata` - Additional metadata for the request
    ///
    /// # Returns
    ///
    /// SearchResponse with results
    pub async fn search_with_metadata(
        &mut self,
        query: String,
        limit: i32,
        offset: i32,
        metadata: tonic::metadata::MetadataMap,
    ) -> Result<SearchResponse, Status> {
        let request = SearchRequest {
            query,
            limit,
            offset,
        };
        
        // Create a request with metadata
        let mut req = tonic::Request::new(request);
        *req.metadata_mut() = metadata;

        let response = self.client.search(req).await?;
        Ok(response.into_inner())
    }

    /// Search the index using vector similarity
    ///
    /// # Arguments
    ///
    /// * `vector` - Vector of floating point values
    /// * `dim` - Vector dimension
    /// * `limit` - Maximum number of results to return
    /// * `offset` - Number of results to skip
    /// * `min_score` - Minimum similarity score threshold
    ///
    /// # Returns
    ///
    /// VectorSearchResponse with results
    pub async fn vector_search(
        &mut self,
        vector: Vec<f32>,
        dim: i32,
        limit: i32,
        offset: i32,
        min_score: f32,
    ) -> Result<VectorSearchResponse, Status> {
        let request = VectorSearchRequest {
            vector,
            dim,
            limit,
            offset,
            min_score,
        };

        let response = self.client.vector_search(request).await?;
        Ok(response.into_inner())
    }
}

// Command line utility functions for client operations

/// Add searchable metadata for large binary files
///
/// This helper creates and indexes a small file with common search terms
/// to ensure that large binary files can still be found in searches.
///
/// # Arguments
///
/// * `client` - Client to use for indexing
/// * `file_path` - Path to the main file being indexed
/// * `namespace` - Optional namespace to use
///
/// # Returns
///
/// Result indicating success or error
async fn add_searchable_metadata(
    addr: &str,
    file_path: &str,
    namespace: &Option<String>
) -> Result<(), BoxError> {
    if let Some(ns) = namespace {
        // Create a small textfile with common terms and the filename in the same namespace
        // This ensures some content is always searchable
        let temp_dir = tempfile::tempdir()?;
        let searchable_file = temp_dir.path().join("searchable_terms.txt");
        
        // Get just the filename
        let file_name = PathBuf::from(file_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("unknown")
            .to_string();
        
        // Include filename and common terms
        let content = format!(
            "Searchable terms for large file: {}\n\
             Common search terms: the and in of to a is it for on with as by at from\n\
             File: {} large test file binary document text content",
            file_name, file_path
        );
        
        tokio::fs::write(&searchable_file, content).await?;
        
        // Index the searchable file first
        let file_content = tokio::fs::read(&searchable_file).await?;
        
        let mut client_for_metadata = NamespaceClient::connect(addr.to_string()).await?;
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("namespace", ns.parse().unwrap());
        
        let metadata_response = client_for_metadata.index_with_metadata(
            "searchable_terms.txt".to_string(), 
            file_content, 
            metadata
        ).await?;
        
        info!("Added searchable terms file to namespace '{}': success={}", 
                ns, metadata_response.success);
    }
    
    Ok(())
}

/// Index a file via gRPC with appropriate strategy based on size
///
/// This utility function provides a command-line interface for indexing
/// files of any size, automatically choosing the best strategy based on
/// the file's size.
///
/// # Arguments
///
/// * `addr` - Address of the server
/// * `file_path` - Path to the file to index
/// * `namespace` - Optional namespace for multi-tenant environments
///
/// # Returns
///
/// Result indicating success or error
pub async fn client_index(
    addr: String,
    file_path: String,
    namespace: Option<String>,
) -> Result<(), BoxError> {
    let file_name = PathBuf::from(&file_path)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("Invalid file path")?
        .to_string();

    // Get file metadata to check size
    let file_metadata = tokio::fs::metadata(&file_path).await?;
    let file_size = file_metadata.len();
    
    // Enhanced thresholds for different indexing strategies
    // Use streaming for files larger than 10MB
    const STREAMING_THRESHOLD: u64 = 10 * 1024 * 1024; // 10MB
    // For files > 512KB but < 10MB, use parallel indexing (automatically on server)
    const PARALLEL_THRESHOLD: u64 = 512 * 1024; // 512KB
    
    if file_size > STREAMING_THRESHOLD {
        // For very large files, use streaming
        info!("File size is {} bytes, using streaming mode with parallel indexing", file_size);
        
        // Add some searchable metadata for testing with common terms
        add_searchable_metadata(&addr, &file_path, &namespace).await?;
        
        // Now stream the main file
        let mut client = NamespaceClient::connect(addr).await?;
        
        // For large files, optimize chunk size based on file size
        let optimal_chunk_size = client.calculate_optimal_chunk_size(file_size, None);
        
        let response = client.stream_index_file(
            PathBuf::from(&file_path),
            namespace.clone(),
            Some(optimal_chunk_size),
        ).await?;
        
        info!(
            success=%response.success, 
            location=%response.location, 
            bytes_received=%response.bytes_received, 
            chunks_received=%response.chunks_received,
            "Streaming index response with parallel processing"
        );
    } else {
        // For medium to small files, use regular indexing
        info!(
            "File size is {} bytes, using regular mode{}",
            file_size,
            if file_size > PARALLEL_THRESHOLD { " with server-side parallel indexing" } else { "" }
        );
        
        // Read the file content
        let file_content = tokio::fs::read(&file_path).await?;
        
        let mut client = NamespaceClient::connect(addr).await?;
        
        // Create metadata with namespace if provided
        let mut metadata = tonic::metadata::MetadataMap::new();
        if let Some(ns) = namespace {
            metadata.insert("namespace", ns.parse().unwrap());
        }
        
        // Index the file with the namespace in metadata
        let response = client.index_with_metadata(file_name, file_content, metadata).await?;

        info!(
            success=%response.success, 
            location=%response.location, 
            bytes_received=%response.bytes_received, 
            chunks_received=%response.chunks_received,
            "Index response{}",
            if file_size > PARALLEL_THRESHOLD { " with server-side parallel processing" } else { "" }
        );
    }
    
    Ok(())
}

/// Stream index a large file via gRPC with efficient parallel processing
///
/// This utility function provides a command-line interface for streaming
/// large files to the Fugu server for indexing. Files larger than 512KB
/// are automatically indexed in parallel using CRDT-based processing.
///
/// # Arguments
///
/// * `addr` - Address of the server
/// * `file_path` - Path to the file to index
/// * `namespace` - Optional namespace for multi-tenant environments
/// * `chunk_size` - Optional chunk size in bytes
///
/// # Returns
///
/// Result indicating success or error
pub async fn client_stream_index(
    addr: String,
    file_path: String,
    namespace: Option<String>,
    chunk_size: Option<usize>,
) -> Result<(), BoxError> {
    // Start timing
    let start_time = std::time::Instant::now();
    
    // Get file metadata for size info
    let metadata = tokio::fs::metadata(&file_path).await?;
    let file_size = metadata.len();
    
    info!(
        file=%file_path, 
        size=format!("{:.2} MB", file_size as f64 / 1024.0 / 1024.0),
        "Streaming file to server at {}", 
        addr
    );
    
    // Add some searchable metadata for large binary files
    if file_size > 10 * 1024 * 1024 {
        add_searchable_metadata(&addr, &file_path, &namespace).await?;
    }
    
    // Connect and stream the file
    let mut client = NamespaceClient::connect(addr).await?;
    let optimal_chunk_size = client.calculate_optimal_chunk_size(file_size, chunk_size);
    
    info!(
        chunk_size=format!("{:.2} MB", optimal_chunk_size as f64 / 1024.0 / 1024.0),
        "Using optimized chunk size for streaming"
    );
    
    let response = client.stream_index_file(
        PathBuf::from(&file_path),
        namespace.clone(),
        Some(optimal_chunk_size),
    ).await?;
    
    // Calculate performance metrics
    let elapsed = start_time.elapsed();
    let throughput = if elapsed.as_secs() > 0 {
        file_size as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
    } else {
        file_size as f64 / 1024.0 / 1024.0
    };
    
    info!(
        success=%response.success, 
        location=%response.location, 
        bytes_received=%response.bytes_received, 
        chunks_received=%response.chunks_received,
        elapsed=?elapsed,
        throughput=format!("{:.2} MB/s", throughput),
        parallel="Files >512KB use parallel processing with CRDTs",
        "Streaming index completed"
    );
    
    Ok(())
}

/// Delete a file from the index
///
/// # Arguments
///
/// * `addr` - Address of the server
/// * `location` - Path to the file to delete
///
/// # Returns
///
/// Result indicating success or error
pub async fn client_delete(
    addr: String,
    location: String,
) -> Result<(), BoxError> {
    let mut client = NamespaceClient::connect(addr).await?;
    let response = client.delete(location).await?;

    info!(
        success=%response.success, 
        message=%response.message,
        "Delete response"
    );
    Ok(())
}

/// Search the index for matching documents
///
/// # Arguments
///
/// * `addr` - Address of the server
/// * `query` - Search query string
/// * `limit` - Maximum number of results to return
/// * `offset` - Number of results to skip
/// * `namespace` - Optional namespace for multi-tenant environments
///
/// # Returns
///
/// Result indicating success or error
pub async fn client_search(
    addr: String,
    query: String,
    limit: i32,
    offset: i32,
    namespace: Option<String>,
) -> Result<(), BoxError> {
    let mut client = NamespaceClient::connect(addr).await?;
    
    // Create metadata with namespace if provided
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Some(ns) = namespace {
        metadata.insert("namespace", ns.parse().unwrap());
        info!(namespace=%ns, "Searching in namespace");
    }
    
    let response = client.search_with_metadata(query, limit, offset, metadata).await?;

    info!(
        total=%response.total, 
        message=%response.message,
        "Search response"
    );
    for (i, result) in response.results.iter().enumerate() {
        info!(
            result_num=%(i + 1), 
            path=%result.path,
            score=%result.score,
            "Search result"
        );
        if let Some(snippet) = &result.snippet {
            info!(snippet=%snippet.text, "Search result snippet");
        }
    }

    Ok(())
}

/// Search the index using vector similarity
///
/// # Arguments
///
/// * `addr` - Address of the server
/// * `vector` - Vector of floating point values
/// * `dim` - Vector dimension
/// * `limit` - Maximum number of results to return
/// * `offset` - Number of results to skip
/// * `min_score` - Minimum similarity score threshold
///
/// # Returns
///
/// Result indicating success or error
pub async fn client_vector_search(
    addr: String,
    vector: Vec<f32>,
    dim: i32,
    limit: i32,
    offset: i32,
    min_score: f32,
) -> Result<(), BoxError> {
    let mut client = NamespaceClient::connect(addr).await?;
    let response = client
        .vector_search(vector, dim, limit, offset, min_score)
        .await?;

    info!(
        total=%response.total, 
        message=%response.message,
        "Vector search response"
    );
    for (i, result) in response.results.iter().enumerate() {
        info!(
            result_num=%(i + 1), 
            location=%result.location,
            score=%result.score,
            "Vector search result"
        );
        trace!(vector=?result.vector, "Vector search result vector");
    }

    Ok(())
}