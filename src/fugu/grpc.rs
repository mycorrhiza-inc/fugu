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
use tonic::{transport::Server, Request, Response, Status};

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
    VectorSearchResult,
};

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
    async fn unload_node(&self, namespace: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = self.nodes.write().await;
        
        if let Some(mut node) = nodes.remove(namespace) {
            node.unload_index().await?;
        }
        
        Ok(())
    }
}

#[tonic::async_trait]
impl Namespace for NamespaceService {
    async fn index(
        &self,
        request: Request<IndexRequest>,
    ) -> Result<Response<IndexResponse>, Status> {
        let request_inner = request.into_inner();
        let file = request_inner.file.clone().unwrap_or_default();

        // Log the received request
        println!("Received index request for file: {}", file.name);

        // Here we would implement the actual indexing logic
        // This would typically call the Node to handle indexing the file

        // For now, return a simple success response
        let response = IndexResponse {
            success: true,
            location: format!("/{}", file.name),
        };

        Ok(Response::new(response))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let location = request.into_inner().location;

        println!("Received delete request for location: {}", location);

        // Here we would implement the actual delete logic

        let response = DeleteResponse {
            success: true,
            message: format!("File at {} deleted successfully", location),
        };

        Ok(Response::new(response))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let namespace = "default"; // In a real implementation, this might come from the request

        println!(
            "Received search request: query='{}', limit={}, offset={}",
            req.query, req.limit, req.offset
        );

        // Get or create a node for this namespace
        let mut node = self.get_node(namespace).await;
        
        // Load the index (this won't do anything if already loaded)
        node.load_index().await.map_err(|e| {
            Status::internal(format!("Failed to load index: {}", e))
        })?;
        
        // Here we would implement the actual search logic using the node's index
        // For now, return a mock response
        let snippet = Snippet {
            path: "example/path".to_string(),
            text: format!("Sample text containing '{}'", req.query),
            start: 0,
            end: 10,
        };

        let result = SearchResult {
            path: "/example/document.txt".to_string(),
            score: 0.95,
            snippet: Some(snippet),
        };

        let response = SearchResponse {
            results: vec![result],
            total: 1,
            message: "Search completed successfully".to_string(),
        };

        // In a real system with memory constraints, we might unload indices 
        // that haven't been used for a while
        // For demonstration, let's not unload after each request
        // node.unload_index().await.map_err(|e| {
        //     Status::internal(format!("Failed to unload index: {}", e))
        // })?;

        Ok(Response::new(response))
    }

    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSearchResponse>, Status> {
        let req = request.into_inner();

        println!(
            "Received vector search request: dim={}, limit={}, offset={}",
            req.dim, req.limit, req.offset
        );

        // Here we would implement the actual vector search logic
        // For now, return a mock response
        let result = VectorSearchResult {
            location: "/example/vector_doc.txt".to_string(),
            score: 0.87,
            vector: req.vector.clone(), // Echo back the vector for demonstration
        };

        let response = VectorSearchResponse {
            results: vec![result],
            total: 1,
            message: "Vector search completed successfully".to_string(),
        };

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
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    let namespace_service = NamespaceService::new(path);
    println!("Starting gRPC server on {}", addr);
    
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
                // Add timeout to prevent hanging indefinitely
                let shutdown_result = tokio::time::timeout(
                    tokio::time::Duration::from_secs(10), // 10 second timeout
                    shutdown_signal
                ).await;
                
                if shutdown_result.is_err() {
                    println!("Shutdown signal timed out, forcing shutdown");
                }
                
                println!("Graceful shutdown signal received, unloading all indices");
                
                // Unload all indices on shutdown (with timeout)
                let nodes = namespace_service_clone.nodes.read().await;
                for (namespace, _) in nodes.iter() {
                    // Add timeout for each unload operation
                    match tokio::time::timeout(
                        tokio::time::Duration::from_secs(5), // 5 second timeout per namespace
                        namespace_service_clone.unload_node(namespace)
                    ).await {
                        Ok(Ok(_)) => println!("Successfully unloaded node for namespace {}", namespace),
                        Ok(Err(e)) => eprintln!("Error unloading node for namespace {}: {}", namespace, e),
                        Err(_) => eprintln!("Timeout unloading node for namespace {}", namespace),
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
                        println!("Server auto-shutdown after timeout");
                    }
                }
                
                // Unload all indices on shutdown
                let nodes = namespace_service_clone.nodes.read().await;
                for (namespace, _) in nodes.iter() {
                    if let Err(_e) = tokio::time::timeout(
                        tokio::time::Duration::from_secs(5),
                        namespace_service_clone.unload_node(namespace)
                    ).await {
                        eprintln!("Timeout unloading node for namespace {}", namespace);
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
    pub async fn connect(addr: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client = namespace::namespace_client::NamespaceClient::connect(addr).await?;
        Ok(Self { client })
    }

    pub async fn index(
        &mut self,
        file_name: String,
        file_content: Vec<u8>,
    ) -> Result<IndexResponse, Status> {
        let file = File {
            name: file_name,
            body: file_content,
        };

        let request = IndexRequest { file: Some(file) };

        let response = self.client.index(request).await?;
        Ok(response.into_inner())
    }

    pub async fn delete(&mut self, location: String) -> Result<DeleteResponse, Status> {
        let request = DeleteRequest { location };
        let response = self.client.delete(request).await?;
        Ok(response.into_inner())
    }

    pub async fn search(
        &mut self,
        query: String,
        limit: i32,
        offset: i32,
    ) -> Result<SearchResponse, Status> {
        let request = SearchRequest {
            query,
            limit,
            offset,
        };

        let response = self.client.search(request).await?;
        Ok(response.into_inner())
    }

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

// Command line utility functions for the client
pub async fn client_index(
    addr: String,
    file_path: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_name = PathBuf::from(&file_path)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("Invalid file path")?
        .to_string();

    let file_content = tokio::fs::read(&file_path).await?;

    let mut client = NamespaceClient::connect(addr).await?;
    let response = client.index(file_name, file_content).await?;

    println!(
        "Index response: success={}, location={}",
        response.success, response.location
    );
    Ok(())
}

pub async fn client_delete(
    addr: String,
    location: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = NamespaceClient::connect(addr).await?;
    let response = client.delete(location).await?;

    println!(
        "Delete response: success={}, message={}",
        response.success, response.message
    );
    Ok(())
}

pub async fn client_search(
    addr: String,
    query: String,
    limit: i32,
    offset: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = NamespaceClient::connect(addr).await?;
    let response = client.search(query, limit, offset).await?;

    println!(
        "Search response: total={}, message={}",
        response.total, response.message
    );
    for (i, result) in response.results.iter().enumerate() {
        println!(
            "Result #{}: path={}, score={}",
            i + 1,
            result.path,
            result.score
        );
        if let Some(snippet) = &result.snippet {
            println!("  Snippet: {}", snippet.text);
        }
    }

    Ok(())
}

pub async fn client_vector_search(
    addr: String,
    vector: Vec<f32>,
    dim: i32,
    limit: i32,
    offset: i32,
    min_score: f32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = NamespaceClient::connect(addr).await?;
    let response = client
        .vector_search(vector, dim, limit, offset, min_score)
        .await?;

    println!(
        "Vector search response: total={}, message={}",
        response.total, response.message
    );
    for (i, result) in response.results.iter().enumerate() {
        println!(
            "Result #{}: location={}, score={}",
            i + 1,
            result.location,
            result.score
        );
        println!("  Vector: {:?}", result.vector);
    }

    Ok(())
}

