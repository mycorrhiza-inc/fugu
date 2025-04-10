use std::path::PathBuf;
use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};

use crate::fugu::node::Node;
use crate::fugu::server::FuguServer;
use crate::fugu::wal::WALCMD;

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
pub struct NamespaceService {
    server: FuguServer,
    wal_sender: mpsc::Sender<WALCMD>,
}

impl NamespaceService {
    pub fn new(path: PathBuf) -> Self {
        let server = FuguServer::new(path);
        let wal_sender = server.get_wal_sender();

        Self { server, wal_sender }
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

        println!(
            "Received search request: query='{}', limit={}, offset={}",
            req.query, req.limit, req.offset
        );

        // Here we would implement the actual search logic
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

// Function to start the gRPC server
pub async fn start_grpc_server(
    path: PathBuf,
    addr: String,
    ready_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    let namespace_service = NamespaceService::new(path);

    println!("Starting gRPC server on {}", addr);
    
    // Signal that the server is ready
    if let Some(tx) = ready_tx {
        let _ = tx.send(());
    }

    Server::builder()
        .add_service(NamespaceServer::new(namespace_service))
        .serve(addr)
        .await?;

    Ok(())
}

// Client implementation for the namespace service
pub struct NamespaceClient {
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

