/// gRPC server and client implementation for remote access to Fugu
///
/// This module provides:
/// - A gRPC server exposing Fugu functionality
/// - A client implementation for remote operations
/// - Handling of index, delete, search, and vector search operations
/// - Command-line utilities for interacting with the server

// Public modules
pub mod server;
pub mod client;
pub mod utils;

// For internal use
mod error;

// Re-export key types for convenience
pub use server::{NamespaceService, start_grpc_server};
pub use client::NamespaceClient;
pub use utils::{client_index, client_stream_index, client_delete, client_search};

// Add Send + Sync to make sure our error types meet requirements
pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;

// Include the generated protobuf code
pub mod namespace {
    tonic::include_proto!("namespace");
}

// Re-export key protobuf types for convenience
pub use namespace::{
    namespace_server::{Namespace, NamespaceServer},
    DeleteRequest, DeleteResponse, File, IndexRequest, IndexResponse, SearchRequest,
    SearchResponse, SearchResult, Snippet, VectorSearchRequest, VectorSearchResponse,
    StreamIndexChunk,
};