/// Fugu is a high-performance search engine with gRPC interface
/// 
/// The `fugu` module provides a complete search solution with:
/// - Persistent indexing via write-ahead logging
/// - Inverted index for fast text search
/// - Namespace-based organization for multi-tenant support
/// - gRPC interface for remote operations
use std::path::PathBuf;

pub mod server;
use server::FuguServer;

/// Creates a new Fugu server instance
///
/// # Arguments
///
/// * `path` - Path where the server will store its data and write-ahead log
///
/// # Returns
///
/// Returns a new `FuguServer` instance
pub fn new(path: PathBuf) -> FuguServer {
    FuguServer::new(path)
}

pub mod node;
mod test;
pub mod wal;
pub mod index;
pub mod grpc;
#[cfg(test)]
mod test_grpc;
#[cfg(test)]
mod test_client;
#[cfg(test)]
mod search_test;