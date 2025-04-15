/// Server module provides the main Fugu server implementation
///
/// This module has been moved to grpc/server.rs.
/// This file remains for backward compatibility purposes.

pub use crate::fugu::grpc::server::NamespaceService;

// Deprecated - use NamespaceService instead
#[deprecated(since = "0.1.0", note = "Use NamespaceService instead")]
pub struct FuguServer(NamespaceService);

impl FuguServer {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self(NamespaceService::new(path))
    }
}