/// Error handling for the gRPC module
use std::fmt;
use tonic::Status;
// We don't need tracing::error import here

/// Custom error type for gRPC operations
#[derive(Debug)]
pub enum GrpcError {
    /// I/O error from filesystem operations
    Io(std::io::Error),
    /// Error from tonic/gRPC
    Tonic(Status),
    /// Node operations error
    Node(String),
    /// Error from index operations
    Index(String),
    /// Timeout error
    Timeout(String),
    /// Generic error with message
    Other(String),
}

impl fmt::Display for GrpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrpcError::Io(err) => write!(f, "IO error: {}", err),
            GrpcError::Tonic(status) => write!(f, "gRPC error: {}", status),
            GrpcError::Node(msg) => write!(f, "Node error: {}", msg),
            GrpcError::Index(msg) => write!(f, "Index error: {}", msg),
            GrpcError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            GrpcError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for GrpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GrpcError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for GrpcError {
    fn from(err: std::io::Error) -> Self {
        GrpcError::Io(err)
    }
}

impl From<Status> for GrpcError {
    fn from(status: Status) -> Self {
        GrpcError::Tonic(status)
    }
}

impl From<GrpcError> for Status {
    fn from(err: GrpcError) -> Self {
        match err {
            GrpcError::Tonic(status) => status,
            GrpcError::Io(io_err) => 
                Status::internal(format!("IO error: {}", io_err)),
            GrpcError::Node(msg) => 
                Status::internal(format!("Node error: {}", msg)),
            GrpcError::Index(msg) => 
                Status::internal(format!("Index error: {}", msg)),
            GrpcError::Timeout(msg) => 
                Status::deadline_exceeded(format!("Timeout: {}", msg)),
            GrpcError::Other(msg) => 
                Status::internal(msg),
        }
    }
}

// Removed conflicting From implementation since there's already a blanket implementation
// for Box<dyn std::error::Error + Send + Sync>

/// Helper macro to convert any error to a tonic::Status
#[macro_export]
macro_rules! to_status {
    ($expr:expr, $code:ident, $msg:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => {
                error!("{}: {}", $msg, err);
                return Err(tonic::Status::$code(format!("{}: {}", $msg, err)));
            }
        }
    };
}

/// Helper function to convert a Node error to a BoxError
pub fn node_err_to_box<E: std::fmt::Display>(err: E) -> crate::fugu::grpc::BoxError {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        format!("{}", err)
    ))
}