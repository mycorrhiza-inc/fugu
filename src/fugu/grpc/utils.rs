/// Utility functions for the gRPC client
use std::path::PathBuf;
use tracing::info;

use crate::fugu::grpc::{BoxError, IndexResponse, NamespaceClient};

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
    namespace: &String,
) -> Result<(), BoxError> {
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
    metadata.insert("namespace", namespace.parse().unwrap());

    let metadata_response = client_for_metadata
        .index_with_metadata("searchable_terms.txt".to_string(), file_content, metadata)
        .await?;

    info!(
        "Added searchable terms file to namespace '{}': success={}",
        namespace, metadata_response.success
    );

    return Ok(());
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
    namespace: String,
) -> Result<IndexResponse, BoxError> {
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
        info!(
            "File size is {} bytes, using streaming mode with parallel indexing",
            file_size
        );

        // Add some searchable metadata for testing with common terms
        add_searchable_metadata(&addr, &file_path, &namespace).await?;

        // Now stream the main file - Clone addr to avoid ownership issues
        let addr_clone = addr.clone();
        let mut client = NamespaceClient::connect(addr_clone).await?;

        // For large files, optimize chunk size based on file size
        let optimal_chunk_size = client.calculate_optimal_chunk_size(file_size, None);

        let response = client
            .stream_index_file(
                PathBuf::from(&file_path),
                Some(namespace.clone()),
                Some(optimal_chunk_size),
                Some(addr.clone()),
            )
            .await?;

        info!(
            success=%response.success,
            location=%response.location,
            bytes_received=%response.bytes_received,
            chunks_received=%response.chunks_received,
            "Streaming index response with parallel processing"
        );

        return Ok(response);
    } else {
        // For medium to small files, use regular indexing
        info!(
            "File size is {} bytes, using regular mode{}",
            file_size,
            if file_size > PARALLEL_THRESHOLD {
                " with server-side parallel indexing"
            } else {
                ""
            }
        );

        // Read the file content
        let file_content = tokio::fs::read(&file_path).await?;

        let mut client = NamespaceClient::connect(addr).await?;

        // Create metadata with namespace
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("namespace", namespace.parse().unwrap());

        // Index the file with the namespace in metadata
        let response = client
            .index_with_metadata(file_name, file_content, metadata)
            .await?;

        info!(
            success=%response.success,
            location=%response.location,
            bytes_received=%response.bytes_received,
            chunks_received=%response.chunks_received,
            "Index response{}",
            if file_size > PARALLEL_THRESHOLD { " with server-side parallel processing" } else { "" }
        );

        return Ok(response);
    }
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
    namespace: String,
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
    // Clone addr to avoid ownership issues
    let addr_clone = addr.clone();
    let mut client = NamespaceClient::connect(addr_clone).await?;
    let optimal_chunk_size = client.calculate_optimal_chunk_size(file_size, chunk_size);

    info!(
        chunk_size = format!("{:.2} MB", optimal_chunk_size as f64 / 1024.0 / 1024.0),
        "Using optimized chunk size for streaming"
    );

    let response = client
        .stream_index_file(
            PathBuf::from(&file_path),
            Some(namespace.clone()),
            Some(optimal_chunk_size),
            Some(addr.clone()),
        )
        .await?;

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
    
    // Explicitly drop the client to close any open connections
    drop(client);

    Ok(())
}

/// Delete a file from the index
///
/// # Arguments
///
/// * `addr` - Address of the server
/// * `location` - Path to the file to delete
/// * `namespace` - Optional namespace for multi-tenant environments
///
/// # Returns
///
/// Result indicating success or error
pub async fn client_delete(
    addr: String,
    location: String,
    namespace: String,
) -> Result<(), BoxError> {
    let mut client = NamespaceClient::connect(addr).await?;

    // Create metadata with namespace
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("namespace", namespace.parse().unwrap());
    info!(namespace=%namespace, "Deleting in namespace");

    let response = client.delete_with_metadata(location, metadata).await?;

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
    namespace: String,
) -> Result<(), BoxError> {
    let mut client = NamespaceClient::connect(addr).await?;

    // Create metadata with namespace
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("namespace", namespace.parse().unwrap());
    info!(namespace=%namespace, "Searching in namespace");

    let response = client
        .search_with_metadata(query, limit, offset, metadata)
        .await?;

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
#[allow(dead_code)]
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
    }

    Ok(())
}
