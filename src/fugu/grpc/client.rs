/// Client implementation for the Fugu gRPC service
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::io::AsyncReadExt;
use tracing::{info, error};
use tonic::Status;

use crate::fugu::grpc::{
    BoxError, namespace, DeleteResponse, IndexResponse, SearchResponse, 
    VectorSearchResponse, StreamIndexChunk
};

/// Client implementation for the namespace service
///
/// This client:
/// - Provides remote access to Fugu functionality
/// - Handles connection establishment
/// - Offers methods for all supported operations
/// - Simplifies gRPC interaction
pub struct NamespaceClient {
    /// The underlying gRPC client
    pub client: namespace::namespace_client::NamespaceClient<tonic::transport::Channel>,
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
    
    /// Create a new client without keeping a persistent connection
    /// 
    /// This helps with clean shutdown after operations are complete
    pub async fn connect_oneshot(addr: String) -> Result<Self, BoxError> {
        let channel = tonic::transport::Channel::from_shared(addr)?
            .connect()
            .await?;
        
        let client = namespace::namespace_client::NamespaceClient::new(channel);
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
    pub fn calculate_optimal_chunk_size(&self, file_size: u64, requested_size: Option<usize>) -> usize {
        if let Some(size) = requested_size {
            size
        } else {
            // Use system page size for chunk processing
            let page_size = 8192; // Default to 8KB as one page size
            page_size
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
        // Use the metadata version with default empty metadata
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
        let file = namespace::File {
            name: file_name,
            body: file_content,
        };

        let request = namespace::IndexRequest { file: Some(file) };
        
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
    /// * `server_addr` - Server address to reconnect after streaming
    ///
    /// # Returns
    ///
    /// IndexResponse with results
    pub async fn stream_index_file(
        &mut self,
        file_path: PathBuf,
        namespace: Option<String>,
        chunk_size: Option<usize>,
        server_addr: Option<String>,
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
        
        // Create the stream sender with optimized capacity and a completion channel
        let (tx, rx) = mpsc::channel::<StreamIndexChunk>(channel_capacity);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        
        // Track start time for performance monitoring
        let start_time = std::time::Instant::now();
        
        // Spawn a task to read the file and send chunks
        tokio::spawn(async move {
            let mut buffer = vec![0u8; chunk_size];
            let mut chunk_count = 0;
            let mut is_first = true;
            let mut bytes_sent = 0;
            let mut last_progress_log = std::time::Instant::now();
            let mut sent_last_chunk = false;
            
            loop {
                // Read a chunk
                let bytes_read = match reader.read(&mut buffer).await {
                    Ok(n) => n,
                    Err(e) => {
                        error!("Error reading file: {}", e);
                        // Ensure we send a terminating chunk on error
                        if !sent_last_chunk {
                            let final_chunk = StreamIndexChunk {
                                file_name: if is_first { file_name.clone() } else { String::new() },
                                chunk_data: Vec::new(),
                                is_last: true,
                                namespace: if is_first { namespace.clone().unwrap_or_default() } else { String::new() },
                            };
                            let _ = tx.send(final_chunk).await;
                            sent_last_chunk = true;
                        }
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
                        if let Err(e) = tx.send(last_chunk).await {
                            error!("Failed to send empty last chunk: {}", e);
                        } else {
                            sent_last_chunk = true;
                        }
                    } else if !sent_last_chunk {
                        // If we reached EOF but didn't mark the last chunk we sent as last
                        // Send an explicit empty last chunk to ensure server knows we're done
                        let final_chunk = StreamIndexChunk {
                            file_name: String::new(),
                            chunk_data: Vec::new(),
                            is_last: true,
                            namespace: String::new(),
                        };
                        if let Err(e) = tx.send(final_chunk).await {
                            error!("Failed to send explicit last chunk: {}", e);
                        } else {
                            sent_last_chunk = true;
                        }
                    }
                    break;
                }
                
                chunk_count += 1;
                bytes_sent += bytes_read;
                let is_last_chunk = bytes_sent >= file_size as usize;
                
                if is_last_chunk {
                    sent_last_chunk = true;
                }
                
                // Create the chunk
                let chunk = StreamIndexChunk {
                    // Only include file_name in the first chunk
                    file_name: if is_first { file_name.clone() } else { String::new() },
                    chunk_data: buffer[0..bytes_read].to_vec(),
                    is_last: is_last_chunk,
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
                if is_last_chunk {
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
                sent_last_chunk=%sent_last_chunk,
                "Finished sending chunks for streaming"
            );
            
            // Explicitly drop tx to close the channel, which will terminate the stream
            // This ensures the server knows the stream is complete and should close the connection
            drop(tx);
            
            // Signal that streaming is complete
            let _ = done_tx.send(());
            
            // Logging that we've completed streaming and sent the close signal
            info!("Streaming complete, connection will be closed");
        });
        
        // Create the streaming request
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let request = tonic::Request::new(stream);
        
        // Send the streaming request and wait for the response
        let response = self.client.stream_index(request).await?;
        
        // Wait for the stream to complete (either success or error)
        match tokio::time::timeout(std::time::Duration::from_secs(5), done_rx).await {
            Ok(_) => info!("File streaming task completed normally"),
            Err(_) => info!("File streaming task timed out, but server response was received"),
        }
        
        // Ensure the response is returned before dropping connection
        let inner_response = response.into_inner();
        
        // Let connection close itself when the response is done processing
        
        // Create a new client for the next operation if needed
        if let Some(addr) = server_addr {
            *self = Self::connect(addr).await?;
        }
        
        Ok(inner_response)
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
        // Default implementation without metadata
        self.delete_with_metadata(location, tonic::metadata::MetadataMap::new()).await
    }
    
    /// Delete a file from the index with metadata
    ///
    /// # Arguments
    ///
    /// * `location` - Path to the file to delete
    /// * `metadata` - Additional metadata for the request
    ///
    /// # Returns
    ///
    /// DeleteResponse with results
    pub async fn delete_with_metadata(
        &mut self,
        location: String,
        metadata: tonic::metadata::MetadataMap,
    ) -> Result<DeleteResponse, Status> {
        let request = namespace::DeleteRequest { location };
        
        // Create a request with metadata
        let mut req = tonic::Request::new(request);
        *req.metadata_mut() = metadata;
        
        let response = self.client.delete(req).await?;
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
        let request = namespace::SearchRequest {
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
        // Default implementation without metadata
        self.vector_search_with_metadata(vector, dim, limit, offset, min_score, tonic::metadata::MetadataMap::new()).await
    }

    /// Search the index using vector similarity with metadata
    ///
    /// # Arguments
    ///
    /// * `vector` - Vector of floating point values
    /// * `dim` - Vector dimension
    /// * `limit` - Maximum number of results to return
    /// * `offset` - Number of results to skip
    /// * `min_score` - Minimum similarity score threshold
    /// * `metadata` - Additional metadata for the request
    ///
    /// # Returns
    ///
    /// VectorSearchResponse with results
    pub async fn vector_search_with_metadata(
        &mut self,
        vector: Vec<f32>,
        dim: i32,
        limit: i32,
        offset: i32,
        min_score: f32,
        metadata: tonic::metadata::MetadataMap,
    ) -> Result<VectorSearchResponse, Status> {
        let request = namespace::VectorSearchRequest {
            vector,
            dim,
            limit,
            offset,
            min_score,
        };

        // Create a request with metadata
        let mut req = tonic::Request::new(request);
        *req.metadata_mut() = metadata;

        let response = self.client.vector_search(req).await?;
        Ok(response.into_inner())
    }
}