# Fugu gRPC API

Fugu provides a powerful gRPC API for remote operations. This document covers the API design, available operations, and client usage examples.

## API Overview

The gRPC API is defined in Protocol Buffer files located in the `/proto` directory. The primary service is the `Namespace` service, which provides operations for indexing, searching, and managing documents.

## Service Definition

The `Namespace` service provides the following operations:

```protobuf
service Namespace {
  // Index a file
  rpc Index(IndexRequest) returns (IndexResponse);
  
  // Stream-based file indexing for large files
  rpc StreamIndex(stream StreamIndexChunk) returns (IndexResponse);
  
  // Delete a file from the index
  rpc Delete(DeleteRequest) returns (DeleteResponse);
  
  // Search for documents
  rpc Search(SearchRequest) returns (SearchResponse);
  
  // Search using vector similarity
  rpc VectorSearch(VectorSearchRequest) returns (VectorSearchResponse);
}
```

## Message Types

### Index Operations

```protobuf
message File {
  string name = 1;
  bytes body = 2;
}

message IndexRequest {
  File file = 1;
}

message IndexResponse {
  bool success = 1;
  string location = 2;
  int64 bytes_received = 3;
  int64 chunks_received = 4;
}

message StreamIndexChunk {
  string file_name = 1;
  bytes chunk_data = 2;
  bool is_last = 3;
  string namespace = 4;
}
```

### Delete Operations

```protobuf
message DeleteRequest {
  string location = 1;
}

message DeleteResponse {
  bool success = 1;
  string message = 2;
}
```

### Search Operations

```protobuf
message SearchRequest {
  string query = 1;
  int32 limit = 2;
  int32 offset = 3;
}

message SearchResponse {
  repeated SearchResult results = 1;
  int32 total = 2;
  string message = 3;
}

message SearchResult {
  string path = 1;
  float score = 2;
  Snippet snippet = 3;
}

message Snippet {
  string path = 1;
  string text = 2;
  int32 start = 3;
  int32 end = 4;
}
```

### Vector Search Operations

```protobuf
message VectorSearchRequest {
  repeated float vector = 1;
  int32 dim = 2;
  int32 limit = 3;
  int32 offset = 4;
  float min_score = 5;
}

message VectorSearchResponse {
  repeated VectorSearchResult results = 1;
  int32 total = 2;
  string message = 3;
}

message VectorSearchResult {
  string location = 1;
  float score = 2;
  repeated float vector = 3;
}
```

## Using the API

### Client Initialization

```rust
use crate::fugu::grpc::NamespaceClient;

async fn connect_client() -> Result<NamespaceClient, Box<dyn std::error::Error>> {
    let client = NamespaceClient::connect("http://localhost:50051").await?;
    Ok(client)
}
```

### Namespaces in Metadata

For multi-tenant operations, the namespace is specified in the request metadata:

```rust
let mut metadata = tonic::metadata::MetadataMap::new();
metadata.insert("namespace", "my_namespace".parse().unwrap());
```

### Indexing a Document

```rust
async fn index_document(
    client: &mut NamespaceClient,
    file_name: String,
    content: Vec<u8>,
    namespace: Option<String>
) -> Result<(), Box<dyn std::error::Error>> {
    // Create metadata with namespace
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Some(ns) = namespace {
        metadata.insert("namespace", ns.parse().unwrap());
    }
    
    // Send the request
    let response = client.index_with_metadata(
        file_name,
        content,
        metadata
    ).await?;
    
    println!("Indexed document: {}", response.location);
    Ok(())
}
```

### Streaming Large Files

```rust
async fn stream_large_file(
    client: &mut NamespaceClient,
    file_path: std::path::PathBuf,
    namespace: Option<String>
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.stream_index_file(
        file_path,
        namespace,
        None  // Use optimal chunk size
    ).await?;
    
    println!("Streamed file: {}", response.location);
    Ok(())
}
```

### Performing a Search

```rust
async fn search_documents(
    client: &mut NamespaceClient,
    query: String,
    limit: i32,
    offset: i32,
    namespace: Option<String>
) -> Result<(), Box<dyn std::error::Error>> {
    // Create metadata with namespace
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Some(ns) = namespace {
        metadata.insert("namespace", ns.parse().unwrap());
    }
    
    // Send the request
    let response = client.search_with_metadata(
        query,
        limit,
        offset,
        metadata
    ).await?;
    
    println!("Found {} results", response.total);
    for result in response.results {
        println!("{}: score {}", result.path, result.score);
        if let Some(snippet) = result.snippet {
            println!("  {}", snippet.text);
        }
    }
    
    Ok(())
}
```

### Deleting a Document

```rust
async fn delete_document(
    client: &mut NamespaceClient,
    location: String,
    namespace: Option<String>
) -> Result<(), Box<dyn std::error::Error>> {
    // Create metadata with namespace
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Some(ns) = namespace {
        metadata.insert("namespace", ns.parse().unwrap());
    }
    
    // Send the request
    let response = client.delete_with_metadata(
        location,
        metadata
    ).await?;
    
    println!("Delete result: {}", response.message);
    Ok(())
}
```

## Server Implementation

The server implementation is in `src/fugu/grpc.rs` and provides:

1. **NamespaceService**: The main service implementation
2. **Concurrency Management**: Handles concurrent client requests
3. **Namespace Isolation**: Routes operations to the appropriate namespace node
4. **Error Handling**: Provides meaningful error messages to clients
5. **Logging**: Detailed logging of API operations

## API Security

The API does not include built-in authentication or authorization. In production environments, consider:

1. Deploying behind an API gateway with authentication
2. Using gRPC interceptors for custom authentication
3. Implementing TLS for secure communication
4. Restricting access to trusted networks

## Performance Considerations

- For large files, use the StreamIndex method instead of Index
- When performing many small operations, batch them when possible
- Consider compression for large transfers
- Set appropriate timeouts for long-running operations