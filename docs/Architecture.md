# Fugu Architecture

This document provides a detailed overview of Fugu's architecture, describing how the components interact and the design principles behind the system.

## System Overview

Fugu is a high-performance search engine with a gRPC interface, designed for efficient text search and document management. The architecture is built around the following core principles:

1. **Durability**: Write-ahead logging ensures no data loss
2. **Isolation**: Namespace-based isolation for multi-tenant operation
3. **Performance**: Optimized for fast indexing and search
4. **Scalability**: Supports large documents and high query volumes
5. **Simplicity**: Straightforward API and configuration

## Component Architecture

Fugu consists of several key components that work together:

```
┌─────────────────────────────────────────────┐
│              gRPC Interface                  │
└───────────────┬─────────────────────────────┘
                │
┌───────────────▼─────────────────────────────┐
│              FuguServer                      │
└───┬───────────┬───────────────────────┬─────┘
    │           │                       │
┌───▼───┐   ┌───▼───┐               ┌───▼───┐
│ Node 1 │   │ Node 2 │    ...       │ Node n │
└───┬───┘   └───┬───┘               └───┬───┘
    │           │                       │
┌───▼───┐   ┌───▼───┐               ┌───▼───┐
│ Index │   │ Index │    ...       │ Index │
└───┬───┘   └───┬───┘               └───┬───┘
    │           │                       │
└───▼───────────▼───────────────────────▼───┘
                    WAL System
```

### Core Components

1. **gRPC Interface**: The primary API for client interactions
2. **FuguServer**: The main server that manages nodes
3. **Node**: Namespace-specific handler for operations
4. **Index**: The inverted index data structure
5. **WAL**: Write-ahead log for durability

## Data Flow

### Indexing Flow

1. Client sends an IndexRequest through gRPC
2. NamespaceService receives the request and extracts namespace
3. The appropriate Node is located or created
4. The Node loads its index if not already loaded
5. The document is tokenized using WhitespaceTokenizer
6. Tokens are added to the InvertedIndex
7. Operations are logged to the WAL
8. The response is returned to the client

### Search Flow

1. Client sends a SearchRequest through gRPC
2. NamespaceService receives the request and extracts namespace
3. The appropriate Node is located or created
4. The Node loads its index if not already loaded
5. The query is tokenized
6. The InvertedIndex is searched for matching documents
7. Results are scored using BM25 or TF-IDF algorithm
8. The sorted results are returned to the client

### Delete Flow

1. Client sends a DeleteRequest through gRPC
2. NamespaceService receives the request and extracts namespace
3. The appropriate Node is located or created
4. The Node loads its index if not already loaded
5. The document is removed from the InvertedIndex
6. The deletion is logged to the WAL
7. The response is returned to the client

## Component Details

### FuguServer

The FuguServer is responsible for:
- Managing the overall server lifecycle
- Handling the write-ahead log (WAL)
- Maintaining background tasks
- Ensuring clean shutdown
- Managing configuration

### Node

Each Node represents a single namespace and:
- Manages a dedicated inverted index
- Handles namespace-specific operations
- Loads and unloads index data
- Communicates with the WAL

### InvertedIndex

The InvertedIndex is the core data structure that:
- Maps terms to documents and positions
- Supports fast text search
- Implements BM25 scoring
- Manages document storage
- Handles concurrent operations

### WAL (Write-Ahead Log)

The WAL system ensures data durability by:
- Logging operations before they're applied
- Supporting recovery after crashes
- Providing namespace isolation
- Optimizing write performance with batching
- Managing concurrent write operations

### gRPC Service

The gRPC service provides:
- Remote API access for clients
- Streaming for large files
- Result pagination
- Vector search capabilities
- Namespace isolation through metadata

## Concurrency Model

Fugu uses Tokio for asynchronous programming and concurrency:

1. **Task-Based Concurrency**: Operations are executed as Tokio tasks
2. **Shared State**: State is shared using Arc and RwLock
3. **Channel Communication**: Components communicate through channels
4. **Lock Minimization**: Design minimizes contention on shared resources

## Persistence Strategy

Fugu uses a multi-layered persistence strategy:

1. **Write-Ahead Log**: All operations are first written to the WAL
2. **Inverted Index**: The index is periodically flushed to disk
3. **Document Storage**: Original documents are stored in a separate database
4. **Namespace Isolation**: Each namespace has its own storage area

## Performance Optimizations

Fugu includes several performance optimizations:

1. **Parallel Processing**: Large files are processed in parallel
2. **CRDT-Based Merging**: Concurrent results are merged using CRDTs
3. **Batched Operations**: WAL writes are batched for efficiency
4. **Chunked Processing**: Large files are processed in chunks
5. **Adaptive Chunk Sizing**: Chunk size adapts to file size
6. **Background Flushing**: Data is flushed asynchronously
7. **Position Caching**: Term positions are cached for efficiency

## Scaling Considerations

While Fugu is primarily designed as a single-server system, it includes features that aid in scaling:

1. **Namespace Isolation**: Allows logical separation of data
2. **Efficient Resource Usage**: Indexes are loaded only when needed
3. **Low Memory Footprint**: Streaming processing for large files
4. **Controlled Concurrency**: Limits on concurrent operations

For larger deployments, consider:
- Running multiple instances with different namespace subsets
- Load balancing gRPC requests across instances
- Distributing large indexing operations

## Fault Tolerance

Fugu provides fault tolerance through:

1. **Write-Ahead Logging**: Ensures operation durability
2. **Graceful Degradation**: Continues serving even if some components fail
3. **Recovery Mechanisms**: Automatic recovery after crashes
4. **Error Isolation**: Namespace-based isolation limits failure impact

## Future Architecture Extensions

The architecture is designed to support future extensions:

1. **Distributed Indexing**: Splitting indexes across multiple servers
2. **Replication**: Maintaining multiple copies for redundancy
3. **Advanced Search**: Semantic search and embeddings support
4. **Sharding**: Horizontal scaling through data partitioning