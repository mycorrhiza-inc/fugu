# Indexing in Fugu

Fugu provides powerful document indexing capabilities that enable fast and efficient text search. This document covers the indexing process, supported features, and best practices.

## Indexing Basics

Indexing is the process of analyzing documents and creating data structures that enable fast retrieval during search operations. Fugu uses an inverted index, which maps terms to the documents containing them.

### The Indexing Process

1. **Tokenization**: Documents are broken into tokens (terms) using whitespace.
2. **Position Recording**: The position of each term in the document is recorded.
3. **Term Frequency**: The frequency of each term is calculated.
4. **Index Storage**: The index is stored persistently for future searches.

## How to Index Documents

### Using the Command Line

To index a file in the default namespace:

```bash
cargo run -- namespace index --file /path/to/document.txt --addr http://localhost:50051
```

To index a file in a specific namespace:

```bash
cargo run -- add --namespace my_namespace /path/to/file.txt --addr http://localhost:50051
```

### Using the gRPC API

For programmatic access, Fugu provides a gRPC API:

```rust
let mut client = NamespaceClient::connect("http://localhost:50051").await?;
let response = client.index_with_metadata(
    "filename.txt".to_string(),
    file_content,
    metadata
).await?;
```

## Large File Indexing

Fugu intelligently handles large files with parallel processing:

- Files under 512KB use standard single-threaded indexing
- Files between 512KB and 10MB use parallel processing with server-side optimization
- Files over 10MB use streaming with chunked processing

When indexing large files, use the stream_index method:

```bash
cargo run -- stream-index /path/to/large/file.txt --namespace my_namespace
```

## Indexing Options

Fugu provides several indexing options:

- **Namespace Isolation**: Each namespace has its own isolated index
- **Document IDs**: By default, the filename is used as the document ID
- **Binary Content Handling**: Binary files are handled with fallback mechanisms for searchability

## Special Features

### Parallel Processing with CRDTs

For large files, Fugu uses Conflict-free Replicated Data Types (CRDTs) to enable efficient parallel processing:

- Files are split into multiple chunks
- Each chunk is processed concurrently
- Results are merged using CRDT operations
- This approach provides significant performance improvements for large files

### Binary File Handling

Fugu intelligently handles binary files:

1. First attempts to read files as UTF-8 text
2. Falls back to lossy UTF-8 conversion for binary content
3. Creates minimal searchable content for binary files
4. Stores minimal metadata to ensure findability

## Index Persistence

Indexes are persisted to disk in the namespace directory structure:

```
~/.fugu/namespaces/my_namespace/index/
```

The index consists of several components:

- **Main Index**: Maps terms to documents and positions
- **Document Storage**: Contains the original document content
- **Document-Term Mapping**: Enables efficient document deletion

## Performance Metrics

During indexing, Fugu collects performance metrics:

- Index time
- Document count
- Term frequency distribution
- Storage efficiency

These metrics are available for monitoring and tuning.