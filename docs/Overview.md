# Fugu - High-Performance Search Engine

Fugu is a high-performance search engine built in Rust with a gRPC API, designed for efficient text search and document indexing.

## Core Features

- **Text Search**: Fast text search using whitespace tokenization and the BM25 algorithm
- **Inverted Index**: Efficiently maps terms to document locations for quick lookups
- **Multi-Tenant Support**: Isolates data with namespace-based organization
- **gRPC Interface**: Provides a modern API for remote operations
- **Write-Ahead Logging**: Ensures data durability during crashes or unexpected shutdowns
- **Parallel Processing**: Efficiently handles large files with concurrent operations
- **Command Line Interface**: Simplifies server management and client operations

## Architecture

Fugu consists of several key components:

1. **Server**: The main daemon that manages the search engine lifecycle
2. **Node**: Namespace-specific instances that handle indexing and search within their context
3. **Index**: The core data structure that enables efficient text search
4. **WAL (Write-Ahead Log)**: Ensures durability of operations
5. **gRPC Service**: Provides the remote API for client interactions
6. **Configuration**: Manages file paths and system settings

## Data Flow

The typical data flow in Fugu follows this pattern:

1. A client sends a request to the gRPC server
2. The server routes the request to the appropriate namespace Node
3. The Node loads its index if not already loaded
4. The operation (index, search, delete) is performed on the index
5. Changes are logged to the WAL for durability
6. Results are returned to the client

## Directory Structure

```
/src
  /cmd              # Command-line interface
  /fugu             # Core library components
    config.rs       # Configuration management
    grpc.rs         # gRPC server and client
    index.rs        # Inverted index implementation
    mod.rs          # Module definitions
    node.rs         # Namespace node implementation
    server.rs       # Main server implementation
    wal.rs          # Write-ahead logging
/proto              # Protocol buffer definitions
/docs               # Documentation
/tests              # Test suite
```

## Getting Started

To start using Fugu, refer to the following documentation:

- [Installation](Installation.md) - How to install and configure Fugu
- [Server](Server.md) - Running and managing the Fugu server
- [Namespaces](Namespaces.md) - Working with multi-tenant isolation
- [Indexing](Indexing.md) - Adding documents to the search engine
- [Searching](Searching.md) - Querying for documents
- [Security](Security.md) - Security considerations and best practices
- [Configuration](Configuration.md) - Configuring Fugu for your environment
- [gRPC API](GRPC.md) - Using the remote API
- [Command Line](CommandLine.md) - Available CLI commands and options
- [Architecture](Architecture.md) - Detailed system design and component interactions