# Fugu Configuration

Fugu uses a directory-based configuration system that is simple to manage and extend. This document covers the configuration architecture, options, and best practices.

## Configuration Architecture

The core of Fugu's configuration is the `ConfigManager` which:
- Manages all file and directory paths
- Ensures directories exist and are accessible
- Provides a consistent interface for path resolution
- Supports both default and custom configurations

## Base Directory

By default, Fugu stores all its data in the `~/.fugu` directory. This can be overridden with a custom path when initializing the server or performing operations.

### Directory Structure

```
~/.fugu/
  /logs           # Log files
  /namespaces     # Namespace data
    /default      # Default namespace
      /index      # Index files
        /index    # Main index database
        /docs     # Document content database
        /doc_terms # Document-to-terms mapping
      /wal.bin    # Write-ahead log
    /custom_ns    # Custom namespaces follow same pattern
  /tmp            # Temporary files
  /wal            # Global WAL files
```

## Configuration Options

### Server Configuration

When starting the server, you can configure:

```bash
cargo run -- up [OPTIONS]
```

Available options:
- `--port PORT`: Set the gRPC server port (default: 50051)
- `--daemon`: Run as a background daemon
- `--pid-file FILE`: Custom PID file location (default: /tmp/fugu.pid)
- `--log-file FILE`: Custom log file location (default: /tmp/fugu.log)
- `--timeout SECONDS`: Automatic shutdown timeout (for testing)

### Search Configuration

When performing searches:

```bash
cargo run -- search [OPTIONS] "query"
```

Available options:
- `--namespace NAMESPACE`: Namespace to search in
- `--limit LIMIT`: Maximum results to return (default: 10)
- `--offset OFFSET`: Results to skip (default: 0)
- `--addr ADDRESS`: Server address (default: http://127.0.0.1:50051)

### Index Configuration

For indexing operations:

```bash
cargo run -- namespace index [OPTIONS]
```

Available options:
- `--file FILE`: File to index
- `--addr ADDRESS`: Server address (default: http://127.0.0.1:50051)

## Configuration API

### Creating a Custom Configuration

```rust
use crate::fugu::config::ConfigManager;

// Create with default path (~/.fugu)
let config = ConfigManager::new(None);

// Create with custom path
let config = ConfigManager::new(Some(PathBuf::from("/path/to/custom/dir")));
```

### Path Resolution

The ConfigManager provides methods to access various paths:

```rust
// Get base directory
let base_dir = config.base_dir();

// Get logs directory
let logs_dir = config.logs_dir();

// Get namespace directory
let ns_dir = config.namespace_dir("my_namespace");

// Get WAL path for a namespace
let wal_path = config.namespace_wal_path("my_namespace");

// Get index path for a namespace
let index_path = config.namespace_index_path("my_namespace");

// Get temp directory
let temp_dir = config.temp_dir();
```

## BM25 Configuration

Fugu's search algorithm uses BM25 with configurable parameters:

```rust
// Default values in index.rs
k1: 1.2,  // Term frequency saturation parameter
b: 0.75,  // Document length normalization parameter
```

These parameters control the balance between:
- Term frequency (how often a term appears in a document)
- Document length normalization (accounting for length differences)

## WAL Configuration

The WAL system has several tunable parameters:

```rust
// Constants in wal.rs
MAX_CONCURRENT_WRITERS: 8      // Maximum writers to the WAL
BUFFER_SIZE: 1024 * 1024       // 1MB buffer size for batching operations
FLUSH_INTERVAL_MS: 100         // Flush interval in milliseconds
```

## Environment Variable Support

Fugu uses the `HOME` environment variable to determine the default location for the `.fugu` directory. If `HOME` is not available, it falls back to `/tmp`.

## Best Practices

1. **Namespace Organization**: Create logical namespaces for different document collections
2. **Custom Base Directory**: For production use, specify a custom base directory on a reliable filesystem
3. **Regular Backups**: Back up the entire Fugu data directory for disaster recovery
4. **Monitoring**: Check the logs directory for errors and performance issues