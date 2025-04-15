# Fugu Command Line Interface

Fugu provides a comprehensive command-line interface for server management, document operations, and configuration. This document covers available commands, options, and common use cases.

## Command Structure

Fugu commands follow this general structure:

```
cargo run -- [COMMAND] [SUBCOMMAND] [OPTIONS] [ARGUMENTS]
```

## Server Management Commands

### Starting the Server

```bash
# Start in foreground mode
cargo run -- up

# Start as a daemon
cargo run -- up --daemon

# Start with custom port
cargo run -- up --port 8080

# Start with custom PID and log files
cargo run -- up --daemon --pid-file /var/run/fugu.pid --log-file /var/log/fugu.log

# Start with a timeout (for testing)
cargo run -- up --timeout 300  # Shutdown after 5 minutes
```

### Stopping the Server

```bash
# Graceful shutdown
cargo run -- down

# Forced shutdown
cargo run -- down --force

# With custom PID file
cargo run -- down --pid-file /var/run/fugu.pid
```

### Checking Server Status

```bash
# Check server status
cargo run -- status

# Check status with custom PID file
cargo run -- status --pid-file /var/run/fugu.pid
```

## Document Operations

### Indexing Documents

```bash
# Index a file in the default namespace
cargo run -- namespace index --file /path/to/document.txt

# Add a file to a specific namespace
cargo run -- add --namespace my_namespace /path/to/file.txt

# Index with custom server address
cargo run -- add --namespace my_namespace --addr http://192.168.1.10:50051 /path/to/file.txt
```

### Searching for Documents

```bash
# Basic search in default namespace
cargo run -- search "search query"

# Search in specific namespace with limits
cargo run -- search --namespace my_namespace --limit 20 --offset 10 "search query"

# Search with custom server address
cargo run -- search --addr http://192.168.1.10:50051 "search query"
```

### Deleting Documents

```bash
# Delete a document from the default namespace
cargo run -- namespace delete --location "/document.txt"

# Delete with custom server address
cargo run -- namespace delete --location "/document.txt" --addr http://192.168.1.10:50051
```

## Namespace Management

### Namespace Operations

```bash
# Check status of a namespace
cargo run -- namespace my_namespace --status

# Initialize a namespace
cargo run -- namespace my_namespace --init

# Rebuild index for a namespace
cargo run -- namespace my_namespace --reindex
```

### Namespace Subcommands

```bash
# Start a namespace
cargo run -- namespace my_namespace up

# Stop a namespace
cargo run -- namespace my_namespace down

# Force shutdown of a namespace
cargo run -- namespace my_namespace down --force
```

## Command Reference

### Primary Commands

| Command | Description |
|---------|-------------|
| `up` | Start the Fugu server |
| `down` | Stop the Fugu server |
| `status` | Check server status |
| `search` | Search for documents |
| `add` | Add a document to a namespace |
| `namespace` | Namespace operations |

### Global Options

These options can be used with most commands:

| Option | Description |
|--------|-------------|
| `--addr` | Server address (default: http://127.0.0.1:50051) |
| `--namespace` | Target namespace (default: "default") |

### Server Options

| Option | Description |
|--------|-------------|
| `--port` | Port number (default: 50051) |
| `--daemon` | Run as a background daemon |
| `--pid-file` | Path to PID file (default: /tmp/fugu.pid) |
| `--log-file` | Path to log file (default: /tmp/fugu.log) |
| `--timeout` | Automatic shutdown timeout in seconds |

### Search Options

| Option | Description |
|--------|-------------|
| `--limit` | Maximum results to return (default: 10) |
| `--offset` | Number of results to skip (default: 0) |

## Command Examples

### Complete Workflow Example

```bash
# Start the server
cargo run -- up --daemon

# Add documents to different namespaces
cargo run -- add --namespace docs /path/to/documentation.txt
cargo run -- add --namespace code /path/to/source_code.rs

# Search in specific namespaces
cargo run -- search --namespace docs "configuration"
cargo run -- search --namespace code "function"

# Delete a document
cargo run -- namespace delete --location "/documentation.txt"

# Stop the server
cargo run -- down
```

### Batch Processing Example

```bash
# Start the server
cargo run -- up --daemon

# Index multiple files (shell script example)
for file in /data/documents/*.txt; do
  cargo run -- add --namespace batch_docs "$file"
done

# Search across the batch
cargo run -- search --namespace batch_docs --limit 100 "important topic"

# Stop the server
cargo run -- down
```

## Exit Codes

The CLI returns the following exit codes:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Command-line parsing error |
| 3 | Server error |
| 4 | Client error |