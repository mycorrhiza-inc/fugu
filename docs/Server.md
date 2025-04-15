# Fugu Server

The Fugu server is the main component that provides indexing and search functionality. It manages namespaces, handles client requests, and ensures data durability.

## Server Architecture

The server consists of several key components:

1. **FuguServer**: The main server implementation that manages the lifecycle.
2. **Write-Ahead Log (WAL)**: Ensures durability of operations.
3. **Namespace Management**: Handles isolation between different namespaces.
4. **gRPC Interface**: Provides remote API access.

## Running the Server

### Starting in Foreground Mode

To start the server in the foreground:

```bash
cargo run -- up
```

This will start the server with the default configuration, listening on port 50051.

### Starting as a Daemon

To run the server as a background daemon:

```bash
cargo run -- up --daemon
```

When running as a daemon, the server writes logs to `/tmp/fugu.log` by default, and creates a PID file at `/tmp/fugu.pid`.

### Customizing Server Options

You can customize various server options:

```bash
cargo run -- up [OPTIONS]
```

Available options:

- `--port PORT`: Specify the port number (default: 50051)
- `--daemon`: Run as a background daemon
- `--pid-file FILE`: Specify a custom PID file location
- `--log-file FILE`: Specify a custom log file location
- `--timeout SECONDS`: Set an automatic shutdown timeout (useful for testing)

## Stopping the Server

To gracefully shut down the server:

```bash
cargo run -- down
```

For a forced shutdown:

```bash
cargo run -- down --force
```

## Checking Server Status

To check the status of the server:

```bash
cargo run -- status
```

This will show if the server is running and provide basic metrics.

## Server Lifecycle

1. **Initialization**: The server loads configuration and prepares resources.
2. **WAL Processing**: A background task is started to process Write-Ahead Log commands.
3. **gRPC Service**: The gRPC service is started to handle client requests.
4. **Request Handling**: Client requests are routed to the appropriate namespace nodes.
5. **Shutdown**: On shutdown, all pending operations are flushed to disk.

## Server Configuration

The server uses a directory-based configuration system. By default, it stores all data in `~/.fugu`. You can customize this with the `--config` option.

## Performance Considerations

- The server is designed to handle concurrent requests efficiently.
- For large deployments, consider:
  - Increasing the WAL channel capacity (default is 1000)
  - Adjusting the maximum concurrent writers (default is 8)
  - Tuning the flush interval (default is 100ms)

## Monitoring and Logging

The server logs various events using the tracing framework:

- `info`: Normal operational events
- `warn`: Potential issues that don't affect functionality
- `error`: Errors that may affect functionality
- `debug`: Detailed information for troubleshooting (only with debug builds)

When running in daemon mode, logs are written to the specified log file.