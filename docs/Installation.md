# Installation Guide

This guide covers the installation and setup of Fugu, a high-performance search engine with a gRPC API.

## Prerequisites

Before installing Fugu, ensure you have the following prerequisites:

- Rust toolchain (rustc, cargo) - version 1.50.0 or later
- Protobuf compiler (protoc) - version 3.14.0 or later
- Git

## Installing from Source

1. Clone the repository:

```bash
git clone https://github.com/yourusername/fugu.git
cd fugu
```

2. Build the project:

```bash
cargo build --release
```

3. Verify the installation:

```bash
cargo run -- --version
```

## Directory Setup

By default, Fugu stores its data in `~/.fugu`. This includes:

- Configuration files
- Namespace data
- Index files
- Write-ahead logs

You can customize this location by providing a custom path when running commands.

### Directory Structure

```
~/.fugu/
  /logs           # Log files
  /namespaces     # Namespace data
    /default      # Default namespace
      /index      # Index files
      /wal.bin    # Write-ahead log
    /custom_ns    # Custom namespaces
  /tmp            # Temporary files
```

## Configuration

Fugu uses a simple directory-based configuration system. No explicit configuration file is needed, but you can customize various aspects through command-line arguments.

### Custom Base Directory

To use a custom base directory:

```bash
cargo run -- --config-path /path/to/custom/directory up
```

## Next Steps

Once you have Fugu installed, you can:

1. [Start the server](Server.md)
2. [Create namespaces](Namespaces.md)
3. [Index your first documents](Indexing.md)
4. [Run your first search](Searching.md)

## Troubleshooting

### Common Issues

**Protobuf Compiler Missing:**

```
error: failed to run custom build command for `fugu`
...
protoc: command not found
```

Solution: Install the protobuf compiler:
- Ubuntu/Debian: `sudo apt install protobuf-compiler`
- macOS: `brew install protobuf`

**Permission Denied:**

```
Error: Permission denied (os error 13)
```

Solution: Check the permissions of your data directory and ensure your user has write access.

**Port Already in Use:**

```
Error: Address already in use (os error 98)
```

Solution: Change the port using the `--port` option or stop the other application using the port.