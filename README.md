# Fugu

Fugu is a high-performance search engine built in Rust with a gRPC API.

## Features

- Text search with whitespace tokenization
- Inverted index for fast lookup
- Multiple namespaces support
- gRPC server and client
- WAL (Write-Ahead Log) for data durability

## Getting Started

### Prerequisites

- Rust toolchain (rustc, cargo)
- Protobuf compiler (for gRPC)

### Installation

```bash
git clone https://github.com/yourusername/fugu.git
cd fugu
cargo build
```

### Running the Server

```bash
# Run the server in foreground mode
cargo run -- up

# Run as a daemon
cargo run -- up --daemon
```

### Client Operations

```bash
# Index a document
cargo run -- namespace index --file /path/to/document.txt

# Search
cargo run -- namespace search --query "search term" --limit 10

# Delete a document
cargo run -- namespace delete --location "/document.txt"
```

## Development

### Testing

The Fugu test suite is organized into several categories:

1. **Unit Tests**: Tests basic functionality of individual components
2. **Integration Tests**: Tests end-to-end functionality of the system
3. **Performance Tests**: Benchmarks for various operations
4. **Client Tests**: Tests specifically for client operations

Run tests using the test script:

```bash
# Run only unit tests (default)
./tests/run_tests.sh

# Run all test suites
./tests/run_tests.sh --all

# Run specific test suites
./tests/run_tests.sh --unit
./tests/run_tests.sh --integration
./tests/run_tests.sh --perf
./tests/run_tests.sh --client

# Run multiple test suites
./tests/run_tests.sh --unit --integration
```

For more detailed test documentation, see `tests/README.md`.

## Architecture

Fugu consists of several key components:

- **Inverted Index**: Core data structure for text search
- **GRPC Server**: Provides API for client interaction
- **WAL (Write-Ahead Log)**: Ensures durability of operations
- **Command Line Interface**: For server management and client operations

## License

[MIT](LICENSE)

## Acknowledgments

- This project uses the Tokio async runtime
- gRPC implementation with Tonic
- Inverted index inspired by search engine literature