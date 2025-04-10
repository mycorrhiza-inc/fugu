# Fugu Test Suite

This directory contains the test suite for the Fugu search engine.

## Test Categories

The Fugu test suite is organized into several categories:

1. **Unit Tests**: Tests basic functionality of individual components
2. **Integration Tests**: Tests end-to-end functionality of the system
3. **Performance Tests**: Benchmarks for various operations
4. **Client Tests**: Tests specifically for client operations

## Running Tests

The `run_tests.sh` script is used to run various test suites:

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

### Test Options Explained

- `--unit`: Basic functionality tests
- `--integration`: End-to-end GRPC client/server tests
- `--perf`: Performance benchmark tests (index/search/delete operations)
- `--client`: Client operation tests with a live server
- `--all`: Run all test categories
- `--help`: Display usage information

## Running Tests with Cargo

You can also run tests directly with Cargo:

```bash
# Run unit tests only
cargo test --lib -- --skip test_insert_performance --skip test_search_performance --skip test_text_search_performance --skip test_delete_performance --skip test_flush_performance

# Run integration tests
cargo test --features integration-tests --test grpc_tests

# Run performance tests
cargo test --features performance-tests --lib -- test_insert_performance test_search_performance test_text_search_performance test_delete_performance test_flush_performance
```

## Test Files

- `grpc_tests.rs`: Integration tests for GRPC functionality
- `run_tests.sh`: Script to automate running different test suites