# Fugu Test Suite

This directory contains the test suite for the Fugu search engine.

## New Shared Server Architecture

The Fugu test suite has been updated to use a single shared server instance for all integration tests. This improves test efficiency and resource usage.

### How it works

1. The shared server is managed by code in `shared_test_fixture.rs`.
2. Tests use the `ensure_server_running()` function to get a server URL.
3. The server is started only once (using thread-safe initialization).
4. A cleanup routine ensures the server is shut down when tests complete.

### Benefits

- Faster test execution (no server startup/shutdown for each test)
- More realistic testing of server state between operations
- Less resource usage (only one server instance)
- Simpler test code (server management is centralized)

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

## Performance Test Visualizations

When running performance tests with the `--perf` flag, the system automatically generates visualizations if Python 3 is available:

```bash
./tests/run_tests.sh --perf
```

### Visualization Features

- **Bar Charts**: Shows p10, p50, p90, and p99 percentiles for each performance test
- **Trend Charts**: Tracks performance changes over time across multiple test runs
- **Data Storage**: Performance data is stored in JSON format for historical comparisons

### Visualization Options

```bash
# Generate visualizations without running tests (uses existing data)
python3 tests/perf_visualize.py --no-run

# Specify a custom history file
python3 tests/perf_visualize.py --history-file=/path/to/history.json

# Specify a custom output directory
python3 tests/perf_visualize.py --output-dir=/path/to/output

# For CI/CD pipelines: Run tests and generate visualizations in one step
./tests/run_perf_viz.sh
```

Visualization results are saved to `tests/perf_results/` by default. The visualizations are automatically generated whenever you run the performance tests.

## Test Files

- `main.rs`: Initializes and cleans up the shared test server
- `shared_test_fixture.rs`: Contains shared server management code
- `search_tests.rs`: Tests for search functionality
- `case_insensitive_search_test.rs`: Tests for case-insensitive search
- `grpc_tests.rs`: Integration tests for GRPC functionality
- `document_cache_test.rs`: Tests for document caching
- `hot_cold_loading_tests.rs`: Performance tests for hot vs cold loading
- `run_tests.sh`: Script to automate running different test suites
- `perf_visualize.py`: Script to generate performance test visualizations