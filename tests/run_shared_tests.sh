#!/bin/bash
# Run the integration tests with the shared server

set -e

echo "Running Fugu integration tests with shared server..."

# Run the tests
RUST_BACKTRACE=1 cargo test --test main --test search_tests --test case_insensitive_search_test --test grpc_tests

echo "All tests completed successfully!"