#!/bin/bash
set -e

# Parse command line arguments
RUN_UNIT_TESTS=true
RUN_INTEGRATION_TESTS=false
RUN_PERFORMANCE_TESTS=false
RUN_CLIENT_TESTS=false
RUN_ALL=false

print_usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  --all                  Run all test suites"
  echo "  --unit                 Run unit tests only"
  echo "  --integration          Run integration tests only"
  echo "  --perf                 Run performance tests only"
  echo "  --client               Run client operation tests only"
  echo "  --help                 Display this help message"
}

# Process command line arguments
if [ $# -eq 0 ]; then
  # Default behavior: run unit tests only
  RUN_UNIT_TESTS=true
else
  for arg in "$@"; do
    case $arg in
      --all)
        RUN_ALL=true
        RUN_UNIT_TESTS=true
        RUN_INTEGRATION_TESTS=true
        RUN_PERFORMANCE_TESTS=true
        RUN_CLIENT_TESTS=true
        ;;
      --unit)
        RUN_UNIT_TESTS=true
        ;;
      --integration)
        RUN_INTEGRATION_TESTS=true
        ;;
      --perf)
        RUN_PERFORMANCE_TESTS=true
        ;;
      --client)
        RUN_CLIENT_TESTS=true
        ;;
      --help)
        print_usage
        exit 0
        ;;
      *)
        echo "Unknown option: $arg"
        print_usage
        exit 1
        ;;
    esac
  done
fi

# Build the project first
echo "Building project..."
cargo build

# Set environment variable for client tests
export CARGO_BIN_EXE_fugu="$(pwd)/target/debug/fugu"

# Run unit tests if requested
if [ "$RUN_UNIT_TESTS" = true ]; then
  echo "Running unit tests..."
  # Run unit tests excluding performance tests
  cargo test --lib -- --skip test_insert_performance --skip test_search_performance --skip test_text_search_performance --skip test_delete_performance --skip test_flush_performance
fi

# Run integration tests if requested
if [ "$RUN_INTEGRATION_TESTS" = true ]; then
  echo "Running integration tests..."
  cargo test --features integration-tests --test grpc_tests
fi

# Run performance tests if requested
if [ "$RUN_PERFORMANCE_TESTS" = true ]; then
  echo "Running performance tests..."
  cargo test --features performance-tests --lib -- test_insert_performance test_search_performance test_text_search_performance test_delete_performance test_flush_performance
fi

# Run client operation tests if requested
if [ "$RUN_CLIENT_TESTS" = true ]; then
  echo "Running client operation tests..."
  echo "Starting the GRPC server in the background..."
  # Start the GRPC server on port 50053
  cargo run -- up &
  SERVER_PID=$!

  # Give the server time to start
  sleep 2

  echo "Testing client operations..."
  # Create a test file
  echo "This is a test document for testing the Fugu search engine" > /tmp/test_doc.txt

  # Test the index command
  echo "Testing index operation..."
  cargo run -- namespace index --addr http://127.0.0.1:50053 --file /tmp/test_doc.txt

  # Test the search command
  echo "Testing search operation..."
  cargo run -- namespace search --addr http://127.0.0.1:50053 --query "test" --limit 10

  # Test the delete command
  echo "Testing delete operation..."
  cargo run -- namespace delete --addr http://127.0.0.1:50053 --location "/test_doc.txt"

  # Clean up
  echo "Cleaning up..."
  kill $SERVER_PID
  rm -f /tmp/test_doc.txt
fi

# Clean up temp files
rm -rf /tmp/fugu_test_data

if [ "$RUN_ALL" = true ]; then
  echo "All test suites completed successfully!"
elif [ "$RUN_UNIT_TESTS" = true ] && [ "$RUN_INTEGRATION_TESTS" = false ] && [ "$RUN_PERFORMANCE_TESTS" = false ] && [ "$RUN_CLIENT_TESTS" = false ]; then
  echo "Unit tests completed successfully!"
elif [ "$RUN_INTEGRATION_TESTS" = true ] && [ "$RUN_UNIT_TESTS" = false ] && [ "$RUN_PERFORMANCE_TESTS" = false ] && [ "$RUN_CLIENT_TESTS" = false ]; then
  echo "Integration tests completed successfully!"
elif [ "$RUN_PERFORMANCE_TESTS" = true ] && [ "$RUN_UNIT_TESTS" = false ] && [ "$RUN_INTEGRATION_TESTS" = false ] && [ "$RUN_CLIENT_TESTS" = false ]; then
  echo "Performance tests completed successfully!"
elif [ "$RUN_CLIENT_TESTS" = true ] && [ "$RUN_UNIT_TESTS" = false ] && [ "$RUN_INTEGRATION_TESTS" = false ] && [ "$RUN_PERFORMANCE_TESTS" = false ]; then
  echo "Client operation tests completed successfully!"
else
  echo "Selected test suites completed successfully!"
fi