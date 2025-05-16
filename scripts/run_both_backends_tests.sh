#!/bin/bash
set -e

# Colors for output formatting
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to run tests with a specific backend
run_backend_tests() {
  test_pattern=$1

  echo -e "\n${YELLOW}==== Running $test_pattern tests  ====${NC}"

  # Run the tests with the specified backend
  if cargo test $test_pattern -- --nocapture; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    return 0
  else
    echo -e "${RED}✗ Some tests failed${NC}"
    return 1
  fi
}

# Set environment for more verbose test output
export RUST_BACKTRACE=1
export RUST_LOG=debug

# Run document statistics tests with both backends
echo -e "${YELLOW}Running document statistics tests...${NC}"
run_backend_tests "document_statistics_test"

# Run query tests with both backends
echo -e "\n${YELLOW}Running query tests...${NC}"
run_backend_tests "query_test"

# Run inverted index tests with both backends
echo -e "\n${YELLOW}Running inverted index tests...${NC}"
run_backend_tests "test_inverted_index"

echo -e "\n${GREEN}All tests completed!${NC}"

