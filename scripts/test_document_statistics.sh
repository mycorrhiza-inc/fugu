#!/bin/bash
set -e

# Set environment for more verbose test output
export RUST_BACKTRACE=1
export RUST_LOG=debug

echo "==== Testing document statistics ===="
cargo test document_statistics_tests -- --nocapture

echo ""
echo "All tests completed successfully!"

