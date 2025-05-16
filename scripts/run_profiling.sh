#!/bin/bash

# Create output directories if they don't exist
mkdir -p profiling_results/flamegraphs
mkdir -p profiling_results/pprof

# Set environment variables
echo "Setting up environment for profiling..."
export RUSTFLAGS='-g'

# Clean any previous benchmark results
echo "Cleaning previous benchmark results..."
cargo clean --bench

# Run each benchmark with profiling
echo "Running serialization benchmark..."
cargo bench --bench serialization_benchmark

echo "Running db operations benchmark..."
cargo bench --bench db_operations_benchmark

echo "Running query benchmark..."
cargo bench --bench query_benchmark

echo "Running large document benchmark..."
cargo bench --bench large_doc_benchmark

echo "Profiling complete! Flamegraph files are available in profiling_results/flamegraphs/"
echo "To view the results, open the SVG files in a web browser"