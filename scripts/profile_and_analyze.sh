#!/bin/bash

# Profile and analyze database performance
echo "=== FuguDB Profiling and Analysis Tool ==="

# Step 1: Run profiling
echo "Step 1: Running profiling benchmarks (this may take several minutes)..."
./scripts/run_profiling.sh

# Step 2: Analyze results
echo "Step 2: Analyzing profiling results..."
./scripts/analyze_profiles.py

echo "=== Profiling and analysis complete! ==="
echo "You can view the detailed analysis in PROFILING_ANALYSIS.md"
echo "Raw flamegraphs are available in profiling_results/flamegraphs/"