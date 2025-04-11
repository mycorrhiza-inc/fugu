#!/bin/bash
set -e

# This script runs performance tests and generates visualizations
# It's designed to be used in CI/CD pipelines

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
  echo "Error: Python 3 is required but not found"
  exit 1
fi

# Check for dependencies
if ! python3 -c "import matplotlib" 2>/dev/null || ! python3 -c "import numpy" 2>/dev/null; then
  echo "Installing required Python dependencies..."
  pip3 install -r tests/requirements.txt
fi

# Run performance tests and generate visualizations
echo "Running performance tests and generating visualizations..."
python3 tests/perf_visualize.py

# Check if visualizations were created
if [ -d "tests/perf_results" ]; then
  echo "Performance visualizations generated successfully in tests/perf_results/"
  ls -la tests/perf_results/
  exit 0
else
  echo "Error: Failed to generate performance visualizations"
  exit 1
fi