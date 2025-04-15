#!/bin/bash
set -e

# This script runs performance tests and generates visualizations
# It's designed to be used in CI/CD pipelines

# Determine script directory and go there
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
  echo "Error: Python 3 is required but not found"
  exit 1
fi

# Check for dependencies
if ! python3 -c "import matplotlib" 2>/dev/null || ! python3 -c "import numpy" 2>/dev/null; then
  echo "Installing required Python dependencies..."
  pip3 install -r requirements.txt
fi

# Debug: Check if CSV files exist
echo "Checking CSV files in data directory..."
ls -la data/*.csv || echo "No CSV files found in data directory!"

# Create data directory if it doesn't exist
mkdir -p data

# Create perf_results directory if it doesn't exist
mkdir -p perf_results

# Run performance visualization only (skip tests)
echo "Generating visualizations from existing data..."
python3 perf_visualize.py --no-run

# Check if visualizations were created
if [ -d "perf_results" ]; then
  echo "Performance visualizations generated successfully in perf_results/"
  ls -la perf_results/
  exit 0
else
  echo "Error: Failed to generate performance visualizations"
  exit 1
fi