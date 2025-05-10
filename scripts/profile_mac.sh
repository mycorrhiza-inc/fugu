#!/bin/bash
# Mac-specific profiling script using Instruments and flamegraph

set -e

# Default values
BENCHMARK="all"
OUTPUT_DIR="./profiling_results"
FLAMEGRAPH=true
USE_INSTRUMENTS=true

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --bench)
      BENCHMARK="$2"
      shift 2
      ;;
    --output)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --no-flamegraph)
      FLAMEGRAPH=false
      shift
      ;;
    --no-instruments)
      USE_INSTRUMENTS=false
      shift
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --bench BENCHMARK    Specific benchmark to run (db_operations, serialization, query, or all)"
      echo "  --output DIR         Output directory for profiling results (default: ./profiling_results)"
      echo "  --no-flamegraph      Disable flamegraph generation"
      echo "  --no-instruments     Disable Apple Instruments profiling"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Create output directory
mkdir -p "$OUTPUT_DIR"
echo "Profiling results will be stored in $OUTPUT_DIR"

# Function to run a benchmark with flamegraph
run_benchmark_flamegraph() {
  local bench_name=$1
  echo "Running $bench_name benchmark with flamegraph..."
  
  # Run with flamegraph profiling
  # Note: PProfProfiler is already configured in the benchmark code
  cargo bench --bench "$bench_name"
  
  # Move the flamegraph output to our results directory
  # (Criterion generates flamegraphs in target/criterion by default)
  mkdir -p "$OUTPUT_DIR/flamegraph"
  if [ -d "target/criterion" ]; then
    find target/criterion -name "*.svg" -exec cp {} "$OUTPUT_DIR/flamegraph/" \;
    echo "Flamegraph results saved to $OUTPUT_DIR/flamegraph/"
  else
    echo "No flamegraph results found in target/criterion"
  fi
}

# Function to run a benchmark with Instruments
run_benchmark_instruments() {
  local bench_name=$1
  echo "Running $bench_name benchmark with Apple Instruments..."
  
  # First build the benchmark binary
  cargo build --release --bench "$bench_name"
  
  # Get the path to the benchmark binary
  BENCH_BIN="target/release/deps/${bench_name}-*"
  BENCH_PATH=$(echo $BENCH_BIN)
  
  if [ ! -f "$BENCH_PATH" ]; then
    echo "Could not find benchmark binary at $BENCH_PATH"
    return 1
  fi
  
  # Create a temporary directory for the Instruments trace
  TRACE_DIR="$OUTPUT_DIR/instruments"
  mkdir -p "$TRACE_DIR"
  TRACE_FILE="$TRACE_DIR/${bench_name}_trace.trace"
  
  echo "Starting Instruments profiling for $bench_name..."
  # Using Time Profiler instrument to capture CPU usage
  # The -D flag suppresses the UI
  xcrun xctrace record --template "Time Profiler" --output "$TRACE_FILE" --launch -- "$BENCH_PATH" --bench
  
  echo "Instruments trace saved to $TRACE_FILE"
  echo "You can open this file with Instruments.app to analyze the results"
}

# Main execution logic
if [ "$BENCHMARK" = "all" ] || [ "$BENCHMARK" = "db_operations" ]; then
  if [ "$FLAMEGRAPH" = true ]; then
    run_benchmark_flamegraph "db_operations_benchmark"
  fi
  if [ "$USE_INSTRUMENTS" = true ]; then
    run_benchmark_instruments "db_operations_benchmark"
  fi
fi

if [ "$BENCHMARK" = "all" ] || [ "$BENCHMARK" = "serialization" ]; then
  if [ "$FLAMEGRAPH" = true ]; then
    run_benchmark_flamegraph "serialization_benchmark"
  fi
  if [ "$USE_INSTRUMENTS" = true ]; then
    run_benchmark_instruments "serialization_benchmark"
  fi
fi

if [ "$BENCHMARK" = "all" ] || [ "$BENCHMARK" = "query" ]; then
  if [ "$FLAMEGRAPH" = true ]; then
    run_benchmark_flamegraph "query_benchmark"
  fi
  if [ "$USE_INSTRUMENTS" = true ]; then
    run_benchmark_instruments "query_benchmark"
  fi
fi

echo "Profiling complete. Results are available in $OUTPUT_DIR"