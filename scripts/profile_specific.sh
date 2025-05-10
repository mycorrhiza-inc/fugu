#!/bin/bash
# Script to profile specific functions or modules in Fugu

set -e

# Default values
TARGET_BINARY="target/release/fugu"
OUTPUT_DIR="./profiling_results/specific"
DURATION=30
TARGET_FUNCTION=""
USE_INSTRUMENTS=true

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --binary)
      TARGET_BINARY="$2"
      shift 2
      ;;
    --output)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --function)
      TARGET_FUNCTION="$2"
      shift 2
      ;;
    --no-instruments)
      USE_INSTRUMENTS=false
      shift
      ;;
    --help)
      echo "Usage: $0 [options] -- [program arguments]"
      echo "Options:"
      echo "  --binary PATH        Path to the binary to profile (default: target/release/fugu)"
      echo "  --output DIR         Output directory for profiling results (default: ./profiling_results/specific)"
      echo "  --duration SEC       Duration in seconds to run profiling (default: 30)"
      echo "  --function NAME      Focus on a specific function name (optional)"
      echo "  --no-instruments     Disable Apple Instruments profiling"
      echo "  -- ARGS              Arguments to pass to the target program"
      exit 0
      ;;
    --)
      shift
      PROGRAM_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Ensure the binary exists
if [ ! -f "$TARGET_BINARY" ]; then
  echo "Error: Target binary not found at $TARGET_BINARY"
  echo "Run 'cargo build --release' first"
  exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"
echo "Profiling results will be stored in $OUTPUT_DIR"

# Function to run with Apple Instruments
run_with_instruments() {
  local binary=$1
  shift
  local args=("$@")
  
  echo "Starting Instruments profiling for $binary..."
  echo "Running for $DURATION seconds..."
  
  # Create trace file path
  local timestamp=$(date +%Y%m%d_%H%M%S)
  local trace_file="$OUTPUT_DIR/${timestamp}_profile.trace"
  
  # Run with Time Profiler instrument
  # The --time-limit option sets the duration
  # The --template option selects the Time Profiler instrument
  if [ -n "$TARGET_FUNCTION" ]; then
    echo "Focusing on function: $TARGET_FUNCTION"
    # Use custom instrument configuration with function filter
    xcrun xctrace record --time-limit "$DURATION" \
      --template "Time Profiler" \
      --output "$trace_file" \
      --launch -- "$binary" "${args[@]}"
  else
    # Standard profiling without function filter
    xcrun xctrace record --time-limit "$DURATION" \
      --template "Time Profiler" \
      --output "$trace_file" \
      --launch -- "$binary" "${args[@]}"
  fi
  
  echo "Instruments trace saved to $trace_file"
  echo "You can open this file with Instruments.app to analyze the results"
}

# Function to generate sampling profile with sample command
run_with_sample() {
  local binary=$1
  shift
  local args=("$@")
  
  echo "Starting sampling profiling for $binary..."
  echo "Running for $DURATION seconds..."
  
  # Create output file path
  local timestamp=$(date +%Y%m%d_%H%M%S)
  local sample_file="$OUTPUT_DIR/${timestamp}_sample.txt"
  
  # Start the process in the background
  "$binary" "${args[@]}" &
  local pid=$!
  
  # Run the sample command against the process
  echo "Sampling process $pid for $DURATION seconds..."
  sample "$pid" "$DURATION" -file "$sample_file"
  
  # Kill the process if it's still running
  if kill -0 $pid 2>/dev/null; then
    kill $pid
  fi
  
  echo "Sampling profile saved to $sample_file"
}

# Main execution logic
if [ "$USE_INSTRUMENTS" = true ]; then
  run_with_instruments "$TARGET_BINARY" "${PROGRAM_ARGS[@]}"
else
  run_with_sample "$TARGET_BINARY" "${PROGRAM_ARGS[@]}"
fi

echo "Profiling complete. Results are available in $OUTPUT_DIR"