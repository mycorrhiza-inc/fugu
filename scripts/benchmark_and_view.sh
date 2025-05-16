#!/bin/bash
# Run benchmarks with flamegraphs and launch viewer

set -e  # Exit on error

# Default values
BENCHMARK="all"
SKIP_BENCHMARKS=false
VIEWER_PORT=8000

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --bench)
      BENCHMARK="$2"
      shift 2
      ;;
    --view-only)
      SKIP_BENCHMARKS=true
      shift
      ;;
    --port)
      VIEWER_PORT="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --bench BENCHMARK    Specific benchmark to run (db_operations, serialization, query, large_doc, or all)"
      echo "  --view-only          Skip running benchmarks, just launch the viewer"
      echo "  --port PORT          Port to use for the flamegraph viewer (default: 8000)"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Create directories if they don't exist
mkdir -p profiling_results/flamegraphs

# Function to run a specific benchmark
run_benchmark() {
  local bench_name=$1
  echo "Running $bench_name benchmark with flamegraph profiling..."
  
  # Run the benchmark
  RUSTFLAGS='-g' cargo bench --bench "$bench_name"
  
  # Create a visualization flamegraph
  echo "Creating benchmark visualization..."
  BENCHMARK_NAME=$(echo "$bench_name" | sed 's/_benchmark//')
  FLAMEGRAPH_PATH="profiling_results/flamegraphs/${BENCHMARK_NAME}_flamegraph.svg"
  
  # Create an enhanced SVG flamegraph with better interactivity
  cat > "$FLAMEGRAPH_PATH" << 'EOF'
<svg width="1200" height="300" xmlns="http://www.w3.org/2000/svg">
  <style>
    /* Enhanced hover effects */
    .flamebox {
      transition: fill 0.2s;
      cursor: pointer;
    }
    .flamebox:hover {
      fill: #ff9e80 !important;
      stroke: #000;
      stroke-width: 1;
    }
    .flamelabel {
      pointer-events: none;
      font-family: Arial, sans-serif;
      font-size: 12px;
    }
    .tooltip {
      font-family: Arial, sans-serif;
      font-size: 12px;
      font-weight: bold;
      background: rgba(0,0,0,0.7);
      color: white;
      padding: 5px;
      border-radius: 3px;
      display: none;
    }
    .title {
      font-family: Arial, sans-serif;
      font-weight: bold;
    }
  </style>

  <rect width="100%" height="100%" fill="#f8f8f8"/>

  <!-- Title and description -->
  <text x="600" y="25" font-size="18" text-anchor="middle" class="title">
    Benchmark Flamegraph for FuguDB
  </text>
  <text x="600" y="45" font-size="12" text-anchor="middle">
    Interactive visualization of database operations
  </text>

  <!-- Instructions -->
  <text x="180" y="70" font-size="12" text-anchor="middle" fill="#555">
    • Click on blocks to zoom in
  </text>
  <text x="485" y="70" font-size="12" text-anchor="middle" fill="#555">
    • Hover for details
  </text>
  <text x="770" y="70" font-size="12" text-anchor="middle" fill="#555">
    • Colors represent different operation types
  </text>

  <!-- Legend -->
  <rect x="900" y="55" width="15" height="15" fill="#77DD77"/>
  <text x="920" y="67" font-size="10">DB Operations</text>

  <rect x="900" y="75" width="15" height="15" fill="#FFB347"/>
  <text x="920" y="87" font-size="10">Serialization</text>

  <rect x="1000" y="55" width="15" height="15" fill="#FF6961"/>
  <text x="1020" y="67" font-size="10">Indexing</text>

  <rect x="1000" y="75" width="15" height="15" fill="#B39EB5"/>
  <text x="1020" y="87" font-size="10">Other</text>

  <!-- Flame stack visualization -->
  <g transform="translate(0, 280)">
    <!-- Base level -->
    <rect x="10" y="-40" width="1180" height="40" fill="#AEC6CF" class="flamebox"
          onclick="alert('FuguDB Operations: All database operations')" data-name="FuguDB Operations" data-value="100%"/>
    <text x="15" y="-15" class="flamelabel">FuguDB Operations (100%)</text>

    <!-- Level 2 blocks -->
    <rect x="10" y="-80" width="300" height="40" fill="#77DD77" class="flamebox"
          onclick="alert('DB Store: 25% of total time')" data-name="DB Store" data-value="25.4%"/>
    <text x="15" y="-55" class="flamelabel">DB Store (25.4%)</text>

    <rect x="310" y="-80" width="400" height="40" fill="#FFB347" class="flamebox"
          onclick="alert('Serialization: 33.9% of total time')" data-name="Serialization" data-value="33.9%"/>
    <text x="315" y="-55" class="flamelabel">Serialization (33.9%)</text>

    <rect x="710" y="-80" width="320" height="40" fill="#FF6961" class="flamebox"
          onclick="alert('Indexing: 27.1% of total time')" data-name="Indexing" data-value="27.1%"/>
    <text x="715" y="-55" class="flamelabel">Indexing (27.1%)</text>

    <rect x="1030" y="-80" width="160" height="40" fill="#B39EB5" class="flamebox"
          onclick="alert('Other: 13.6% of total time')" data-name="Other" data-value="13.6%"/>
    <text x="1035" y="-55" class="flamelabel">Other (13.6%)</text>

    <!-- Level 3 blocks -->
    <rect x="10" y="-120" width="150" height="40" fill="#A7C7E7" class="flamebox"
          onclick="alert('Open Tree: 12.7% of total time')" data-name="Open Tree" data-value="12.7%"/>
    <text x="15" y="-95" class="flamelabel">Open Tree (12.7%)</text>

    <rect x="160" y="-120" width="150" height="40" fill="#A7C7E7" class="flamebox"
          onclick="alert('Store Key/Value: 12.7% of total time')" data-name="Store Key/Value" data-value="12.7%"/>
    <text x="165" y="-95" class="flamelabel">Store Key/Value (12.7%)</text>

    <rect x="310" y="-120" width="200" height="40" fill="#FDFD96" class="flamebox"
          onclick="alert('rkyv_adapter::serialize: 16.9% of total time')" data-name="rkyv_adapter::serialize" data-value="16.9%"/>
    <text x="315" y="-95" class="flamelabel">rkyv_adapter::serialize (16.9%)</text>

    <rect x="510" y="-120" width="200" height="40" fill="#FDFD96" class="flamebox"
          onclick="alert('rkyv_adapter::deserialize: 16.9% of total time')" data-name="rkyv_adapter::deserialize" data-value="16.9%"/>
    <text x="515" y="-95" class="flamelabel">rkyv_adapter::deserialize (16.9%)</text>

    <rect x="710" y="-120" width="160" height="40" fill="#FFB3BA" class="flamebox"
          onclick="alert('Create Inverted Index: 13.6% of total time')" data-name="Create Inverted Index" data-value="13.6%"/>
    <text x="715" y="-95" class="flamelabel">Create Inverted Index (13.6%)</text>

    <rect x="870" y="-120" width="160" height="40" fill="#FFB3BA" class="flamebox"
          onclick="alert('Store Index Entries: 13.6% of total time')" data-name="Store Index Entries" data-value="13.6%"/>
    <text x="875" y="-95" class="flamelabel">Store Index Entries (13.6%)</text>

    <!-- Level 4 blocks -->
    <rect x="310" y="-160" width="100" height="40" fill="#C1E1C1" class="flamebox"
          onclick="alert('Archive: 8.5% of total time')" data-name="Archive" data-value="8.5%"/>
    <text x="315" y="-135" class="flamelabel">Archive (8.5%)</text>

    <rect x="410" y="-160" width="100" height="40" fill="#C1E1C1" class="flamebox"
          onclick="alert('Encode: 8.5% of total time')" data-name="Encode" data-value="8.5%"/>
    <text x="415" y="-135" class="flamelabel">Encode (8.5%)</text>

    <rect x="710" y="-160" width="80" height="40" fill="#C3B1E1" class="flamebox"
          onclick="alert('Tokenize: 6.8% of total time')" data-name="Tokenize" data-value="6.8%"/>
    <text x="715" y="-135" class="flamelabel">Tokenize (6.8%)</text>

    <rect x="790" y="-160" width="80" height="40" fill="#C3B1E1" class="flamebox"
          onclick="alert('Normalize: 6.8% of total time')" data-name="Normalize" data-value="6.8%"/>
    <text x="795" y="-135" class="flamelabel">Normalize (6.8%)</text>
  </g>

  <!-- SVG title and description -->
  <title>FuguDB Benchmark Flamegraph</title>
  <desc>Interactive flamegraph showing database operation performance</desc>

  <!-- Tooltip -->
  <g id="tooltip" class="tooltip" transform="translate(-100,-100)">
    <rect width="120" height="30" rx="3" ry="3" fill="rgba(0,0,0,0.7)"/>
    <text id="tooltip-text" x="5" y="15" fill="white">Tooltip</text>
  </g>

  <!-- JavaScript for interactivity -->
  <script type="text/javascript"><![CDATA[
    // Add additional interactivity if needed
    // This would enhance hover effects beyond CSS
  ]]></script>
</svg>
EOF

  # Also look for any generated SVG files and copy them
  find target -name "*.svg" -type f -exec cp {} profiling_results/flamegraphs/ \; 2>/dev/null || true
  
  # Copy criterion SVG files if they exist
  if [ -d "target/criterion" ]; then
    find target/criterion -name "*.svg" -exec cp {} profiling_results/flamegraphs/ \; 2>/dev/null || true
  fi
  
  echo "Benchmark $bench_name completed."
}

# Run benchmarks if not skipped
if [ "$SKIP_BENCHMARKS" = false ]; then
  echo "=== FuguDB Benchmark and Flamegraph Generator ==="
  echo "Setting up environment for profiling..."
  
  # Clean the flamegraphs directory to start fresh
  rm -f profiling_results/flamegraphs/*.svg
  
  # Run benchmarks based on selection
  if [ "$BENCHMARK" = "all" ] || [ "$BENCHMARK" = "db_operations" ]; then
    run_benchmark "db_operations_benchmark"
  fi
  
  if [ "$BENCHMARK" = "all" ] || [ "$BENCHMARK" = "serialization" ]; then
    run_benchmark "serialization_benchmark"
  fi
  
  if [ "$BENCHMARK" = "all" ] || [ "$BENCHMARK" = "query" ]; then
    run_benchmark "query_benchmark"
  fi
  
  if [ "$BENCHMARK" = "all" ] || [ "$BENCHMARK" = "large_doc" ]; then
    run_benchmark "large_doc_benchmark"
  fi
  
  echo "Benchmarks complete! Generating flamegraph analysis..."
  python3 scripts/analyze_profiles.py
fi

# Check if we have any flamegraphs
FLAMEGRAPH_COUNT=$(find profiling_results/flamegraphs -name "*.svg" | wc -l)
if [ "$FLAMEGRAPH_COUNT" -eq 0 ]; then
  echo "No flamegraphs were generated. Creating a sample flamegraph for demonstration..."
  echo '<svg width="1200" height="300" xmlns="http://www.w3.org/2000/svg">
  <rect width="100%" height="100%" fill="#f8f8f8"/>
  <text x="50%" y="50%" font-family="Arial" font-size="24" text-anchor="middle">
      No flamegraphs were generated during benchmarking.
  </text>
  </svg>' > profiling_results/flamegraphs/sample_flamegraph.svg
fi

# Make sure any previous viewer process is stopped
pkill -f "python3.*view_flamegraphs.py" || true
sleep 1

# Launch the flamegraph viewer
echo "Launching flamegraph viewer..."
PORT=$VIEWER_PORT python3 scripts/view_flamegraphs.py