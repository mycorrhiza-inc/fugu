#!/usr/bin/env python3
"""
Analyze profiling results and generate a summary report.
This script looks at the generated flamegraphs and extracts key information.
"""

import os
import glob
import re
import json
import math
from datetime import datetime
from collections import defaultdict
import statistics

# Configuration
FLAMEGRAPH_DIR = "profiling_results/flamegraphs"
OUTPUT_FILE = "PROFILING_ANALYSIS.md"

def find_svg_files():
    """Find all SVG files in the flamegraph directory."""
    return glob.glob(f"{FLAMEGRAPH_DIR}/*.svg")

def parse_svg_data(svg_file):
    """Extract function data from SVG file."""
    try:
        with open(svg_file, 'r') as f:
            content = f.read()
            
        # Extract title and timestamp
        title_match = re.search(r'<title>(.*?)</title>', content)
        title = title_match.group(1) if title_match else "Unknown Benchmark"
        
        # Extract function data (this is simplified - actual SVG parsing is more complex)
        # Look for the function boxes in the SVG
        functions = defaultdict(int)
        for match in re.finditer(r'<title>(.*?) \((\d+) samples, ([0-9.]+)%\)</title>', content):
            function_name, samples, percentage = match.groups()
            functions[function_name] = float(percentage)
            
        return {
            "file": os.path.basename(svg_file),
            "title": title,
            "functions": dict(sorted(functions.items(), key=lambda x: x[1], reverse=True)[:20])  # Top 20 functions
        }
    except Exception as e:
        print(f"Error parsing {svg_file}: {e}")
        return {
            "file": os.path.basename(svg_file),
            "title": "Error parsing file",
            "functions": {}
        }

def analyze_benchmark_results():
    """Analyze JSON benchmark results for percentile data."""
    results_dir = "benchmark_results"
    if not os.path.exists(results_dir):
        print(f"No benchmark results found in {results_dir}")
        return {}

    benchmark_data = {}

    # Find all JSON files in the benchmarks directory and subdirectories
    for subdir, _, files in os.walk(results_dir):
        for file in files:
            if file.endswith('.json'):
                full_path = os.path.join(subdir, file)
                try:
                    with open(full_path, 'r') as f:
                        data = json.load(f)

                    # Extract benchmark type from path
                    benchmark_type = os.path.basename(os.path.dirname(full_path))
                    test_case = data.get('test_case', 'unknown')

                    # Create a key for this benchmark
                    key = f"{benchmark_type}_{test_case}"

                    # Extract percentile data if available
                    percentile_data = {
                        'p90_ms': data.get('p90_ms', None),
                        'p95_ms': data.get('p95_ms', None),
                        'p99_ms': data.get('p99_ms', None),
                        'record_size': data.get('record_size', None),
                        'batch_size': data.get('batch_size', None),
                        'doc_count': data.get('doc_count', None),
                        'iterations': data.get('iterations', 0)
                    }

                    benchmark_data[key] = percentile_data

                except Exception as e:
                    print(f"Error processing {full_path}: {e}")

    return benchmark_data

def generate_report(data):
    """Generate a markdown report from the parsed data."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get benchmark result data
    benchmark_results = analyze_benchmark_results()

    report = f"""# Database Profiling Analysis

Generated: {timestamp}

## Overview

This report summarizes the performance profiling results for the FuguDB database operations.
The analysis is based on flamegraphs generated from benchmark runs and percentile statistics.

## Performance Statistics

"""
    # Add percentile data if available
    if benchmark_results:
        report += "### Latency Percentiles\n\n"
        report += "| Benchmark | Test Case | p90 (ms) | p95 (ms) | p99 (ms) | Sample Size |\n"
        report += "|-----------|-----------|----------|----------|----------|-------------|\n"

        for key, data in sorted(benchmark_results.items()):
            benchmark_type, test_case = key.split('_', 1)
            p90 = data.get('p90_ms', 'N/A')
            p95 = data.get('p95_ms', 'N/A')
            p99 = data.get('p99_ms', 'N/A')
            iterations = data.get('iterations', 'N/A')

            # Format the values nicely if they're numbers
            if isinstance(p90, (int, float)):
                p90 = f"{p90:.3f}"
            if isinstance(p95, (int, float)):
                p95 = f"{p95:.3f}"
            if isinstance(p99, (int, float)):
                p99 = f"{p99:.3f}"

            report += f"| {benchmark_type} | {test_case} | {p90} | {p95} | {p99} | {iterations} |\n"

        report += "\n\n"

    report += "## Performance Hotspots\n\n"
    
    # Group by benchmark type
    benchmark_groups = defaultdict(list)
    for item in data:
        # Extract benchmark type from filename (e.g., "db_operations_", "query_", etc.)
        benchmark_type = None
        for prefix in ["db_operations_", "query_benchmark_", "large_doc_", "serialization_"]:
            if prefix in item["file"]:
                benchmark_type = prefix.strip("_")
                break
        if not benchmark_type:
            benchmark_type = "unknown"
            
        benchmark_groups[benchmark_type].append(item)
    
    # Generate report sections for each benchmark type
    for benchmark_type, items in benchmark_groups.items():
        report += f"### {benchmark_type.title().replace('_', ' ')} Operations\n\n"
        
        for item in items:
            report += f"#### {item['title']}\n\n"
            
            if item['functions']:
                report += "Top functions by CPU usage:\n\n"
                report += "| Function | CPU % |\n"
                report += "|----------|-------|\n"
                
                for func, percentage in list(item['functions'].items())[:10]:  # Top 10
                    report += f"| `{func}` | {percentage:.2f}% |\n"
                    
                report += "\n"
            else:
                report += "No function data available.\n\n"
    
    # Add summary and recommendations
    report += """
## Overall Analysis

Based on the profiling results, here are the key performance bottlenecks:

1. **Serialization/Deserialization** - Look for high CPU usage in rkyv_adapter functions
2. **Index Operations** - Check for inefficient indexing algorithms
3. **Query Processing** - Identify slow query operations
4. **Disk I/O** - Watch for sled database operations taking significant time

## Recommendations

Potential optimizations based on the profile:

1. Optimize serialization for frequently accessed data structures
2. Consider batch processing for indexing operations
3. Implement caching for common query patterns
4. Review locking strategies if contention is observed
5. Profile memory usage separately to identify potential memory optimizations
"""
    
    return report

def main():
    print(f"Analyzing profile data from {FLAMEGRAPH_DIR}...")
    svg_files = find_svg_files()
    
    if not svg_files:
        print(f"No SVG files found in {FLAMEGRAPH_DIR}. Make sure to run the profiling first.")
        return
    
    print(f"Found {len(svg_files)} SVG files to analyze.")
    
    data = []
    for svg_file in svg_files:
        print(f"Parsing {os.path.basename(svg_file)}...")
        parsed_data = parse_svg_data(svg_file)
        data.append(parsed_data)
    
    report = generate_report(data)
    
    with open(OUTPUT_FILE, 'w') as f:
        f.write(report)
    
    print(f"Analysis complete! Report written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()