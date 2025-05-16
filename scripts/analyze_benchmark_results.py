#!/usr/bin/env python3
"""
Script to analyze and visualize benchmark results from Criterion output
"""

import os
import sys
import json
import glob
import argparse
from pathlib import Path
from datetime import datetime


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze benchmark results from Criterion")
    parser.add_argument(
        "--results-dir",
        type=str,
        default="./benchmark_results",
        help="Directory containing benchmark results"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./benchmark_analysis",
        help="Directory to store analysis results"
    )

    return parser.parse_args()


def load_benchmark_data(results_dir):
    """Load benchmark data from output directories"""
    print(f"Loading benchmark data from {results_dir}...")

    # Check for sled and fjall results
    results_path = os.path.join(results_dir, "results.json")

    # Initialize result data
    results = {}

    # Try to load sled results
    if os.path.exists(results_path):
        try:
            with open(results_path, 'r') as f:
                results["fjall"] = f.read()
                print("Loaded fjall benchmark results")
        except Exception as e:
            print(f"Error loading {results_path}: {e}")
    else:
        print(f"Warning: No fjall results found at {results_path}")

    return results


def generate_summary_report(benchmark_data, output_dir):
    """Generate a simple summary report comparing sled and fjall"""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create a simple report file
    report_path = os.path.join(output_dir, "summary.md")

    with open(report_path, 'w') as f:
        f.write("# Sled vs Fjall Benchmark Results\n\n")
        f.write(f"Generated: {datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S')}\n\n")

        # Add information about available results
        f.write("## Available Benchmark Data\n\n")
        for backend, data in benchmark_data.items():
            f.write(f"### {backend.capitalize()}\n\n")
            f.write(f"- Raw results available in `{backend}/results.json`\n")
            if data:
                f.write(f"- Data size: {len(data)} bytes\n")
            else:
                f.write("- No data available\n")
            f.write("\n")

        # Add instructions for manual analysis
        f.write("## Manual Analysis Instructions\n\n")
        f.write("To perform a detailed analysis of these benchmark results:\n\n")
        f.write("1. Install Criterion's analysis tools (`cargo install critcmp`)\n")
        f.write("2. Compare the results manually using:\n")
        f.write("   ```\n")
        f.write("   critcmp sled fjall --baseline sled\n")
        f.write("   ```\n\n")

    print(f"Summary report generated at {report_path}")


def main():
    args = parse_args()

    # Load benchmark data
    data = load_benchmark_data(args.results_dir)

    if not data:
        print("No benchmark data found!")
        return 1

    # Generate summary report
    generate_summary_report(data, args.output)

    print(f"Analysis complete! Results available in {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
