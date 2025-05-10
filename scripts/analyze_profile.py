#!/usr/bin/env python3
"""
Profile analysis script for Fugu performance data.
Used to extract and summarize data from profiling results.
"""

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze profiling data for Fugu")
    parser.add_argument(
        "--results-dir", 
        type=str, 
        default="./profiling_results",
        help="Directory containing profiling results"
    )
    parser.add_argument(
        "--benchmark", 
        type=str, 
        choices=["db_operations", "serialization", "query", "all"],
        default="all",
        help="Which benchmark results to analyze"
    )
    parser.add_argument(
        "--output", 
        type=str, 
        default="./profiling_analysis",
        help="Directory to store analysis results"
    )
    parser.add_argument(
        "--compare", 
        type=str,
        help="Directory with previous results to compare against"
    )
    
    return parser.parse_args()


def extract_criterion_data(results_dir, benchmark):
    """Extract performance data from Criterion results"""
    criterion_dir = Path("target/criterion")
    
    if not criterion_dir.exists():
        print(f"Error: Criterion results not found in {criterion_dir}")
        return None
    
    benchmark_dirs = []
    if benchmark == "all":
        # Find all benchmark directories
        for path in criterion_dir.iterdir():
            if path.is_dir() and not path.name.startswith("."):
                benchmark_dirs.append(path)
    else:
        # Find specific benchmark directories
        for path in criterion_dir.iterdir():
            if path.is_dir() and benchmark in path.name:
                benchmark_dirs.append(path)
    
    benchmark_data = {}
    
    for bench_dir in benchmark_dirs:
        bench_name = bench_dir.name
        benchmark_data[bench_name] = {}
        
        # Process each test case in the benchmark
        for test_dir in bench_dir.iterdir():
            if test_dir.is_dir() and not test_dir.name.startswith("."):
                try:
                    # Read the estimates.json file
                    estimates_file = test_dir / "estimates.json"
                    if estimates_file.exists():
                        with open(estimates_file, 'r') as f:
                            estimates = json.load(f)
                            benchmark_data[bench_name][test_dir.name] = estimates
                except Exception as e:
                    print(f"Error reading {estimates_file}: {e}")
    
    return benchmark_data


def generate_analysis_report(benchmark_data, output_dir):
    """Generate performance analysis report from benchmark data"""
    if not benchmark_data:
        print("No benchmark data to analyze")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "performance_analysis.md")
    
    with open(report_path, 'w') as f:
        f.write("# Performance Analysis Report\n\n")
        
        for benchmark, test_cases in benchmark_data.items():
            f.write(f"## {benchmark}\n\n")
            
            # Create a table of results
            f.write("| Test Case | Mean Time (ns) | Throughput | Min Time | Max Time |\n")
            f.write("|-----------|---------------|------------|----------|----------|\n")
            
            for test_name, estimates in test_cases.items():
                mean_time = estimates.get("mean", {}).get("point_estimate", 0)
                min_time = estimates.get("mean", {}).get("lower_bound", 0)
                max_time = estimates.get("mean", {}).get("upper_bound", 0)
                
                # Calculate throughput (ops/sec)
                throughput = 1_000_000_000 / mean_time if mean_time > 0 else 0
                
                f.write(f"| {test_name} | {mean_time:.2f} | {throughput:.2f} ops/sec | {min_time:.2f} | {max_time:.2f} |\n")
            
            f.write("\n")
    
    print(f"Analysis report generated at {report_path}")
    return report_path


def create_visualizations(benchmark_data, output_dir):
    """Create visualization charts for benchmark data"""
    if not benchmark_data:
        return
    
    charts_dir = os.path.join(output_dir, "charts")
    os.makedirs(charts_dir, exist_ok=True)
    
    for benchmark, test_cases in benchmark_data.items():
        # Create performance bar chart
        test_names = []
        mean_times = []
        
        for test_name, estimates in test_cases.items():
            test_names.append(test_name)
            mean_time = estimates.get("mean", {}).get("point_estimate", 0) / 1_000_000  # Convert to ms
            mean_times.append(mean_time)
        
        if not test_names:
            continue
            
        plt.figure(figsize=(12, 6))
        bars = plt.bar(test_names, mean_times)
        plt.title(f"{benchmark} Performance")
        plt.xlabel("Test Case")
        plt.ylabel("Mean Time (ms)")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Add values on top of bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f"{height:.2f}ms",
                    ha='center', va='bottom', rotation=0)
        
        chart_path = os.path.join(charts_dir, f"{benchmark}_performance.png")
        plt.savefig(chart_path)
        plt.close()
        
        print(f"Chart generated: {chart_path}")


def compare_with_previous(current_data, previous_dir, output_dir):
    """Compare current benchmark results with previous results"""
    if not os.path.exists(previous_dir):
        print(f"Previous results directory not found: {previous_dir}")
        return
    
    try:
        # Extract previous benchmark data
        previous_data = extract_criterion_data(previous_dir, "all")
        
        if not previous_data:
            print("No previous benchmark data found for comparison")
            return
        
        # Create comparison report
        comparison_dir = os.path.join(output_dir, "comparison")
        os.makedirs(comparison_dir, exist_ok=True)
        report_path = os.path.join(comparison_dir, "comparison_report.md")
        
        with open(report_path, 'w') as f:
            f.write("# Performance Comparison Report\n\n")
            
            for benchmark, current_tests in current_data.items():
                if benchmark not in previous_data:
                    continue
                    
                f.write(f"## {benchmark}\n\n")
                
                # Create a comparison table
                f.write("| Test Case | Current (ms) | Previous (ms) | Change | Change % |\n")
                f.write("|-----------|-------------|--------------|--------|----------|\n")
                
                previous_tests = previous_data[benchmark]
                
                for test_name, current_estimates in current_tests.items():
                    if test_name not in previous_tests:
                        continue
                        
                    current_mean = current_estimates.get("mean", {}).get("point_estimate", 0) / 1_000_000  # Convert to ms
                    previous_mean = previous_tests[test_name].get("mean", {}).get("point_estimate", 0) / 1_000_000
                    
                    change = current_mean - previous_mean
                    change_pct = (change / previous_mean) * 100 if previous_mean > 0 else 0
                    
                    # Format the change percentage with a sign and color indicator
                    if change_pct < 0:
                        change_pct_str = f"**-{abs(change_pct):.2f}%** 🟢"  # Green for improvement
                    elif change_pct > 0:
                        change_pct_str = f"**+{change_pct:.2f}%** 🔴"  # Red for regression
                    else:
                        change_pct_str = f"0.00% ⚪"
                    
                    f.write(f"| {test_name} | {current_mean:.2f} | {previous_mean:.2f} | {change:.2f} | {change_pct_str} |\n")
                
                f.write("\n")
        
        print(f"Comparison report generated at {report_path}")
        return report_path
    
    except Exception as e:
        print(f"Error comparing benchmark results: {e}")
        return None


def main():
    args = parse_args()
    
    # Extract data from Criterion results
    benchmark_data = extract_criterion_data(args.results_dir, args.benchmark)
    
    if not benchmark_data:
        print("No benchmark data found to analyze")
        return 1
    
    # Generate analysis report
    report_path = generate_analysis_report(benchmark_data, args.output)
    
    # Create visualizations
    create_visualizations(benchmark_data, args.output)
    
    # Compare with previous results if requested
    if args.compare:
        compare_with_previous(benchmark_data, args.compare, args.output)
    
    print(f"Analysis complete. Results available in {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())