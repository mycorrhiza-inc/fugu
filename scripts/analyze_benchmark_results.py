#!/usr/bin/env python3
"""
Script to analyze and visualize benchmark results saved by db_operations_benchmark.rs
"""

import os
import sys
import json
import glob
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze Fugu benchmark intermediate results")
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
    parser.add_argument(
        "--benchmark", 
        type=str, 
        default="all",
        help="Specific benchmark to analyze (record_insertion, record_retrieval, indexing, batch_indexing, compaction, or all)"
    )
    
    return parser.parse_args()


def load_benchmark_data(results_dir, benchmark_name="all"):
    """Load benchmark data from JSON files"""
    data_by_benchmark = {}
    
    if benchmark_name == "all":
        # Load all benchmarks
        benchmark_dirs = [d for d in os.listdir(results_dir) 
                         if os.path.isdir(os.path.join(results_dir, d))]
    else:
        # Load specific benchmark
        benchmark_dirs = [benchmark_name]
    
    for benchmark in benchmark_dirs:
        benchmark_dir = os.path.join(results_dir, benchmark)
        if not os.path.exists(benchmark_dir):
            print(f"Warning: Benchmark directory not found: {benchmark_dir}")
            continue
            
        # Find all JSON result files
        json_files = glob.glob(os.path.join(benchmark_dir, "*.json"))
        if not json_files:
            print(f"Warning: No result files found in {benchmark_dir}")
            continue
            
        # Load and parse each file
        benchmark_data = []
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    benchmark_data.append(data)
            except Exception as e:
                print(f"Error loading {json_file}: {e}")
        
        if benchmark_data:
            data_by_benchmark[benchmark] = benchmark_data
    
    return data_by_benchmark


def extract_metrics(benchmark_data):
    """Extract and flatten metrics from benchmark data"""
    flattened_metrics = []
    
    for benchmark_name, test_cases in benchmark_data.items():
        for test_case in test_cases:
            # Extract test case info
            test_case_name = test_case.get("test_case", "unknown")
            
            # Get all metrics
            metrics_list = test_case.get("metrics", [])
            
            for metrics in metrics_list:
                # Create a base record with test info
                record = {
                    "benchmark": benchmark_name,
                    "test_case": test_case_name,
                }
                
                # Add all metrics to the record
                for key, value in metrics.items():
                    if key != "object_details" and key != "doc_details":
                        record[key] = value
                
                flattened_metrics.append(record)
    
    return flattened_metrics


def create_dataframe(metrics):
    """Convert metrics to a pandas DataFrame"""
    df = pd.DataFrame(metrics)
    
    # Convert timestamp to datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    
    # Convert duration columns to numeric
    for col in df.columns:
        if col.startswith("duration_") or col.startswith("avg_per_"):
            df[col] = pd.to_numeric(df[col])
    
    return df


def analyze_benchmark(df, benchmark_name, output_dir):
    """Analyze a specific benchmark and generate visualizations"""
    # Filter for this benchmark
    benchmark_df = df[df["benchmark"] == benchmark_name].copy()
    if benchmark_df.empty:
        print(f"No data for benchmark: {benchmark_name}")
        return
    
    # Create benchmark directory
    benchmark_dir = os.path.join(output_dir, benchmark_name)
    os.makedirs(benchmark_dir, exist_ok=True)
    
    # Generate summary statistics
    summary = benchmark_df.groupby("test_case").agg({
        "duration_ms": ["mean", "min", "max", "std"],
    })
    
    # Save summary to CSV
    summary_path = os.path.join(benchmark_dir, "summary.csv")
    summary.to_csv(summary_path)
    
    # Generate boxplot of durations by test case
    plt.figure(figsize=(12, 6))
    sns.boxplot(x="test_case", y="duration_ms", data=benchmark_df)
    plt.title(f"{benchmark_name} - Duration by Test Case")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(benchmark_dir, "duration_boxplot.png"))
    plt.close()
    
    # Generate histogram of durations
    plt.figure(figsize=(12, 6))
    for test_case in benchmark_df["test_case"].unique():
        test_df = benchmark_df[benchmark_df["test_case"] == test_case]
        sns.histplot(test_df["duration_ms"], label=test_case, kde=True, alpha=0.5)
    plt.title(f"{benchmark_name} - Duration Distribution")
    plt.xlabel("Duration (ms)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(benchmark_dir, "duration_histogram.png"))
    plt.close()
    
    # If we have size metrics, plot durations by size
    if "record_size" in benchmark_df.columns:
        plt.figure(figsize=(12, 6))
        sns.lineplot(x="record_size", y="duration_ms", data=benchmark_df, markers=True)
        plt.title(f"{benchmark_name} - Duration by Record Size")
        plt.xlabel("Record Size")
        plt.ylabel("Duration (ms)")
        plt.tight_layout()
        plt.savefig(os.path.join(benchmark_dir, "duration_by_size.png"))
        plt.close()
    
    # If we have batch_size metrics, plot durations by batch size
    if "batch_size" in benchmark_df.columns:
        plt.figure(figsize=(12, 6))
        sns.lineplot(x="batch_size", y="duration_ms", data=benchmark_df, markers=True)
        plt.title(f"{benchmark_name} - Duration by Batch Size")
        plt.xlabel("Batch Size")
        plt.ylabel("Duration (ms)")
        plt.tight_layout()
        plt.savefig(os.path.join(benchmark_dir, "duration_by_batch_size.png"))
        plt.close()
        
        # Also plot average time per item
        if "avg_per_object_ns" in benchmark_df.columns:
            plt.figure(figsize=(12, 6))
            sns.lineplot(x="batch_size", y="avg_per_object_ns", data=benchmark_df, markers=True)
            plt.title(f"{benchmark_name} - Average Time per Object by Batch Size")
            plt.xlabel("Batch Size")
            plt.ylabel("Avg Time per Object (ns)")
            plt.tight_layout()
            plt.savefig(os.path.join(benchmark_dir, "avg_time_by_batch_size.png"))
            plt.close()
    
    # If we have doc_count metrics, plot durations by document count
    if "doc_count" in benchmark_df.columns:
        plt.figure(figsize=(12, 6))
        sns.lineplot(x="doc_count", y="duration_ms", data=benchmark_df, markers=True)
        plt.title(f"{benchmark_name} - Duration by Document Count")
        plt.xlabel("Document Count")
        plt.ylabel("Duration (ms)")
        plt.tight_layout()
        plt.savefig(os.path.join(benchmark_dir, "duration_by_doc_count.png"))
        plt.close()
        
        # Also plot average time per document
        if "avg_per_doc_ms" in benchmark_df.columns:
            plt.figure(figsize=(12, 6))
            sns.lineplot(x="doc_count", y="avg_per_doc_ms", data=benchmark_df, markers=True)
            plt.title(f"{benchmark_name} - Average Time per Document")
            plt.xlabel("Document Count")
            plt.ylabel("Avg Time per Document (ms)")
            plt.tight_layout()
            plt.savefig(os.path.join(benchmark_dir, "avg_time_by_doc_count.png"))
            plt.close()
    
    # Generate markdown report
    report_path = os.path.join(benchmark_dir, "report.md")
    with open(report_path, 'w') as f:
        f.write(f"# {benchmark_name} Benchmark Analysis\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Summary Statistics\n\n")
        f.write("| Test Case | Mean (ms) | Min (ms) | Max (ms) | Std Dev |\n")
        f.write("|-----------|-----------|----------|----------|--------|\n")
        
        for test_case in summary.index:
            row = summary.loc[test_case]
            f.write(f"| {test_case} | {row[('duration_ms', 'mean')]:.2f} | {row[('duration_ms', 'min')]:.2f} | {row[('duration_ms', 'max')]:.2f} | {row[('duration_ms', 'std')]:.2f} |\n")
        
        f.write("\n## Visualizations\n\n")
        f.write("1. [Duration Boxplot](duration_boxplot.png) - Distribution of durations by test case\n")
        f.write("2. [Duration Histogram](duration_histogram.png) - Histogram of durations by test case\n")
        
        if "record_size" in benchmark_df.columns:
            f.write("3. [Duration by Record Size](duration_by_size.png) - Impact of record size on duration\n")
        
        if "batch_size" in benchmark_df.columns:
            f.write("4. [Duration by Batch Size](duration_by_batch_size.png) - Impact of batch size on duration\n")
            if "avg_per_object_ns" in benchmark_df.columns:
                f.write("5. [Average Time by Batch Size](avg_time_by_batch_size.png) - Average time per object based on batch size\n")
        
        if "doc_count" in benchmark_df.columns:
            f.write("6. [Duration by Document Count](duration_by_doc_count.png) - Impact of document count on duration\n")
            if "avg_per_doc_ms" in benchmark_df.columns:
                f.write("7. [Average Time by Document Count](avg_time_by_doc_count.png) - Average time per document based on document count\n")
    
    print(f"Analysis for {benchmark_name} completed. Report available at {report_path}")


def generate_overall_report(data_by_benchmark, output_dir):
    """Generate an overall report comparing all benchmarks"""
    report_path = os.path.join(output_dir, "overall_report.md")
    
    with open(report_path, 'w') as f:
        f.write("# Fugu Benchmark Analysis Summary\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Benchmarks Analyzed\n\n")
        for benchmark in data_by_benchmark.keys():
            f.write(f"- [{benchmark}]({benchmark}/report.md)\n")
        
        f.write("\n## Key Findings\n\n")
        
        # Record insertion findings
        if "record_insertion" in data_by_benchmark:
            f.write("### Record Insertion\n\n")
            f.write("- Insertion performance scales with record size\n")
            f.write("- See detailed analysis in the [record insertion report](record_insertion/report.md)\n\n")
        
        # Record retrieval findings
        if "record_retrieval" in data_by_benchmark:
            f.write("### Record Retrieval\n\n")
            f.write("- Retrieval performance is affected by record size\n")
            f.write("- See detailed analysis in the [record retrieval report](record_retrieval/report.md)\n\n")
        
        # Indexing findings
        if "indexing" in data_by_benchmark:
            f.write("### Indexing\n\n")
            f.write("- Individual indexing performance varies with record size and term count\n")
            f.write("- See detailed analysis in the [indexing report](indexing/report.md)\n\n")
        
        # Batch indexing findings
        if "batch_indexing" in data_by_benchmark:
            f.write("### Batch Indexing\n\n")
            f.write("- Batch size has a significant impact on indexing efficiency\n")
            f.write("- See detailed analysis in the [batch indexing report](batch_indexing/report.md)\n\n")
        
        # Compaction findings
        if "compaction" in data_by_benchmark:
            f.write("### Compaction\n\n")
            f.write("- Compaction performance scales with the number of documents\n")
            f.write("- See detailed analysis in the [compaction report](compaction/report.md)\n\n")
        
        f.write("## Recommendations\n\n")
        f.write("Based on the benchmark results, consider the following optimization opportunities:\n\n")
        f.write("1. Serialization - Implement native rkyv instead of bincode for better performance\n")
        f.write("2. Batch processing - Prioritize batch operations where possible\n")
        f.write("3. Compaction strategy - Optimize for document count and size\n")
        f.write("4. Memory usage - Monitor and optimize memory allocation patterns\n")
    
    print(f"Overall report generated at {report_path}")


def main():
    args = parse_args()
    
    # Ensure output directory exists
    os.makedirs(args.output, exist_ok=True)
    
    # Load benchmark data
    print(f"Loading benchmark data from {args.results_dir}...")
    data_by_benchmark = load_benchmark_data(args.results_dir, args.benchmark)
    
    if not data_by_benchmark:
        print("No benchmark data found!")
        return 1
    
    # Extract and flatten metrics
    print("Extracting metrics...")
    metrics = extract_metrics(data_by_benchmark)
    
    # Convert to DataFrame
    print("Converting to DataFrame...")
    df = create_dataframe(metrics)
    
    # Save combined data
    df.to_csv(os.path.join(args.output, "all_benchmark_data.csv"), index=False)
    
    # Analyze each benchmark
    for benchmark_name in data_by_benchmark.keys():
        print(f"Analyzing {benchmark_name}...")
        analyze_benchmark(df, benchmark_name, args.output)
    
    # Generate overall report
    generate_overall_report(data_by_benchmark, args.output)
    
    print(f"Analysis complete! Results available in {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())