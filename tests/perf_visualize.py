#!/usr/bin/env python3
import subprocess
import re
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
import json
import argparse
import glob

# Define the pattern to match performance output lines
PATTERN = r"(\w+) performance \(μs\): p10=(\d+), p50=(\d+), p90=(\d+), p99=(\d+)"

def run_performance_tests():
    """Run the performance tests and return the output."""
    print("Running performance tests...")
    
    # First check if we're in the project root directory
    if os.path.exists("tests/run_tests.sh"):
        script_path = "tests/run_tests.sh"
    elif os.path.exists("run_tests.sh"):
        script_path = "run_tests.sh"
    else:
        print("Error: run_tests.sh script not found. Make sure you run this script from the project root.")
        print("Attempting to run cargo test directly...")
        cmd = ["cargo", "test", "--features", "performance-tests", "--lib", "--", 
               "test_insert_performance", "test_search_performance", 
               "test_text_search_performance", "test_delete_performance", 
               "test_flush_performance"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout
    
    cmd = ["bash", script_path, "--perf"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout
    except Exception as e:
        print(f"Error running performance tests: {e}")
        return ""

def parse_performance_data(output):
    """Parse the performance test output and extract test data."""
    tests = []
    matches = re.finditer(PATTERN, output)
    
    for match in matches:
        test_name = match.group(1)
        p10 = int(match.group(2))
        p50 = int(match.group(3))
        p90 = int(match.group(4))
        p99 = int(match.group(5))
        
        tests.append({
            "name": test_name,
            "p10": p10, 
            "p50": p50,
            "p90": p90,
            "p99": p99
        })
    
    return tests

def save_performance_data(tests, history_file="tests/perf_history.json"):
    """Save the performance data to a JSON file with timestamp."""
    timestamp = datetime.now().isoformat()
    
    # Create entry with timestamp
    entry = {
        "timestamp": timestamp,
        "tests": tests
    }
    
    # Load existing history if available
    history = []
    if os.path.exists(history_file):
        with open(history_file, "r") as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = []
    
    # Add new entry and save
    history.append(entry)
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(history_file), exist_ok=True)
    
    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)
    
    return history

def load_csv_data(data_dir="tests/data"):
    """Load CSV performance data files."""
    csv_results = {}
    csv_files = glob.glob(f"{data_dir}/*.csv")
    
    for csv_file in csv_files:
        test_name = os.path.basename(csv_file).replace(".csv", "")
        try:
            # Load CSV into pandas DataFrame
            df = pd.read_csv(csv_file)
            # Calculate percentiles
            if not df.empty:
                duration_micros = df['duration_us']
                p10 = np.percentile(duration_micros, 10)
                p50 = np.percentile(duration_micros, 50)
                p90 = np.percentile(duration_micros, 90)
                p99 = np.percentile(duration_micros, 99)
                
                # Store the results
                csv_results[test_name] = {
                    "name": test_name,
                    "p10": int(p10),
                    "p50": int(p50),
                    "p90": int(p90),
                    "p99": int(p99),
                    "raw_data": df
                }
        except Exception as e:
            print(f"Error loading CSV file {csv_file}: {e}")
    
    return csv_results

def create_bar_chart(tests, output_dir="tests/perf_results"):
    """Create a bar chart visualizing the current performance test results."""
    test_names = [test["name"] for test in tests]
    p10_values = [test["p10"] for test in tests]
    p50_values = [test["p50"] for test in tests]
    p90_values = [test["p90"] for test in tests]
    p99_values = [test["p99"] for test in tests]
    
    x = np.arange(len(test_names))
    width = 0.2
    
    fig = plt.figure(figsize=(12, 7))
    ax = plt.subplot(111)
    
    ax.bar(x - width*1.5, p10_values, width, label='p10', color='green')
    ax.bar(x - width/2, p50_values, width, label='p50', color='blue')
    ax.bar(x + width/2, p90_values, width, label='p90', color='orange')
    ax.bar(x + width*1.5, p99_values, width, label='p99', color='red')
    
    ax.set_title('Performance Test Results')
    ax.set_ylabel('Microseconds (μs) - lower is better')
    ax.set_xticks(x)
    ax.set_xticklabels(test_names)
    
    # Add values on top of bars
    for i, test in enumerate(tests):
        ax.text(i - width*1.5, test["p10"], str(test["p10"]), ha='center', va='bottom', fontsize=9, rotation=90)
        ax.text(i - width/2, test["p50"], str(test["p50"]), ha='center', va='bottom', fontsize=9, rotation=90)
        ax.text(i + width/2, test["p90"], str(test["p90"]), ha='center', va='bottom', fontsize=9, rotation=90)
        ax.text(i + width*1.5, test["p99"], str(test["p99"]), ha='center', va='bottom', fontsize=9, rotation=90)
    
    ax.legend()
    plt.tight_layout()
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save the figure
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    plt.savefig(f"{output_dir}/perf_results_{timestamp}.png", dpi=300)
    plt.savefig(f"{output_dir}/perf_results_latest.png", dpi=300)
    
    plt.close()

def create_trend_charts(history, output_dir="tests/perf_results"):
    """Create trend charts showing how performance has changed over time."""
    if not history or len(history) < 2:
        print("Not enough historical data to create trend charts")
        return
    
    # Get all timestamps and order them
    timestamps = [entry["timestamp"] for entry in history]
    formatted_timestamps = [datetime.fromisoformat(ts).strftime("%m-%d %H:%M") for ts in timestamps]
    
    # Get all test names
    all_tests = set()
    for entry in history:
        for test in entry["tests"]:
            all_tests.add(test["name"])
    
    # Create a trend chart for each test
    for test_name in all_tests:
        plt.figure(figsize=(14, 7))
        
        # Extract data for this test across all timestamps
        p10_values = []
        p50_values = []
        p90_values = []
        p99_values = []
        valid_timestamps = []
        
        for i, entry in enumerate(history):
            for test in entry["tests"]:
                if test["name"] == test_name:
                    p10_values.append(test["p10"])
                    p50_values.append(test["p50"])
                    p90_values.append(test["p90"])
                    p99_values.append(test["p99"])
                    valid_timestamps.append(formatted_timestamps[i])
        
        # Create the plot if we have data
        if valid_timestamps:
            plt.plot(valid_timestamps, p10_values, 'g-', marker='o', label='p10')
            plt.plot(valid_timestamps, p50_values, 'b-', marker='o', label='p50')
            plt.plot(valid_timestamps, p90_values, 'orange', marker='o', label='p90')
            plt.plot(valid_timestamps, p99_values, 'r-', marker='o', label='p99')
            
            plt.title(f'{test_name} Performance Trend')
            plt.ylabel('Microseconds (μs) - lower is better')
            plt.xlabel('Test Run')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.legend()
            
            # Save the figure
            os.makedirs(output_dir, exist_ok=True)
            plt.savefig(f"{output_dir}/{test_name}_trend.png", dpi=300)
            plt.close()

def create_distribution_plot(csv_results, output_dir="tests/perf_results"):
    """Create distribution plots from raw CSV data."""
    if not csv_results:
        print("No CSV data found to create distribution plots")
        return
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a distribution plot for each test
    for test_name, data in csv_results.items():
        if 'raw_data' in data:
            plt.figure(figsize=(10, 6))
            
            # Create histogram
            plt.hist(data['raw_data']['duration_us'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
            
            # Add vertical lines for percentiles
            plt.axvline(data['p10'], color='green', linestyle='dashed', linewidth=1, label=f'p10: {int(data["p10"])}μs')
            plt.axvline(data['p50'], color='blue', linestyle='dashed', linewidth=1, label=f'p50: {int(data["p50"])}μs')
            plt.axvline(data['p90'], color='orange', linestyle='dashed', linewidth=1, label=f'p90: {int(data["p90"])}μs')
            plt.axvline(data['p99'], color='red', linestyle='dashed', linewidth=1, label=f'p99: {int(data["p99"])}μs')
            
            plt.title(f'{test_name} Performance Distribution')
            plt.xlabel('Duration (μs)')
            plt.ylabel('Frequency')
            plt.legend()
            plt.tight_layout()
            
            # Save the figure
            plt.savefig(f"{output_dir}/{test_name}_distribution.png", dpi=300)
            plt.close()

def compare_operations(csv_results, output_dir="tests/perf_results"):
    """Create comparison plots between unit tests and integration tests."""
    if not csv_results:
        print("No CSV data found to create comparison plots")
        return
    
    # Group the tests by operation type (insert, search, delete)
    operation_groups = {
        "insert": ["insert_performance", "integration_index"],
        "search": ["search_performance", "text_search_performance", "integration_search"],
        "delete": ["delete_performance", "integration_delete"],
    }
    
    # Create a comparison chart for each operation type
    for operation, test_group in operation_groups.items():
        # Collect tests that exist in our data
        available_tests = [test for test in test_group if test in csv_results]
        
        if len(available_tests) > 0:
            # Prepare the data
            test_names = []
            p10_values = []
            p50_values = []
            p90_values = []
            p99_values = []
            
            for test in available_tests:
                test_names.append(test)
                p10_values.append(csv_results[test]["p10"])
                p50_values.append(csv_results[test]["p50"])
                p90_values.append(csv_results[test]["p90"])
                p99_values.append(csv_results[test]["p99"])
            
            # Create the chart
            x = np.arange(len(test_names))
            width = 0.2
            
            fig, ax = plt.subplots(figsize=(10, 6))
            
            ax.bar(x - width*1.5, p10_values, width, label='p10', color='green')
            ax.bar(x - width/2, p50_values, width, label='p50', color='blue')
            ax.bar(x + width/2, p90_values, width, label='p90', color='orange')
            ax.bar(x + width*1.5, p99_values, width, label='p99', color='red')
            
            ax.set_title(f'{operation.capitalize()} Performance Comparison')
            ax.set_ylabel('Microseconds (μs) - lower is better')
            ax.set_xticks(x)
            ax.set_xticklabels(test_names)
            
            # Add values on top of bars
            for i in range(len(test_names)):
                ax.text(i - width*1.5, p10_values[i], str(p10_values[i]), ha='center', va='bottom', fontsize=9)
                ax.text(i - width/2, p50_values[i], str(p50_values[i]), ha='center', va='bottom', fontsize=9)
                ax.text(i + width/2, p90_values[i], str(p90_values[i]), ha='center', va='bottom', fontsize=9)
                ax.text(i + width*1.5, p99_values[i], str(p99_values[i]), ha='center', va='bottom', fontsize=9)
            
            ax.legend()
            plt.tight_layout()
            
            # Save the figure
            os.makedirs(output_dir, exist_ok=True)
            plt.savefig(f"{output_dir}/{operation}_comparison.png", dpi=300)
            plt.close()

def check_dependencies():
    """Check if required dependencies are installed."""
    try:
        import matplotlib
        import numpy
        import pandas
        return True
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Please install required dependencies with:")
        print("pip3 install -r tests/requirements.txt")
        return False

def main():
    # First check dependencies
    if not check_dependencies():
        return

    parser = argparse.ArgumentParser(description='Run and visualize performance tests')
    parser.add_argument('--no-run', action='store_true', help='Skip running tests and just visualize existing data')
    parser.add_argument('--history-file', default='tests/perf_history.json', help='Path to the performance history file')
    parser.add_argument('--output-dir', default='tests/perf_results', help='Directory to save visualizations')
    parser.add_argument('--data-dir', default='tests/data', help='Directory with CSV data files')
    
    args = parser.parse_args()
    
    tests = None
    history = []
    
    if not args.no_run:
        # Run the tests and parse the output
        output = run_performance_tests()
        tests = parse_performance_data(output)
        
        if not tests:
            print("No performance test results found in logs. Using CSV data for visualization.")
    
    # Load and process CSV data
    csv_results = load_csv_data(args.data_dir)
    
    if csv_results:
        print(f"Loaded performance data for {len(csv_results)} tests from CSV files")
        
        # If we didn't get tests from the output logs, create them from CSV data
        if not tests:
            tests = [data for name, data in csv_results.items() if "raw_data" not in data]
            
        # Save the performance data
        if tests:
            history = save_performance_data(tests, args.history_file)
    else:
        print("No CSV performance data files found")
        if not tests:
            print("No performance data available from either logs or CSV files")
            
            # Load existing history as a fallback
            if os.path.exists(args.history_file):
                with open(args.history_file, "r") as f:
                    try:
                        history = json.load(f)
                        if history and len(history) > 0:
                            tests = history[-1]["tests"]  # Get the most recent test results
                        else:
                            print("No performance data found in history file")
                            return
                    except json.JSONDecodeError:
                        print("Error decoding history file")
                        return
            else:
                print(f"History file {args.history_file} not found")
                print("Run performance tests first with:")
                print("./tests/run_tests.sh --perf")
                return
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Create visualizations
    if tests:
        create_bar_chart(tests, args.output_dir)
        print(f"Bar chart created in {args.output_dir}")
    
    # Create trend charts if we have enough data
    if len(history) >= 2:
        create_trend_charts(history, args.output_dir)
        print(f"Trend charts created in {args.output_dir}")
    elif len(history) == 1:
        print("Only one data point available. Run tests again to generate trend charts.")
    
    # Create distribution plots from CSV data
    if csv_results:
        create_distribution_plot(csv_results, args.output_dir)
        print(f"Distribution plots created in {args.output_dir}")
        
        # Create comparison plots
        compare_operations(csv_results, args.output_dir)
        print(f"Operation comparison plots created in {args.output_dir}")
    
    print(f"Visualizations saved to: {args.output_dir}")

if __name__ == "__main__":
    main()