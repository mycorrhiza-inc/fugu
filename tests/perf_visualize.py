#!/usr/bin/env python3
import subprocess
import re
import os
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import json
import argparse

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

def create_bar_chart(tests, output_dir="tests/perf_results"):
    """Create a bar chart visualizing the current performance test results."""
    test_names = [test["name"] for test in tests]
    p10_values = [test["p10"] for test in tests]
    p50_values = [test["p50"] for test in tests]
    p90_values = [test["p90"] for test in tests]
    p99_values = [test["p99"] for test in tests]
    
    x = np.arange(len(test_names))
    width = 0.2
    
    fig, ax = plt.figure(figsize=(12, 7)), plt.subplot(111)
    
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

def check_dependencies():
    """Check if required dependencies are installed."""
    try:
        import matplotlib
        import numpy
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
    
    args = parser.parse_args()
    
    tests = None
    history = []
    
    if not args.no_run:
        # Run the tests and parse the output
        output = run_performance_tests()
        tests = parse_performance_data(output)
        
        if not tests:
            print("No performance test results found. Make sure tests are running correctly.")
            return
        
        # Save the performance data
        history = save_performance_data(tests, args.history_file)
    else:
        # Load existing history
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
    
    print(f"Visualizations saved to: {args.output_dir}")

if __name__ == "__main__":
    main()