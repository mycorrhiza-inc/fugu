#!/usr/bin/env python3
import subprocess
import re
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime
import json
import argparse
import glob

# Define the pattern to match performance output lines
# This pattern matches lines like "insert performance (μs): p10=123, p50=456, p90=789, p99=1234"
PATTERN = r"(\w+) performance \(μs\): p10=(\d+), p50=(\d+), p90=(\d+), p99=(\d+)"

# Alternative patterns to try if the primary pattern doesn't match
ALTERNATIVE_PATTERNS = [
    r"Performance test (\w+): p10=(\d+)µs, p50=(\d+)µs, p90=(\d+)µs, p99=(\d+)µs",  # Using micro symbol
    r"(\w+) performance: p10=(\d+)us, p50=(\d+)us, p90=(\d+)us, p99=(\d+)us",      # Using 'us' notation
    r"Test (\w+): p10: (\d+) µs, p50: (\d+) µs, p90: (\d+) µs, p99: (\d+) µs"       # Different spacing
]


def run_performance_tests():
    """Run the performance tests and return the output."""
    print("Running performance tests...")

    # First check if we're in the project root directory
    if os.path.exists("tests/run_tests.sh"):
        script_path = "tests/run_tests.sh"
        print(f"Found run_tests.sh at: {os.path.abspath('tests/run_tests.sh')}")
    elif os.path.exists("run_tests.sh"):
        script_path = "run_tests.sh"
        print(f"Found run_tests.sh at: {os.path.abspath('run_tests.sh')}")
    else:
        print("Error: run_tests.sh script not found. Make sure you run this script from the project root.")
        print(f"Current working directory: {os.getcwd()}")
        print("Directory contents:")
        for item in os.listdir('.'):
            print(f"  - {item}")
        
        print("Attempting to run cargo test directly...")
        cmd = ["cargo", "test", "--features", "performance-tests", "--lib", "--",
               "test_insert_performance", "test_search_performance",
               "test_text_search_performance", "test_delete_performance"]
        print(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(f"Command exit code: {result.returncode}")
        if result.returncode != 0:
            print("Command failed with error:")
            print(result.stderr)
        return result.stdout

    cmd = ["bash", script_path, "--perf"]
    print(f"Executing command: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        print(f"Command exit code: {result.returncode}")
        if result.returncode != 0:
            print("Command failed with error:")
            print(result.stderr)
        return result.stdout
    except Exception as e:
        print(f"Error running performance tests: {e}")
        print(f"Exception type: {type(e).__name__}")
        return ""


def parse_performance_data(output):
    """Parse the performance test output and extract test data."""
    print("Parsing performance data from test output...")
    
    if not output:
        print("Warning: No output to parse")
        return []
    
    # Function to process matches from any pattern
    def process_matches(pattern, output, pattern_name="primary"):
        matches = re.finditer(pattern, output)
        match_count = 0
        results = []
        
        for match in matches:
            match_count += 1
            test_name = match.group(1)
            p10 = int(match.group(2))
            p50 = int(match.group(3))
            p90 = int(match.group(4))
            p99 = int(match.group(5))

            print(f"Found performance data for test: {test_name} (using {pattern_name} pattern)")
            print(f"  Performance metrics (μs): p10={p10}, p50={p50}, p90={p90}, p99={p99}")
            
            results.append({
                "name": test_name,
                "p10": p10,
                "p50": p50,
                "p90": p90,
                "p99": p99
            })
        
        return results, match_count
    
    # Try the primary pattern first
    tests, match_count = process_matches(PATTERN, output)
    
    # If no matches, try the alternative patterns
    if match_count == 0:
        print("Primary pattern didn't match any results, trying alternative patterns...")
        
        for i, alt_pattern in enumerate(ALTERNATIVE_PATTERNS):
            print(f"Trying alternative pattern {i+1}: {alt_pattern}")
            alt_tests, alt_match_count = process_matches(alt_pattern, output, f"alternative {i+1}")
            
            if alt_match_count > 0:
                print(f"Alternative pattern {i+1} found {alt_match_count} matches")
                tests.extend(alt_tests)
                match_count += alt_match_count
                # Don't break here, try all patterns to get the maximum number of results
    
    # Report results
    if match_count == 0:
        print("\nWARNING: NO PERFORMANCE DATA MATCHES FOUND IN OUTPUT")
        print("Here are the first 500 characters of output for debugging:")
        print(output[:min(500, len(output))] + "..." if len(output) > 500 else output)
        print("\nPatterns being searched for:")
        print(f"Primary:    {PATTERN}")
        for i, pattern in enumerate(ALTERNATIVE_PATTERNS):
            print(f"Alternative {i+1}: {pattern}")
            
        # Look for any numerical data that might help debug the pattern
        print("\nSearching for any lines containing performance metrics...")
        for line in output.split('\n'):
            if any(term in line.lower() for term in ['performance', 'μs', 'us', 'micros', 'p50', 'p90']):
                print(f"Potential performance line: {line}")
    else:
        print(f"\nSuccessfully parsed {match_count} performance test results")
        # Check for duplicates (same test name)
        test_names = [test["name"] for test in tests]
        duplicates = {name for name in test_names if test_names.count(name) > 1}
        if duplicates:
            print(f"Warning: Found duplicate test names: {', '.join(duplicates)}")
            # Keep only the first occurrence of each test
            unique_tests = []
            seen_names = set()
            for test in tests:
                if test["name"] not in seen_names:
                    unique_tests.append(test)
                    seen_names.add(test["name"])
            print(f"Removed {len(tests) - len(unique_tests)} duplicate entries")
            tests = unique_tests
    
    return tests


def save_performance_data(tests, history_file="tests/perf_history.json"):
    """Save the performance data to a JSON file with timestamp."""
    print(f"Saving performance data for {len(tests)} tests to {history_file}")
    
    if not tests:
        print("Warning: No test data to save")
        return []
        
    timestamp = datetime.now().isoformat()
    print(f"Timestamp: {timestamp}")

    # Create entry with timestamp
    entry = {
        "timestamp": timestamp,
        "tests": tests
    }

    # Load existing history if available
    history = []
    if os.path.exists(history_file):
        print(f"Loading existing history file from {history_file}")
        try:
            with open(history_file, "r") as f:
                try:
                    history = json.load(f)
                    print(f"Loaded {len(history)} existing data points")
                except json.JSONDecodeError as e:
                    print(f"Warning: History file is not valid JSON: {e}")
                    print("Creating new history file")
                    history = []
        except Exception as e:
            print(f"Error opening history file: {e}")
            print("Creating new history file")
            history = []
    else:
        print(f"History file does not exist. Creating new file at {history_file}")

    # Add new entry and save
    history.append(entry)
    print(f"Added new entry. History now contains {len(history)} data points")

    # Create directory if it doesn't exist
    try:
        history_dir = os.path.dirname(history_file)
        if history_dir:  # Only try to create if there's a directory part
            print(f"Ensuring directory exists: {history_dir}")
            os.makedirs(history_dir, exist_ok=True)
    except Exception as e:
        print(f"Error creating directory for history file: {e}")
        print(f"Will attempt to save directly to {history_file}")

    # Save the updated history
    try:
        print(f"Writing {len(history)} data points to {history_file}")
        with open(history_file, "w") as f:
            json.dump(history, f, indent=2)
        print("Successfully saved history file")
    except Exception as e:
        print(f"Error saving history file: {e}")
        import traceback
        print(traceback.format_exc())

    return history


def load_csv_data(data_dir="tests/data"):
    """Load CSV performance data files."""
    print(f"Loading CSV performance data from directory: {data_dir}")
    
    # Adjust the path for relative vs absolute path
    # If we're in the 'tests' directory already, we need to use 'data'
    # If we're in the root directory, we need to use 'tests/data'
    if os.path.basename(os.getcwd()) == 'tests':
        adjusted_data_dir = "data"
    else:
        adjusted_data_dir = data_dir
    
    print(f"Current working directory: {os.getcwd()}")
    print(f"Adjusted data directory: {adjusted_data_dir}")
    
    # Check if directory exists
    if not os.path.exists(adjusted_data_dir):
        print(f"Warning: Data directory {adjusted_data_dir} does not exist")
        print("Creating directory...")
        try:
            os.makedirs(adjusted_data_dir, exist_ok=True)
            print(f"Created directory: {adjusted_data_dir}")
        except Exception as e:
            print(f"Error creating directory: {e}")
        return {}
        
    csv_results = {}
    
    # Try multiple glob patterns to find CSV files
    csv_files = glob.glob(f"{adjusted_data_dir}/*.csv")
    if not csv_files:
        # Try direct use of os.listdir as a fallback
        print(f"Trying alternative method to find CSV files...")
        try:
            csv_files = [f"{adjusted_data_dir}/{f}" for f in os.listdir(adjusted_data_dir) 
                        if f.endswith('.csv')]
            print(f"Found {len(csv_files)} CSV files using listdir")
        except Exception as e:
            print(f"Error listing directory: {e}")
    
    if not csv_files:
        print(f"No CSV files found in {adjusted_data_dir}")
        print("Directory contents:")
        try:
            for item in os.listdir(adjusted_data_dir):
                print(f"  - {item}")
        except Exception as e:
            print(f"Error listing directory contents: {e}")
        return {}
        
    print(f"Found {len(csv_files)} CSV files")
    
    for csv_file in csv_files:
        test_name = os.path.basename(csv_file).replace(".csv", "")
        print(f"Processing CSV file: {csv_file} (test: {test_name})")
        
        try:
            # Load CSV into pandas DataFrame
            df = pd.read_csv(csv_file)
            print(f"  File loaded successfully. Row count: {len(df)}")
            
            # Check for required columns
            if 'duration_us' not in df.columns:
                print(f"  Error: Required column 'duration_us' not found in {csv_file}")
                print(f"  Available columns: {', '.join(df.columns)}")
                continue
                
            # Calculate percentiles
            if df.empty:
                print(f"  Warning: DataFrame is empty for {csv_file}")
                continue
                
            duration_micros = df['duration_us']
            p10 = np.percentile(duration_micros, 10)
            p50 = np.percentile(duration_micros, 50)
            p90 = np.percentile(duration_micros, 90)
            p99 = np.percentile(duration_micros, 99)

            print(f"  Performance metrics (μs): p10={int(p10)}, p50={int(p50)}, p90={int(p90)}, p99={int(p99)}")
            
            # Store the results
            csv_results[test_name] = {
                "name": test_name,
                "p10": int(p10),
                "p50": int(p50),
                "p90": int(p90),
                "p99": int(p99),
                "raw_data": df
            }
        except pd.errors.EmptyDataError:
            print(f"  Error: {csv_file} is empty")
        except pd.errors.ParserError:
            print(f"  Error: Could not parse {csv_file}. Check file format.")
        except Exception as e:
            print(f"  Error loading CSV file {csv_file}: {e}")
            print(f"  Exception type: {type(e).__name__}")
            import traceback
            print(traceback.format_exc())

    print(f"Successfully loaded {len(csv_results)} CSV data files")
    return csv_results


def create_bar_chart(tests, output_dir="tests/perf_results"):
    """Create a bar chart visualizing the current performance test results."""
    print(f"Creating bar chart for {len(tests)} tests")
    
    if not tests:
        print("Error: No test data available for bar chart")
        return
    
    try:
        # Extract data from tests
        print("Extracting test data for chart...")
        test_names = [test["name"] for test in tests]
        p10_values = [test["p10"] for test in tests]
        p50_values = [test["p50"] for test in tests]
        p90_values = [test["p90"] for test in tests]
        p99_values = [test["p99"] for test in tests]
        
        print(f"Test names: {test_names}")
        
        # Set up the plot
        x = np.arange(len(test_names))
        width = 0.2

        print("Creating figure...")
        fig = plt.figure(figsize=(12, 7))
        ax = plt.subplot(111)

        # Create the bars
        print("Adding bar data...")
        ax.bar(x - width*1.5, p10_values, width, label='p10', color='green')
        ax.bar(x - width/2, p50_values, width, label='p50', color='blue')
        ax.bar(x + width/2, p90_values, width, label='p90', color='orange')
        ax.bar(x + width*1.5, p99_values, width, label='p99', color='red')

        # Set labels and title
        ax.set_title('Performance Test Results')
        ax.set_ylabel('Microseconds (μs) - lower is better')
        ax.set_xticks(x)
        ax.set_xticklabels(test_names)

        # Add values on top of bars
        print("Adding text labels...")
        for i, test in enumerate(tests):
            ax.text(i - width*1.5, test["p10"], str(test["p10"]),
                    ha='center', va='bottom', fontsize=9, rotation=90)
            ax.text(i - width/2, test["p50"], str(test["p50"]),
                    ha='center', va='bottom', fontsize=9, rotation=90)
            ax.text(i + width/2, test["p90"], str(test["p90"]),
                    ha='center', va='bottom', fontsize=9, rotation=90)
            ax.text(i + width*1.5, test["p99"], str(test["p99"]),
                    ha='center', va='bottom', fontsize=9, rotation=90)

        ax.legend()
        plt.tight_layout()

        # Create output directory if it doesn't exist
        print(f"Ensuring output directory exists: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)

        # Save the figure
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_file = f"{output_dir}/perf_results_{timestamp}.png"
        latest_file = f"{output_dir}/perf_results_latest.png"
        
        print(f"Saving chart to {timestamped_file}")
        plt.savefig(timestamped_file, dpi=300)
        
        print(f"Saving chart to {latest_file}")
        plt.savefig(latest_file, dpi=300)
        
        # Check if files were created successfully
        if os.path.exists(timestamped_file):
            print(f"Successfully saved timestamped chart ({os.path.getsize(timestamped_file)} bytes)")
        else:
            print(f"Warning: Failed to save timestamped chart")
            
        if os.path.exists(latest_file):
            print(f"Successfully saved latest chart ({os.path.getsize(latest_file)} bytes)")
        else:
            print(f"Warning: Failed to save latest chart")

        plt.close()
        print("Bar chart creation complete")
        
    except KeyError as e:
        print(f"Error: Missing required key in test data: {e}")
        print("Test data structure:")
        for i, test in enumerate(tests):
            print(f"  Test {i}: {test}")
    except Exception as e:
        print(f"Error creating bar chart: {e}")
        import traceback
        print(traceback.format_exc())


def create_trend_charts(history, output_dir="tests/perf_results"):
    """Create trend charts showing how performance has changed over time."""
    print(f"Creating trend charts from {len(history) if history else 0} history entries")
    
    if not history:
        print("Error: No history data available")
        return
        
    if len(history) < 2:
        print("Not enough historical data to create trend charts (minimum 2 entries required)")
        return

    try:
        # Get all timestamps and order them
        print("Processing timestamp data...")
        timestamps = []
        formatted_timestamps = []
        
        for entry in history:
            if "timestamp" not in entry:
                print(f"Warning: History entry missing timestamp: {entry}")
                continue
                
            try:
                ts = entry["timestamp"]
                timestamps.append(ts)
                formatted_ts = datetime.fromisoformat(ts).strftime("%m-%d %H:%M")
                formatted_timestamps.append(formatted_ts)
            except (ValueError, TypeError) as e:
                print(f"Error parsing timestamp '{ts}': {e}")
                continue
                
        if not timestamps:
            print("Error: No valid timestamps found in history")
            return
            
        print(f"Found {len(timestamps)} valid timestamps")

        # Get all test names
        print("Collecting all unique test names...")
        all_tests = set()
        for entry in history:
            if "tests" not in entry:
                print(f"Warning: History entry missing tests array: {entry}")
                continue
                
            for test in entry["tests"]:
                if "name" not in test:
                    print(f"Warning: Test missing name: {test}")
                    continue
                all_tests.add(test["name"])
                
        if not all_tests:
            print("Error: No test names found in history")
            return
            
        print(f"Found {len(all_tests)} unique tests: {', '.join(all_tests)}")

        # Create a trend chart for each test
        charts_created = 0
        for test_name in all_tests:
            print(f"Creating trend chart for test: {test_name}")
            plt.figure(figsize=(14, 7))

            # Extract data for this test across all timestamps
            p10_values = []
            p50_values = []
            p90_values = []
            p99_values = []
            valid_timestamps = []

            data_points = 0
            for i, entry in enumerate(history):
                if i >= len(formatted_timestamps):
                    print(f"Warning: Index mismatch - no timestamp for entry {i}")
                    continue
                    
                if "tests" not in entry:
                    continue
                    
                test_found = False
                for test in entry["tests"]:
                    if test.get("name") == test_name:
                        test_found = True
                        try:
                            p10_values.append(test["p10"])
                            p50_values.append(test["p50"])
                            p90_values.append(test["p90"])
                            p99_values.append(test["p99"])
                            valid_timestamps.append(formatted_timestamps[i])
                            data_points += 1
                        except KeyError as e:
                            print(f"Warning: Test missing required data: {e}")
                            print(f"Test data: {test}")
                
                if not test_found:
                    print(f"Test '{test_name}' not found in history entry {i}")

            # Create the plot if we have data
            if valid_timestamps:
                print(f"Plotting trend with {len(valid_timestamps)} data points")
                plt.plot(valid_timestamps, p10_values,
                         'g-', marker='o', label='p10')
                plt.plot(valid_timestamps, p50_values,
                         'b-', marker='o', label='p50')
                plt.plot(valid_timestamps, p90_values,
                         'orange', marker='o', label='p90')
                plt.plot(valid_timestamps, p99_values,
                         'r-', marker='o', label='p99')

                plt.title(f'{test_name} Performance Trend')
                plt.ylabel('Microseconds (μs) - lower is better')
                plt.xlabel('Test Run')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.legend()

                # Save the figure
                print(f"Ensuring output directory exists: {output_dir}")
                os.makedirs(output_dir, exist_ok=True)
                
                chart_file = f"{output_dir}/{test_name}_trend.png"
                print(f"Saving trend chart to: {chart_file}")
                plt.savefig(chart_file, dpi=300)
                
                # Verify the file was created
                if os.path.exists(chart_file):
                    print(f"Successfully saved trend chart ({os.path.getsize(chart_file)} bytes)")
                    charts_created += 1
                else:
                    print(f"Warning: Failed to save trend chart")
                    
                plt.close()
            else:
                print(f"No valid data points found for test '{test_name}', skipping chart")
                plt.close()
        
        print(f"Created {charts_created} trend charts")
        
    except Exception as e:
        print(f"Error creating trend charts: {e}")
        import traceback
        print(traceback.format_exc())


def create_distribution_plot(csv_results, output_dir="tests/perf_results"):
    """Create distribution plots from raw CSV data."""
    print(f"Creating distribution plots from {len(csv_results) if csv_results else 0} CSV results")
    
    if not csv_results:
        print("No CSV data found to create distribution plots")
        return

    # Create output directory if it doesn't exist
    print(f"Ensuring output directory exists: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    # Track success/failure stats
    plots_created = 0
    plots_failed = 0

    # Create a distribution plot for each test
    for test_name, data in csv_results.items():
        print(f"Processing distribution plot for test: {test_name}")
        
        if 'raw_data' not in data:
            print(f"Error: No raw data found for test '{test_name}', skipping")
            plots_failed += 1
            continue
            
        try:
            raw_data = data['raw_data']
            if 'duration_us' not in raw_data.columns:
                print(f"Error: No duration_us column in raw data for test '{test_name}', skipping")
                print(f"Available columns: {', '.join(raw_data.columns)}")
                plots_failed += 1
                continue
                
            if raw_data.empty:
                print(f"Error: Empty raw data for test '{test_name}', skipping")
                plots_failed += 1
                continue
                
            print(f"Creating histogram with {len(raw_data)} data points")
            plt.figure(figsize=(10, 6))

            # Create histogram
            plt.hist(raw_data['duration_us'], bins=30,
                    alpha=0.7, color='skyblue', edgecolor='black')

            # Add vertical lines for percentiles
            for percentile, color, name in [
                ('p10', 'green', 'p10'),
                ('p50', 'blue', 'p50'),
                ('p90', 'orange', 'p90'),
                ('p99', 'red', 'p99')
            ]:
                if percentile in data:
                    value = int(data[percentile])
                    print(f"Adding {name} line at {value}μs")
                    plt.axvline(value, color=color, linestyle='dashed',
                                linewidth=1, label=f'{name}: {value}μs')
                else:
                    print(f"Warning: Missing {percentile} in data")

            plt.title(f'{test_name} Performance Distribution')
            plt.xlabel('Duration (μs)')
            plt.ylabel('Frequency')
            plt.legend()
            plt.tight_layout()

            # Save the figure
            output_file = f"{output_dir}/{test_name}_distribution.png"
            print(f"Saving distribution plot to: {output_file}")
            plt.savefig(output_file, dpi=300)
            
            # Verify the file was created
            if os.path.exists(output_file):
                print(f"Successfully saved distribution plot ({os.path.getsize(output_file)} bytes)")
                plots_created += 1
            else:
                print(f"Warning: Failed to save distribution plot")
                plots_failed += 1
                
            plt.close()
            
        except Exception as e:
            print(f"Error creating distribution plot for '{test_name}': {e}")
            import traceback
            print(traceback.format_exc())
            plots_failed += 1
            # Make sure we close the plot if an error occurred
            try:
                plt.close()
            except:
                pass
    
    print(f"Distribution plot creation complete: {plots_created} created, {plots_failed} failed")


def create_integrated_distribution_plots(csv_results, output_dir="tests/perf_results"):
    """Create integrated distribution plots from raw CSV data for p-distributions."""
    print(f"Creating integrated distribution plots")
    
    if not csv_results:
        print("No CSV data found to create integrated distribution plots")
        return

    # Create output directory if it doesn't exist
    print(f"Ensuring output directory exists: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    # Map the standard tests to their corresponding integration tests
    operation_mapping = {
        "insert": {
            "unit": "insert_performance",
            "integration": "integration_index"
        },
        "search": {
            "unit": "search_performance",
            "integration": "integration_search"
        },
        "delete": {
            "unit": "delete_performance",
            "integration": "integration_delete"
        },
    }

    # Process each operation type
    for operation_name, tests in operation_mapping.items():
        unit_test = tests["unit"]
        integration_test = tests["integration"]
        
        print(f"Processing integrated distribution for {operation_name} operations")
        print(f"  Unit test: {unit_test}")
        print(f"  Integration test: {integration_test}")
        
        # Check if both tests exist in our data
        if unit_test not in csv_results:
            print(f"  Error: Unit test '{unit_test}' not found in data, skipping")
            continue
            
        if integration_test not in csv_results:
            print(f"  Error: Integration test '{integration_test}' not found in data, skipping")
            continue
            
        # Get the data for both tests
        unit_data = csv_results[unit_test]
        integration_data = csv_results[integration_test]
        
        # Check for raw data
        if 'raw_data' not in unit_data:
            print(f"  Error: No raw data for unit test '{unit_test}', skipping")
            continue
            
        if 'raw_data' not in integration_data:
            print(f"  Error: No raw data for integration test '{integration_test}', skipping")
            continue
            
        unit_raw = unit_data['raw_data']
        integration_raw = integration_data['raw_data']
        
        # Check for duration column
        if 'duration_us' not in unit_raw.columns:
            print(f"  Error: No duration_us column in unit test data, skipping")
            continue
            
        if 'duration_us' not in integration_raw.columns:
            print(f"  Error: No duration_us column in integration test data, skipping")
            continue
            
        # Create the plot
        try:
            plt.figure(figsize=(12, 7))
            
            # Create histograms for both tests
            # Use alpha for transparency to see overlapping areas
            plt.hist(unit_raw['duration_us'], bins=30, 
                     alpha=0.6, color='skyblue', edgecolor='black', 
                     label=f'{unit_test}')
            plt.hist(integration_raw['duration_us'], bins=30, 
                     alpha=0.6, color='lightgreen', edgecolor='black', 
                     label=f'{integration_test}')
            
            # Add vertical lines for percentiles from both tests
            for test_name, data, line_style in [(unit_test, unit_data, 'solid'), 
                                               (integration_test, integration_data, 'dashed')]:
                for percentile, color, name in [
                    ('p10', 'green', 'p10'),
                    ('p50', 'blue', 'p50'),
                    ('p90', 'orange', 'p90'),
                    ('p99', 'red', 'p99')
                ]:
                    if percentile in data:
                        value = int(data[percentile])
                        if test_name == unit_test:
                            label = f'{test_name} {name}: {value}μs'
                        else:
                            label = f'{test_name} {name}: {value}μs'
                        print(f"  Adding {label}")
                        plt.axvline(value, color=color, linestyle=line_style,
                                    linewidth=1.5, label=label)
                    else:
                        print(f"  Warning: Missing {percentile} in {test_name} data")
            
            # Set the plot title and labels
            plt.title(f'Integrated {operation_name.capitalize()} Performance Distribution')
            plt.xlabel('Duration (μs)')
            plt.ylabel('Frequency')
            plt.legend(loc='upper right')
            plt.tight_layout()
            
            # Save the figure - generate two versions of each file to ensure compatibility
            # with both naming conventions used in the documentation

            # First, save with the operation name format (e.g., integrated_insert_distribution.png)
            output_file = f"{output_dir}/integrated_{operation_name}_distribution.png"
            print(f"  Saving integrated distribution plot to: {output_file}")
            plt.savefig(output_file, dpi=300)
            
            # Second, save with integration test name if it differs from the operation name 
            # (especially for "insert" -> "integration_index")
            if operation_name == "insert":
                test_output_file = f"{output_dir}/integrated_index_distribution.png"
                print(f"  Saving additional copy to: {test_output_file}")
                plt.savefig(test_output_file, dpi=300)
            
            # Verify the files were created
            if os.path.exists(output_file):
                print(f"  Successfully saved integrated distribution plot ({os.path.getsize(output_file)} bytes)")
            else:
                print(f"  Warning: Failed to save integrated distribution plot")
                
            plt.close()
            
        except Exception as e:
            print(f"  Error creating integrated distribution plot for '{operation_name}': {e}")
            import traceback
            print(traceback.format_exc())
            try:
                plt.close()
            except:
                pass

    print("Integrated distribution plot creation complete")


def compare_operations(csv_results, output_dir="tests/perf_results"):
    """Create comparison plots between unit tests and integration tests."""
    print(f"Creating operation comparison plots from {len(csv_results) if csv_results else 0} CSV results")
    
    if not csv_results:
        print("No CSV data found to create comparison plots")
        return

    # Group the tests by operation type (insert, search, delete)
    operation_groups = {
        "insert": ["insert_performance", "integration_index"],
        "search": ["search_performance", "text_search_performance", "integration_search"],
        "delete": ["delete_performance", "integration_delete"],
        # Add hot/cold performance comparison group
        "hot_cold": ["cold_search_performance", "hot_search_performance"]
    }
    
    print(f"Defined operation groups: {operation_groups}")

    # Create output directory if it doesn't exist
    print(f"Ensuring output directory exists: {output_dir}")
    os.makedirs(output_dir, exist_ok=True)
    
    # Track success/failure stats
    plots_created = 0
    plots_failed = 0

    # Create a comparison chart for each operation type
    for operation, test_group in operation_groups.items():
        print(f"\nProcessing comparison for operation: {operation}")
        print(f"Tests in this group: {', '.join(test_group)}")
        
        # Collect tests that exist in our data
        available_tests = [test for test in test_group if test in csv_results]
        print(f"Tests available in data: {', '.join(available_tests) if available_tests else 'None'}")

        if len(available_tests) > 0:
            try:
                # Prepare the data
                print("Preparing data for comparison chart...")
                test_names = []
                p10_values = []
                p50_values = []
                p90_values = []
                p99_values = []

                for test in available_tests:
                    print(f"Adding test '{test}' to comparison")
                    test_data = csv_results[test]
                    
                    try:
                        test_names.append(test)
                        p10_values.append(test_data["p10"])
                        p50_values.append(test_data["p50"])
                        p90_values.append(test_data["p90"])
                        p99_values.append(test_data["p99"])
                    except KeyError as e:
                        print(f"Error: Missing key in test data: {e}")
                        print(f"Available keys: {', '.join(test_data.keys())}")
                        
                # Only proceed if we have valid data
                if not test_names:
                    print(f"No valid test data for operation '{operation}', skipping")
                    plots_failed += 1
                    continue
                        
                # Create the chart
                print("Creating comparison chart...")
                x = np.arange(len(test_names))
                width = 0.2

                fig, ax = plt.subplots(figsize=(10, 6))

                # Plot the bars
                ax.bar(x - width*1.5, p10_values, width,
                       label='p10', color='green')
                ax.bar(x - width/2, p50_values, width, label='p50', color='blue')
                ax.bar(x + width/2, p90_values, width, label='p90', color='orange')
                ax.bar(x + width*1.5, p99_values, width, label='p99', color='red')

                # Set appropriate title based on the operation type
                title = f'{operation.capitalize()} Performance Comparison'
                if operation == "hot_cold":
                    title = "Hot vs Cold Query Performance Comparison"
                    
                ax.set_title(title)
                ax.set_ylabel('Microseconds (μs) - lower is better')
                ax.set_xticks(x)
                
                # Format test names more nicely for hot_cold comparison
                display_names = test_names
                if operation == "hot_cold":
                    display_names = [name.replace("_performance", "").replace("_", " ").title() for name in test_names]
                
                ax.set_xticklabels(display_names)

                # Add values on top of bars
                print("Adding value labels...")
                for i in range(len(test_names)):
                    ax.text(
                        i - width*1.5, p10_values[i], str(p10_values[i]), ha='center', va='bottom', fontsize=9)
                    ax.text(
                        i - width/2, p50_values[i], str(p50_values[i]), ha='center', va='bottom', fontsize=9)
                    ax.text(
                        i + width/2, p90_values[i], str(p90_values[i]), ha='center', va='bottom', fontsize=9)
                    ax.text(
                        i + width*1.5, p99_values[i], str(p99_values[i]), ha='center', va='bottom', fontsize=9)

                ax.legend()
                plt.tight_layout()

                # Save the figure
                output_file = f"{output_dir}/{operation}_comparison.png"
                print(f"Saving comparison chart to: {output_file}")
                plt.savefig(output_file, dpi=300)
                
                # Verify the file was created
                if os.path.exists(output_file):
                    print(f"Successfully saved comparison chart ({os.path.getsize(output_file)} bytes)")
                    plots_created += 1
                else:
                    print(f"Warning: Failed to save comparison chart")
                    plots_failed += 1
                    
                plt.close()
                
            except Exception as e:
                print(f"Error creating comparison chart for operation '{operation}': {e}")
                import traceback
                print(traceback.format_exc())
                plots_failed += 1
                # Make sure we close the plot if an error occurred
                try:
                    plt.close()
                except:
                    pass
        else:
            print(f"No tests available for operation '{operation}', skipping comparison")
    
    print(f"Operation comparison plot creation complete: {plots_created} created, {plots_failed} failed")


def create_server_reload_visualization(csv_results, output_dir="tests/perf_results"):
    """Create visualization for server reload performance."""
    print("Creating server reload visualization chart")
    
    if "server_reload_performance" not in csv_results:
        print("No server reload performance data found")
        return
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Get the data
        reload_data = csv_results["server_reload_performance"]
        raw_data = reload_data.get("raw_data")
        
        if raw_data is None or raw_data.empty:
            print("No raw data available for server reload performance")
            return
            
        # Create a simple bar chart for reload time
        plt.figure(figsize=(8, 6))
        
        # Extract the duration value (usually just one value)
        duration_us = raw_data["duration_us"].iloc[0] if len(raw_data) > 0 else 0
        duration_ms = duration_us / 1000  # Convert to milliseconds for better readability
        
        # Create bar chart
        plt.bar(["Server Reload Time"], [duration_ms], color='blue')
        
        # Add labels
        plt.ylabel('Time (milliseconds)')
        plt.title('Server Reload Time with Existing Data')
        
        # Add value on top of bar
        plt.text(0, duration_ms, f"{int(duration_ms)} ms", 
                 ha='center', va='bottom', fontsize=12)
        
        # Adjust y-axis to start from 0 and go slightly above the bar
        plt.ylim(0, duration_ms * 1.2)
        
        # Save the figure
        output_file = f"{output_dir}/server_reload_performance.png"
        plt.savefig(output_file, dpi=300)
        
        # Verify the file was created
        if os.path.exists(output_file):
            print(f"Successfully saved server reload chart ({os.path.getsize(output_file)} bytes)")
        else:
            print(f"Warning: Failed to save server reload chart")
            
        plt.close()
        
    except Exception as e:
        print(f"Error creating server reload visualization: {e}")
        import traceback
        print(traceback.format_exc())
        try:
            plt.close()
        except:
            pass


def check_dependencies():
    """Check if required dependencies are installed."""
    print("Checking required dependencies...")
    
    missing_deps = []
    installed_deps = []
    
    # List all required dependencies with versions if possible
    dependencies = [
        ("matplotlib", "matplotlib"),
        ("numpy", "numpy"),
        ("pandas", "pandas")
    ]
    
    for module_name, import_name in dependencies:
        try:
            module = __import__(import_name)
            version = getattr(module, "__version__", "unknown version")
            installed_deps.append(f"{module_name} ({version})")
        except ImportError as e:
            missing_deps.append(module_name)
            print(f"Missing dependency: {module_name} - {e}")
    
    if missing_deps:
        print("\n===== DEPENDENCY CHECK FAILED =====")
        print(f"Missing dependencies: {', '.join(missing_deps)}")
        print("\nPlease install required dependencies with:")
        print("pip3 install -r tests/requirements.txt")
        
        # Check if the requirements file exists
        if os.path.exists("tests/requirements.txt"):
            print("\nContents of requirements.txt:")
            try:
                with open("tests/requirements.txt", "r") as f:
                    print(f.read())
            except Exception as e:
                print(f"Error reading requirements.txt: {e}")
        else:
            print("\nWarning: tests/requirements.txt not found in current directory")
            print(f"Current directory: {os.getcwd()}")
            
        return False
    else:
        print("\n===== DEPENDENCY CHECK PASSED =====")
        print("All required dependencies are installed:")
        for dep in installed_deps:
            print(f"  - {dep}")
        return True


def main():
    print("="*80)
    print("PERFORMANCE VISUALIZATION SCRIPT STARTING")
    print("="*80)
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python version: {sys.version}")
    
    # First check dependencies
    if not check_dependencies():
        print("Dependency check failed. Exiting.")
        return

    # Get script directory for path resolution
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    parser = argparse.ArgumentParser(
        description='Run and visualize performance tests')
    parser.add_argument('--no-run', action='store_true',
                        help='Skip running tests and just visualize existing data')
    parser.add_argument('--history-file', default=os.path.join(script_dir, 'perf_history.json'),
                        help='Path to the performance history file')
    parser.add_argument('--output-dir', default=os.path.join(script_dir, 'perf_results'),
                        help='Directory to save visualizations')
    parser.add_argument('--data-dir', default=os.path.join(script_dir, 'data'),
                        help='Directory with CSV data files')
    parser.add_argument('--integrated-only', action='store_true',
                        help='Only generate integrated p-distribution charts')

    args = parser.parse_args()
    print(f"Command line arguments: {args}")

    tests = None
    history = []

    # STEP 1: Run tests if requested
    if not args.no_run:
        print("\n" + "="*40)
        print("STEP 1: RUNNING PERFORMANCE TESTS")
        print("="*40)
        output = run_performance_tests()
        tests = parse_performance_data(output)

        if not tests:
            print("No performance test results found in logs. Will try CSV data for visualization.")
    else:
        print("\nSkipping test run (--no-run specified)")

    # STEP 2: Load CSV data
    print("\n" + "="*40)
    print("STEP 2: LOADING CSV DATA")
    print("="*40)
    csv_results = load_csv_data(args.data_dir)

    if csv_results:
        print(f"Loaded performance data for {len(csv_results)} tests from CSV files")

        # If we didn't get tests from the output logs, create them from CSV data
        if not tests:
            print("Using CSV data to create test results")
            tests = [data for name, data in csv_results.items()
                     if "raw_data" not in data]
            print(f"Created {len(tests)} test entries from CSV data")

        # STEP 3: Save performance data
        if tests:
            print("\n" + "="*40)
            print("STEP 3: SAVING PERFORMANCE DATA")
            print("="*40)
            print(f"Saving performance data to history file: {args.history_file}")
            history = save_performance_data(tests, args.history_file)
            print(f"History now contains {len(history)} data points")
    else:
        print("No CSV performance data files found")
        if not tests:
            print("No performance data available from either logs or CSV files")

            # STEP 4: Load existing history as a fallback
            print("\n" + "="*40)
            print("STEP 4: ATTEMPTING TO LOAD HISTORY AS FALLBACK")
            print("="*40)
            
            if os.path.exists(args.history_file):
                print(f"Loading history from: {args.history_file}")
                try:
                    with open(args.history_file, "r") as f:
                        try:
                            history = json.load(f)
                            print(f"Successfully loaded history with {len(history)} data points")
                            
                            if history and len(history) > 0:
                                # Get the most recent test results
                                tests = history[-1]["tests"]
                                print(f"Using most recent test results with {len(tests)} tests")
                            else:
                                print("History file exists but contains no data")
                                return
                        except json.JSONDecodeError as e:
                            print(f"Error decoding history file: {e}")
                            print("First 100 bytes of file:")
                            with open(args.history_file, "r") as f2:
                                print(f2.read(100))
                            return
                except Exception as e:
                    print(f"Error opening history file: {e}")
                    return
            else:
                print(f"History file {args.history_file} not found")
                print("Run performance tests first with:")
                print("./tests/run_tests.sh --perf")
                return

    # STEP 5: Create visualizations
    print("\n" + "="*40)
    print("STEP 5: CREATING VISUALIZATIONS")
    print("="*40)
    
    # Create output directory if it doesn't exist
    print(f"Creating output directory: {args.output_dir}")
    os.makedirs(args.output_dir, exist_ok=True)

    if args.integrated_only:
        print("\nGenerating only integrated p-distribution charts as requested")
        
        # Create integrated distribution plots
        if csv_results:
            print("\nCreating integrated p-distribution plots...")
            try:
                create_integrated_distribution_plots(csv_results, args.output_dir)
                print(f"Integrated p-distribution plots created in {args.output_dir}")
            except Exception as e:
                print(f"Error creating integrated p-distribution plots: {e}")
                import traceback
                print(traceback.format_exc())
    else:
        # Create bar chart
        if tests:
            print("\nCreating bar chart...")
            try:
                create_bar_chart(tests, args.output_dir)
                print(f"Bar chart created in {args.output_dir}")
            except Exception as e:
                print(f"Error creating bar chart: {e}")
                import traceback
                print(traceback.format_exc())

        # Create trend charts if we have enough data
        if len(history) >= 2:
            print("\nCreating trend charts...")
            try:
                create_trend_charts(history, args.output_dir)
                print(f"Trend charts created in {args.output_dir}")
            except Exception as e:
                print(f"Error creating trend charts: {e}")
                import traceback
                print(traceback.format_exc())
        elif len(history) == 1:
            print("\nOnly one data point available. Run tests again to generate trend charts.")

        # Create distribution plots from CSV data
        if csv_results:
            print("\nCreating distribution plots...")
            try:
                create_distribution_plot(csv_results, args.output_dir)
                print(f"Distribution plots created in {args.output_dir}")
            except Exception as e:
                print(f"Error creating distribution plots: {e}")
                import traceback
                print(traceback.format_exc())

            # Create integrated distribution plots
            print("\nCreating integrated p-distribution plots...")
            try:
                create_integrated_distribution_plots(csv_results, args.output_dir)
                print(f"Integrated p-distribution plots created in {args.output_dir}")
            except Exception as e:
                print(f"Error creating integrated p-distribution plots: {e}")
                import traceback
                print(traceback.format_exc())

            # Create comparison plots
            print("\nCreating operation comparison plots...")
            try:
                compare_operations(csv_results, args.output_dir)
                print(f"Operation comparison plots created in {args.output_dir}")
            except Exception as e:
                print(f"Error creating comparison plots: {e}")
                import traceback
                print(traceback.format_exc())
                
            # Create server reload visualization (if data available)
            print("\nCreating server reload visualization...")
            try:
                create_server_reload_visualization(csv_results, args.output_dir)
                print(f"Server reload visualization created in {args.output_dir}")
            except Exception as e:
                print(f"Error creating server reload visualization: {e}")
                import traceback
                print(traceback.format_exc())

    print("\n" + "="*80)
    print("PERFORMANCE VISUALIZATION COMPLETE")
    print(f"Visualizations saved to: {args.output_dir}")
    print("="*80)


if __name__ == "__main__":
    main()