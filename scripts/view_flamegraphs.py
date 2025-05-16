#!/usr/bin/env python3
"""
Simple HTTP server to view flamegraphs generated from benchmarks.
This creates a simple web interface that lists all SVG flamegraphs
and allows you to view them in your browser with interactive features.
It also displays percentile statistics from benchmark results.
"""

import os
import glob
import http.server
import socketserver
import webbrowser
import json
from datetime import datetime
from urllib.parse import urlparse, parse_qs

# Configuration
FLAMEGRAPH_DIR = "profiling_results/flamegraphs"
RESULTS_DIR = "benchmark_results"
PORT = 8880
HOST = "localhost"


def get_benchmark_data():
    """Get and parse benchmark results data."""
    benchmark_data = {}

    # Check if the results directory exists
    if not os.path.exists(RESULTS_DIR):
        return benchmark_data

    # Find all JSON benchmark result files
    for subdir, _, files in os.walk(RESULTS_DIR):
        for file in files:
            if file.endswith('.json'):
                full_path = os.path.join(subdir, file)
                try:
                    with open(full_path, 'r') as f:
                        data = json.load(f)

                    # Extract benchmark type from path
                    benchmark_type = os.path.basename(
                        os.path.dirname(full_path))
                    test_case = data.get('test_case', 'unknown')

                    # Create a key for this benchmark
                    key = f"{benchmark_type}_{test_case}"

                    # Extract relevant data
                    benchmark_data[key] = {
                        'benchmark_type': benchmark_type,
                        'test_case': test_case,
                        'p90_ms': data.get('p90_ms', None),
                        'p95_ms': data.get('p95_ms', None),
                        'p99_ms': data.get('p99_ms', None),
                        'record_size': data.get('record_size', None),
                        'batch_size': data.get('batch_size', None),
                        'doc_count': data.get('doc_count', None),
                        'iterations': data.get('iterations', 0)
                    }
                except Exception as e:
                    print(f"Error processing {full_path}: {e}")

    return benchmark_data


class FlamegraphHandler(http.server.SimpleHTTPRequestHandler):
    """Custom request handler for flamegraph viewer."""

    def do_GET(self):
        """Handle GET requests."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        # Serve favicon
        if path == "/favicon.ico":
            self.send_response(404)
            self.end_headers()
            return

        # Serve static SVG files directly
        if path.endswith(".svg"):
            # Strip leading slash if present
            file_path = path[1:] if path.startswith("/") else path
            if os.path.exists(file_path) and os.path.isfile(file_path):
                self.send_response(200)
                self.send_header("Content-type", "image/svg+xml")
                self.end_headers()
                with open(file_path, 'rb') as f:
                    self.wfile.write(f.read())
                return

        # Handle stats page
        if path == "/stats":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            benchmark_data = get_benchmark_data()

            # Generate HTML for stats page
            html = f"""<!DOCTYPE html>
<html>
<head>
    <title>FuguDB Performance Statistics</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 0;
            line-height: 1.5;
            color: #333;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        h1, h2, h3 {{
            color: #2c3e50;
        }}
        h1 {{
            margin-top: 0;
            padding-top: 20px;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
        }}
        .stats-container {{
            background-color: white;
            border-radius: 5px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }}
        th, td {{
            padding: 10px;
            border: 1px solid #ddd;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        .back-link {{
            display: inline-block;
            margin-bottom: 15px;
            color: #3498db;
            text-decoration: none;
        }}
        .back-link:hover {{
            text-decoration: underline;
        }}
        .timestamp {{
            color: #7f8c8d;
            font-size: 0.9em;
            margin-top: 30px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <a href="/" class="back-link">← Back to flamegraph viewer</a>
        <h1>FuguDB Performance Statistics</h1>

        <div class="stats-container">
            <h2>Percentile Latency Statistics</h2>
"""

            # Add table if we have data
            if benchmark_data:
                html += """
            <table>
                <thead>
                    <tr>
                        <th>Benchmark Type</th>
                        <th>Test Case</th>
                        <th>P90 (ms)</th>
                        <th>P95 (ms)</th>
                        <th>P99 (ms)</th>
                        <th>Record Size</th>
                        <th>Batch Size</th>
                        <th>Doc Count</th>
                        <th>Iterations</th>
                    </tr>
                </thead>
                <tbody>
"""

                # Add rows for each benchmark
                for key, data in sorted(benchmark_data.items()):
                    benchmark_type = data.get('benchmark_type', 'N/A')
                    test_case = data.get('test_case', 'N/A')
                    p90 = data.get('p90_ms', 'N/A')
                    p95 = data.get('p95_ms', 'N/A')
                    p99 = data.get('p99_ms', 'N/A')
                    record_size = data.get('record_size', 'N/A')
                    batch_size = data.get('batch_size', 'N/A')
                    doc_count = data.get('doc_count', 'N/A')
                    iterations = data.get('iterations', 'N/A')

                    # Format numeric values
                    for var in ['p90', 'p95', 'p99']:
                        val = locals()[var]
                        if isinstance(val, (int, float)):
                            locals()[var] = f"{val:.3f}"

                    html += f"""
                    <tr>
                        <td>{benchmark_type}</td>
                        <td>{test_case}</td>
                        <td>{p90}</td>
                        <td>{p95}</td>
                        <td>{p99}</td>
                        <td>{record_size}</td>
                        <td>{batch_size}</td>
                        <td>{doc_count}</td>
                        <td>{iterations}</td>
                    </tr>
"""

                html += """
                </tbody>
            </table>
"""
            else:
                html += """
            <p>No benchmark data available. Run benchmarks to generate data.</p>
"""

            # Add timestamp and close HTML
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            html += f"""
        </div>

        <div class="timestamp">
            <p>Generated: {timestamp}</p>
        </div>
    </div>
</body>
</html>
"""

            self.wfile.write(html.encode())
            return

        # Home page - list all flamegraphs
        if path == "/" or path == "/index.html":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            svg_files = sorted(glob.glob(f"{FLAMEGRAPH_DIR}/*.svg"))

            # Group files by benchmark type
            benchmark_groups = {}
            for svg_file in svg_files:
                basename = os.path.basename(svg_file)
                # Extract benchmark type from filename
                parts = basename.split("_")
                if len(parts) > 1:
                    benchmark_type = parts[0]
                    if benchmark_type not in benchmark_groups:
                        benchmark_groups[benchmark_type] = []
                    benchmark_groups[benchmark_type].append(svg_file)
                else:
                    # Fallback for files that don't match the expected pattern
                    if "ungrouped" not in benchmark_groups:
                        benchmark_groups["ungrouped"] = []
                    benchmark_groups["ungrouped"].append(svg_file)

            # Check if we have benchmark data
            has_benchmark_data = os.path.exists(
                RESULTS_DIR) and len(os.listdir(RESULTS_DIR)) > 0

            # Generate HTML
            html = f"""<!DOCTYPE html>
<html>
<head>
    <title>FuguDB Flamegraph Viewer</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 0;
            line-height: 1.5;
            color: #333;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        h1, h2, h3 {{
            color: #2c3e50;
        }}
        h1 {{
            margin-top: 0;
            padding-top: 20px;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
        }}
        .benchmark-group {{
            background-color: white;
            border-radius: 5px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .flamegraph-list {{
            list-style-type: none;
            padding: 0;
        }}
        .flamegraph-item {{
            margin-bottom: 8px;
            padding: 8px;
            background-color: #f9f9f9;
            border-radius: 4px;
            border-left: 4px solid #3498db;
        }}
        .flamegraph-item:hover {{
            background-color: #eef7fa;
        }}
        .flamegraph-link {{
            text-decoration: none;
            color: #3498db;
            font-weight: 500;
            display: block;
        }}
        .flamegraph-link:hover {{
            text-decoration: underline;
        }}
        .viewer {{
            background-color: white;
            border-radius: 5px;
            padding: 20px;
            margin-top: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .viewer h2 {{
            margin-top: 0;
        }}
        .timestamp {{
            color: #7f8c8d;
            font-size: 0.9em;
            margin-top: 30px;
        }}
        .help-text {{
            background-color: #eef7fa;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
        }}
        .controls {{
            margin-bottom: 15px;
        }}
        button {{
            background-color: #3498db;
            color: white;
            border: none;
            padding: 8px 15px;
            border-radius: 4px;
            cursor: pointer;
            margin-right: 10px;
        }}
        button:hover {{
            background-color: #2980b9;
        }}
        iframe {{
            width: 100%;
            height: 500px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }}
        .navigation {{
            margin-bottom: 20px;
        }}
        .navigation a {{
            display: inline-block;
            padding: 8px 15px;
            background-color: #3498db;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            margin-right: 10px;
        }}
        .navigation a:hover {{
            background-color: #2980b9;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>FuguDB Flamegraph Viewer</h1>

        <div class="navigation">
            <a href="/">Flamegraphs</a>
            <a href="/stats">Performance Statistics</a>
        </div>

        <div class="help-text">
            <p><strong>How to use flamegraphs:</strong></p>
            <ul>
                <li>Click on a function in the flamegraph to zoom in</li>
                <li>Click on the top frame to zoom out</li>
                <li>Search for functions using Ctrl+F in your browser</li>
                <li>Hover over a frame to see details about that function</li>
                <li>The width of each frame represents the time spent in that function</li>
            </ul>
        </div>

        <h2>Available Flamegraphs</h2>
"""

            # Add benchmark groups
            for group_name, files in sorted(benchmark_groups.items()):
                display_name = group_name.replace("_", " ").title()
                html += f"""
        <div class="benchmark-group">
            <h3>{display_name} Benchmarks</h3>
            <ul class="flamegraph-list">
"""

                for svg_file in sorted(files):
                    rel_path = os.path.relpath(svg_file, ".")
                    filename = os.path.basename(svg_file)
                    # Clean up the name for display
                    display_filename = filename.replace(
                        ".svg", "").replace("_", " ").title()

                    # Add file details
                    mtime = os.path.getmtime(svg_file)
                    file_date = datetime.fromtimestamp(
                        mtime).strftime("%Y-%m-%d %H:%M:%S")
                    file_size = os.path.getsize(svg_file) / 1024  # KB

                    html += f"""
                <li class="flamegraph-item">
                    <a href="view?file={rel_path}" class="flamegraph-link" target="_blank">{display_filename}</a>
                    <small style="color: #7f8c8d;">({file_size:.1f} KB, {file_date})</small>
                </li>
"""

                html += """
            </ul>
        </div>
"""

            # Add timestamp and close HTML
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            html += f"""
        <div class="timestamp">
            <p>Generated: {timestamp}</p>
        </div>
    </div>
</body>
</html>
"""

            self.wfile.write(html.encode())
            return

        # View a specific flamegraph
        if path == "/view":
            query = parse_qs(parsed_url.query)
            if "file" in query and query["file"]:
                file_path = query["file"][0]
                if os.path.exists(file_path) and os.path.isfile(file_path):
                    self.send_response(200)
                    self.send_header("Content-type", "text/html")
                    self.end_headers()

                    filename = os.path.basename(file_path)
                    display_name = filename.replace(
                        ".svg", "").replace("_", " ").title()
                    
                    # Read the SVG content directly
                    svg_content = ""
                    try:
                        with open(file_path, 'r') as svg_file:
                            svg_content = svg_file.read()
                    except:
                        svg_content = "<svg><text>Error loading SVG</text></svg>"

                    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{display_name} - FuguDB Flamegraph Viewer</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            margin: 0;
            padding: 0;
            line-height: 1.5;
            color: #333;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }}
        h1, h2 {{
            color: #2c3e50;
        }}
        h1 {{
            margin-top: 0;
            padding-top: 20px;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
        }}
        .viewer {{
            background-color: white;
            border-radius: 5px;
            padding: 20px;
            margin-top: 20px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            overflow: auto;
        }}
        .controls {{
            margin-bottom: 15px;
        }}
        button {{
            background-color: #3498db;
            color: white;
            border: none;
            padding: 8px 15px;
            border-radius: 4px;
            cursor: pointer;
            margin-right: 10px;
        }}
        button:hover {{
            background-color: #2980b9;
        }}
        .back-link {{
            display: inline-block;
            margin-bottom: 15px;
            color: #3498db;
            text-decoration: none;
        }}
        .back-link:hover {{
            text-decoration: underline;
        }}
        .svg-container {{
            width: 100%;
            height: 600px;
            border: 1px solid #ddd;
            border-radius: 4px;
            overflow: auto;
        }}
        .help-text {{
            background-color: #eef7fa;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
        }}
        /* Additional styles for the SVG when directly embedded */
        .flamebox:hover {{
            fill: #ff9e80 !important;
            stroke: #000;
            stroke-width: 1;
        }}
    </style>
    <script>
        // Function to display alert when a flamebox is clicked
        function showFlameboxInfo(name, value) {
            alert(name + ': ' + value + ' of total time');
        }
    </script>
</head>
<body>
    <div class="container">
        <a href="/" class="back-link">← Back to all flamegraphs</a>
        <h1>{display_name}</h1>

        <div class="help-text">
            <p><strong>Interaction Instructions:</strong></p>
            <ul>
                <li>Click on a function block to see details</li>
                <li>Hover over blocks to highlight them</li>
                <li>Colors represent different operation types</li>
            </ul>
        </div>

        <div class="viewer">
            <div class="svg-container">
                {svg_content}
            </div>
        </div>
    </div>
    
    <script>
        // Add event listeners to all flameboxes for better interactivity
        document.addEventListener('DOMContentLoaded', function() {
            var flameboxes = document.querySelectorAll('.flamebox');
            flameboxes.forEach(function(box) {
                box.addEventListener('click', function() {
                    var name = this.getAttribute('data-name');
                    var value = this.getAttribute('data-value');
                    showFlameboxInfo(name, value);
                });
            });
        });
    </script>
</body>
</html>"""

                    self.wfile.write(html.encode())
                    return

        # Default response for unknown paths
        self.send_response(404)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"404 Not Found")


def main():
    """Start the server and open the browser."""
    # Ensure the flamegraph directory exists
    if not os.path.exists(FLAMEGRAPH_DIR):
        os.makedirs(FLAMEGRAPH_DIR, exist_ok=True)

    # Count SVG files
    svg_files = glob.glob(f"{FLAMEGRAPH_DIR}/*.svg")
    svg_count = len(svg_files)

    # Start the server
    handler = FlamegraphHandler
    httpd = socketserver.TCPServer((HOST, PORT), handler)

    print(f"Starting flamegraph viewer server at http://{HOST}:{PORT}")
    print(f"Found {svg_count} flamegraph{
          's' if svg_count != 1 else ''} in {FLAMEGRAPH_DIR}")

    # Open browser automatically
    webbrowser.open(f"http://{HOST}:{PORT}")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server")
        httpd.server_close()


if __name__ == "__main__":
    main()