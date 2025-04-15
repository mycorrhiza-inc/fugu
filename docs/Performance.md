# Fugu Performance Guide

This document covers Fugu's performance characteristics, optimization strategies, and benchmarking methods.

## Performance Overview

Fugu is designed for high-performance search and indexing operations. Key performance features include:

- Fast inverted index for text search
- Efficient Write-Ahead Log (WAL) with batching
- Parallel processing for large files
- Namespace-based isolation to reduce contention
- Streaming operations for memory efficiency

## Benchmarking

Fugu includes a comprehensive benchmarking suite in the `tests/` directory. The performance tests measure:

1. **Indexing Speed**: Time to index documents of various sizes
2. **Search Latency**: Time to perform searches with various term counts
3. **Deletion Performance**: Time to remove documents from the index
4. **Hot/Cold Performance**: Performance difference between warm and cold caches

### Running Performance Tests

```bash
# Run all performance tests
./tests/run_tests.sh --perf

# Generate visualizations of performance data
python3 tests/perf_visualize.py
```

### Performance Metrics

The system collects and reports detailed metrics:

- **Indexing**: Documents per second, bytes per second
- **Search**: Queries per second, latency percentiles (p50, p90, p99)
- **Memory Usage**: Peak memory consumption during operations
- **Disk Usage**: Storage efficiency and growth patterns

## Optimization Strategies

### Indexing Optimization

For optimal indexing performance:

1. **Use Streaming for Large Files**: Files over 10MB should use the streaming API
2. **Batch Small Files**: Index multiple small files in one operation when possible
3. **Namespace Organization**: Group related documents in the same namespace
4. **Parallel Processing**: Fugu automatically uses parallel processing for files >512KB

```bash
# Optimized streaming for large files
cargo run -- stream-index /path/to/large/file.txt --namespace my_namespace
```

### Search Optimization

For optimal search performance:

1. **Specific Queries**: Use specific terms rather than very common words
2. **Appropriate Limits**: Set reasonable result limits (10-100) to avoid processing overhead
3. **Namespace Targeting**: Search in specific namespaces rather than globally
4. **Warm Caches**: First search may be slower as indexes load into memory

```bash
# Optimized search query
cargo run -- search --namespace specific_ns --limit 20 "specific search terms"
```

### WAL Optimization

The Write-Ahead Log is optimized for performance:

1. **Batching**: Operations are batched to minimize disk I/O
2. **Concurrency Control**: Limited concurrent writers prevent contention
3. **Background Flushing**: Asynchronous flushing minimizes blocking
4. **Namespace Isolation**: Each namespace has its own WAL file

## Memory Management

Fugu employs several strategies for efficient memory usage:

1. **On-Demand Loading**: Indexes are loaded only when needed
2. **Streaming Processing**: Large files are processed in chunks
3. **Bounded Caches**: Caches have size limits to prevent unbounded growth
4. **Explicit Unloading**: Indexes can be unloaded to free memory

## Storage Efficiency

The system optimizes storage usage:

1. **Compressed Term Storage**: Common terms are stored efficiently
2. **Position Deduplication**: Repeated term positions are stored once
3. **Binary-Safe Handling**: Binary files are stored with minimal overhead
4. **Incremental Updates**: Only changes are written to disk

## Parallel Processing

For large files, Fugu uses parallel processing with Conflict-free Replicated Data Types (CRDTs):

1. **File Chunking**: Files are split into optimal-sized chunks
2. **Concurrent Processing**: Each chunk is processed in parallel
3. **CRDT Merging**: Results are merged using CRDT operations
4. **Worker Management**: Number of workers adapts to system resources

## Performance Tuning

### Server Tuning

The following parameters can be tuned for performance:

1. **WAL Batch Size**: Adjust `BUFFER_SIZE` in `wal.rs` (default: 1MB)
2. **Concurrent Writers**: Adjust `MAX_CONCURRENT_WRITERS` in `wal.rs` (default: 8)
3. **Flush Interval**: Adjust `FLUSH_INTERVAL_MS` in `wal.rs` (default: 100ms)
4. **Recent Operations Cache**: Adjust `max_recent_ops` in `WAL` struct (default: 1000)

### BM25 Parameter Tuning

For search relevance tuning, adjust the BM25 parameters:

1. **k1**: Controls term frequency saturation (default: 1.2)
2. **b**: Controls document length normalization (default: 0.75)

Higher k1 values give more weight to term frequency, while higher b values normalize more aggressively for document length.

## Performance Visualization

The `tests/perf_visualize.py` script generates visualizations:

1. **Distribution Plots**: Show performance distribution
2. **Comparison Charts**: Compare different operations
3. **Timeline Graphs**: Track performance changes over time

Results are saved to `tests/perf_results/` as PNG images.

## Performance Bottlenecks

Common performance bottlenecks and solutions:

1. **Disk I/O**: Use SSDs for faster storage performance
2. **Memory Constraints**: Reduce `max_recent_ops` for lower memory usage
3. **CPU Utilization**: Adjust `MAX_CONCURRENT_WRITERS` based on CPU cores
4. **Large Index Size**: Split data across namespaces for better caching
5. **Network Latency**: Use chunked streaming for large file transfers

## Scaling Guidelines

As your deployment grows, consider these scaling guidelines:

1. **Namespace Sharding**: Distribute namespaces across multiple instances
2. **Horizontal Scaling**: Run multiple Fugu instances with load balancing
3. **Resource Allocation**: Provide adequate CPU, memory, and disk resources
4. **Monitoring**: Track performance metrics to identify bottlenecks
5. **Regular Maintenance**: Schedule index optimizations during low-usage periods