use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use fugu::db::{FuguDB, TREE_RECORDS};
use fugu::object::{ArchivableObjectRecord, ObjectIndex, ObjectRecord};
use fugu::rkyv_adapter;
use pprof::criterion::{Output, PProfProfiler};
use rand::Rng;
use rand::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

// Import database backend based on feature flag
use fjall;
use fjall::KvSeparationOptions;

// Helper function to calculate percentiles
fn percentile(sorted_values: &[u128], percentile: f64) -> u128 {
    if sorted_values.is_empty() {
        return 0;
    }

    let index = (sorted_values.len() as f64 * percentile).ceil() as usize - 1;
    let bounded_index = index.min(sorted_values.len() - 1);
    sorted_values[bounded_index]
}

// Directory for saving benchmark intermediate results
const RESULTS_DIR: &str = "./benchmark_results";

// Generate test records of different sizes
fn create_test_record(id: &str, text_size: usize) -> ObjectRecord {
    let text = "Lorem ipsum dolor sit amet ".repeat(text_size / 30 + 1);
    let text = if text.len() > text_size {
        text[0..text_size].to_string()
    } else {
        text
    };

    ObjectRecord {
        id: id.to_string(),
        text,
        metadata: json!({
            "benchmark": true,
            "size": text_size,
            "timestamp": chrono::Utc::now().timestamp(),
            "properties": {
                "type": "benchmark",
                "category": "db_operations",
            }
        }),
    }
}

// Create an ObjectIndex from a record by tokenizing its text
fn create_object_index(record: &ObjectRecord) -> ObjectIndex {
    let mut inverted_index: HashMap<String, Vec<usize>> = HashMap::new();

    // Simple word tokenization and position tracking
    let words: Vec<&str> = record.text.split_whitespace().collect();
    for (pos, word) in words.iter().enumerate() {
        let normalized_word = word.to_lowercase();
        let positions = inverted_index
            .entry(normalized_word)
            .or_insert_with(Vec::new);
        positions.push(pos);
    }

    ObjectIndex {
        object_id: record.id.clone(),
        inverted_index,
    }
}

// Setup a temporary fjall database for benchmarking
fn setup_temp_db() -> (FuguDB, tempfile::TempDir) {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let db_path = temp_dir.path().to_str().unwrap();
    let keyspace = fjall::Config::new(db_path)
        .temporary(true) // Use temporary flag for benchmarks
        .open()
        .expect("Failed to open test fjall keyspace");
    let fugu_db = FuguDB::new(keyspace);

    // Initialize the database
    fugu_db.init_db();

    (fugu_db, temp_dir)
}

// Ensure results directory exists
fn ensure_results_dir() -> std::io::Result<()> {
    if !Path::new(RESULTS_DIR).exists() {
        fs::create_dir_all(RESULTS_DIR)?;
    }
    Ok(())
}

// Helper to save intermediate results
fn save_intermediate_results(
    benchmark_name: &str,
    test_case: &str,
    metrics: &HashMap<String, serde_json::Value>,
) -> std::io::Result<()> {
    ensure_results_dir()?;

    // Create benchmark subdirectory
    let benchmark_dir = format!("{}/{}", RESULTS_DIR, benchmark_name);
    if !Path::new(&benchmark_dir).exists() {
        fs::create_dir_all(&benchmark_dir)?;
    }

    // Create results file
    let timestamp = chrono::Utc::now().timestamp();
    let file_path = format!("{}/{}_{}.json", benchmark_dir, test_case, timestamp);

    let json = serde_json::to_string_pretty(metrics)?;
    let mut file = File::create(file_path)?;
    file.write_all(json.as_bytes())?;

    Ok(())
}

// Benchmark record insertion
fn bench_record_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_insertion_fjall");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (fugu_db, _temp_dir) = setup_temp_db();

            // Create metrics container
            let mut all_metrics = Vec::new();

            b.iter_with_setup(
                || {
                    // Setup: Create a new record for each iteration
                    let mut rng = rand::rng();
                    let id = format!(
                        "bench_insert_{}_{}_{}",
                        size,
                        chrono::Utc::now().timestamp_millis(),
                        rng.next_u32()
                    );

                    // Track metrics for this iteration
                    let metrics = HashMap::from([
                        ("operation".to_string(), json!("insert")),
                        ("record_size".to_string(), json!(size)),
                        ("record_id".to_string(), json!(id.clone())),
                        (
                            "timestamp".to_string(),
                            json!(chrono::Utc::now().timestamp_millis()),
                        ),
                    ]);

                    (create_test_record(&id, size), metrics)
                },
                |(record, mut metrics)| {
                    // Benchmark: Insert the record into the database
                    let start_time = Instant::now();

                    // Use the appropriate database implementation
                    let records_partition = fugu_db
                        .keyspace()
                        .open_partition(
                            TREE_RECORDS,
                            fjall::PartitionCreateOptions::default()
                                .with_kv_separation(KvSeparationOptions::default()),
                        )
                        .unwrap();
                    let archivable = ArchivableObjectRecord::from(&record);
                    let serialized = rkyv_adapter::serialize(&archivable).unwrap();
                    let _ = records_partition.insert(&record.id, serialized.to_vec());

                    // Record timing metrics
                    let elapsed = start_time.elapsed();
                    metrics.insert("duration_ns".to_string(), json!(elapsed.as_nanos()));
                    metrics.insert("duration_ms".to_string(), json!(elapsed.as_millis()));

                    metrics.insert("db_backend".to_string(), json!("fjall"));

                    // Save metrics for this iteration
                    all_metrics.push(metrics);
                },
            );

            // Calculate percentile statistics from the collected metrics
            let mut durations_ns: Vec<u128> = all_metrics
                .iter()
                .filter_map(|m| {
                    m.get("duration_ns")
                        .and_then(|d| d.as_u64().map(|d| d as u128))
                })
                .collect();

            // Sort durations for percentile calculations
            durations_ns.sort_unstable();

            // Calculate percentiles if we have data
            let (p90, p95, p99) = if !durations_ns.is_empty() {
                (
                    percentile(&durations_ns, 0.90),
                    percentile(&durations_ns, 0.95),
                    percentile(&durations_ns, 0.99),
                )
            } else {
                (0, 0, 0)
            };

            // After all iterations, save aggregated results with percentiles
            let test_case = format!("size_{}", size);
            let aggregated = HashMap::from([
                ("test_case".to_string(), json!(test_case)),
                ("record_size".to_string(), json!(size)),
                ("iterations".to_string(), json!(all_metrics.len())),
                ("p90_ns".to_string(), json!(p90)),
                ("p95_ns".to_string(), json!(p95)),
                ("p99_ns".to_string(), json!(p99)),
                ("p90_ms".to_string(), json!(p90 as f64 / 1_000_000.0)),
                ("p95_ms".to_string(), json!(p95 as f64 / 1_000_000.0)),
                ("p99_ms".to_string(), json!(p99 as f64 / 1_000_000.0)),
                ("metrics".to_string(), json!(all_metrics)),
            ]);

            if let Err(e) = save_intermediate_results("record_insertion", &test_case, &aggregated) {
                eprintln!("Failed to save intermediate results: {}", e);
            }
        });
    }

    group.finish();
}

// Benchmark record retrieval
fn bench_record_retrieval(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_retrieval_fjall");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Setup: Create a database and insert a record
            let (fugu_db, _temp_dir) = setup_temp_db();
            let id = format!("bench_retrieve_{}", size);
            let record = create_test_record(&id, size);

            let records_partition = fugu_db
                .keyspace()
                .open_partition(
                    TREE_RECORDS,
                    fjall::PartitionCreateOptions::default()
                        .with_kv_separation(KvSeparationOptions::default()),
                )
                .unwrap();
            let archivable = ArchivableObjectRecord::from(&record);
            let serialized = rkyv_adapter::serialize(&archivable).unwrap();
            records_partition
                .insert(&record.id, serialized.to_vec())
                .unwrap();

            // Create metrics container for this benchmark
            let mut all_metrics = Vec::new();

            // Benchmark: Retrieve the record
            b.iter(|| {
                let start_time = Instant::now();

                let result = fugu_db.get(black_box(&id));
                assert!(result.is_some());

                // Record timing metrics
                let elapsed = start_time.elapsed();
                let metrics = HashMap::from([
                    ("operation".to_string(), json!("retrieve")),
                    ("record_size".to_string(), json!(size)),
                    ("record_id".to_string(), json!(id.clone())),
                    (
                        "timestamp".to_string(),
                        json!(chrono::Utc::now().timestamp_millis()),
                    ),
                    ("duration_ns".to_string(), json!(elapsed.as_nanos())),
                    ("duration_ms".to_string(), json!(elapsed.as_millis())),
                ]);

                all_metrics.push(metrics);
            });

            // Calculate percentile statistics
            let mut durations_ns: Vec<u128> = all_metrics
                .iter()
                .filter_map(|m| {
                    m.get("duration_ns")
                        .and_then(|d| d.as_u64().map(|d| d as u128))
                })
                .collect();

            // Sort durations for percentile calculations
            durations_ns.sort_unstable();

            // Calculate percentiles if we have data
            let (p90, p95, p99) = if !durations_ns.is_empty() {
                (
                    percentile(&durations_ns, 0.90),
                    percentile(&durations_ns, 0.95),
                    percentile(&durations_ns, 0.99),
                )
            } else {
                (0, 0, 0)
            };

            // After all iterations, save aggregated results with percentiles
            let test_case = format!("size_{}", size);
            let aggregated = HashMap::from([
                ("test_case".to_string(), json!(test_case)),
                ("record_size".to_string(), json!(size)),
                ("iterations".to_string(), json!(all_metrics.len())),
                ("p90_ns".to_string(), json!(p90)),
                ("p95_ns".to_string(), json!(p95)),
                ("p99_ns".to_string(), json!(p99)),
                ("p90_ms".to_string(), json!(p90 as f64 / 1_000_000.0)),
                ("p95_ms".to_string(), json!(p95 as f64 / 1_000_000.0)),
                ("p99_ms".to_string(), json!(p99 as f64 / 1_000_000.0)),
                ("metrics".to_string(), json!(all_metrics)),
            ]);

            if let Err(e) = save_intermediate_results("record_retrieval", &test_case, &aggregated) {
                eprintln!("Failed to save intermediate results: {}", e);
            }
        });
    }

    group.finish();
}

// Benchmark indexing operation
fn bench_indexing(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing_fjall");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Setup: Create a database and a record
            let (fugu_db, _temp_dir) = setup_temp_db();
            let id = format!("bench_index_{}", size);
            let record = create_test_record(&id, size);
            let object_index = create_object_index(&record);

            // Create metrics container for this benchmark
            let mut all_metrics = Vec::new();

            // Benchmark: Index the object
            b.iter_with_setup(
                || create_object_index(&record), // Create a fresh ObjectIndex for each iteration
                |index_copy| {
                    let start_time = Instant::now();
                    fugu_db.index(black_box(index_copy));

                    // Record metrics
                    // Measure elapsed time
                    let elapsed = start_time.elapsed();

                    // Create metrics for this iteration
                    let metrics = HashMap::from([
                        ("operation".to_string(), json!("index")),
                        ("record_size".to_string(), json!(size)),
                        ("record_id".to_string(), json!(id.clone())),
                        (
                            "unique_terms".to_string(),
                            json!(create_object_index(&record).inverted_index.len()),
                        ),
                        (
                            "timestamp".to_string(),
                            json!(chrono::Utc::now().timestamp_millis()),
                        ),
                        ("duration_ns".to_string(), json!(elapsed.as_nanos())),
                        ("duration_ms".to_string(), json!(elapsed.as_millis())),
                    ]);

                    // Save metrics for this iteration
                    all_metrics.push(metrics);
                },
            );

            // Calculate percentile statistics
            let mut durations_ns: Vec<u128> = all_metrics
                .iter()
                .filter_map(|m| {
                    m.get("duration_ns")
                        .and_then(|d| d.as_u64().map(|d| d as u128))
                })
                .collect();

            // Sort durations for percentile calculations
            durations_ns.sort_unstable();

            // Calculate percentiles if we have data
            let (p90, p95, p99) = if !durations_ns.is_empty() {
                (
                    percentile(&durations_ns, 0.90),
                    percentile(&durations_ns, 0.95),
                    percentile(&durations_ns, 0.99),
                )
            } else {
                (0, 0, 0)
            };

            // After all iterations, save aggregated results with percentiles
            let test_case = format!("size_{}", size);
            let aggregated = HashMap::from([
                ("test_case".to_string(), json!(test_case)),
                ("record_size".to_string(), json!(size)),
                (
                    "unique_terms".to_string(),
                    json!(create_object_index(&record).inverted_index.len()),
                ),
                ("iterations".to_string(), json!(all_metrics.len())),
                ("p90_ns".to_string(), json!(p90)),
                ("p95_ns".to_string(), json!(p95)),
                ("p99_ns".to_string(), json!(p99)),
                ("p90_ms".to_string(), json!(p90 as f64 / 1_000_000.0)),
                ("p95_ms".to_string(), json!(p95 as f64 / 1_000_000.0)),
                ("p99_ms".to_string(), json!(p99 as f64 / 1_000_000.0)),
                ("metrics".to_string(), json!(all_metrics)),
            ]);

            if let Err(e) = save_intermediate_results("indexing", &test_case, &aggregated) {
                eprintln!("Failed to save intermediate results: {}", e);
            }
        });
    }

    group.finish();
}

// Benchmark parallel indexing operation
fn bench_parallel_indexing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_indexing_fjall");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Setup: Create a database and a record
            let (fugu_db, _temp_dir) = setup_temp_db();
            let id = format!("bench_index_{}", size);
            let record = create_test_record(&id, size);
            let object_index = create_object_index(&record);

            // Create metrics container for this benchmark
            let mut all_metrics = Vec::new();

            // Benchmark: Index the object using regular indexing
            b.iter_with_setup(
                || create_object_index(&record), // Create a fresh ObjectIndex for each iteration
                |index_copy| {
                    let start_time = Instant::now();
                    fugu_db.index(black_box(index_copy));

                    // Record metrics
                    // Measure elapsed time
                    let elapsed = start_time.elapsed();

                    // Create metrics for this iteration
                    let metrics = HashMap::from([
                        ("operation".to_string(), json!("parallel_index")),
                        ("record_size".to_string(), json!(size)),
                        ("record_id".to_string(), json!(id.clone())),
                        (
                            "unique_terms".to_string(),
                            json!(create_object_index(&record).inverted_index.len()),
                        ),
                        (
                            "timestamp".to_string(),
                            json!(chrono::Utc::now().timestamp_millis()),
                        ),
                        ("duration_ns".to_string(), json!(elapsed.as_nanos())),
                        ("duration_ms".to_string(), json!(elapsed.as_millis())),
                    ]);

                    // Save metrics for this iteration
                    all_metrics.push(metrics);
                },
            );

            // Calculate percentile statistics
            let mut durations_ns: Vec<u128> = all_metrics
                .iter()
                .filter_map(|m| {
                    m.get("duration_ns")
                        .and_then(|d| d.as_u64().map(|d| d as u128))
                })
                .collect();

            // Sort durations for percentile calculations
            durations_ns.sort_unstable();

            // Calculate percentiles if we have data
            let (p90, p95, p99) = if !durations_ns.is_empty() {
                (
                    percentile(&durations_ns, 0.90),
                    percentile(&durations_ns, 0.95),
                    percentile(&durations_ns, 0.99),
                )
            } else {
                (0, 0, 0)
            };

            // After all iterations, save aggregated results with percentiles
            let test_case = format!("size_{}", size);
            let aggregated = HashMap::from([
                ("test_case".to_string(), json!(test_case)),
                ("record_size".to_string(), json!(size)),
                (
                    "unique_terms".to_string(),
                    json!(create_object_index(&record).inverted_index.len()),
                ),
                ("iterations".to_string(), json!(all_metrics.len())),
                ("p90_ns".to_string(), json!(p90)),
                ("p95_ns".to_string(), json!(p95)),
                ("p99_ns".to_string(), json!(p99)),
                ("p90_ms".to_string(), json!(p90 as f64 / 1_000_000.0)),
                ("p95_ms".to_string(), json!(p95 as f64 / 1_000_000.0)),
                ("p99_ms".to_string(), json!(p99 as f64 / 1_000_000.0)),
                ("metrics".to_string(), json!(all_metrics)),
            ]);

            if let Err(e) = save_intermediate_results("parallel_indexing", &test_case, &aggregated)
            {
                eprintln!("Failed to save intermediate results: {}", e);
            }
        });
    }

    group.finish();
}

// Benchmark batch indexing
fn bench_batch_indexing(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_indexing_fjall");

    for batch_size in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                // Setup: Create a database and multiple records
                let (fugu_db, _temp_dir) = setup_temp_db();

                // Create metrics container for this benchmark
                let mut all_metrics = Vec::new();

                b.iter_with_setup(
                    || {
                        // Create a batch of objects to index
                        let mut objects = Vec::with_capacity(batch_size);
                        let mut object_details = Vec::with_capacity(batch_size);

                        for i in 0..batch_size {
                            let id = format!(
                                "bench_batch_{}_{}",
                                i,
                                chrono::Utc::now().timestamp_millis()
                            );
                            let record = create_test_record(&id, 1000); // Fixed size for batch test
                            let object_index = create_object_index(&record);

                            // Track object details
                            object_details.push(HashMap::from([
                                ("record_id".to_string(), json!(id.clone())),
                                (
                                    "terms".to_string(),
                                    json!(create_object_index(&record).inverted_index.len()),
                                ),
                            ]));

                            objects.push(object_index);
                        }

                        (objects, object_details)
                    },
                    |(objects, object_details)| {
                        // Benchmark: Batch index the objects
                        let start_time = Instant::now();

                        fugu_db.batch_index(black_box(objects));

                        // Record timing metrics
                        let elapsed = start_time.elapsed();
                        let metrics = HashMap::from([
                            ("operation".to_string(), json!("batch_index")),
                            ("batch_size".to_string(), json!(batch_size)),
                            ("object_details".to_string(), json!(object_details)),
                            (
                                "timestamp".to_string(),
                                json!(chrono::Utc::now().timestamp_millis()),
                            ),
                            ("duration_ns".to_string(), json!(elapsed.as_nanos())),
                            ("duration_ms".to_string(), json!(elapsed.as_millis())),
                            (
                                "avg_per_object_ns".to_string(),
                                json!(elapsed.as_nanos() as f64 / batch_size as f64),
                            ),
                        ]);

                        all_metrics.push(metrics);
                    },
                );

                // Calculate percentile statistics
                let mut durations_ns: Vec<u128> = all_metrics
                    .iter()
                    .filter_map(|m| {
                        m.get("duration_ns")
                            .and_then(|d| d.as_u64().map(|d| d as u128))
                    })
                    .collect();

                // Sort durations for percentile calculations
                durations_ns.sort_unstable();

                // Calculate percentiles if we have data
                let (p90, p95, p99) = if !durations_ns.is_empty() {
                    (
                        percentile(&durations_ns, 0.90),
                        percentile(&durations_ns, 0.95),
                        percentile(&durations_ns, 0.99),
                    )
                } else {
                    (0, 0, 0)
                };

                // After all iterations, save aggregated results with percentiles
                let test_case = format!("batch_size_{}", batch_size);
                let aggregated = HashMap::from([
                    ("test_case".to_string(), json!(test_case)),
                    ("batch_size".to_string(), json!(batch_size)),
                    ("iterations".to_string(), json!(all_metrics.len())),
                    ("p90_ns".to_string(), json!(p90)),
                    ("p95_ns".to_string(), json!(p95)),
                    ("p99_ns".to_string(), json!(p99)),
                    ("p90_ms".to_string(), json!(p90 as f64 / 1_000_000.0)),
                    ("p95_ms".to_string(), json!(p95 as f64 / 1_000_000.0)),
                    ("p99_ms".to_string(), json!(p99 as f64 / 1_000_000.0)),
                    ("metrics".to_string(), json!(all_metrics)),
                ]);

                if let Err(e) = save_intermediate_results("batch_indexing", &test_case, &aggregated)
                {
                    eprintln!("Failed to save intermediate results: {}", e);
                }
            },
        );
    }

    group.finish();
}

// Benchmark parallel batch indexing
fn bench_parallel_batch_indexing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_batch_indexing_fjall");

    for batch_size in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                // Setup: Create a database and multiple records
                let (fugu_db, _temp_dir) = setup_temp_db();

                // Create metrics container for this benchmark
                let mut all_metrics = Vec::new();

                b.iter_with_setup(
                    || {
                        // Create a batch of objects to index
                        let mut objects = Vec::with_capacity(batch_size);
                        let mut object_details = Vec::with_capacity(batch_size);

                        for i in 0..batch_size {
                            let id = format!(
                                "bench_parallel_batch_{}_{}",
                                i,
                                chrono::Utc::now().timestamp_millis()
                            );
                            let record = create_test_record(&id, 1000); // Fixed size for batch test
                            let object_index = create_object_index(&record);

                            // Track object details
                            object_details.push(HashMap::from([
                                ("record_id".to_string(), json!(id.clone())),
                                (
                                    "terms".to_string(),
                                    json!(create_object_index(&record).inverted_index.len()),
                                ),
                            ]));

                            objects.push(object_index);
                        }

                        (objects, object_details)
                    },
                    |(objects, object_details)| {
                        // Benchmark: Parallel batch index the objects
                        let start_time = Instant::now();

                        fugu_db.batch_index(black_box(objects));

                        // Record timing metrics
                        let elapsed = start_time.elapsed();
                        let metrics = HashMap::from([
                            ("operation".to_string(), json!("parallel_batch_index")),
                            ("batch_size".to_string(), json!(batch_size)),
                            ("object_details".to_string(), json!(object_details)),
                            (
                                "timestamp".to_string(),
                                json!(chrono::Utc::now().timestamp_millis()),
                            ),
                            ("duration_ns".to_string(), json!(elapsed.as_nanos())),
                            ("duration_ms".to_string(), json!(elapsed.as_millis())),
                            (
                                "avg_per_object_ns".to_string(),
                                json!(elapsed.as_nanos() as f64 / batch_size as f64),
                            ),
                        ]);

                        all_metrics.push(metrics);
                    },
                );

                // Calculate percentile statistics
                let mut durations_ns: Vec<u128> = all_metrics
                    .iter()
                    .filter_map(|m| {
                        m.get("duration_ns")
                            .and_then(|d| d.as_u64().map(|d| d as u128))
                    })
                    .collect();

                // Sort durations for percentile calculations
                durations_ns.sort_unstable();

                // Calculate percentiles if we have data
                let (p90, p95, p99) = if !durations_ns.is_empty() {
                    (
                        percentile(&durations_ns, 0.90),
                        percentile(&durations_ns, 0.95),
                        percentile(&durations_ns, 0.99),
                    )
                } else {
                    (0, 0, 0)
                };

                // After all iterations, save aggregated results with percentiles
                let test_case = format!("batch_size_{}", batch_size);
                let aggregated = HashMap::from([
                    ("test_case".to_string(), json!(test_case)),
                    ("batch_size".to_string(), json!(batch_size)),
                    ("iterations".to_string(), json!(all_metrics.len())),
                    ("p90_ns".to_string(), json!(p90)),
                    ("p95_ns".to_string(), json!(p95)),
                    ("p99_ns".to_string(), json!(p99)),
                    ("p90_ms".to_string(), json!(p90 as f64 / 1_000_000.0)),
                    ("p95_ms".to_string(), json!(p95 as f64 / 1_000_000.0)),
                    ("p99_ms".to_string(), json!(p99 as f64 / 1_000_000.0)),
                    ("metrics".to_string(), json!(all_metrics)),
                ]);

                if let Err(e) =
                    save_intermediate_results("parallel_batch_indexing", &test_case, &aggregated)
                {
                    eprintln!("Failed to save intermediate results: {}", e);
                }
            },
        );
    }

    group.finish();
}

// Benchmark compaction process
fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_fjall");

    for doc_count in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(doc_count),
            doc_count,
            |b, &doc_count| {
                // Create metrics container for this benchmark
                let mut all_metrics = Vec::new();

                b.iter_with_setup(
                    || {
                        // Setup: Create a database and add documents
                        let (mut fugu_db, _temp_dir) = setup_temp_db();
                        let mut doc_details = Vec::with_capacity(doc_count);

                        // Add documents and queue them for compaction
                        let setup_start = Instant::now();
                        for i in 0..doc_count {
                            let id = format!("bench_compact_{}", i);
                            let record = create_test_record(&id, 1000);
                            let object_index = create_object_index(&record);

                            // Add to DB and index using the appropriate implementation
                            let records_partition = fugu_db
                                .keyspace()
                                .open_partition(
                                    TREE_RECORDS,
                                    fjall::PartitionCreateOptions::default()
                                        .with_kv_separation(KvSeparationOptions::default()),
                                )
                                .unwrap();
                            let archivable = ArchivableObjectRecord::from(&record);
                            let serialized = rkyv_adapter::serialize(&archivable).unwrap();
                            records_partition
                                .insert(&record.id, serialized.to_vec())
                                .unwrap();

                            // Track object details
                            doc_details.push(HashMap::from([
                                ("record_id".to_string(), json!(id.clone())),
                                (
                                    "terms".to_string(),
                                    json!(create_object_index(&record).inverted_index.len()),
                                ),
                            ]));

                            // Index and queue for compaction
                            fugu_db.index(object_index);
                        }
                        let setup_duration = setup_start.elapsed();

                        // Include setup metrics with the database
                        (
                            fugu_db,
                            HashMap::from([
                                (
                                    "setup_duration_ms".to_string(),
                                    json!(setup_duration.as_millis()),
                                ),
                                ("doc_count".to_string(), json!(doc_count)),
                                ("doc_details".to_string(), json!(doc_details)),
                            ]),
                        )
                    },
                    |(mut db, setup_metrics)| {
                        // Benchmark: Run the compaction process
                        let start_time = Instant::now();

                        db.compact();

                        // Record metrics
                        let elapsed = start_time.elapsed();
                        let mut metrics = setup_metrics.clone();
                        metrics.insert("operation".to_string(), json!("compact"));
                        metrics.insert(
                            "timestamp".to_string(),
                            json!(chrono::Utc::now().timestamp_millis()),
                        );
                        metrics.insert("duration_ns".to_string(), json!(elapsed.as_nanos()));
                        metrics.insert("duration_ms".to_string(), json!(elapsed.as_millis()));
                        metrics.insert(
                            "avg_per_doc_ms".to_string(),
                            json!(elapsed.as_millis() as f64 / doc_count as f64),
                        );

                        all_metrics.push(metrics);
                    },
                );

                // Calculate percentile statistics
                let mut durations_ns: Vec<u128> = all_metrics
                    .iter()
                    .filter_map(|m| {
                        m.get("duration_ns")
                            .and_then(|d| d.as_u64().map(|d| d as u128))
                    })
                    .collect();

                // Sort durations for percentile calculations
                durations_ns.sort_unstable();

                // Calculate percentiles if we have data
                let (p90, p95, p99) = if !durations_ns.is_empty() {
                    (
                        percentile(&durations_ns, 0.90),
                        percentile(&durations_ns, 0.95),
                        percentile(&durations_ns, 0.99),
                    )
                } else {
                    (0, 0, 0)
                };

                // After all iterations, save aggregated results with percentiles
                let test_case = format!("doc_count_{}", doc_count);
                let aggregated = HashMap::from([
                    ("test_case".to_string(), json!(test_case)),
                    ("doc_count".to_string(), json!(doc_count)),
                    ("iterations".to_string(), json!(all_metrics.len())),
                    ("p90_ns".to_string(), json!(p90)),
                    ("p95_ns".to_string(), json!(p95)),
                    ("p99_ns".to_string(), json!(p99)),
                    ("p90_ms".to_string(), json!(p90 as f64 / 1_000_000.0)),
                    ("p95_ms".to_string(), json!(p95 as f64 / 1_000_000.0)),
                    ("p99_ms".to_string(), json!(p99 as f64 / 1_000_000.0)),
                    ("metrics".to_string(), json!(all_metrics)),
                ]);

                if let Err(e) = save_intermediate_results("compaction", &test_case, &aggregated) {
                    eprintln!("Failed to save intermediate results: {}", e);
                }
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(50)  // Increased sample size for more reliable percentile statistics
        .measurement_time(Duration::from_secs(5))  // Longer measurement time for better profiling data
        .warm_up_time(Duration::from_secs(2))  // Proper warm-up time
        .with_profiler(PProfProfiler::new(
            100, // Sampling frequency
            Output::Flamegraph(None)
        ))
        // Add percentile measurements for p90, p95 and p99
        .significance_level(0.01)
        .noise_threshold(0.02)
        .configure_from_args();
    targets = bench_record_insertion, bench_record_retrieval, bench_parallel_indexing, bench_batch_indexing, bench_parallel_batch_indexing, bench_compaction
}
criterion_main!(benches);
