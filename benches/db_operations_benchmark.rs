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
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

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

// Setup a temporary database for benchmarking
fn setup_temp_db() -> (FuguDB, tempfile::TempDir) {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let db_path = temp_dir.path().to_str().unwrap();
    let db = sled::open(db_path).expect("Failed to open test database");
    let fugu_db = FuguDB::new(db);

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
    let mut group = c.benchmark_group("record_insertion");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let (fugu_db, _temp_dir) = setup_temp_db();

            // Create metrics container
            let mut all_metrics = Vec::new();

            b.iter_with_setup(
                || {
                    // Setup: Create a new record for each iteration
                    let mut rng = rand::thread_rng();
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

                    let records_tree = fugu_db.db().open_tree(TREE_RECORDS).unwrap();
                    let archivable = ArchivableObjectRecord::from(&record);
                    let serialized = rkyv_adapter::serialize(&archivable).unwrap();
                    let _ = records_tree.insert(record.id.as_bytes(), serialized.to_vec());

                    // Record timing metrics
                    let elapsed = start_time.elapsed();
                    metrics.insert("duration_ns".to_string(), json!(elapsed.as_nanos()));
                    metrics.insert("duration_ms".to_string(), json!(elapsed.as_millis()));

                    // Save metrics for this iteration
                    all_metrics.push(metrics);
                },
            );

            // After all iterations, save aggregated results
            let test_case = format!("size_{}", size);
            let aggregated = HashMap::from([
                ("test_case".to_string(), json!(test_case)),
                ("record_size".to_string(), json!(size)),
                ("iterations".to_string(), json!(all_metrics.len())),
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
    let mut group = c.benchmark_group("record_retrieval");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Setup: Create a database and insert a record
            let (fugu_db, _temp_dir) = setup_temp_db();
            let id = format!("bench_retrieve_{}", size);
            let record = create_test_record(&id, size);

            // Insert the record
            let records_tree = fugu_db.db().open_tree(TREE_RECORDS).unwrap();
            let archivable = ArchivableObjectRecord::from(&record);
            let serialized = rkyv_adapter::serialize(&archivable).unwrap();
            records_tree
                .insert(record.id.as_bytes(), serialized.to_vec())
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

            // After all iterations, save aggregated results
            let test_case = format!("size_{}", size);
            let aggregated = HashMap::from([
                ("test_case".to_string(), json!(test_case)),
                ("record_size".to_string(), json!(size)),
                ("iterations".to_string(), json!(all_metrics.len())),
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
    let mut group = c.benchmark_group("indexing");

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
            b.iter(|| {
                let start_time = Instant::now();
                let obj_index = object_index.clone();
                fugu_db.index(black_box(obj_index));

                // Record metrics
                let elapsed = start_time.elapsed();
                let metrics = HashMap::from([
                    ("operation".to_string(), json!("index")),
                    ("record_size".to_string(), json!(size)),
                    ("record_id".to_string(), json!(id.clone())),
                    (
                        "unique_terms".to_string(),
                        json!(object_index.inverted_index.len()),
                    ),
                    (
                        "timestamp".to_string(),
                        json!(chrono::Utc::now().timestamp_millis()),
                    ),
                    ("duration_ns".to_string(), json!(elapsed.as_nanos())),
                    ("duration_ms".to_string(), json!(elapsed.as_millis())),
                ]);

                all_metrics.push(metrics);
            });

            // After all iterations, save aggregated results
            let test_case = format!("size_{}", size);
            let aggregated = HashMap::from([
                ("test_case".to_string(), json!(test_case)),
                ("record_size".to_string(), json!(size)),
                (
                    "unique_terms".to_string(),
                    json!(object_index.inverted_index.len()),
                ),
                ("iterations".to_string(), json!(all_metrics.len())),
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
    let mut group = c.benchmark_group("parallel_indexing");

    for size in [100, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            // Setup: Create a database and a record
            let (fugu_db, _temp_dir) = setup_temp_db();
            let id = format!("bench_index_{}", size);
            let record = create_test_record(&id, size);
            let object_index = create_object_index(&record);

            // Create metrics container for this benchmark
            let mut all_metrics = Vec::new();

            // Benchmark: Index the object using the new parallel indexing
            b.iter(|| {
                let start_time = Instant::now();
                let obj_index = object_index.clone();
                // Using the new parallel indexing method
                fugu_db.parallel_index(black_box(obj_index));

                // Record metrics
                let elapsed = start_time.elapsed();
                let metrics = HashMap::from([
                    ("operation".to_string(), json!("parallel_index")),
                    ("record_size".to_string(), json!(size)),
                    ("record_id".to_string(), json!(id.clone())),
                    (
                        "unique_terms".to_string(),
                        json!(object_index.inverted_index.len()),
                    ),
                    (
                        "timestamp".to_string(),
                        json!(chrono::Utc::now().timestamp_millis()),
                    ),
                    ("duration_ns".to_string(), json!(elapsed.as_nanos())),
                    ("duration_ms".to_string(), json!(elapsed.as_millis())),
                ]);

                all_metrics.push(metrics);
            });

            // After all iterations, save aggregated results
            let test_case = format!("size_{}", size);
            let aggregated = HashMap::from([
                ("test_case".to_string(), json!(test_case)),
                ("record_size".to_string(), json!(size)),
                (
                    "unique_terms".to_string(),
                    json!(object_index.inverted_index.len()),
                ),
                ("iterations".to_string(), json!(all_metrics.len())),
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
    let mut group = c.benchmark_group("batch_indexing");

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
                                    json!(object_index.inverted_index.len()),
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

                // After all iterations, save aggregated results
                let test_case = format!("batch_size_{}", batch_size);
                let aggregated = HashMap::from([
                    ("test_case".to_string(), json!(test_case)),
                    ("batch_size".to_string(), json!(batch_size)),
                    ("iterations".to_string(), json!(all_metrics.len())),
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
    let mut group = c.benchmark_group("parallel_batch_indexing");

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
                                    json!(object_index.inverted_index.len()),
                                ),
                            ]));

                            objects.push(object_index);
                        }

                        (objects, object_details)
                    },
                    |(objects, object_details)| {
                        // Benchmark: Parallel batch index the objects
                        let start_time = Instant::now();

                        fugu_db.parallel_batch_index(black_box(objects));

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

                // After all iterations, save aggregated results
                let test_case = format!("batch_size_{}", batch_size);
                let aggregated = HashMap::from([
                    ("test_case".to_string(), json!(test_case)),
                    ("batch_size".to_string(), json!(batch_size)),
                    ("iterations".to_string(), json!(all_metrics.len())),
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
    let mut group = c.benchmark_group("compaction");

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

                            // Add to DB and index
                            let records_tree = fugu_db.db().open_tree(TREE_RECORDS).unwrap();
                            let archivable = ArchivableObjectRecord::from(&record);
                            let serialized = rkyv_adapter::serialize(&archivable).unwrap();
                            records_tree
                                .insert(record.id.as_bytes(), serialized.to_vec())
                                .unwrap();

                            // Track object details
                            doc_details.push(HashMap::from([
                                ("record_id".to_string(), json!(id.clone())),
                                (
                                    "terms".to_string(),
                                    json!(object_index.inverted_index.len()),
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

                // After all iterations, save aggregated results
                let test_case = format!("doc_count_{}", doc_count);
                let aggregated = HashMap::from([
                    ("test_case".to_string(), json!(test_case)),
                    ("doc_count".to_string(), json!(doc_count)),
                    ("iterations".to_string(), json!(all_metrics.len())),
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
        .sample_size(10)  // Reduced sample size
        .measurement_time(Duration::from_millis(500))  // Reduced measurement time (default is 5s)
        .warm_up_time(Duration::from_millis(300))  // Reduced warm-up time (default is 3s)
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_record_insertion, bench_record_retrieval,  bench_parallel_indexing, bench_batch_indexing, bench_parallel_batch_indexing, bench_compaction
}
criterion_main!(benches);
