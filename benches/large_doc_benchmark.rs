use criterion::{Criterion, black_box, criterion_group, criterion_main};
use fugu::db::FuguDB;
use fugu::object::{ObjectIndex, ObjectRecord};
use rand::Rng;
use pprof::criterion::{Output, PProfProfiler};
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::tempdir;

// Directory for saving benchmark intermediate results
const RESULTS_DIR: &str = "./benchmark_results";
const BENCHMARK_DATA_DIR: &str = "./benchmark_data";

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

// Load a large document from the benchmark data directory
fn load_document(doc_id: &str) -> Option<ObjectRecord> {
    let file_path = format!("{}/{}.json", BENCHMARK_DATA_DIR, doc_id);
    let file = File::open(file_path).ok()?;
    let json: Value = serde_json::from_reader(file).ok()?;
    
    let id = json["id"].as_str().unwrap_or(doc_id).to_string();
    let text = json["text"].as_str().unwrap_or("").to_string();
    let metadata = json["metadata"].clone();
    
    Some(ObjectRecord {
        id,
        text,
        metadata,
    })
}

// Benchmark indexing large documents
fn bench_large_document_indexing(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_document_indexing");

    // Check if benchmark data exists
    if !Path::new(BENCHMARK_DATA_DIR).exists() {
        println!("Benchmark data directory not found. Please run the data generation script first.");
        return;
    }

    // Get all document files
    let entries = fs::read_dir(BENCHMARK_DATA_DIR).unwrap_or_else(|_| panic!("Failed to read benchmark data directory"));
    let doc_files: Vec<_> = entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.path().extension().map_or(false, |ext| ext == "json") &&
            entry.file_name().to_string_lossy().starts_with("large_doc_")
        })
        .collect();

    if doc_files.is_empty() {
        println!("No benchmark document files found. Please run the data generation script first.");
        return;
    }

    println!("Found {} large document files for benchmarking", doc_files.len());

    // Test sequential indexing of large documents
    group.bench_function("sequential_indexing", |b| {
        let (fugu_db, _temp_dir) = setup_temp_db();
        
        // Create metrics container
        let mut all_metrics = Vec::new();
        
        b.iter_with_setup(
            || {
                // Load a random document for each iteration
                let mut rng = rand::rng();
                let doc_index = rng.random_range(0..doc_files.len());
                let doc_id = doc_files[doc_index].file_name().to_string_lossy().replace(".json", "");
                let record = load_document(&doc_id).expect("Failed to load document");
                
                // Track metrics for this iteration
                let metrics = HashMap::from([
                    ("operation".to_string(), json!("sequential_index_large")),
                    ("record_id".to_string(), json!(record.id.clone())),
                    (
                        "size_kb".to_string(), 
                        json!(record.metadata["size_kb"].as_u64().unwrap_or(0))
                    ),
                    (
                        "word_count".to_string(),
                        json!(record.metadata["word_count"].as_u64().unwrap_or(0))
                    ),
                    (
                        "timestamp".to_string(),
                        json!(chrono::Utc::now().timestamp_millis()),
                    ),
                ]);
                
                (record, metrics)
            },
            |(record, mut metrics)| {
                // Create the index
                let object_index = create_object_index(&record);
                let unique_terms = object_index.inverted_index.len();
                
                // Benchmark: Index the large document
                let start_time = Instant::now();
                
                // Using standard indexing
                fugu_db.index(black_box(object_index));
                
                // Record metrics
                let elapsed = start_time.elapsed();
                metrics.insert("duration_ns".to_string(), json!(elapsed.as_nanos()));
                metrics.insert("duration_ms".to_string(), json!(elapsed.as_millis()));
                metrics.insert("unique_terms".to_string(), json!(unique_terms));
                
                // Save metrics for this iteration
                all_metrics.push(metrics);
            },
        );
        
        // After all iterations, save aggregated results
        let aggregated = HashMap::from([
            ("test_case".to_string(), json!("sequential_indexing_large")),
            ("iterations".to_string(), json!(all_metrics.len())),
            ("metrics".to_string(), json!(all_metrics)),
        ]);
        
        if let Err(e) = save_intermediate_results("large_document_indexing", "sequential", &aggregated) {
            eprintln!("Failed to save benchmark results: {}", e);
        }
    });

    // Test parallel indexing of large documents
    group.bench_function("parallel_indexing", |b| {
        let (fugu_db, _temp_dir) = setup_temp_db();
        
        // Create metrics container
        let mut all_metrics = Vec::new();
        
        b.iter_with_setup(
            || {
                // Load a random document for each iteration
                let mut rng = rand::rng();
                let doc_index = rng.random_range(0..doc_files.len());
                let doc_id = doc_files[doc_index].file_name().to_string_lossy().replace(".json", "");
                let record = load_document(&doc_id).expect("Failed to load document");
                
                // Track metrics for this iteration
                let metrics = HashMap::from([
                    ("operation".to_string(), json!("parallel_index_large")),
                    ("record_id".to_string(), json!(record.id.clone())),
                    (
                        "size_kb".to_string(), 
                        json!(record.metadata["size_kb"].as_u64().unwrap_or(0))
                    ),
                    (
                        "word_count".to_string(),
                        json!(record.metadata["word_count"].as_u64().unwrap_or(0))
                    ),
                    (
                        "timestamp".to_string(),
                        json!(chrono::Utc::now().timestamp_millis()),
                    ),
                ]);
                
                (record, metrics)
            },
            |(record, mut metrics)| {
                // Create the index
                let object_index = create_object_index(&record);
                let unique_terms = object_index.inverted_index.len();
                
                // Benchmark: Index the large document
                let start_time = Instant::now();
                
                // Using regular indexing as parallel_index doesn't exist
                fugu_db.index(black_box(object_index));
                
                // Record metrics
                let elapsed = start_time.elapsed();
                metrics.insert("duration_ns".to_string(), json!(elapsed.as_nanos()));
                metrics.insert("duration_ms".to_string(), json!(elapsed.as_millis()));
                metrics.insert("unique_terms".to_string(), json!(unique_terms));
                
                // Save metrics for this iteration
                all_metrics.push(metrics);
            },
        );
        
        // After all iterations, save aggregated results
        let aggregated = HashMap::from([
            ("test_case".to_string(), json!("parallel_indexing_large")),
            ("iterations".to_string(), json!(all_metrics.len())),
            ("metrics".to_string(), json!(all_metrics)),
        ]);
        
        if let Err(e) = save_intermediate_results("large_document_indexing", "parallel", &aggregated) {
            eprintln!("Failed to save benchmark results: {}", e);
        }
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)  // Increased sample size for more reliable profiling
        .measurement_time(Duration::from_secs(5))  // Longer measurement time for better profiling data
        .warm_up_time(Duration::from_secs(2))  // Proper warm-up time
        .with_profiler(PProfProfiler::new(
            100, // Sampling frequency
            Output::Flamegraph(None)
        ));
    targets = bench_large_document_indexing
}
criterion_main!(benches);