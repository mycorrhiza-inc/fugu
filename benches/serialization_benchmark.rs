use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use fugu::db::{deserialize_positions, serialize_positions};
use fugu::object::{ArchivableObjectRecord, ObjectRecord};
use fugu::rkyv_adapter;
use pprof::criterion::{Output, PProfProfiler};
use serde_json::json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

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

// Ensure results directory exists
fn ensure_results_dir() -> std::io::Result<()> {
    if !std::path::Path::new(RESULTS_DIR).exists() {
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
    if !std::path::Path::new(&benchmark_dir).exists() {
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
                "category": "serialization",
            }
        }),
    }
}

// Benchmark serialization
fn bench_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize");

    for size in [100, 1000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let record = create_test_record(&format!("bench_{}", size), size);
            let archivable = ArchivableObjectRecord::from(&record);

            // Create metrics container for this benchmark
            let mut all_metrics = Vec::new();

            b.iter_with_setup(
                || {}, // No setup needed
                |()| {
                    let start_time = Instant::now();

                    let result = rkyv_adapter::serialize(black_box(&archivable));
                    assert!(result.is_ok());
                    let serialized = result.unwrap();

                    // Record timing metrics
                    let elapsed = start_time.elapsed();
                    let metrics = HashMap::from([
                        ("operation".to_string(), json!("serialize")),
                        ("record_size".to_string(), json!(size)),
                        ("serialized_size".to_string(), json!(serialized.len())),
                        (
                            "timestamp".to_string(),
                            json!(chrono::Utc::now().timestamp_millis()),
                        ),
                        ("duration_ns".to_string(), json!(elapsed.as_nanos())),
                        ("duration_ms".to_string(), json!(elapsed.as_millis())),
                    ]);

                    all_metrics.push(metrics);

                    serialized
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
                ("iterations".to_string(), json!(all_metrics.len())),
                ("p90_ns".to_string(), json!(p90)),
                ("p95_ns".to_string(), json!(p95)),
                ("p99_ns".to_string(), json!(p99)),
                ("p90_ms".to_string(), json!(p90 as f64 / 1_000_000.0)),
                ("p95_ms".to_string(), json!(p95 as f64 / 1_000_000.0)),
                ("p99_ms".to_string(), json!(p99 as f64 / 1_000_000.0)),
                ("metrics".to_string(), json!(all_metrics)),
            ]);

            if let Err(e) = save_intermediate_results("serialization", &test_case, &aggregated) {
                eprintln!("Failed to save intermediate results: {}", e);
            }
        });
    }

    group.finish();
}

// Benchmark deserialization
fn bench_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize");

    for size in [100, 1000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let record = create_test_record(&format!("bench_{}", size), size);
            let archivable = ArchivableObjectRecord::from(&record);
            let serialized = rkyv_adapter::serialize(&archivable).unwrap();

            b.iter(|| {
                let result =
                    rkyv_adapter::deserialize::<ArchivableObjectRecord>(black_box(&serialized));
                assert!(result.is_ok());
                result.unwrap()
            });
        });
    }

    group.finish();
}

// Benchmark full serialization + deserialization cycle
fn bench_ser_deser_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("ser_deser_cycle");

    for size in [100, 1000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let record = create_test_record(&format!("bench_{}", size), size);
            let archivable = ArchivableObjectRecord::from(&record);

            b.iter(|| {
                // Serialize
                let serialized = rkyv_adapter::serialize(black_box(&archivable)).unwrap();

                // Deserialize
                let deserialized =
                    rkyv_adapter::deserialize::<ArchivableObjectRecord>(black_box(&serialized))
                        .unwrap();

                // Convert back to ObjectRecord
                let _final_record = ObjectRecord::from(deserialized);
            });
        });
    }

    group.finish();
}

// Benchmark comparing rkyv_adapter vs direct bincode usage
fn bench_adapter_vs_bincode(c: &mut Criterion) {
    let mut group = c.benchmark_group("adapter_vs_bincode");

    for size in [1000, 10_000, 100_000].iter() {
        let record = create_test_record(&format!("bench_{}", size), *size);
        let archivable = ArchivableObjectRecord::from(&record);

        // Using rkyv_adapter
        group.bench_with_input(
            BenchmarkId::new("rkyv_adapter", size),
            &archivable,
            |b, record| {
                b.iter(|| {
                    let serialized = rkyv_adapter::serialize(black_box(record)).unwrap();
                    let _deserialized =
                        rkyv_adapter::deserialize::<ArchivableObjectRecord>(&serialized).unwrap();
                });
            },
        );

        // Using bincode directly
        group.bench_with_input(
            BenchmarkId::new("bincode_direct", size),
            &archivable,
            |b, record| {
                b.iter(|| {
                    let serialized = bincode::serde::encode_to_vec(
                        black_box(record),
                        bincode::config::standard(),
                    )
                    .unwrap();
                    let (_deserialized, _): (ArchivableObjectRecord, usize) =
                        bincode::serde::decode_from_slice(&serialized, bincode::config::standard())
                            .unwrap();
                });
            },
        );
    }

    group.finish();
}

// Benchmark position vector serialization (new optimized functions)
fn bench_positions(c: &mut Criterion) {
    let mut group = c.benchmark_group("position_vectors");

    // Test small position vectors (should benefit from caching)
    for size in [1, 2, 5, 10, 50].iter() {
        let positions: Vec<u64> = (0..*size).collect();
        let serialized = serialize_positions(&positions).unwrap();

        // Test serialization with cache
        group.bench_with_input(BenchmarkId::new("serialize", size), &positions, |b, pos| {
            b.iter(|| serialize_positions(pos));
        });

        // Test deserialization with cache
        group.bench_with_input(
            BenchmarkId::new("deserialize", size),
            &serialized,
            |b, ser| {
                b.iter(|| deserialize_positions(ser));
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
    targets = bench_serialize, bench_deserialize, bench_ser_deser_cycle, bench_adapter_vs_bincode, bench_positions
}
criterion_main!(benches);

