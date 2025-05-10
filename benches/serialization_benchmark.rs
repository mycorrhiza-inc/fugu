use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use fugu::object::{ObjectRecord, ArchivableObjectRecord};
use fugu::rkyv_adapter;
use fugu::db::{serialize_positions, deserialize_positions};
use serde_json::json;
use pprof::criterion::{Output, PProfProfiler};

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
            
            b.iter(|| {
                let result = rkyv_adapter::serialize(black_box(&archivable));
                assert!(result.is_ok());
                result.unwrap()
            });
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
                let result = rkyv_adapter::deserialize::<ArchivableObjectRecord>(black_box(&serialized));
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
                let deserialized = rkyv_adapter::deserialize::<ArchivableObjectRecord>(black_box(&serialized)).unwrap();
                
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
                    let _deserialized = rkyv_adapter::deserialize::<ArchivableObjectRecord>(&serialized).unwrap();
                });
            }
        );
        
        // Using bincode directly
        group.bench_with_input(
            BenchmarkId::new("bincode_direct", size), 
            &archivable, 
            |b, record| {
                b.iter(|| {
                    let serialized = bincode::serde::encode_to_vec(black_box(record), bincode::config::standard()).unwrap();
                    let (_deserialized, _): (ArchivableObjectRecord, usize) = 
                        bincode::serde::decode_from_slice(&serialized, bincode::config::standard()).unwrap();
                });
            }
        );
    }
    
    group.finish();
}

// Benchmark position vector serialization (new optimized functions)
fn bench_positions(c: &mut Criterion) {
    let mut group = c.benchmark_group("position_vectors");

    // Test small position vectors (should benefit from caching)
    for size in [1, 2, 5, 10, 50].iter() {
        let positions: Vec<usize> = (0..*size).collect();
        let serialized = serialize_positions(&positions);

        // Test serialization with cache
        group.bench_with_input(
            BenchmarkId::new("serialize", size),
            &positions,
            |b, pos| {
                b.iter(|| serialize_positions(black_box(pos)));
            }
        );

        // Test deserialization with cache
        group.bench_with_input(
            BenchmarkId::new("deserialize", size),
            &serialized,
            |b, ser| {
                b.iter(|| deserialize_positions(black_box(ser)));
            }
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_serialize, bench_deserialize, bench_ser_deser_cycle, bench_adapter_vs_bincode, bench_positions
}
criterion_main!(benches);