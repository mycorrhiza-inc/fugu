use fugu::db::{FuguDB, TREE_RECORDS};
use rand::Rng;
use std::time::{Duration, Instant};
use tempfile::tempdir;

const NUM_OPERATIONS: usize = 10000;
const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 128;

fn generate_random_string(size: usize) -> String {
    // Simplified version without using distributions
    let mut rng = rand::thread_rng();
    (0..size)
        .map(|_| {
            // Generate ASCII alphanumeric characters (0-9, A-Z, a-z)
            let char_code = match rng.gen_range(0..62) {
                n @ 0..=9 => n + 48,     // 0-9
                n @ 10..=35 => n + 55,   // A-Z
                n @ 36..=61 => n + 61,   // a-z
                _ => unreachable!(),
            };
            char::from(char_code as u8)
        })
        .collect()
}

fn generate_random_data(count: usize) -> Vec<(String, String)> {
    let mut data = Vec::with_capacity(count);
    for _ in 0..count {
        let key = generate_random_string(KEY_SIZE);
        let value = generate_random_string(VALUE_SIZE);
        data.push((key, value));
    }
    data
}

struct BenchmarkResults {
    write_time: Duration,
    read_time: Duration,
    batch_write_time: Duration,
    scan_time: Duration,
    delete_time: Duration,
}

// Run single operations benchmark
fn benchmark_single_operations(fugu_db: &FuguDB, data: &[(String, String)]) -> BenchmarkResults {
    // Initialize database
    fugu_db.init_db();
    
    let tree_handle = fugu_db.open_tree(TREE_RECORDS).expect("Failed to open RECORDS tree");
    
    // Benchmark writes
    let write_start = Instant::now();
    for (key, value) in data.iter() {
        tree_handle.insert(key, value.as_bytes()).expect("Failed to insert");
    }
    let write_time = write_start.elapsed();
    println!("Single writes: {:?} ({} ops/sec)", write_time, 
             NUM_OPERATIONS as f64 / write_time.as_secs_f64());
    
    // Benchmark reads
    let read_start = Instant::now();
    for (key, _) in data.iter() {
        let _ = tree_handle.get(key).expect("Failed to get");
    }
    let read_time = read_start.elapsed();
    println!("Single reads: {:?} ({} ops/sec)", read_time, 
             NUM_OPERATIONS as f64 / read_time.as_secs_f64());
    
    // Benchmark batch writes
    // First remove all data
    for (key, _) in data.iter() {
        let _ = tree_handle.remove(key).expect("Failed to remove");
    }
    
    let batch_write_start = Instant::now();
    let mut batch = fugu_db.create_batch();
    for (key, value) in data.iter() {
        batch.insert(&tree_handle, key, value.as_bytes());
    }
    fugu_db.apply_batch(&tree_handle, batch).expect("Failed to apply batch");
    let batch_write_time = batch_write_start.elapsed();
    println!("Batch writes: {:?} ({} ops/sec)", batch_write_time, 
             NUM_OPERATIONS as f64 / batch_write_time.as_secs_f64());
    
    // Benchmark scan
    let scan_start = Instant::now();
    
    // Get the iterator
    let mut count = 0;
    if let Ok(iter) = tree_handle.iter() {
        for item in iter {
            if let Ok(_) = item {
                count += 1;
            }
        }
    }
    let scan_time = scan_start.elapsed();
    println!("Scan {} items: {:?}", count, scan_time);
    
    // Benchmark deletes
    let delete_start = Instant::now();
    for (key, _) in data.iter() {
        let _ = tree_handle.remove(key).expect("Failed to remove");
    }
    let delete_time = delete_start.elapsed();
    println!("Single deletes: {:?} ({} ops/sec)", delete_time, 
             NUM_OPERATIONS as f64 / delete_time.as_secs_f64());
    
    BenchmarkResults {
        write_time,
        read_time,
        batch_write_time,
        scan_time,
        delete_time,
    }
}

fn main() {
    println!("Running benchmarks to compare sled and fjall performance");
    println!("Generating random data for {} operations", NUM_OPERATIONS);
    let data = generate_random_data(NUM_OPERATIONS);

    // Run sled benchmark
    #[cfg(feature = "use-sled")]
    {
        println!("\n=== Sled Benchmark ===");
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();
        let db = sled::open(temp_path).expect("Failed to open test database");
        let fugu_db = FuguDB::new(db);
        let sled_results = benchmark_single_operations(&fugu_db, &data);
        
        println!("\nSled Benchmark Summary:");
        println!("Write: {:?} ({} ops/sec)", sled_results.write_time, 
                 NUM_OPERATIONS as f64 / sled_results.write_time.as_secs_f64());
        println!("Read: {:?} ({} ops/sec)", sled_results.read_time, 
                 NUM_OPERATIONS as f64 / sled_results.read_time.as_secs_f64());
        println!("Batch Write: {:?} ({} ops/sec)", sled_results.batch_write_time, 
                 NUM_OPERATIONS as f64 / sled_results.batch_write_time.as_secs_f64());
        println!("Scan: {:?}", sled_results.scan_time);
        println!("Delete: {:?} ({} ops/sec)", sled_results.delete_time, 
                 NUM_OPERATIONS as f64 / sled_results.delete_time.as_secs_f64());
    }

    // Run fjall benchmark
    #[cfg(feature = "use-fjall")]
    {
        println!("\n=== Fjall Benchmark ===");
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();
        
        // Create optimized configuration
        let keyspace = fjall::Config::new(temp_path)
            .cache_size(512 * 1024 * 1024)  // 512MB cache
            .max_write_buffer_size(64 * 1024 * 1024)  // 64MB write buffer
            .compaction_workers(4)  // 4 compaction worker threads
            .flush_workers(2)  // 2 flush worker threads
            .manual_journal_persist(false)  // Auto-persist
            .fsync_ms(Some(100))  // Fsync every 100ms
            .open()
            .expect("Failed to open fjall keyspace");
            
        let fugu_db = FuguDB::new(keyspace);
        let fjall_results = benchmark_single_operations(&fugu_db, &data);
        
        println!("\nFjall Benchmark Summary:");
        println!("Write: {:?} ({} ops/sec)", fjall_results.write_time, 
                 NUM_OPERATIONS as f64 / fjall_results.write_time.as_secs_f64());
        println!("Read: {:?} ({} ops/sec)", fjall_results.read_time, 
                 NUM_OPERATIONS as f64 / fjall_results.read_time.as_secs_f64());
        println!("Batch Write: {:?} ({} ops/sec)", fjall_results.batch_write_time, 
                 NUM_OPERATIONS as f64 / fjall_results.batch_write_time.as_secs_f64());
        println!("Scan: {:?}", fjall_results.scan_time);
        println!("Delete: {:?} ({} ops/sec)", fjall_results.delete_time, 
                 NUM_OPERATIONS as f64 / fjall_results.delete_time.as_secs_f64());
    }
}