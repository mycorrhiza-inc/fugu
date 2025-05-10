use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use fugu::db::FuguDB;
use fugu::object::{ObjectRecord, ArchivableObjectRecord, ObjectIndex};
use fugu::query::{QueryEngine, QueryConfig};
use fugu::rkyv_adapter;
use pprof::criterion::{Output, PProfProfiler};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use rand::prelude::*;

// Generate a test record with controlled text content
fn create_test_record(id: &str, content: &str) -> ObjectRecord {
    ObjectRecord {
        id: id.to_string(),
        text: content.to_string(),
        metadata: json!({
            "benchmark": true,
            "size": content.len(),
            "timestamp": chrono::Utc::now().timestamp(),
            "properties": {
                "type": "benchmark",
                "category": "query_performance",
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

// Setup a test database with corpus data
fn setup_test_db_with_corpus(doc_count: usize, content_type: &str) -> (FuguDB, tempfile::TempDir) {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let db_path = temp_dir.path().to_str().unwrap();
    let db = sled::open(db_path).expect("Failed to open test database");
    let fugu_db = FuguDB::new(db);
    
    // Initialize the database
    fugu_db.init_db();
    
    // Content samples based on the requested type
    let samples = match content_type {
        "technical" => vec![
            "Rust is a multi-paradigm programming language focused on performance and safety, especially safe concurrency.",
            "The sled database is an embedded database providing a concurrent B+ tree implementation.",
            "Serialization is the process of converting data structures into a format that can be stored or transmitted.",
            "Database indexing is a technique to optimize the performance of data retrieval operations.",
            "Benchmarking is the act of measuring and comparing the performance of software or hardware systems.",
            "Memory safety is achieved in Rust through its ownership system, preventing null pointer dereferencing.",
            "The query engine processes search requests by translating them into database operations.",
            "Compaction is the process of merging multiple data structures to improve read performance.",
            "Tokio is an asynchronous runtime for Rust that enables scalable network applications.",
            "Profiling is the process of measuring where a program spends its time and resources.",
        ],
        "literary" => vec![
            "It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness.",
            "Call me Ishmael. Some years ago—never mind how long precisely—having little or no money in my purse.",
            "All happy families are alike; each unhappy family is unhappy in its own way.",
            "It was a bright cold day in April, and the clocks were striking thirteen.",
            "In my younger and more vulnerable years my father gave me some advice that I've been turning over in my mind ever since.",
            "The sky above the port was the color of television, tuned to a dead channel.",
            "Many years later, as he faced the firing squad, Colonel Aureliano Buendía was to remember that distant afternoon.",
            "It was a pleasure to burn. It was a special pleasure to see things eaten, to see things blackened and changed.",
            "Far out in the uncharted backwaters of the unfashionable end of the western spiral arm of the Galaxy lies a small unregarded yellow sun.",
            "When Gregor Samsa woke up one morning from unsettling dreams, he found himself changed in his bed into a monstrous vermin.",
        ],
        "random" => vec![
            "The quick brown fox jumps over the lazy dog",
            "Pack my box with five dozen liquor jugs",
            "How vexingly quick daft zebras jump",
            "Sphinx of black quartz, judge my vow",
            "Jackdaws love my big sphinx of quartz",
            "Five quacking zephyrs jolt my wax bed",
            "The five boxing wizards jump quickly",
            "Crazy Fredrick bought many very exquisite opal jewels",
            "Quick zephyrs blow, vexing daft Jim",
            "Amazingly few discotheques provide jukeboxes",
        ],
        _ => vec![
            "Default sample text for testing query performance",
            "Another default text sample with some different words",
            "This is a third sample with more unique terms",
            "Fourth sample contains still more words for variety",
            "Fifth text sample ensures adequate corpus diversity",
        ],
    };
    
    // Generate and insert records
    for i in 0..doc_count {
        // Select a sample (with rotation if needed)
        let sample_idx = i % samples.len();
        let base_content = samples[sample_idx];
        
        // Create record with repeating content to reach desired size
        let repeat_factor = (1000 / base_content.len()).max(1);
        let content = base_content.repeat(repeat_factor);
        
        let id = format!("bench_query_{}_{}", content_type, i);
        let record = create_test_record(&id, &content);
        
        // Add to the database 
        let records_tree = fugu_db.db().open_tree(fugu::db::TREE_RECORDS).unwrap();
        let archivable = ArchivableObjectRecord::from(&record);
        let serialized = rkyv_adapter::serialize(&archivable).unwrap();
        records_tree.insert(record.id.as_bytes(), serialized.to_vec()).unwrap();
        
        // Create index
        let object_index = create_object_index(&record);
        fugu_db.index(object_index);
    }
    
    // Compact the database
    let mut fugu_db_mut = fugu_db.clone();
    fugu_db_mut.compact();

    (fugu_db, temp_dir)
}

// Create a QueryEngine for testing
fn create_query_engine(fugu_db: FuguDB, config: Option<QueryConfig>) -> QueryEngine {
    let db_arc = Arc::new(fugu_db);
    let config = config.unwrap_or_default();
    QueryEngine::new(db_arc, config)
}

// Benchmark simple text search queries
fn bench_text_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("text_search");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    
    // Test different corpus sizes
    for doc_count in [10, 50, 100].iter() {
        // Test with different content types
        for content_type in ["technical", "literary", "random"].iter() {
            group.bench_with_input(
                BenchmarkId::new(content_type.to_string(), doc_count), 
                &(*content_type, *doc_count), 
                |b, &(content_type, doc_count)| {
                    // Setup the test database
                    let (fugu_db, _temp_dir) = setup_test_db_with_corpus(doc_count, content_type);
                    let engine = create_query_engine(fugu_db, None);
                    
                    // Select appropriate query terms based on content type
                    let query_terms = match content_type {
                        "technical" => vec!["database", "performance", "serialization", "rust programming", "memory"],
                        "literary" => vec!["time", "years", "remember", "pleasure", "morning"],
                        "random" => vec!["quick", "fox jumps", "sphinx", "wizards", "opal"],
                        _ => vec!["sample", "text", "words"],
                    };
                    
                    // Run benchmarks with random term selection
                    b.iter(|| {
                        let mut rng = rand::rng();
                        let term_idx = rng.random_range(0..query_terms.len());
                        let term = query_terms[term_idx];
                        let result = engine.search_text(black_box(term), Some(10));
                        assert!(result.is_ok());
                    });
                }
            );
        }
    }
    
    group.finish();
}

// Benchmark phrase search queries
fn bench_phrase_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("phrase_search");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    
    // Create a larger corpus for phrase searching
    let doc_count = 50;
    let (fugu_db, _temp_dir) = setup_test_db_with_corpus(doc_count, "technical");
    let engine = create_query_engine(fugu_db, None);
    
    // Test different phrase lengths
    let phrases = [
        "database",  // Single term
        "database performance",  // Two terms
        "database performance optimization",  // Three terms
        "\"database performance optimization techniques\"",  // Quoted phrase
    ];
    
    for (i, phrase) in phrases.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::from_parameter(i+1), 
            phrase, 
            |b, phrase| {
                b.iter(|| {
                    let result = engine.search_text(black_box(phrase), Some(10));
                    assert!(result.is_ok());
                });
            }
        );
    }
    
    group.finish();
}

// Benchmark JSON query processing
fn bench_json_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_query");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    
    // Create a test database
    let doc_count = 50;
    let (fugu_db, _temp_dir) = setup_test_db_with_corpus(doc_count, "technical");
    let engine = create_query_engine(fugu_db, None);
    
    // Test different JSON query complexities
    let queries = [
        r#"{"query": "database", "top_k": 5}"#,
        r#"{"query": "database performance", "top_k": 10}"#,
        r#"{"query": "database OR performance", "top_k": 10, "offset": 5}"#,
        r#"{
            "query": "database AND performance",
            "top_k": 10,
            "filters": [
                {
                    "type": "term",
                    "field": "benchmark",
                    "value": "true"
                }
            ]
        }"#,
    ];
    
    for (i, query) in queries.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::from_parameter(i+1), 
            query, 
            |b, query| {
                b.iter(|| {
                    let result = engine.search_json(black_box(query));
                    assert!(result.is_ok());
                });
            }
        );
    }
    
    group.finish();
}

// Benchmark the query engine's internal operations
fn bench_query_engine_internals(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_engine_internals");
    
    // Test with different BM25 parameter configurations
    let configs = [
        QueryConfig {
            bm25_k1: 1.2,
            bm25_b: 0.75,
            ..Default::default()
        },
        QueryConfig {
            bm25_k1: 1.5,
            bm25_b: 0.8,
            ..Default::default()
        },
        QueryConfig {
            bm25_k1: 1.0,
            bm25_b: 0.5,
            ..Default::default()
        },
    ];

    for (i, config) in configs.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::from_parameter(i+1),
            config,
            |b, config| {
                // Create a fresh database for each test
                let (fugu_db, _temp_dir) = setup_test_db_with_corpus(100, "technical");
                let engine = create_query_engine(fugu_db, Some(config.clone()));
                
                b.iter(|| {
                    let result = engine.search_text(black_box("database performance"), Some(10));
                    assert!(result.is_ok());
                });
            }
        );
    }
    
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .sample_size(10);  // Reduced sample size since these are more expensive tests
    targets = bench_text_search, bench_phrase_search, bench_json_query, bench_query_engine_internals
}
criterion_main!(benches);