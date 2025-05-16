#[cfg(test)]
mod document_statistics_tests {
    use crate::db::FuguDB;
    use crate::object::{ArchivableObjectRecord, ObjectRecord};
    use crate::query::{QueryConfig, QueryEngine};
    use crate::rkyv_adapter;
    use fjall;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::tempdir;

    // Helper to create a test database with specified number of documents
    fn create_test_db_with_docs(doc_count: usize) -> (FuguDB, tempfile::TempDir) {
        // Create a temporary directory for test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let db_path = temp_dir.path().to_str().unwrap();

        // Open database based on the enabled feature
        let fugudb = {
            let keyspace = fjall::Config::new(db_path)
                .cache_size(64 * 1024 * 1024) // 64MB cache for test
                .open()
                .expect("Failed to open test fjall keyspace");
            FuguDB::new(keyspace)
        };

        // Initialize database trees
        fugudb.init_db();

        // Create and insert test documents with controlled vocabulary
        let vocabulary = vec![
            "apple",
            "banana",
            "cherry",
            "date",
            "elderberry",
            "fig",
            "grape",
            "honeydew",
            "imbe",
            "jackfruit",
            "kiwi",
            "lemon",
            "mango",
            "nectarine",
            "orange",
            "papaya",
            "quince",
            "raspberry",
            "strawberry",
            "tangerine",
        ];

        let records_tree = fugudb.open_tree(crate::db::TREE_RECORDS).unwrap();

        for i in 0..doc_count {
            // Create document with known word counts
            // Each document contains a selection of words from the vocabulary
            // with controlled distribution for predictable statistics
            let word_count = 20 + (i % 10) * 5; // Documents have 20, 25, 30, ... 65 words
            let mut text = String::new();

            for j in 0..word_count {
                let word_idx = (i + j) % vocabulary.len();
                text.push_str(vocabulary[word_idx]);
                text.push(' ');
            }

            let record = ObjectRecord {
                id: format!("doc{}", i),
                text,
                metadata: json!({"index": i}),
            };

            let archivable = ArchivableObjectRecord::from(&record);
            let serialized = rkyv_adapter::serialize(&archivable).unwrap();
            records_tree
                .insert(record.id.as_bytes(), serialized)
                .unwrap();
        }

        (fugudb, temp_dir)
    }

    #[test]
    fn test_document_count() {
        // Test with different document counts
        for &doc_count in &[10, 50, 100] {
            // Create test database
            let (fugudb, _temp_dir) = create_test_db_with_docs(doc_count);

            // Create query engine
            let config = QueryConfig::default();
            let engine = QueryEngine::new(Arc::new(fugudb), config);

            // Get document statistics
            let (counted_docs, avg_length, _) = engine.collect_document_statistics().unwrap();

            // Verify document count
            assert_eq!(
                counted_docs, doc_count,
                "Document count should match for {} documents",
                doc_count
            );

            // Verify average document length is reasonable
            assert!(
                avg_length > 0.0,
                "Average document length should be positive"
            );
            println!(
                "Average document length for {} documents: {}",
                doc_count, avg_length
            );
        }
    }

    #[test]
    fn test_term_frequencies() {
        // Create test database with specific content
        let (fugudb, _temp_dir) = create_test_db_with_docs(10);

        // Create query engine
        let config = QueryConfig::default();
        let engine = QueryEngine::new(Arc::new(fugudb), config);

        // Get document statistics
        let (doc_count, _, term_freqs) = engine.collect_document_statistics().unwrap();

        // Verify we have the expected document count
        assert_eq!(doc_count, 10, "Should have 10 documents");

        // Verify we have term frequencies for our vocabulary words
        let vocabulary_words = ["apple", "banana", "cherry", "grape", "orange"];
        for word in &vocabulary_words {
            // Convert &str to String for HashMap lookup
            let word_string = word.to_string();
            assert!(
                term_freqs.contains_key(&word_string),
                "Term frequencies should include '{}'",
                word
            );

            let freq = term_freqs.get(&word_string).unwrap();
            assert!(*freq > 0, "Frequency for '{}' should be positive", word);

            // Log the frequencies for inspection
            println!("Frequency for '{}': {}", word, freq);
        }
    }

    #[test]
    fn test_empty_database() {
        // Create empty database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let db_path = temp_dir.path().to_str().unwrap();

        let fugudb = {
            let keyspace = fjall::Config::new(db_path)
                .cache_size(64 * 1024 * 1024)
                .open()
                .expect("Failed to open test fjall keyspace");
            FuguDB::new(keyspace)
        };

        // Initialize database trees
        fugudb.init_db();

        // Create query engine
        let config = QueryConfig::default();
        let engine = QueryEngine::new(Arc::new(fugudb), config);

        // Get document statistics
        let (doc_count, avg_length, term_freqs) = engine.collect_document_statistics().unwrap();

        // Verify empty stats
        assert_eq!(doc_count, 0, "Empty database should have 0 documents");
        assert_eq!(
            avg_length, 0.0,
            "Empty database should have 0.0 average length"
        );
        assert!(
            term_freqs.is_empty(),
            "Empty database should have empty term frequencies"
        );
    }

    #[test]
    fn test_statistics_with_edge_cases() {
        // Create a temporary directory for test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let db_path = temp_dir.path().to_str().unwrap();

        // Open database based on the enabled feature
        let fugudb = {
            let keyspace = fjall::Config::new(db_path)
                .cache_size(64 * 1024 * 1024)
                .open()
                .expect("Failed to open test fjall keyspace");
            FuguDB::new(keyspace)
        };

        // Initialize database trees
        fugudb.init_db();

        let records_tree = fugudb.open_tree(crate::db::TREE_RECORDS).unwrap();

        // Add some edge case documents
        let edge_cases = [
            // Empty document
            ObjectRecord {
                id: "empty".to_string(),
                text: "".to_string(),
                metadata: json!({}),
            },
            // Single word document
            ObjectRecord {
                id: "single".to_string(),
                text: "word".to_string(),
                metadata: json!({}),
            },
            // Document with multiple word repetitions
            ObjectRecord {
                id: "repetitive".to_string(),
                text: "repeat repeat repeat repeat repeat".to_string(),
                metadata: json!({}),
            },
            // Document with punctuation and mixed case
            ObjectRecord {
                id: "mixed".to_string(),
                text: "This is a Mixed-Case sentence, with punctuation!".to_string(),
                metadata: json!({}),
            },
            // Document with very long words
            ObjectRecord {
                id: "long".to_string(),
                text: "supercalifragilisticexpialidocious antidisestablishmentarianism".to_string(),
                metadata: json!({}),
            },
        ];

        for record in &edge_cases {
            let archivable = ArchivableObjectRecord::from(record);
            let serialized = rkyv_adapter::serialize(&archivable).unwrap();
            records_tree
                .insert(record.id.as_bytes(), serialized)
                .unwrap();
        }

        // Create query engine
        let config = QueryConfig::default();
        let engine = QueryEngine::new(Arc::new(fugudb), config);

        // Get document statistics
        let (doc_count, avg_length, term_freqs) = engine.collect_document_statistics().unwrap();

        // Verify document count
        assert_eq!(
            doc_count,
            edge_cases.len(),
            "Should have {} documents",
            edge_cases.len()
        );

        // Verify term frequencies for specific cases
        assert!(
            term_freqs.contains_key(&"repeat".to_string()),
            "Should have 'repeat' in term frequencies"
        );
        assert_eq!(
            *term_freqs.get(&"repeat".to_string()).unwrap(),
            1,
            "Should count 'repeat' in one document"
        );

        assert!(
            term_freqs.contains_key(&"word".to_string()),
            "Should have 'word' in term frequencies"
        );
        assert_eq!(
            *term_freqs.get(&"word".to_string()).unwrap(),
            1,
            "Should count 'word' in one document"
        );

        // Verify that empty document was handled
        println!("Average document length: {}", avg_length);
        println!("Term frequencies: {:?}", term_freqs);
    }
}

