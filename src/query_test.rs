#[cfg(test)]
mod query_tests {
    use crate::db::FuguDB;
    use crate::object::{ObjectIndex, ObjectRecord};
    use crate::query::*;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::tempdir;

    // Helper to create a test database with sample data
    fn create_test_db() -> FuguDB {
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

        // Create and insert test documents
        let documents = vec![
            ObjectRecord {
                id: "doc1".to_string(),
                text: "This is a test document about Rust programming".to_string(),
                metadata: json!({"category": "programming", "tags": ["rust", "test"]}),
            },
            ObjectRecord {
                id: "doc2".to_string(),
                text: "Another document talking about databases and search engines".to_string(),
                metadata: json!({"category": "databases", "tags": ["search", "indexing"]}),
            },
            ObjectRecord {
                id: "doc3".to_string(),
                text: "Rust is a systems programming language focused on safety".to_string(),
                metadata: json!({"category": "programming", "tags": ["rust", "systems"]}),
            },
        ];

        // Get RECORDS tree using our unified API
        let records_tree = fugudb
            .open_tree(crate::db::TREE_RECORDS)
            .expect("Failed to open RECORDS tree");

        // Insert documents into database
        for doc in &documents {
            // Convert to archivable form
            let archivable = crate::object::ArchivableObjectRecord::from(doc);

            // Serialize and insert
            let serialized = crate::rkyv_adapter::serialize(&archivable)
                .expect("Failed to serialize test document");
            records_tree
                .insert(doc.id.as_bytes(), serialized)
                .expect("Failed to insert test document");

            // Create and index document terms
            let mut inverted_index = HashMap::new();
            let terms: Vec<&str> = doc.text.split_whitespace().collect();

            for (pos, term) in terms.iter().enumerate() {
                let term_lower = term.to_lowercase();
                inverted_index
                    .entry(term_lower)
                    .or_insert_with(Vec::new)
                    .push(pos);
            }

            // Create object index
            let object_index = ObjectIndex {
                object_id: doc.id.clone(),
                field_name: "text".to_string(),
                inverted_index,
            };

            // Index object
            fugudb.index(object_index);
        }

        fugudb
    }

    #[test]
    fn test_query_engine_search() {
        // Create test database with sample data
        let db = create_test_db();

        // Create query engine with default config
        let config = QueryConfig::default();
        let engine = QueryEngine::new(Arc::new(db), config);

        // Test simple query
        let results = engine.search_text("rust", None).unwrap();

        // Should find 2 documents with "rust" term
        assert_eq!(results.total_hits, 2);
        assert_eq!(results.hits.len(), 2);

        // Check document IDs
        let doc_ids: Vec<String> = results.hits.iter().map(|hit| hit.id.clone()).collect();

        assert!(doc_ids.contains(&"doc1".to_string()));
        assert!(doc_ids.contains(&"doc3".to_string()));

        // Test query with multiple terms
        let mut results = engine.search_text("rust programming", None).unwrap();

        // Documents with both terms should score higher
        assert!(results.hits.len() > 0);
        results
            .hits
            .sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap());
        assert_eq!(results.hits[0].id, "doc1");

        // Test query with no matches
        let results = engine.search_text("nonexistent term", None).unwrap();
        assert_eq!(results.total_hits, 0);
        assert_eq!(results.hits.len(), 0);
    }

    #[test]
    fn test_json_query() {
        // Create test database with sample data
        let db = create_test_db();

        // Create query engine with default config
        let config = QueryConfig::default();
        let engine = QueryEngine::new(Arc::new(db), config);

        // Test JSON query
        let json_query = r#"
        {
            "query": "rust programming",
            "top_k": 2
        }
        "#;

        let query_json: serde_json::Value = serde_json::from_str(json_query).unwrap();
        let mut results = engine.search_json(query_json, None).unwrap();

        // Should respect top_k limit
        assert!(results.hits.len() <= 2);

        // Should find documents with both terms
        if !results.hits.is_empty() {
            results
                .hits
                .sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap());

            assert_eq!(results.hits[0].id, "doc1");
        }
    }

    #[test]
    fn test_query_highlights() {
        // Create test database with sample data
        let db = create_test_db();

        // Create query engine with highlighting enabled
        let mut config = QueryConfig::default();
        config.highlight_snippets = true;

        let engine = QueryEngine::new(Arc::new(db), config);

        // Test query with highlighting
        let results = engine.search_text("rust systems", None).unwrap();

        // Check that we have highlights for matching terms
        if !results.hits.is_empty() {
            let hit = &results.hits[0];
            assert!(hit.highlights.is_some());

            let highlights = hit.highlights.as_ref().unwrap();
            assert!(!highlights.is_empty());
            for highlight in highlights {
                println!("{}", highlight);
            }
            println!("resulting highlights");

            // Check that the highlights contain the highlighted terms
            for highlight in highlights {
                assert!(
                    highlight.to_lowercase().contains("**rust**")
                        || highlight.to_lowercase().contains("**systems**")
                );
            }
        }
    }
}
