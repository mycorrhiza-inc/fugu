#[cfg(test)]
mod tests {
    use crate::db::{FuguDB, deserialize_positions, serialize_positions};
    use crate::object::ObjectIndex;
    use std::collections::HashMap;
    use tempfile::tempdir;

    // Helper function to create a test database
    fn create_test_db() -> (FuguDB, tempfile::TempDir) {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();

        // Initialize the appropriate database backend based on feature flags
        let keyspace = fjall::Config::new(temp_path)
            .cache_size(64 * 1024 * 1024) // 64MB cache for test
            .open()
            .expect("Failed to open test keyspace");

        // Create FuguDB instance
        let fugu_db = FuguDB::new(keyspace);

        // Initialize database
        fugu_db.init_db();

        (fugu_db, temp_dir)
    }

    #[test]
    fn test_inverted_index_creation() {
        let (db, _temp_dir) = create_test_db();

        // Create a simple object index with a few terms
        let mut inverted_index = HashMap::new();
        inverted_index.insert("test".to_string(), vec![1, 5, 10]);
        inverted_index.insert("example".to_string(), vec![2, 6, 11]);

        let object_index = ObjectIndex {
            object_id: "test_object_1".to_string(),
            field_name: "text".to_string(),
            inverted_index,
        };

        // Index the object
        db.index(object_index);

        // Verify the index was created correctly
        let index_tree_name = format!("{}{}", crate::db::PREFIX_RECORD_INDEX_TREE, "test_object_1");
        let index_tree = db
            .open_tree(&index_tree_name)
            .expect("Failed to open index tree");

        // Check if terms were indexed
        let test_positions = index_tree.get("test").expect("Failed to get 'test' term");
        let example_positions = index_tree
            .get("example")
            .expect("Failed to get 'example' term");

        assert!(
            test_positions.is_some(),
            "Expected positions for 'test', got None"
        );
        assert!(
            example_positions.is_some(),
            "Expected positions for 'example', got None"
        );

        // Verify positions are correct
        let test_pos = deserialize_positions(&test_positions.unwrap())
            .expect("Failed to deserialize positions");
        let example_pos = deserialize_positions(&example_positions.unwrap())
            .expect("Failed to deserialize positions");

        assert_eq!(test_pos, vec![1, 5, 10], "Incorrect positions for 'test'");
        assert_eq!(
            example_pos,
            vec![2, 6, 11],
            "Incorrect positions for 'example'"
        );
    }

    #[test]
    fn test_read_modify_write_pattern() {
        let (db, _temp_dir) = create_test_db();

        // Create first object index
        let mut inverted_index1 = HashMap::new();
        inverted_index1.insert("common".to_string(), vec![1, 2, 3]);

        let object_index1 = ObjectIndex {
            object_id: "test_object_2".to_string(),
            field_name: "text".to_string(),
            inverted_index: inverted_index1,
        };

        // Create second object index with the same term
        let mut inverted_index2 = HashMap::new();
        inverted_index2.insert("common".to_string(), vec![4, 5, 6]);

        let object_index2 = ObjectIndex {
            object_id: "test_object_2".to_string(),
            field_name: "text".to_string(),
            inverted_index: inverted_index2,
        };

        // Index the first object
        db.index(object_index1);

        // Index the second object
        db.index(object_index2);

        // Verify the merged positions
        let index_tree_name = format!("{}{}", crate::db::PREFIX_RECORD_INDEX_TREE, "test_object_2");
        let index_tree = db
            .open_tree(&index_tree_name)
            .expect("Failed to open index tree");

        let common_positions = index_tree
            .get("common")
            .expect("Failed to get 'common' term");

        assert!(
            common_positions.is_some(),
            "Expected positions for 'common', got None"
        );

        // Verify positions contain all values from both operations
        let positions = deserialize_positions(&common_positions.unwrap())
            .expect("Failed to deserialize positions");
        let expected_positions: Vec<u64> = vec![1, 2, 3, 4, 5, 6];

        // Check that all expected positions are present
        // (order might vary between implementations so we just check membership)
        assert_eq!(
            positions.len(),
            expected_positions.len(),
            "Wrong number of positions"
        );
        for &pos in &expected_positions {
            assert!(positions.contains(&pos), "Missing position {}", pos);
        }
    }

    #[test]
    fn test_batch_indexing() {
        let (db, _temp_dir) = create_test_db();

        // Create multiple object indices
        let mut inverted_index1 = HashMap::new();
        inverted_index1.insert("batch".to_string(), vec![1, 2]);

        let object_index1 = ObjectIndex {
            object_id: "batch_object_1".to_string(),
            field_name: "text".to_string(),
            inverted_index: inverted_index1,
        };

        let mut inverted_index2 = HashMap::new();
        inverted_index2.insert("batch".to_string(), vec![3, 4]);

        let object_index2 = ObjectIndex {
            object_id: "batch_object_2".to_string(),
            field_name: "text".to_string(),
            inverted_index: inverted_index2,
        };

        // Batch index them
        db.batch_index(vec![object_index1, object_index2]);
        println!("passed first");

        // Verify both indices were created
        let index_tree_name1 = format!(
            "{}{}",
            crate::db::PREFIX_RECORD_INDEX_TREE,
            "batch_object_1"
        );
        let index_tree1 = db
            .open_tree(&index_tree_name1)
            .expect("Failed to open index tree 1");

        let index_tree_name2 = format!(
            "{}{}",
            crate::db::PREFIX_RECORD_INDEX_TREE,
            "batch_object_2"
        );
        let index_tree2 = db
            .open_tree(&index_tree_name2)
            .expect("Failed to open index tree 2");

        // Check index 1
        let batch_positions1 = index_tree1
            .get("batch")
            .expect("Failed to get 'batch' term from index 1");
        assert!(
            batch_positions1.is_some(),
            "Expected positions for 'batch' in index 1, got None"
        );

        let positions1 = deserialize_positions(&batch_positions1.unwrap())
            .expect("Failed to deserialize positions");
        assert_eq!(
            positions1,
            vec![1, 2],
            "Incorrect positions for 'batch' in index 1"
        );

        // Check index 2
        let batch_positions2 = index_tree2
            .get("batch")
            .expect("Failed to get 'batch' term from index 2");
        assert!(
            batch_positions2.is_some(),
            "Expected positions for 'batch' in index 2, got None"
        );

        let positions2 = deserialize_positions(&batch_positions2.unwrap())
            .expect("Failed to deserialize positions");
        assert_eq!(
            positions2,
            vec![3, 4],
            "Incorrect positions for 'batch' in index 2"
        );
    }

    #[test]
    fn test_serialization_helpers() {
        // Test serialization/deserialization of positions
        let positions = vec![1u64, 5, 10, 100];

        // Serialize
        let serialized = serialize_positions(&positions).expect("Failed to serialize positions");

        // Deserialize
        let deserialized =
            deserialize_positions(&serialized).expect("Failed to deserialize positions");

        // Verify
        assert_eq!(
            deserialized, positions,
            "Deserialized positions don't match original"
        );
    }
}
