use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

// Constants for tree/partition names
pub const TREE_RECORDS: &str = "records";
pub const TREE_FILTERS: &str = "filters";
pub const TREE_GLOBAL_INDEX: &str = "global_index";
pub const PREFIX_FILTER_INDEX: &str = "filter:";
pub const PREFIX_RECORD_INDEX_TREE: &str = "index:";

// Database handle abstraction
#[derive(Clone)]
pub enum FuguDBBackend {
    #[cfg(feature = "use-sled")]
    Sled(sled::Db),
    #[cfg(feature = "use-fjall")]
    Fjall(fjall::Keyspace),
}

// Tree handle abstraction
#[derive(Clone)]
pub enum TreeHandle {
    #[cfg(feature = "use-sled")]
    Sled(sled::Tree),
    #[cfg(feature = "use-fjall")]
    Fjall(fjall::PartitionHandle),
}

// Batch operation abstraction
#[derive(Clone)]
pub enum BatchOperation {
    #[cfg(feature = "use-sled")]
    Sled(sled::Batch),
    #[cfg(feature = "use-fjall")]
    Fjall(Arc<fjall::Batch>), // Use Arc to provide Clone for fjall::Batch
}

/// Unified database wrapper that works with either sled or fjall
#[derive(Clone)]
pub struct FuguDB {
    pub backend: FuguDBBackend,
    // Cache open trees/partitions for better performance
    trees: HashMap<String, TreeHandle>,
}

impl FuguDB {
    #[cfg(feature = "use-sled")]
    pub fn new(db: sled::Db) -> Self {
        Self {
            backend: FuguDBBackend::Sled(db),
            trees: HashMap::new(),
        }
    }

    #[cfg(feature = "use-fjall")]
    pub fn new(keyspace: fjall::Keyspace) -> Self {
        Self {
            backend: FuguDBBackend::Fjall(keyspace),
            trees: HashMap::new(),
        }
    }

    /// Initialize the database by creating necessary trees/partitions
    pub fn init_db(&self) {
        // Create standard trees if they don't exist
        let standard_trees = [TREE_RECORDS, TREE_FILTERS, TREE_GLOBAL_INDEX];
        for tree_name in standard_trees.iter() {
            if let Err(e) = self.open_tree(tree_name) {
                error!("Failed to create tree {}: {:?}", tree_name, e);
            }
        }
    }

    /// Compact the database to reclaim space
    pub fn compact(&mut self) {
        match &self.backend {
            #[cfg(feature = "use-sled")]
            FuguDBBackend::Sled(db) => {
                // For sled, we compact individual trees and the main db
                if let Err(e) = db.flush() {
                    error!("Failed to flush sled db: {:?}", e);
                }

                // Try to compact each tree
                for tree_name in self.trees.keys() {
                    if let Ok(tree) = db.open_tree(tree_name) {
                        if let Err(e) = tree.flush() {
                            error!("Failed to flush tree {}: {:?}", tree_name, e);
                        }
                    }
                }
            },
            #[cfg(feature = "use-fjall")]
            FuguDBBackend::Fjall(keyspace) => {
                // For fjall, compaction is usually handled automatically
                // We can trigger a manual flush with fsync to persist changes
                if let Err(e) = keyspace.persist(fjall::PersistMode::SyncAll) {
                    error!("Failed to persist fjall keyspace: {:?}", e);
                }

                // Clear internal tree cache to ensure fresh handles
                self.trees.clear();
            },
            #[allow(unreachable_patterns)]
            _ => error!("Unknown database backend in compact()"),
        }
    }

    /// Access to the raw backend for operations not covered by the unified API
    #[cfg(feature = "use-sled")]
    pub fn db(&self) -> &sled::Db {
        match &self.backend {
            FuguDBBackend::Sled(db) => db,
            #[allow(unreachable_patterns)]
            _ => panic!("Not using sled backend"),
        }
    }

    /// Access to the raw backend for operations not covered by the unified API
    #[cfg(feature = "use-fjall")]
    pub fn keyspace(&self) -> &fjall::Keyspace {
        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => keyspace,
            #[allow(unreachable_patterns)]
            _ => panic!("Not using fjall backend"),
        }
    }

    /// Gets all partition names in the keyspace (fjall-specific)
    #[cfg(feature = "use-fjall")]
    pub fn partition_names(&self) -> Result<Vec<String>> {
        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => {
                // Fjall doesn't have a direct partition_names method, so we need to implement it
                // We'll use the list_partitions method to get all partition handles
                // In newer versions of fjall, list_partitions returns a Vec directly, not a Result
                let partitions = keyspace.list_partitions();

                // Extract the names from each partition
                let mut names = Vec::new();
                for partition in partitions {
                    // In newer fjall versions, partition might be the name directly or have a name() method
                    // So we need to handle both cases
                    names.push(partition.to_string());
                }

                Ok(names)
            },
            #[allow(unreachable_patterns)]
            _ => panic!("Not using fjall backend"),
        }
    }

    /// Open (or create if it doesn't exist) a tree/partition
    pub fn open_tree(&self, name: &str) -> Result<TreeHandle> {
        match &self.backend {
            #[cfg(feature = "use-sled")]
            FuguDBBackend::Sled(db) => {
                let tree = db.open_tree(name).context("Failed to open sled tree")?;
                Ok(TreeHandle::Sled(tree))
            }
            #[cfg(feature = "use-fjall")]
            FuguDBBackend::Fjall(keyspace) => {
                let partition = keyspace.open_partition(name, fjall::PartitionCreateOptions::default())
                    .map_err(|e| anyhow::anyhow!("Failed to open fjall partition: {:?}", e))?;
                Ok(TreeHandle::Fjall(partition))
            }
        }
    }

    /// Create a new batch operation
    pub fn create_batch(&self) -> BatchOperation {
        match &self.backend {
            #[cfg(feature = "use-sled")]
            FuguDBBackend::Sled(_) => BatchOperation::Sled(sled::Batch::default()),
            #[cfg(feature = "use-fjall")]
            FuguDBBackend::Fjall(keyspace) => {
                // Fjall batches should be created with with_capacity instead of default
                let batch = fjall::Batch::with_capacity(keyspace.clone(), 32); // reasonable starting capacity
                BatchOperation::Fjall(Arc::new(batch))
            },
        }
    }

    /// Apply a batch of operations
    pub fn apply_batch(&self, tree: &TreeHandle, batch: BatchOperation) -> Result<()> {
        match (&self.backend, batch, tree) {
            #[cfg(feature = "use-sled")]
            (FuguDBBackend::Sled(_), BatchOperation::Sled(batch), TreeHandle::Sled(tree)) => {
                tree.apply_batch(batch).context("Failed to apply sled batch")?;
                Ok(())
            }
            #[cfg(feature = "use-fjall")]
            (FuguDBBackend::Fjall(keyspace), BatchOperation::Fjall(batch_arc), _) => {
                match Arc::try_unwrap(batch_arc) {
                    Ok(batch) => {
                        // According to the fjall documentation, Batch has its own commit method
                        // that we should use directly instead of calling through keyspace
                        batch.commit()
                            .map_err(|e| anyhow::anyhow!("Failed to commit fjall batch: {:?}", e))?;

                        // Ensure all changes are persisted
                        keyspace.persist(fjall::PersistMode::SyncAll)
                            .map_err(|e| anyhow::anyhow!("Failed to persist batch changes: {:?}", e))?;
                        Ok(())
                    },
                    Err(arc_batch) => {
                        // In the rare case we can't get exclusive ownership, create a new batch
                        warn!("Could not unwrap Arc<Batch> for exclusive use");

                        // Since we can't easily extract operations from the original batch,
                        // this is a limitation with our approach. In a real implementation,
                        // we might need a different strategy for handling batches.
                        Err(anyhow::anyhow!("Failed to unwrap batch for application"))
                    }
                }
            }
            _ => Err(anyhow::anyhow!("Mismatched database backend and batch type")),
        }
    }

    /// Direct get method for convenience (assumes TREE_RECORDS)
    /// Returns the deserialized ObjectRecord if found and successfully deserialized
    pub fn get(&self, key: &str) -> Option<crate::object::ObjectRecord> {
        if let Ok(tree) = self.open_tree(TREE_RECORDS) {
            if let Ok(Some(data)) = tree.get(key) {
                // Try to deserialize the record
                if let Ok(archivable) = crate::rkyv_adapter::deserialize::<
                    crate::object::ArchivableObjectRecord,
                >(&data) {
                    return Some(crate::object::ObjectRecord::from(archivable));
                }
            }
        }
        None
    }

    /// Index a single object
    pub fn index(&self, object_index: crate::object::ObjectIndex) {
        // Get the object ID for logging
        let object_id = &object_index.object_id;
        tracing::debug!("Indexing object: {}", object_id);

        // Create or get the index tree for this object
        let index_tree_name = format!("{}:{}", PREFIX_RECORD_INDEX_TREE, object_id);
        let index_tree = match self.open_tree(&index_tree_name) {
            Ok(tree) => tree,
            Err(e) => {
                tracing::error!("Failed to open index tree for {}: {:?}", object_id, e);
                return;
            }
        };

        // Create a batch for all index operations
        let mut batch = self.create_batch();

        // Add inverted index entries
        for (term, positions) in &object_index.inverted_index {
            match crate::rkyv_adapter::serialize(positions) {
                Ok(serialized_positions) => {
                    batch.insert(&index_tree, term, serialized_positions);
                }
                Err(e) => {
                    tracing::error!("Failed to serialize positions for term '{}': {:?}", term, e);
                }
            }
        }

        // Apply the batch
        if let Err(e) = self.apply_batch(&index_tree, batch) {
            tracing::error!("Failed to apply index batch for {}: {:?}", object_id, e);
        }
    }

    /// Index multiple objects in batch
    pub fn batch_index(&self, object_indices: Vec<crate::object::ObjectIndex>) {
        for object_index in object_indices {
            self.index(object_index);
        }
    }
}

impl TreeHandle {
    /// Insert a key-value pair
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        match self {
            #[cfg(feature = "use-sled")]
            TreeHandle::Sled(tree) => {
                let result = tree.insert(key.as_ref(), value.as_ref())
                    .context("Failed to insert into sled tree")?;
                Ok(result.map(|bytes| bytes.to_vec()))
            }
            #[cfg(feature = "use-fjall")]
            TreeHandle::Fjall(partition) => {
                let key_str = std::str::from_utf8(key.as_ref())
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 key: {}", e))?;
                let prev = partition.get(key_str)
                    .map_err(|e| anyhow::anyhow!("Failed to get from fjall partition: {:?}", e))?;
                partition.insert(key_str, value.as_ref().to_vec())
                    .map_err(|e| anyhow::anyhow!("Failed to insert into fjall partition: {:?}", e))?;
                Ok(prev.map(|bytes| bytes.to_vec()))
            }
        }
    }

    /// Get a value by key
    pub fn get<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        match self {
            #[cfg(feature = "use-sled")]
            TreeHandle::Sled(tree) => {
                let result = tree.get(key.as_ref())
                    .context("Failed to get from sled tree")?;
                Ok(result.map(|bytes| bytes.to_vec()))
            }
            #[cfg(feature = "use-fjall")]
            TreeHandle::Fjall(partition) => {
                let key_str = std::str::from_utf8(key.as_ref())
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 key: {}", e))?;
                partition.get(key_str)
                    .map_err(|e| anyhow::anyhow!("Failed to get from fjall partition: {:?}", e))
                    .map(|opt| opt.map(|bytes| bytes.to_vec()))
            }
        }
    }

    /// Remove a key-value pair
    pub fn remove<K>(&self, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        match self {
            #[cfg(feature = "use-sled")]
            TreeHandle::Sled(tree) => {
                let result = tree.remove(key.as_ref())
                    .context("Failed to remove from sled tree")?;
                Ok(result.map(|bytes| bytes.to_vec()))
            }
            #[cfg(feature = "use-fjall")]
            TreeHandle::Fjall(partition) => {
                let key_str = std::str::from_utf8(key.as_ref())
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 key: {}", e))?;
                let prev = partition.get(key_str)
                    .map_err(|e| anyhow::anyhow!("Failed to get from fjall partition: {:?}", e))?;
                partition.remove(key_str)
                    .map_err(|e| anyhow::anyhow!("Failed to remove from fjall partition: {:?}", e))?;
                Ok(prev.map(|bytes| bytes.to_vec()))
            }
        }
    }

    /// Iterate over all key-value pairs - unified API
    ///
    /// For sled, this returns the iterator directly.
    /// For fjall, this requires creating the iterator first.
    /// We'll have to handle the result type differently in the calling code.
    pub fn iter(&self) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_>> {
        match self {
            #[cfg(feature = "use-sled")]
            TreeHandle::Sled(tree) => {
                let iter = Box::new(tree.iter().map(|item| {
                    item.map(|(key, value)| (key.to_vec(), value.to_vec()))
                        .context("Failed to iterate over sled tree")
                }));
                Ok(iter)
            }
            #[cfg(feature = "use-fjall")]
            TreeHandle::Fjall(partition) => {
                // In newer versions of fjall, iter() returns an iterator directly
                // without wrapping it in a Result
                let fjall_iter = partition.iter();

                // Convert the Fjall iterator into our unified iterator type
                // Fjall uses a Result<(StrView, Vec<u8>), Error> for its items
                let boxed_iter = Box::new(fjall_iter.map(move |item_result| {
                    match item_result {
                        Ok((key, value)) => {
                            // Convert key to bytes - in fjall, key is a Slice
                            // From the docs, Slice implements Deref<Target=[u8]>
                            // So we can just dereference it to get the raw bytes
                            let key_bytes = key.to_vec();
                            Ok((key_bytes, value.to_vec()))
                        },
                        Err(e) => Err(anyhow::anyhow!("Failed to get item from fjall iterator: {:?}", e)),
                    }
                }));

                Ok(boxed_iter)
            }
            #[allow(unreachable_patterns)]
            _ => Err(anyhow::anyhow!("Unknown database backend")),
        }
    }
}

impl BatchOperation {
    /// Add an insert operation to the batch
    pub fn insert<K, V>(&mut self, tree: &TreeHandle, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        match (self, tree) {
            #[cfg(feature = "use-sled")]
            (BatchOperation::Sled(batch), TreeHandle::Sled(_)) => {
                batch.insert(key.as_ref(), value.as_ref());
            }
            #[cfg(feature = "use-fjall")]
            (BatchOperation::Fjall(batch_arc), TreeHandle::Fjall(partition)) => {
                let key_str = match std::str::from_utf8(key.as_ref()) {
                    Ok(s) => s.to_string(),
                    Err(_) => String::from_utf8_lossy(key.as_ref()).to_string(),
                };

                // Access the batch through the Arc, we need mutable access
                // We use get_mut() as we have exclusive access to the Arc
                let batch = Arc::get_mut(batch_arc).expect("Cannot get mutable reference to batch");
                batch.insert(partition, key_str, value.as_ref().to_vec());
            }
            _ => {
                // Mismatched backend, this will be caught when applying the batch
                debug!("Mismatched database backend and tree type for batch insert");
            }
        }
    }

    /// Add a remove operation to the batch
    pub fn remove<K>(&mut self, tree: &TreeHandle, key: K)
    where
        K: AsRef<[u8]>,
    {
        match (self, tree) {
            #[cfg(feature = "use-sled")]
            (BatchOperation::Sled(batch), TreeHandle::Sled(_)) => {
                batch.remove(key.as_ref());
            }
            #[cfg(feature = "use-fjall")]
            (BatchOperation::Fjall(batch_arc), TreeHandle::Fjall(partition)) => {
                let key_str = match std::str::from_utf8(key.as_ref()) {
                    Ok(s) => s.to_string(),
                    Err(_) => String::from_utf8_lossy(key.as_ref()).to_string(),
                };

                // Access the batch through the Arc, we need mutable access
                let batch = Arc::get_mut(batch_arc).expect("Cannot get mutable reference to batch");
                batch.remove(partition, key_str);
            }
            _ => {
                // Mismatched backend, this will be caught when applying the batch
                debug!("Mismatched database backend and tree type for batch remove");
            }
        }
    }
}

// Helper function to serialize positions to a byte vector
pub fn serialize_positions(positions: &[usize]) -> Vec<u8> {
    // Convert to u64 as that's what our serialization format expects
    let positions_u64: Vec<u64> = positions.iter().map(|&p| p as u64).collect();
    crate::rkyv_adapter::serialize(&positions_u64).unwrap_or_default()
}

// Helper function to deserialize positions from a byte slice
pub fn deserialize_positions(bytes: &[u8]) -> Result<Vec<u64>> {
    let positions: Vec<u64> = crate::rkyv_adapter::deserialize(bytes)
        .map_err(|e| anyhow!("Failed to deserialize positions: {}", e))?;
    Ok(positions)
}

// Tests for the unified database API
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_unified_api_basic_operations() {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();
        
        // Initialize the appropriate database backend based on feature flags
        #[cfg(feature = "use-sled")]
        let db = sled::open(temp_path).expect("Failed to open test database");
        
        #[cfg(feature = "use-fjall")]
        let keyspace = fjall::Config::new(temp_path)
            .cache_size(64 * 1024 * 1024)  // 64MB cache for test
            .open()
            .expect("Failed to open test keyspace");
        
        // Create FuguDB instance
        #[cfg(feature = "use-sled")]
        let fugu_db = FuguDB::new(db);
        
        #[cfg(feature = "use-fjall")]
        let fugu_db = FuguDB::new(keyspace);
        
        // Initialize database
        fugu_db.init_db();
        
        // Test open_tree method
        let tree_handle = fugu_db.open_tree(TREE_RECORDS)
            .expect("Failed to open RECORDS tree");
        
        // Test insert operation
        let test_key = "test_key";
        let test_value = "test_value";
        tree_handle.insert(test_key, test_value.as_bytes())
            .expect("Failed to insert test value");
        
        // Test get operation
        let result = tree_handle.get(test_key)
            .expect("Failed to get test value");
        
        assert!(result.is_some(), "Expected Some value, got None");
        let value_bytes = result.unwrap();
        let value_str = std::str::from_utf8(&value_bytes)
            .expect("Failed to convert bytes to string");
            
        assert_eq!(value_str, test_value, "Retrieved value doesn't match inserted value");
        
        // Test remove operation
        tree_handle.remove(test_key)
            .expect("Failed to remove test value");
            
        // Verify removal
        let result_after_removal = tree_handle.get(test_key)
            .expect("Failed to get test value after removal");
            
        assert!(result_after_removal.is_none(), "Expected None after removal, got Some");
    }
    
    #[test]
    fn test_unified_api_batch_operations() {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();
        
        // Initialize the appropriate database backend based on feature flags
        #[cfg(feature = "use-sled")]
        let db = sled::open(temp_path).expect("Failed to open test database");
        
        #[cfg(feature = "use-fjall")]
        let keyspace = fjall::Config::new(temp_path)
            .cache_size(64 * 1024 * 1024)  // 64MB cache for test
            .open()
            .expect("Failed to open test keyspace");
        
        // Create FuguDB instance
        #[cfg(feature = "use-sled")]
        let fugu_db = FuguDB::new(db);
        
        #[cfg(feature = "use-fjall")]
        let fugu_db = FuguDB::new(keyspace);
        
        // Initialize database
        fugu_db.init_db();
        
        // Test open_tree method
        let tree_handle = fugu_db.open_tree(TREE_RECORDS)
            .expect("Failed to open RECORDS tree");
        
        // Create a batch
        let mut batch = fugu_db.create_batch();
        
        // Add multiple operations to the batch
        let test_keys = ["batch_key1", "batch_key2", "batch_key3"];
        let test_values = ["batch_value1", "batch_value2", "batch_value3"];
        
        for i in 0..3 {
            batch.insert(&tree_handle, test_keys[i], test_values[i].as_bytes());
        }
        
        // Apply the batch
        fugu_db.apply_batch(&tree_handle, batch)
            .expect("Failed to apply batch");
        
        // Verify all items were inserted
        for i in 0..3 {
            let result = tree_handle.get(test_keys[i])
                .expect("Failed to get batch value");
            
            assert!(result.is_some(), "Expected Some value for {}, got None", test_keys[i]);
            let value_bytes = result.unwrap();
            let value_str = std::str::from_utf8(&value_bytes)
                .expect("Failed to convert bytes to string");
                
            assert_eq!(value_str, test_values[i], "Retrieved value doesn't match inserted value for {}", test_keys[i]);
        }
        
        // Create another batch for removal
        let mut removal_batch = fugu_db.create_batch();
        for key in test_keys.iter() {
            removal_batch.remove(&tree_handle, *key);
        }
        
        // Apply the removal batch
        fugu_db.apply_batch(&tree_handle, removal_batch)
            .expect("Failed to apply removal batch");
        
        // Verify all items were removed
        for key in test_keys.iter() {
            let result = tree_handle.get(*key)
                .expect("Failed to get value after batch removal");
            
            assert!(result.is_none(), "Expected None after batch removal, got Some for {}", key);
        }
    }
    
    #[test]
    fn test_error_handling() {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();
        
        // Initialize the appropriate database backend based on feature flags
        #[cfg(feature = "use-sled")]
        let db = sled::open(temp_path).expect("Failed to open test database");
        
        #[cfg(feature = "use-fjall")]
        let keyspace = fjall::Config::new(temp_path)
            .cache_size(64 * 1024 * 1024)  // 64MB cache for test
            .open()
            .expect("Failed to open test keyspace");
        
        // Create FuguDB instance
        #[cfg(feature = "use-sled")]
        let fugu_db = FuguDB::new(db);
        
        #[cfg(feature = "use-fjall")]
        let fugu_db = FuguDB::new(keyspace);
        
        // Initialize database
        fugu_db.init_db();
        
        // Test error handling for non-existent tree/partition
        let non_existent_tree = fugu_db.open_tree("NON_EXISTENT_TREE_TEST");
        
        // This should succeed since we're opening a non-existent tree (which should be created)
        assert!(non_existent_tree.is_ok(), "Opening non-existent tree should succeed, but got error");
        
        // Test error handling in get operation with invalid key
        #[cfg(feature = "use-sled")]
        {
            // In sled, we can test with a tree that definitely doesn't exist
            let result = fugu_db.get("definitely_not_existing_object_id");
            assert!(result.is_none(), "Expected None for non-existent object, got Some");
        }
        
        #[cfg(feature = "use-fjall")]
        {
            // In fjall, we can test with a non-existent object ID
            let result = fugu_db.get("definitely_not_existing_object_id");
            assert!(result.is_none(), "Expected None for non-existent object, got Some");
        }
    }
}