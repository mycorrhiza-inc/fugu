use anyhow::{Context, Result, anyhow};
use fjall;
use fjall::KvSeparationOptions;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use fjall::GarbageCollection;

// Constants for tree/partition names
pub const TREE_RECORDS: &str = "records";
pub const TREE_FILTERS: &str = "filters";
pub const TREE_GLOBAL_INDEX: &str = "global_index";
pub const PREFIX_FILTER_INDEX: &str = "filter$";
pub const PREFIX_RECORD_INDEX_TREE: &str = "index$";

// Database handle abstraction
#[derive(Clone)]
pub enum FuguDBBackend {
    Fjall(fjall::Keyspace),
}

// Tree handle abstraction
#[derive(Clone)]
pub enum TreeHandle {
    Fjall(fjall::PartitionHandle),
}

// Batch operation abstraction
#[derive(Clone)]
pub enum BatchOperation {
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
    pub fn compact(&mut self) -> anyhow::Result<()> {
        let start = Instant::now();
        info!("Starting database compaction");

        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => {
                keyspace
                    .persist(fjall::PersistMode::SyncAll)
                    .map_err(|e| anyhow!("Failed to persist fjall keyspace: {:?}", e))?;

                // First get a list of all partitions
                let partitions = keyspace.list_partitions();

                // Perform maintenance on each partition
                for partition_name in &partitions {
                    debug!("Triggering maintenance for partition: {}", partition_name);

                    // Try to get the partition handle, if it exists
                    if let Ok(partition) = keyspace.open_partition(
                        partition_name,
                        fjall::PartitionCreateOptions::default()
                            .with_kv_separation(KvSeparationOptions::default())
                            .with_kv_separation(KvSeparationOptions::default()),
                    ) {
                        // Fjall uses garbage collection methods instead of maintenance
                        // We'll use both space amplification and staleness-based approaches for thorough cleanup

                        // First, perform garbage collection with space amplification target (1.5 is a reasonable value)
                        if let Err(e) = partition.gc_with_space_amp_target(1.5) {
                            warn!(
                                "Error during space-based GC for partition {}: {:?}",
                                partition_name, e
                            );
                        }

                        // Then use staleness threshold for any remaining garbage (0.8 is fairly aggressive)
                        if let Err(e) = partition.gc_with_staleness_threshold(0.8) {
                            warn!(
                                "Error during staleness-based GC for partition {}: {:?}",
                                partition_name, e
                            );
                        }

                        // Finally, drop any fully stale segments
                        if let Err(e) = partition.gc_drop_stale_segments() {
                            warn!(
                                "Error dropping stale segments for partition {}: {:?}",
                                partition_name, e
                            );
                        } else {
                            debug!(
                                "Successfully ran garbage collection on partition: {}",
                                partition_name
                            );
                        }
                    }
                }

                // Keyspace doesn't have maintenance or GC methods directly
                // We'll just make sure changes are persisted
                if let Err(e) = keyspace.persist(fjall::PersistMode::SyncAll) {
                    warn!("Error during keyspace persistence: {:?}", e);
                } else {
                    debug!("Successfully persisted keyspace changes");
                }

                // Clear internal tree cache to ensure fresh handles
                self.trees.clear();

                info!("Fjall compaction completed in {:?}", start.elapsed());
                Ok(())
            }
        }
    }

    /// Compacts a specific tree/partition by name
    /// This allows for targeted compaction of specific hot partitions
    pub fn compact_tree(&mut self, tree_name: &str) -> anyhow::Result<()> {
        let start = Instant::now();
        debug!("Starting targeted compaction for tree: {}", tree_name);
        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => {
                // Open the partition
                let partition = keyspace
                    .open_partition(
                        tree_name,
                        fjall::PartitionCreateOptions::default()
                            .with_kv_separation(KvSeparationOptions::default()),
                    )
                    .map_err(|e| {
                        anyhow!("Failed to open fjall partition {}: {:?}", tree_name, e)
                    })?;

                // First, ensure all data is persisted for this partition

                keyspace
                    .persist(fjall::PersistMode::SyncAll)
                    .map_err(|e| anyhow!("Failed to persist fjall keyspace: {:?}", e))?;

                // Fjall now uses garbage collection methods instead of maintenance
                // First, perform space-based GC
                partition.gc_with_space_amp_target(1.5).map_err(|e| {
                    anyhow!(
                        "Failed to run space-based GC for fjall partition {}: {:?}",
                        tree_name,
                        e
                    )
                })?;

                // Then staleness-based GC
                partition.gc_with_staleness_threshold(0.8).map_err(|e| {
                    anyhow!(
                        "Failed to run staleness-based GC for fjall partition {}: {:?}",
                        tree_name,
                        e
                    )
                })?;

                // Finally drop fully stale segments
                partition.gc_drop_stale_segments().map_err(|e| {
                    anyhow!(
                        "Failed to drop stale segments for fjall partition {}: {:?}",
                        tree_name,
                        e
                    )
                })?;

                // Remove the tree from our cache to ensure we get a fresh handle next time
                self.trees.remove(tree_name);

                debug!(
                    "Fjall partition {} maintenance completed in {:?}",
                    tree_name,
                    start.elapsed()
                );

                Ok(())
            }
        }
    }

    /// Lightweight maintenance operation that doesn't do a full compaction
    /// This is useful for regular housekeeping without the overhead of full compaction
    pub fn maintenance(&self) -> anyhow::Result<()> {
        let start = Instant::now();

        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => {
                // For fjall, ensure everything is persisted since there's no direct maintenance method
                keyspace
                    .persist(fjall::PersistMode::SyncAll)
                    .map_err(|e| anyhow!("Failed to persist fjall keyspace: {:?}", e))?;

                // We'll do basic maintenance on each partition
                for partition_name in keyspace.list_partitions() {
                    if let Ok(partition) = keyspace.open_partition(
                        &partition_name,
                        fjall::PartitionCreateOptions::default()
                            .with_kv_separation(KvSeparationOptions::default()),
                    ) {
                        // First try to drop any completely stale segments which is a lightweight operation
                        if let Err(e) = partition.gc_drop_stale_segments() {
                            warn!(
                                "Failed to drop stale segments for partition {}: {:?}",
                                partition_name, e
                            );
                        }
                    }
                }

                // Also ensure data is persisted
                keyspace
                    .persist(fjall::PersistMode::SyncAll)
                    .map_err(|e| anyhow!("Failed to persist fjall keyspace: {:?}", e))?;

                debug!("Fjall maintenance completed in {:?}", start.elapsed());
                Ok(())
            }
            #[allow(unreachable_patterns)]
            _ => {
                error!("Unknown database backend in maintenance()");
                Err(anyhow!("Unknown database backend in maintenance()"))
            }
        }
    }

    /// Access to the raw backend for operations not covered by the unified API
    pub fn keyspace(&self) -> &fjall::Keyspace {
        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => keyspace,
        }
    }

    pub fn partition_names(&self) -> Vec<String> {
        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => {
                // Fjall's list_partitions returns a Vec directly, not a Result
                let partitions = keyspace.list_partitions();

                // Convert each partition name string to String
                partitions.into_iter().map(|p| p.to_string()).collect()
            }
            _ => panic!("Not using fjall backend"),
        }
    }

    /// Open (or create if it doesn't exist) a tree/partition
    pub fn open_tree(&self, name: &str) -> Result<TreeHandle> {
        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => {
                match keyspace.open_partition(
                    name,
                    fjall::PartitionCreateOptions::default()
                        .with_kv_separation(KvSeparationOptions::default()),
                ) {
                    Ok(partition_handle) => Ok(TreeHandle::Fjall(partition_handle)),
                    Err(e) => {
                        tracing::error!("Failed to open fjall partition: {:?}", e);
                        Err(anyhow::anyhow!("Failed to open fjall partition: {:?}", e))
                    }
                }
            }
        }
    }

    /// Create a new batch operation
    pub fn create_batch(&self) -> BatchOperation {
        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => {
                // Fjall batches should be created with with_capacity instead of default
                let batch = fjall::Batch::with_capacity(keyspace.clone(), 32); // reasonable starting capacity
                BatchOperation::Fjall(Arc::new(batch))
            }
        }
    }

    /// Apply a batch of operations
    pub fn apply_batch(&self, tree: &TreeHandle, batch: BatchOperation) -> Result<()> {
        match (&self.backend, batch, tree) {
            (FuguDBBackend::Fjall(keyspace), BatchOperation::Fjall(batch_arc), _) => {
                match Arc::try_unwrap(batch_arc) {
                    Ok(batch) => {
                        // According to the fjall documentation, Batch has its own commit method
                        // that we should use directly instead of calling through keyspace
                        batch.commit().map_err(|e| {
                            anyhow::anyhow!("Failed to commit fjall batch: {:?}", e)
                        })?;

                        // Ensure all changes are persisted
                        keyspace.persist(fjall::PersistMode::SyncAll).map_err(|e| {
                            anyhow::anyhow!("Failed to persist batch changes: {:?}", e)
                        })?;
                        Ok(())
                    }
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
            _ => Err(anyhow::anyhow!(
                "Mismatched database backend and batch type"
            )),
        }
    }

    /// Direct get method for convenience (assumes TREE_RECORDS)
    /// Returns the deserialized ObjectRecord if found and successfully deserialized
    pub fn get(&self, key: &str) -> Option<crate::object::ObjectRecord> {
        if let Ok(tree) = self.open_tree(TREE_RECORDS) {
            if let Ok(Some(data)) = tree.get(key) {
                // Try to deserialize the record
                if let Ok(archivable) =
                    crate::rkyv_adapter::deserialize::<crate::object::ArchivableObjectRecord>(&data)
                {
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
        tracing::info!("Indexing object: {}", object_id);

        // Create or get the index tree for this object
        let index_tree_name = format!("{}{}", PREFIX_RECORD_INDEX_TREE, object_id);
        tracing::info!("openning partition : {}", index_tree_name);
        let index_tree = match self.open_tree(&index_tree_name) {
            Ok(tree) => tree,
            Err(e) => {
                tracing::error!("Failed to open index tree for {}: {:?}", object_id, e);
                return;
            }
        };
        tracing::info!("partition opened : {}", index_tree_name);

        // Process each term in the inverted index
        // For fjall backend, we'll handle term position merges differently
        // since we don't have sled's merge operators
        match &self.backend {
            FuguDBBackend::Fjall(_) => {
                // For fjall, we need to use read-modify-write pattern for each term
                for (term, positions) in &object_index.inverted_index {
                    // Read current value if it exists
                    let current_positions = match index_tree.get(term) {
                        Ok(Some(bytes)) => {
                            match deserialize_positions(&bytes) {
                                Ok(pos) => pos,
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to deserialize existing positions for term '{}': {:?}",
                                        term,
                                        e
                                    );
                                    Vec::new() // Start with empty if we can't deserialize
                                }
                            }
                        }
                        Ok(None) => Vec::new(), // No existing positions
                        Err(e) => {
                            tracing::error!(
                                "Failed to get existing positions for term '{}': {:?}",
                                term,
                                e
                            );
                            Vec::new() // Start with empty if there's an error
                        }
                    };

                    // Merge existing positions with new positions
                    let mut merged_positions = current_positions;
                    // Convert positions to u64 for merging (matching our serialization format)
                    let positions_u64: Vec<u64> = positions.iter().map(|&p| p as u64).collect();
                    merged_positions.extend(positions_u64);

                    // Serialize and write back the merged positions
                    match serialize_positions(&merged_positions) {
                        Ok(serialized) => {
                            if let Err(e) = index_tree.insert(term, serialized) {
                                tracing::error!(
                                    "Failed to insert merged positions for term '{}': {:?}",
                                    term,
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to serialize merged positions for term '{}': {:?}",
                                term,
                                e
                            );
                        }
                    }
                }
            }
            #[allow(unreachable_patterns)]
            _ => tracing::error!("Unknown database backend in index()"),
        }
    }

    /// Index multiple objects in batch
    pub fn batch_index(&self, object_indices: Vec<crate::object::ObjectIndex>) {
        let mut partitions_and_objects = HashMap::new();

        // First, group objects by their index partitions
        for object_index in &object_indices {
            let object_id = &object_index.object_id;
            let index_tree_name = format!("{}{}", PREFIX_RECORD_INDEX_TREE, object_id);

            // Create each partition if it doesn't exist
            match self.open_tree(&index_tree_name) {
                Ok(tree) => {
                    partitions_and_objects
                        .entry(index_tree_name)
                        .or_insert_with(Vec::new)
                        .push(object_index);
                }
                Err(e) => {
                    tracing::error!("Failed to open index tree for {}: {:?}", object_id, e);
                }
            }
        }
        // instead of creating individual trees, we just create rkyv hash maps, and save them to
        // disk

        // Now process each partition's objects
        for (index_tree_name, objects) in partitions_and_objects {
            // Open the tree for this partition
            if let Ok(index_tree) = self.open_tree(&index_tree_name) {
                // Process each object one by one since we need the read-modify-write pattern
                for object_ref in objects {
                    // Since we have borrowed the object_indices, we need to work with references
                    let object_index = object_ref;

                    // For each term in the inverted index
                    for (term, positions) in &object_index.inverted_index {
                        // Read current value if it exists
                        let current_positions = match index_tree.get(term) {
                            Ok(Some(bytes)) => {
                                match deserialize_positions(&bytes) {
                                    Ok(pos) => pos,
                                    Err(e) => {
                                        tracing::error!(
                                            "Failed to deserialize existing positions for term '{}': {:?}",
                                            term,
                                            e
                                        );
                                        Vec::new() // Start with empty if we can't deserialize
                                    }
                                }
                            }
                            Ok(None) => Vec::new(), // No existing positions
                            Err(e) => {
                                tracing::error!(
                                    "Failed to get existing positions for term '{}': {:?}",
                                    term,
                                    e
                                );
                                Vec::new() // Start with empty if there's an error
                            }
                        };

                        // Merge existing positions with new positions
                        let mut merged_positions = current_positions;
                        // Convert positions to u64 for merging
                        let positions_u64: Vec<u64> = positions.iter().map(|&p| p as u64).collect();
                        merged_positions.extend(positions_u64);

                        // Serialize and write back the merged positions
                        match serialize_positions(&merged_positions) {
                            Ok(serialized) => {
                                if let Err(e) = index_tree.insert(term, serialized) {
                                    tracing::error!(
                                        "Failed to insert merged positions for term '{}': {:?}",
                                        term,
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to serialize merged positions for term '{}': {:?}",
                                    term,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }

        // Final persistence to ensure all changes are saved

        match &self.backend {
            FuguDBBackend::Fjall(keyspace) => {
                if let Err(e) = keyspace.persist(fjall::PersistMode::SyncAll) {
                    tracing::error!("Failed to persist batch indexing changes: {:?}", e);
                }
            }
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
            TreeHandle::Fjall(partition) => {
                let key_str = std::str::from_utf8(key.as_ref())
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 key: {}", e))?;
                let prev = partition
                    .get(key_str)
                    .map_err(|e| anyhow::anyhow!("Failed to get from fjall partition: {:?}", e))?;
                partition
                    .insert(key_str, value.as_ref().to_vec())
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to insert into fjall partition: {:?}", e)
                    })?;
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
            TreeHandle::Fjall(partition) => {
                let key_str = std::str::from_utf8(key.as_ref())
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 key: {}", e))?;
                partition
                    .get(key_str)
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
            TreeHandle::Fjall(partition) => {
                let key_str = std::str::from_utf8(key.as_ref())
                    .map_err(|e| anyhow::anyhow!("Invalid UTF-8 key: {}", e))?;
                let prev = partition
                    .get(key_str)
                    .map_err(|e| anyhow::anyhow!("Failed to get from fjall partition: {:?}", e))?;
                partition.remove(key_str).map_err(|e| {
                    anyhow::anyhow!("Failed to remove from fjall partition: {:?}", e)
                })?;
                Ok(prev.map(|bytes| bytes.to_vec()))
            }
        }
    }

    /// Iterate over all key-value pairs
    pub fn iter(&self) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_>> {
        match self {
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
                        }
                        Err(e) => Err(anyhow::anyhow!(
                            "Failed to get item from fjall iterator: {:?}",
                            e
                        )),
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
pub fn serialize_positions(positions: &Vec<u64>) -> Result<Vec<u8>> {
    crate::rkyv_adapter::serialize(positions)
        .map_err(|e| anyhow!("Failed to serialize positions: {}", e))
}

// Helper function for backward compatibility with usize positions
pub fn serialize_positions_from_usize(positions: &[usize]) -> Result<Vec<u8>> {
    // Convert to u64 as that's what our serialization format expects
    let positions_u64: Vec<u64> = positions.iter().map(|&p| p as u64).collect();
    serialize_positions(&positions_u64)
}

// Helper function to deserialize positions from a byte slice
pub fn deserialize_positions(bytes: &[u8]) -> Result<Vec<u64>> {
    crate::rkyv_adapter::deserialize(bytes)
        .map_err(|e| anyhow!("Failed to deserialize positions: {}", e))
}

// Tests for the unified database API
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_unified_api_basic_operations() {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();

        // Initialize the appropriate database backend based on feature flags
        let keyspace = fjall::Config::new(temp_path)
            .cache_size(64 * 1024 * 1024) // 64MB cache for test
            .open()
            .expect("Failed to open test keyspace");

        let fugu_db = FuguDB::new(keyspace);

        // Initialize database
        fugu_db.init_db();

        // Test open_tree method
        let tree_handle = fugu_db
            .open_tree(TREE_RECORDS)
            .expect("Failed to open RECORDS tree");

        // Test insert operation
        let test_key = "test_key";
        let test_value = "test_value";
        tree_handle
            .insert(test_key, test_value.as_bytes())
            .expect("Failed to insert test value");

        // Test get operation
        let result = tree_handle.get(test_key).expect("Failed to get test value");

        assert!(result.is_some(), "Expected Some value, got None");
        let value_bytes = result.unwrap();
        let value_str =
            std::str::from_utf8(&value_bytes).expect("Failed to convert bytes to string");

        assert_eq!(
            value_str, test_value,
            "Retrieved value doesn't match inserted value"
        );

        // Test remove operation
        tree_handle
            .remove(test_key)
            .expect("Failed to remove test value");

        // Verify removal
        let result_after_removal = tree_handle
            .get(test_key)
            .expect("Failed to get test value after removal");

        assert!(
            result_after_removal.is_none(),
            "Expected None after removal, got Some"
        );
    }

    #[test]
    fn test_unified_api_batch_operations() {
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

        // Test open_tree method
        let tree_handle = fugu_db
            .open_tree(TREE_RECORDS)
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
        fugu_db
            .apply_batch(&tree_handle, batch)
            .expect("Failed to apply batch");

        // Verify all items were inserted
        for i in 0..3 {
            let result = tree_handle
                .get(test_keys[i])
                .expect("Failed to get batch value");

            assert!(
                result.is_some(),
                "Expected Some value for {}, got None",
                test_keys[i]
            );
            let value_bytes = result.unwrap();
            let value_str =
                std::str::from_utf8(&value_bytes).expect("Failed to convert bytes to string");

            assert_eq!(
                value_str, test_values[i],
                "Retrieved value doesn't match inserted value for {}",
                test_keys[i]
            );
        }

        // Create another batch for removal
        let mut removal_batch = fugu_db.create_batch();
        for key in test_keys.iter() {
            removal_batch.remove(&tree_handle, *key);
        }

        // Apply the removal batch
        fugu_db
            .apply_batch(&tree_handle, removal_batch)
            .expect("Failed to apply removal batch");

        // Verify all items were removed
        for key in test_keys.iter() {
            let result = tree_handle
                .get(*key)
                .expect("Failed to get value after batch removal");

            assert!(
                result.is_none(),
                "Expected None after batch removal, got Some for {}",
                key
            );
        }
    }

    #[test]
    fn test_error_handling() {
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

        // Test error handling for non-existent tree/partition
        let non_existent_tree = fugu_db.open_tree("NON_EXISTENT_TREE_TEST");

        // This should succeed since we're opening a non-existent tree (which should be created)
        assert!(
            non_existent_tree.is_ok(),
            "Opening non-existent tree should succeed, but got error"
        );

        // Test error handling in get operation with invalid key
        {
            // In fjall, we can test with a non-existent object ID
            let result = fugu_db.get("definitely_not_existing_object_id");
            assert!(
                result.is_none(),
                "Expected None for non-existent object, got Some"
            );
        }
    }

    #[test]
    fn test_compaction() {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();

        // Initialize the appropriate database backend based on feature flags
        let keyspace = fjall::Config::new(temp_path)
            .cache_size(64 * 1024 * 1024) // 64MB cache for test
            .open()
            .expect("Failed to open test keyspace");

        // Create FuguDB instance
        let mut fugu_db = FuguDB::new(keyspace);

        // Initialize database
        fugu_db.init_db();

        // Test open_tree method
        let tree_handle = fugu_db
            .open_tree(TREE_RECORDS)
            .expect("Failed to open RECORDS tree");

        // Insert a large number of values to make compaction meaningful
        let test_values: Vec<_> = (0..1000)
            .map(|i| (format!("key_{}", i), format!("value_{}", i)))
            .collect();

        for (key, value) in &test_values {
            tree_handle
                .insert(key, value.as_bytes())
                .expect("Failed to insert test value");
        }

        // Now delete half the values to create "holes" in the database
        for i in 0..500 {
            let key = format!("key_{}", i);
            tree_handle
                .remove(&key)
                .expect("Failed to remove test value");
        }

        // Perform full compaction and verify it completes without error
        assert!(
            fugu_db.compact().is_ok(),
            "Full compaction should succeed without error"
        );

        // Test maintenance method
        assert!(
            fugu_db.maintenance().is_ok(),
            "Maintenance should succeed without error"
        );

        // Test compact_tree on specific trees
        assert!(
            fugu_db.compact_tree(TREE_RECORDS).is_ok(),
            "Compacting specific tree should succeed without error"
        );

        // Verify data is still accessible after compaction
        for i in 500..1000 {
            let key = format!("key_{}", i);
            let result = tree_handle
                .get(&key)
                .expect("Failed to get test value after compaction");

            assert!(
                result.is_some(),
                "Expected Some value for {} after compaction, got None",
                key
            );
            let value_bytes = result.unwrap();
            let value_str =
                std::str::from_utf8(&value_bytes).expect("Failed to convert bytes to string");

            assert_eq!(
                value_str,
                format!("value_{}", i),
                "Retrieved value doesn't match inserted value for {} after compaction",
                key
            );
        }

        // Also verify deleted values are still gone
        for i in 0..500 {
            let key = format!("key_{}", i);
            let result = tree_handle
                .get(&key)
                .expect("Failed to check deleted value after compaction");

            assert!(
                result.is_none(),
                "Expected None for deleted key {} after compaction, got Some",
                key
            );
        }
    }

    #[test]
    fn test_compaction_stress() {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();

        // Initialize the appropriate database backend based on feature flags
        let keyspace = fjall::Config::new(temp_path)
            .cache_size(64 * 1024 * 1024) // 64MB cache for test
            .open()
            .expect("Failed to open test keyspace");

        // Create FuguDB instance
        let mut fugu_db = FuguDB::new(keyspace);

        // Initialize database
        fugu_db.init_db();

        // Create multiple trees for the test
        let trees = ["test_tree1", "test_tree2", "test_tree3"];
        let mut handles = Vec::new();

        for tree_name in &trees {
            let handle = fugu_db
                .open_tree(tree_name)
                .expect(&format!("Failed to open tree {}", tree_name));
            handles.push(handle);
        }

        // Spawn multiple threads to do concurrent writes and compactions
        let threads: Vec<_> = (0..3)
            .map(|thread_idx| {
                let mut db_clone = fugu_db.clone();
                let tree_handle = handles[thread_idx % 3].clone();
                let tree_name = trees[thread_idx % 3].to_string();

                thread::spawn(move || {
                    // Insert some data
                    for i in 0..100 {
                        let key = format!("thread{}_key_{}", thread_idx, i);
                        let value = format!("thread{}_value_{}", thread_idx, i);

                        tree_handle
                            .insert(&key, value.as_bytes())
                            .expect("Failed to insert in stress test");

                        // Occasionally perform maintenance operations
                        if i % 20 == 0 {
                            db_clone.maintenance().expect("Maintenance should succeed");
                        }

                        if i % 50 == 0 {
                            db_clone
                                .compact_tree(&tree_name)
                                .expect("Tree compaction should succeed");
                        }

                        // Small delay to allow other threads to run
                        thread::sleep(Duration::from_millis(1));
                    }

                    // Delete some data to create fragmentation
                    for i in 0..50 {
                        let key = format!("thread{}_key_{}", thread_idx, i);
                        tree_handle
                            .remove(&key)
                            .expect("Failed to remove in stress test");
                    }

                    // One final compaction
                    db_clone.compact().expect("Full compaction should succeed");
                })
            })
            .collect();

        // Wait for all threads to complete
        for thread in threads {
            thread.join().expect("Thread should complete successfully");
        }

        // Verify data integrity after all the concurrent operations
        for (idx, handle) in handles.iter().enumerate() {
            for i in 50..100 {
                // We only check keys that weren't deleted
                let key = format!("thread{}_key_{}", idx, i);
                let result = handle
                    .get(&key)
                    .expect("Failed to get value after stress test");

                assert!(
                    result.is_some(),
                    "Expected Some value for {} after stress test, got None",
                    key
                );

                let value_bytes = result.unwrap();
                let value_str =
                    std::str::from_utf8(&value_bytes).expect("Failed to convert bytes to string");

                assert_eq!(
                    value_str,
                    format!("thread{}_value_{}", idx, i),
                    "Retrieved value doesn't match inserted value for {} after stress test",
                    key
                );
            }
        }
    }
}
