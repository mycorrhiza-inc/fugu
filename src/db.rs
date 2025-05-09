use crate::tracing_utils;
use crate::{ObjectIndex, ObjectRecord, rkyv_adapter};
use crate::object::ArchivableObjectRecord;
use serde::{Deserialize, Serialize};
use sled;
use tokio::sync::mpsc;
use tracing::{Instrument, debug, error, info};

// Define constants for tree names
pub const PREFIX_FILTER_INDEX: &'static str = "FILTER_INDEX";
pub const PREFIX_RECORD_INDEX_TREE: &'static str = "_INDEX";
pub const TREE_FILTERS: &'static str = "FILTERS";
pub const TREE_RECORDS: &'static str = "RECORDS";
pub const TREE_GLOBAL_INDEX: &'static str = "INDEX";

/*
* OBJECT:
 * text: ""
 * metadata: {}
 * created_at: DATE
 * updated_at: DATE
*
*/

// Serializable types for database
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexPosition {
    pub key: String,
    pub position: usize,
}

impl IndexPosition {
    pub fn from_bytes(&self, _b: &[u8]) -> IndexPosition {
        todo!()
    }
    pub fn index(&self) -> String {
        self.key.clone()
    }
    pub fn position(&self) -> usize {
        self.position
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IndexVec {
    pub v: Vec<IndexPosition>,
}

pub struct FuguDB {
    pub db: sled::Db,
    pub to_compact_queue: Option<mpsc::Receiver<String>>, // queue of keys, Option to allow for sharing
    pub mailbox: mpsc::Sender<String>,
    // Using Option to allow for multiple instances to share the same channel
}

// Helper functions
pub fn serialize_positions(input: &Vec<usize>) -> Vec<u8> {
    match rkyv_adapter::serialize(input) {
        Ok(bytes) => bytes.to_vec(),
        Err(e) => {
            error!("Error serializing positions: {:?}", e);
            // Return an empty byte vector as fallback
            Vec::new()
        }
    }
}
pub fn deserialize_positions(input: &[u8]) -> Vec<usize> {
    match rkyv_adapter::deserialize::<Vec<usize>>(input) {
        Ok(positions) => positions,
        Err(e) => {
            error!("Error deserializing positions: {:?}", e);
            // Return an empty Vec as fallback
            Vec::new()
        }
    }
}
// Custom merge operator for token positions
pub fn merge_token_positions(
    _key: &[u8],
    old_value: Option<&[u8]>,
    new_value: &[u8],
) -> Option<Vec<u8>> {
    // Handle the case where old_value is None
    if old_value.is_none() {
        return Some(new_value.to_vec());
    }

    // Get the old value safely
    let old_value_bytes = match old_value {
        Some(bytes) => bytes,
        None => {
            // This shouldn't happen as we checked above, but just in case
            return Some(new_value.to_vec());
        }
    };

    // Safely attempt to deserialize old values
    let oldvec: Vec<u32> = match rkyv_adapter::deserialize::<Vec<u32>>(old_value_bytes) {
        Ok(vec) => vec,
        Err(e) => {
            // If deserialization fails, log the error and return only the new value
            error!("Error deserializing old value in merge: {:?}", e);
            return Some(new_value.to_vec());
        }
    };

    // Safely attempt to deserialize new values
    let newvec: Vec<u32> = match rkyv_adapter::deserialize::<Vec<u32>>(new_value) {
        Ok(vec) => vec,
        Err(e) => {
            // If deserialization fails, log the error and return only the old value
            error!("Error deserializing new value in merge: {:?}", e);
            return old_value.map(|v| v.to_vec());
        }
    };

    // Join the vectors
    let mut joined = oldvec.clone();
    joined.extend(newvec);

    // Safely serialize the merged values
    match rkyv_adapter::serialize(&joined) {
        Ok(bytes) => Some(bytes.to_vec()),
        Err(e) => {
            // If serialization fails, log the error and return null
            error!("Error serializing merged value: {:?}", e);
            None
        }
    }
}

// Implementation of FuguDB
impl FuguDB {
    // Initialize database trees
    pub fn init_db(&self) {
        let span = tracing_utils::db_span("init_trees");
        let _enter = span.enter();

        let trees = [TREE_GLOBAL_INDEX, TREE_FILTERS, TREE_RECORDS];

        for tree_name in trees.iter() {
            match self.db.open_tree(*tree_name) {
                Ok(tree) => {
                    if let Err(e) = tree.flush() {
                        error!("Failed to flush tree {}: {}", tree_name, e);
                    }
                }
                Err(e) => {
                    error!("Failed to open tree {}: {}", tree_name, e);
                }
            }
        }

        debug!("Initialized database trees");
    }

    /// Get an object by its ID
    ///
    /// This method retrieves an object from the RECORDS tree by its ID and deserializes it.
    /// Returns None if the object doesn't exist or if deserialization fails.
    ///
    /// If this is a dummy_item with serialization issues, it will automatically attempt to repair it.
    pub fn get(&self, object_id: &str) -> Option<ObjectRecord> {
        let span = tracing_utils::db_span("get_object");
        let _enter = span.enter();

        debug!("Retrieving object with ID: {}", object_id);

        // Get the RECORDS tree
        let records_tree = match self.db.open_tree(TREE_RECORDS) {
            Ok(tree) => tree,
            Err(e) => {
                error!("Failed to open RECORDS tree: {}", e);
                return None;
            }
        };

        // Try to get the record by ID
        match records_tree.get(object_id.as_bytes()) {
            Ok(Some(value)) => {
                // Try to deserialize the value using our adapter
                match rkyv_adapter::deserialize::<ArchivableObjectRecord>(&value) {
                    Ok(archivable) => {
                        // Convert to regular ObjectRecord
                        let record = ObjectRecord::from(archivable);
                        debug!("Successfully retrieved object with ID: {}", object_id);
                        Some(record)
                    }
                    Err(e) => {
                        error!("Failed to deserialize object {}: {}", object_id, e);

                        // If this is a dummy item with known format issues, try to repair it
                        if object_id.contains("dummy_item") {
                            info!("Attempting to repair corrupted dummy item: {}", object_id);

                            // Create a fresh dummy object with the same ID
                            let mut replacement = crate::create_dummy_object(Some(object_id.to_string()));

                            // Add auto_repaired flag to metadata
                            if let serde_json::Value::Object(ref mut map) = replacement.metadata {
                                map.insert("auto_repaired".to_string(), serde_json::Value::Bool(true));
                            }

                            // Convert to ArchivableObjectRecord for storage
                            let archivable = ArchivableObjectRecord::from(&replacement);

                            // Serialize it using our adapter
                            match rkyv_adapter::serialize(&archivable) {
                                Ok(serialized) => {
                                    // Try to replace the corrupted record
                                    if let Err(err) = records_tree.insert(object_id.as_bytes(), serialized.to_vec()) {
                                        error!("Failed to replace corrupted record: {}", err);
                                        None
                                    } else {
                                        info!("Successfully repaired corrupted record: {}", object_id);
                                        Some(replacement)
                                    }
                                }
                                Err(err) => {
                                    error!("Failed to serialize replacement record: {}", err);
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    }
                }
            }
            Ok(None) => {
                debug!("Object with ID '{}' not found", object_id);
                None
            }
            Err(e) => {
                error!("Error retrieving object {}: {}", object_id, e);
                None
            }
        }
    }

    // Create a new FuguDB instance with a new channel
    pub fn new(db: sled::Db) -> FuguDB {
        let span = tracing_utils::db_span("new");
        let _enter = span.enter();

        let (s, r) = mpsc::channel::<String>(1000);
        debug!("Created database channel with capacity 1000");

        FuguDB {
            db,
            to_compact_queue: Some(r),
            mailbox: s,
        }
    }

    // Create a FuguDB instance with existing sender but no receiver (for API server)
    pub fn new_api_instance(db: sled::Db, mailbox: mpsc::Sender<String>) -> FuguDB {
        let span = tracing_utils::db_span("new_api_instance");
        let _enter = span.enter();

        debug!("Created API database instance with shared channel");

        FuguDB {
            db,
            to_compact_queue: None, // API server doesn't need the receiver
            mailbox,
        }
    }

    // Create a FuguDB instance with existing channel for compactor
    pub fn new_compactor_instance(
        db: sled::Db,
        mailbox: mpsc::Sender<String>,
        queue: mpsc::Receiver<String>,
    ) -> FuguDB {
        let span = tracing_utils::db_span("new_compactor_instance");
        let _enter = span.enter();

        debug!("Created compactor database instance with shared channel");

        FuguDB {
            db,
            to_compact_queue: Some(queue),
            mailbox,
        }
    }

    // Get a cloned reference to the database
    pub fn db(&self) -> sled::Db {
        self.db.clone()
    }

    fn parse_metadata(&self, object: ObjectRecord) {
        let metadata = object.metadata;
    }

    pub fn add_record(object: ObjectRecord) {}

    // Index an object
    fn index_object(&self, object: ObjectIndex) -> String {
        // TODO: index metadata for filtering
        let span = tracing_utils::db_span("db_object_index");
        let _enter = span.enter();

        // Create a new tree for the object

        let treekey = format!("{}:{}", PREFIX_RECORD_INDEX_TREE, object.object_id);

        let newtree = match self.db().open_tree(treekey.clone()) {
            Ok(tree) => tree,
            Err(e) => {
                error!("Failed to open tree for object {}: {}", object.object_id, e);
                // Return early with the tree key, we can't proceed without a tree
                return treekey;
            }
        };

        let mut batch = sled::Batch::default();

        // Add each term to the batch
        for term in object.inverted_index.keys() {
            if let Some(positions) = object.inverted_index.get(term) {
                let _ = batch.insert(term.as_bytes(), serialize_positions(positions));
            } else {
                error!("Term not found in inverted index: {}", term);
            }
        }

        // Apply the batch
        // TODO: actually handle this
        match newtree.apply_batch(batch) {
            Ok(_) => treekey,
            Err(_) => treekey,
        }
    }

    // Public method to index an object and queue it for compaction
    pub fn index(&self, object: ObjectIndex) {
        let span = tracing_utils::db_span("db_index");
        let _enter = span.enter();

        let id = object.object_id.clone();
        let _ = self.index_object(object);
        self.enqueue_for_compact(vec![id]);
    }

    // Index a batch of objects
    pub fn batch_index(&self, objects: Vec<ObjectIndex>) {
        let span = tracing_utils::db_span("db_batch_index");
        let _enter = span.enter();

        let mut queue: Vec<String> = vec![];

        for i in objects {
            let id = i.object_id.clone();
            // Make sure that the tree exists
            let _ = self.index_object(i);
            queue.push(id);
        }

        info!("Batch insertion complete. Inserted {} records", queue.len());
        self.enqueue_for_compact(queue);
        debug!("Sent batch to compaction queue");
    }

    // Add objects to the compaction queue
    fn enqueue_for_compact(&self, objects: Vec<String>) {
        let span = tracing_utils::db_span("enqueue_for_compact");
        let _enter = span.enter();

        debug!("Enqueuing {} objects for compaction", objects.len());

        for object_id in objects {
            match self.mailbox.try_send(object_id.clone()) {
                Ok(_) => {
                    debug!(
                        "Successfully added object {} to compaction queue",
                        object_id
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    error!(
                        "Failed to add object to compaction queue (queue full): {}",
                        object_id.clone()
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    error!(
                        "Failed to add object to compaction queue (channel closed): {}",
                        object_id.clone()
                    );
                }
            }
        }
    }

    // Compact the database
    pub fn compact(&mut self) {
        // Drain the compaction queue if we have a receiver
        let mut objects_to_compact = Vec::new();

        if let Some(ref mut queue) = self.to_compact_queue {
            // Use try_recv in a loop to get all available items
            loop {
                match queue.try_recv() {
                    Ok(object_id) => {
                        info!("Received object {} for compaction", object_id);
                        objects_to_compact.push(object_id);
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        debug!("No more items in compaction queue");
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        error!("Compaction queue channel disconnected");
                        break;
                    }
                }
            }
        } else {
            debug!("This instance does not have a compaction queue receiver");
        }

        let objects_count = objects_to_compact.len();
        if objects_count <= 0 {
            debug!("No objects to compact");
            return;
        }

        // Create a span for the compaction operation
        let span = tracing_utils::compactor_span("compact", Some(objects_count));
        let _enter = span.enter();
        info!("Running compactor! Compacting {} objects", objects_count);

        // Process each object
        for object_id in objects_to_compact {
            // Open the unprocessed tree
            let unprocessed = match self.db().open_tree(format!(
                "{}:{}",
                PREFIX_RECORD_INDEX_TREE,
                object_id.clone()
            )) {
                Ok(tree) => tree,
                Err(e) => {
                    error!("Failed to open tree for compaction: {}", e);
                    continue;
                }
            };

            // Iterate through the tree and merge terms
            let mut iter = unprocessed.iter();
            loop {
                match iter.next() {
                    Some(Ok((key, val))) => {
                        // Open the term tree
                        let term_tree = match self.db().open_tree(&key) {
                            Ok(tree) => tree,
                            Err(e) => {
                                error!("Failed to open term tree: {}", e);
                                continue;
                            }
                        };

                        // Set the merge operator
                        term_tree.set_merge_operator(merge_token_positions);

                        // Merge the value
                        if let Err(e) = term_tree.merge(object_id.clone(), val) {
                            error!("Failed to merge term: {}", e);
                        }
                    }
                    Some(Err(e)) => {
                        error!("Error during compaction: {}", e);
                        break;
                    }
                    None => break,
                }
            }

            info!("Compacted object: {}", object_id);
        }
    }
}

// A background compaction service
pub async fn run_compactor(
    mut db: FuguDB,
    mut shutdown_signal: tokio::sync::oneshot::Receiver<()>,
) {
    let span = tracing_utils::compactor_span("service", None);

    async {
        info!("Starting compactor service");

        // Create an interval for periodic compaction
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(200));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Run the compaction cycle
                    debug!("Running scheduled compaction cycle");

                    // Run compaction and log any issues
                    db.compact();
                }
                _ = &mut shutdown_signal => {
                    info!("Received shutdown signal for compactor service");

                    // Final compaction cycle before shutdown to ensure all items are processed
                    info!("Running final compaction cycle before shutdown");
                    db.compact();

                    break;
                }
            }
        }

        info!("Compactor service shut down");
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_get_object() {
        // Create a temporary directory for the test database
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let temp_path = temp_dir.path().to_str().unwrap();

        // Open a test database
        let db = sled::open(temp_path).expect("Failed to open test database");
        let fugu_db = FuguDB::new(db);

        // Initialize database trees
        fugu_db.init_db();

        // Create a test object
        let test_id = "test_object_1";
        let test_object = crate::ObjectRecord {
            id: test_id.to_string(),
            text: "This is a test object".to_string(),
            metadata: json!({
                "test": true,
                "purpose": "unit testing"
            }),
        };

        // Get the RECORDS tree and insert the test object
        let records_tree = fugu_db.db().open_tree(TREE_RECORDS).expect("Failed to open RECORDS tree");
        let archivable = ArchivableObjectRecord::from(&test_object);
        let serialized = rkyv_adapter::serialize(&archivable).expect("Failed to serialize test object");
        records_tree.insert(test_id.as_bytes(), serialized.to_vec()).expect("Failed to insert test object");

        // Test the get method
        let retrieved_object = fugu_db.get(test_id);
        assert!(retrieved_object.is_some(), "Failed to retrieve test object");

        let retrieved_object = retrieved_object.unwrap();
        assert_eq!(retrieved_object.id, test_id, "Retrieved object ID doesn't match");
        assert_eq!(retrieved_object.text, "This is a test object", "Retrieved object text doesn't match");

        // Test retrieving a non-existent object
        let non_existent = fugu_db.get("non_existent_id");
        assert!(non_existent.is_none(), "Non-existent object should return None");
    }
}
