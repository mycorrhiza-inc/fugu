use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap as StandardHashMap;

#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
pub struct ObjectIndex {
    pub object_id: String,
    pub field_name: String,
    pub inverted_index: StandardHashMap<String, Vec<usize>>, // term : positions
}
// Structure for a object that can be indexed
// Note: We can't directly derive Archive for a struct containing serde_json::Value
// So we'll implement serialization/deserialization for this type separately
#[derive(Serialize, Deserialize, Clone)]
pub struct ObjectRecord {
    pub id: String,
    pub text: String,
    pub metadata: Value,
}

// Define an archivable version without serde_json::Value
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Serialize, Deserialize)]
pub struct ArchivableObjectRecord {
    pub id: String,
    pub text: String,
    pub metadata_json: String, // Store serialized JSON as string
}

impl From<&ObjectRecord> for ArchivableObjectRecord {
    fn from(record: &ObjectRecord) -> Self {
        Self {
            id: record.id.clone(),
            text: record.text.clone(),
            metadata_json: record.metadata.to_string(),
        }
    }
}

impl From<ArchivableObjectRecord> for ObjectRecord {
    fn from(record: ArchivableObjectRecord) -> Self {
        Self {
            id: record.id,
            text: record.text,
            metadata: serde_json::from_str(&record.metadata_json)
                .unwrap_or_else(|_| serde_json::Value::Null),
        }
    }
}
