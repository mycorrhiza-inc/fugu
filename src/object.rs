use serde::{Deserialize, Serialize};
use serde_json::Value;
use tantivy::schema::{FAST, FacetOptions, STORED, STRING, SchemaBuilder, TEXT};

#[derive(Serialize, Deserialize, Clone)]
pub struct ObjectRecord {
    pub id: String,
    //pub name: String,
    pub text: String,
    // pub date_created: i64,
    // pub date_upated: i64,
    // pub date_published: Option<i64>,
    pub metadata: Value,
    //pub data: Option<Value>,
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
                .unwrap_or(serde_json::Value::Null),
        }
    }
}
