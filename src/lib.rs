pub mod cli;
pub mod db;
pub mod object;
pub mod query;
pub mod query_endpoints;
pub mod server;
// pub mod time_index;
#[cfg(test)]
mod query_test;
pub mod rkyv_adapter;
pub mod tracing_utils;

pub use object::{ObjectIndex, ObjectRecord};
pub use query::{QueryConfig, QueryEngine, QueryHit, QueryResults};

use serde_json::json;
use tracing::{debug, info};

// Function to create a demo object with lorem ipsum content
pub fn create_dummy_object(id: Option<String>) -> ObjectRecord {
    let object_id = id.unwrap_or_else(|| "demo_object".to_string());
    debug!("Creating demo object with ID: {}", object_id);

    // Create sample content with lorem ipsum text
    let text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam auctor, \
                  nisl nec ultricies lacinia, nisl nisl aliquet nisl, nec aliquet nisl nisl \
                  sit amet nisl. Donec euismod, nisl nec ultricies lacinia, nisl nisl aliquet \
                  nisl, nec aliquet nisl nisl sit amet nisl. Donec euismod, nisl nec ultricies \
                  lacinia, nisl nisl aliquet nisl, nec aliquet nisl nisl sit amet nisl. \
                  Phasellus vestibulum lorem sed risus ultricies tristique. Nam eget dui. \
                  Etiam rhoncus. Maecenas tempus, tellus eget condimentum rhoncus, sem quam \
                  semper libero, sit amet adipiscing sem neque sed ipsum. Nam quam nunc, blandit \
                  vel, luctus pulvinar, hendrerit id, lorem. Maecenas nec odio et ante tincidunt \
                  tempus. Donec vitae sapien ut libero venenatis faucibus. Nullam quis ante.";

    // Add some sample metadata with proper json object structure
    // This avoids serde_json::Value from using complex dynamic types internally
    let metadata = json!({
        "type": "demo",
        "created_at": chrono::Utc::now().to_rfc3339(),
        "language": "lorem-ipsum",
        "word_count": text.split_whitespace().count().to_string()
    });

    info!(
        "Created demo object with ID: {} and {} metadata fields",
        object_id,
        metadata.as_object().unwrap().len()
    );

    ObjectRecord {
        id: object_id,
        text: text.to_string(),
        metadata,
    }
}
