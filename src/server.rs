use crate::db::{
    FuguDB, PREFIX_FILTER_INDEX, PREFIX_RECORD_INDEX_TREE, TREE_FILTERS, TREE_GLOBAL_INDEX,
    TREE_RECORDS,
};
use crate::object::ArchivableObjectRecord;
use crate::query_endpoints;
use crate::{ObjectIndex, ObjectRecord, rkyv_adapter, tracing_utils};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_map::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::signal;
use tracing::{Instrument, debug, error, info};

// Define a shared application state for dependency injection
pub struct AppState {
    // Using Arc for shared ownership across handlers, but no Mutex since only the compactor writes
    pub db: Arc<FuguDB>,
}

/// A default error response for most API errors.
#[derive(Debug, Serialize)]
pub struct AppError {
    /// An error message.
    pub error: String,

    /// A unique error ID. Not serialized in response.
    #[serde(skip)]
    pub status: StatusCode,

    /// Optional Additional error details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_details: Option<Value>,
}

impl AppError {
    pub fn new(error: &str) -> Self {
        Self {
            error: error.to_string(),
            status: StatusCode::BAD_REQUEST,
            error_details: None,
        }
    }

    pub fn with_status(mut self, status: StatusCode) -> Self {
        self.status = status;
        self
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.error_details = Some(details);
        self
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status;
        let mut res = Json(self).into_response();
        *res.status_mut() = status;
        res
    }
}

#[derive(Debug, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(derive(Debug))]
struct Pagination {
    page: Option<usize>,
    per_page: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(derive(Debug))]
struct FuguSearchQuery {
    query: String,
    namespace: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct AddFileQuery {
    name: String,
    namespace: Option<String>,
    body: String,
}

#[derive(Serialize, Deserialize)]
pub struct DemoIndexRequest {
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct IndexRequest {
    data: ObjectRecord,
}

#[derive(Serialize, Deserialize)]
pub struct BatchIndexRequest {
    objects: Vec<ObjectRecord>,
}

#[derive(Serialize, Deserialize)]
pub struct FileIngestionRequest {
    file_path: String,
    id: Option<String>,
    namespace: Option<String>,
    metadata: Option<Value>,
}

/// Health check endpoint
async fn health() -> &'static str {
    let span = tracing_utils::server_span("/health", "GET");
    let _guard = span.enter();

    debug!("Health check endpoint called");
    "OK"
}

/// Search all namespaces
async fn search(Json(payload): Json<FuguSearchQuery>) -> Json<Value> {
    let span = tracing_utils::server_span("/search", "POST");
    let _guard = span.enter();

    debug!("Search endpoint called with query: {}", payload.query);

    Json(json!({
        "query": payload.query,
        "namespace": payload.namespace
    }))
}

// quick and dirty positional tokenizer
fn word_positions(text: &str) -> impl Iterator<Item = (&str, usize)> {
    let mut start = None;
    text.char_indices()
        .chain(Some((text.len(), ' ')))
        .filter_map(move |(i, c)| {
            if !c.is_whitespace() {
                if start.is_none() {
                    start = Some(i);
                }
                None
            } else {
                if let Some(s) = start {
                    start = None;
                    Some((&text[s..i], s))
                } else {
                    None
                }
            }
        })
}

// Helper function to check if serde_json::Value is effectively empty
fn is_value_empty(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => true,
        serde_json::Value::Bool(false) => false, // false is not empty
        serde_json::Value::Bool(true) => false,  // false is not empty
        serde_json::Value::Number(n) => n.as_f64().map(|x| x == 0.0).unwrap_or(false), // 0 is not empty
        serde_json::Value::String(s) => s.is_empty(),
        serde_json::Value::Array(arr) => arr.is_empty(),
        serde_json::Value::Object(obj) => obj.is_empty(),
    }
}
/// Ingest a single object into the database
async fn ingest_object(
    State(state): State<Arc<AppState>>,
    Json(mut payload): Json<IndexRequest>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/ingest", "POST");
    let _guard = span.enter();

    debug!("Ingest endpoint called with ID: {:?}", payload.data.id);

    // Extract and normalize fields
    let mut object = payload.data;
    let object_id = object.id.clone();

    // Check if the ID is valid
    if object_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Object ID cannot be empty",
                "status": "error"
            })),
        );
    }

    // Check if the object already exists in the database
    let records_handle = match state.db.open_tree(crate::db::TREE_RECORDS) {
        Ok(handle) => handle,
        Err(e) => {
            error!("Failed to open RECORDS collection: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to open RECORDS collection: {}", e),
                    "status": "error"
                })),
            );
        }
    };

    let existing_object = if let Ok(Some(data)) = records_handle.get(&object_id) {
        debug!(
            "Object with ID '{}' already exists, will be updated",
            object_id
        );
        match rkyv_adapter::deserialize::<ArchivableObjectRecord>(&data) {
            Ok(archivable) => {
                debug!("Successfully deserialized existing object");
                Some(archivable)
            }
            Err(e) => {
                debug!("Could not deserialize existing object: {}", e);
                None
            }
        }
    } else {
        None
    };

    // Create the base inverted index for text content
    let mut inverted_indexes: HashMap<String, HashMap<String, Vec<usize>>> = HashMap::new();

    // Main text index
    let mut text_index: HashMap<String, Vec<usize>> = HashMap::new();

    // Split the content into words and create a simple inverted index
    for (word, pos) in word_positions(&object.text) {
        match text_index.entry(word.to_string()) {
            Occupied(mut o) => {
                o.get_mut().push(pos);
            }
            Vacant(v) => {
                v.insert(vec![pos]);
            }
        };
    }

    // Add the main text index to our collection of indexes
    inverted_indexes.insert("_text".to_string(), text_index);

    // Process metadata to extract additional fields and create indexes
    let additional_fields = process_additional_fields(&object);

    // If we have additional fields, merge them into metadata
    // If we have additional fields, merge them into metadata
    if !is_value_empty(&additional_fields) {
        // Create indexes for each additional field using depth-first traversal
        create_field_indexes(&additional_fields, String::new(), &mut inverted_indexes);

        // Merge additional fields into metadata
        merge_into_metadata(&mut object.metadata, additional_fields);
    }

    info!(
        "Created ObjectIndex with ID: {}, containing {} field indexes",
        object_id,
        inverted_indexes.len()
    );
    let indexes_clone = inverted_indexes.clone();

    // Keep a copy of the metadata for the response
    let metadata = object.metadata.clone();
    let text_length = object.text.len();

    // Create variables for response
    let object_id_for_response = object_id.clone();
    let operation_type = if existing_object.is_some() {
        "update"
    } else {
        "create"
    };

    // Clone the object for background task
    let object_clone = object.clone();

    // Create multiple ObjectIndex instances for the database - one for each field
    let mut object_indexes = Vec::new();

    for (field_name, inverted_index) in inverted_indexes {
        object_indexes.push(ObjectIndex {
            object_id: object_id.clone(),
            field_name,
            inverted_index,
        });
    }

    // Get a clone of the shared DB state for the background task
    let db_state = state.db.clone();

    // Add the object to the database in a background task
    tokio::spawn(async move {
        // Log that we're starting the database operations
        info!("Adding object to database: {}", object_id);

        // Queue each field index for indexing
        for object_index in object_indexes {
            db_state.index(object_index);
        }

        // Add the record to the RECORDS tree
        info!("Adding object to RECORDS tree: {}", object_id);
        let records_handle = match db_state.open_tree(crate::db::TREE_RECORDS) {
            Ok(handle) => handle,
            Err(e) => {
                error!("Failed to open RECORDS collection: {}", e);
                return;
            }
        };

        // Convert to archivable format and serialize
        let archivable = ArchivableObjectRecord::from(&object_clone);
        match rkyv_adapter::serialize(&archivable) {
            Ok(serialized) => {
                // Insert the record using our abstracted handle
                let object_id_clone = object_id.clone();
                if let Err(e) = records_handle.insert(object_id_clone, serialized.to_vec()) {
                    error!("Failed to insert record into RECORDS collection: {}", e);
                } else {
                    info!(
                        "Successfully added record to RECORDS collection: {}",
                        object_id
                    );
                }
            }
            Err(e) => {
                error!("Failed to serialize record: {}", e);
            }
        }

        // Log successful completion
        info!("Successfully queued object for indexing: {}", object_id);
    });

    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "message": if existing_object.is_some() { "Object successfully updated and queued for indexing" } else { "Object successfully created and queued for indexing" },
            "object_id": object_id_for_response,
            "operation": operation_type,
            "indexed_fields": indexes_clone.keys().collect::<Vec<_>>(),
            "text_length": text_length,
            "metadata": metadata,
        })),
    )
}

// Extract additional fields from JSON payload that aren't id, text, or metadata
fn process_additional_fields(object_record: &ObjectRecord) -> serde_json::Value {
    // Deserialize the entire object to a Value
    let serialized = serde_json::to_string(object_record).unwrap_or_default();
    let mut value: serde_json::Value =
        serde_json::from_str(&serialized).unwrap_or(serde_json::Value::Null);

    if let serde_json::Value::Object(ref mut map) = value {
        // Remove the standard fields
        map.remove("id");
        map.remove("text");
        map.remove("metadata");

        // Return the remaining fields as a new Value
        serde_json::Value::Object(map.clone())
    } else {
        serde_json::Value::Object(serde_json::Map::new())
    }
}

// Merge additional fields into metadata
fn merge_into_metadata(metadata: &mut serde_json::Value, additional_fields: serde_json::Value) {
    if let serde_json::Value::Object(meta_map) = metadata {
        if let serde_json::Value::Object(add_map) = additional_fields {
            // Transfer all fields from additional fields to metadata
            for (key, value) in add_map {
                meta_map.insert(key, value);
            }
        }
    } else {
        // If metadata wasn't an object, replace it with additional fields
        *metadata = additional_fields;
    }
}

// Recursively create indexes for all fields depth-first
fn create_field_indexes(
    value: &serde_json::Value,
    prefix: String,
    indexes: &mut HashMap<String, HashMap<String, Vec<usize>>>,
) {
    match value {
        serde_json::Value::Object(map) => {
            // Process each field in the object
            for (key, val) in map {
                let field_path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };

                // Recursively process nested objects
                create_field_indexes(val, field_path.clone(), indexes);

                // Create index for this field's string representation
                let field_str = val.to_string();
                if !field_str.is_empty() {
                    let mut field_index: HashMap<String, Vec<usize>> = HashMap::new();

                    // For string values, create term-position pairs
                    if let serde_json::Value::String(s) = val {
                        for (word, pos) in word_positions(s) {
                            match field_index.entry(word.to_string()) {
                                Occupied(mut o) => {
                                    o.get_mut().push(pos);
                                }
                                Vacant(v) => {
                                    v.insert(vec![pos]);
                                }
                            };
                        }
                    } else {
                        // For non-string values, just index the whole value as a term
                        field_index.insert(field_str, vec![0]);
                    }

                    // Only add non-empty indexes
                    if !field_index.is_empty() {
                        indexes.insert(field_path, field_index);
                    }
                }
            }
        }
        serde_json::Value::Array(arr) => {
            // Process each item in the array
            for (i, item) in arr.iter().enumerate() {
                let item_path = format!("{}[{}]", prefix, i);
                create_field_indexes(item, item_path, indexes);
            }
        }
        _ => {
            // For primitive values, index the string representation
            let field_str = value.to_string();
            if !field_str.is_empty() && !prefix.is_empty() {
                let mut field_index = HashMap::new();
                field_index.insert(field_str, vec![0]);
                indexes.insert(prefix, field_index);
            }
        }
    }
}

/// Ingest multiple objects in a batch
async fn batch_ingest(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BatchIndexRequest>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/batch-ingest", "POST");
    let _guard = span.enter();

    debug!(
        "Batch ingest endpoint called with {} objects",
        payload.objects.len()
    );

    // Validate the batch
    if payload.objects.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Batch cannot be empty",
                "status": "error"
            })),
        );
    }

    // Prepare vectors to track objects and their indices
    let mut object_indices = Vec::with_capacity(payload.objects.len());
    let mut summary = Vec::with_capacity(payload.objects.len());
    let mut error_count = 0;

    // Open the RECORDS tree once for checking existing objects
    let records_handle = match state.db.open_tree(crate::db::TREE_RECORDS) {
        Ok(handle) => handle,
        Err(e) => {
            error!("Failed to open RECORDS collection: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to open RECORDS collection: {}", e),
                    "status": "error"
                })),
            );
        }
    };

    // Process each object in the batch
    for object in &payload.objects {
        let object_id = object.id.clone();

        // Skip objects with empty IDs
        if object_id.is_empty() {
            error_count += 1;
            summary.push(json!({
                "id": null,
                "status": "error",
                "error": "Object ID cannot be empty"
            }));
            continue;
        }

        // Check if the object already exists
        let operation = if let Ok(Some(_)) = records_handle.get(&object_id) {
            debug!(
                "Object with ID '{}' already exists, will be updated",
                object_id
            );
            "update"
        } else {
            debug!("Object with ID '{}' is new", object_id);
            "create"
        };

        // Create inverted index for the object
        let mut inverted_index: HashMap<String, Vec<usize>> = std::collections::HashMap::new();

        // Generate position data for each word
        for (word, pos) in word_positions(&object.text) {
            match inverted_index.entry(word.to_string()) {
                Occupied(mut o) => {
                    o.get_mut().push(pos);
                }
                Vacant(v) => {
                    v.insert(vec![pos]);
                }
            };
        }

        // Create ObjectIndex instance
        let object_index = ObjectIndex {
            object_id: object_id.clone(),
            field_name: "text".to_string(),
            inverted_index: inverted_index.clone(),
        };

        // Add to results vector
        object_indices.push(object_index);

        // Add to summary
        summary.push(json!({
            "id": object_id,
            "status": "success",
            "operation": operation,
            "unique_terms": inverted_index.len(),
            "text_length": object.text.len()
        }));
    }

    // Get a clone of the shared DB state for the background task
    let db_state = state.db.clone();

    // Clone the objects for storage in the background task
    let objects_for_storage = payload.objects.clone();

    // Process the batch in a background task
    tokio::spawn(async move {
        // Index all objects
        info!("Batch indexing {} objects", object_indices.len());
        db_state.batch_index(object_indices);

        // Store all objects in the RECORDS tree
        info!("Adding objects to RECORDS tree");
        let records_handle = match db_state.open_tree(crate::db::TREE_RECORDS) {
            Ok(handle) => handle,
            Err(e) => {
                error!("Failed to open RECORDS collection: {}", e);
                return;
            }
        };

        // Process each object
        for object in objects_for_storage {
            // Convert to archivable format and serialize
            let archivable = ArchivableObjectRecord::from(&object);
            match rkyv_adapter::serialize(&archivable) {
                Ok(serialized) => {
                    // Insert the record into the tree
                    let id_clone = object.id.clone();
                    if let Err(e) = records_handle.insert(id_clone, serialized.to_vec()) {
                        error!("Failed to insert record into RECORDS collection: {}", e);
                    } else {
                        debug!(
                            "Successfully added record to RECORDS collection: {}",
                            object.id
                        );
                    }
                }
                Err(e) => {
                    error!("Failed to serialize record {}: {}", object.id, e);
                }
            }
        }

        info!("Batch processing completed");
    });

    // Return summary response
    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "message": "Batch successfully queued for indexing",
            "total_objects": payload.objects.len(),
            "processed": summary.len() - error_count,
            "errors": error_count,
            "objects": summary
        })),
    )
}

/// Ingest content from a file into the database
async fn ingest_file(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<FileIngestionRequest>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/ingest-file", "POST");
    let _guard = span.enter();

    debug!(
        "File ingest endpoint called for file: {}",
        payload.file_path
    );

    // Try to read the file
    let file_content = match tokio::fs::read_to_string(&payload.file_path).await {
        Ok(content) => content,
        Err(e) => {
            error!("Failed to read file {}: {}", payload.file_path, e);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Failed to read file: {}", e),
                    "status": "error"
                })),
            );
        }
    };

    // Generate ID if not provided
    let object_id = match payload.id {
        Some(id) if !id.is_empty() => id,
        _ => {
            // Use filename as ID if none provided
            let path = std::path::Path::new(&payload.file_path);
            match path.file_name() {
                Some(name) => match name.to_str() {
                    Some(name_str) => name_str.to_string(),
                    None => {
                        // Generate timestamp-based ID if filename has invalid UTF-8
                        let timestamp = chrono::Utc::now().timestamp();
                        format!("file_{}", timestamp)
                    }
                },
                None => {
                    // Generate timestamp-based ID if no filename
                    let timestamp = chrono::Utc::now().timestamp();
                    format!("file_{}", timestamp)
                }
            }
        }
    };

    // Check if the object already exists in the database
    let records_handle = match state.db.open_tree(crate::db::TREE_RECORDS) {
        Ok(handle) => handle,
        Err(e) => {
            error!("Failed to open RECORDS collection: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Failed to open RECORDS collection: {}", e),
                    "status": "error"
                })),
            );
        }
    };

    let operation = if let Ok(Some(_)) = records_handle.get(&object_id) {
        debug!(
            "File object with ID '{}' already exists, will be updated",
            object_id
        );
        "update"
    } else {
        debug!("File object with ID '{}' is new", object_id);
        "create"
    };

    // Prepare metadata
    let mut metadata = match payload.metadata {
        Some(meta) => meta,
        None => serde_json::json!({}),
    };

    // Add file info to metadata
    if let serde_json::Value::Object(ref mut map) = metadata {
        map.insert(
            "file_path".to_string(),
            serde_json::Value::String(payload.file_path.clone()),
        );

        map.insert(
            "ingested_at".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );

        // Add namespace if provided
        if let Some(namespace) = payload.namespace {
            map.insert(
                "namespace".to_string(),
                serde_json::Value::String(namespace),
            );
        }

        // Add file size
        map.insert(
            "file_size_bytes".to_string(),
            serde_json::Value::Number(serde_json::Number::from(file_content.len() as u64)),
        );
    }

    // Create the ObjectRecord
    let object = ObjectRecord {
        id: object_id.clone(),
        text: file_content,
        metadata: metadata.clone(),
    };

    // Save copies for later use
    let object_id_for_response = object_id.clone();
    let operation_type_for_response = operation.to_string();
    let text_length = object.text.len();

    // Create inverted index
    let mut inverted_index: HashMap<String, Vec<usize>> = std::collections::HashMap::new();

    // Generate position data for each word
    for (word, pos) in word_positions(&object.text) {
        match inverted_index.entry(word.to_string()) {
            Occupied(mut o) => {
                o.get_mut().push(pos);
            }
            Vacant(v) => {
                v.insert(vec![pos]);
            }
        };
    }

    // Create the ObjectIndex
    let object_index = ObjectIndex {
        object_id: object_id.clone(),
        field_name: "text".to_string(),
        inverted_index: inverted_index.clone(),
    };

    // Save the number of terms for response
    let unique_term_count = inverted_index.len();

    info!(
        "Created ObjectIndex from file with ID: {}, containing {} unique terms",
        object_id,
        inverted_index.len()
    );

    // Get a clone of the shared DB state for the background task
    let db_state = state.db.clone();

    // Clone object for background task
    let object_clone = object.clone();

    // Index and store the object in a background task
    tokio::spawn(async move {
        // Log that we're starting the database operations
        info!("Adding file object to database: {}", object_id);

        // Queue the object for indexing
        db_state.index(object_index);

        // Add the record to the RECORDS tree
        info!("Adding file object to RECORDS tree: {}", object_id);
        let records_handle = match db_state.open_tree(crate::db::TREE_RECORDS) {
            Ok(handle) => handle,
            Err(e) => {
                error!("Failed to open RECORDS collection: {}", e);
                return;
            }
        };

        // Convert to archivable format and serialize
        let archivable = ArchivableObjectRecord::from(&object_clone);
        match rkyv_adapter::serialize(&archivable) {
            Ok(serialized) => {
                // Insert the record into the tree
                let obj_id_clone = object_id.clone();
                if let Err(e) = records_handle.insert(obj_id_clone, serialized.to_vec()) {
                    error!("Failed to insert record into RECORDS collection: {}", e);
                } else {
                    info!(
                        "Successfully added file record to RECORDS tree: {}",
                        object_id
                    );
                }
            }
            Err(e) => {
                error!("Failed to serialize file record: {}", e);
            }
        }

        info!("Successfully queued file for indexing: {}", object_id);
    });

    // Return success response
    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "message": if operation_type_for_response == "update" { "File successfully updated and queued for indexing" } else { "File successfully created and queued for indexing" },
            "object_id": object_id_for_response,
            "operation": operation_type_for_response,
            "unique_terms": unique_term_count,
            "text_length": text_length,
            "metadata": metadata,
        })),
    )
}

/// Search in a specific namespace
async fn search_namespace(
    Path(namespace): Path<String>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/search/{}", namespace), "POST");
    let _guard = span.enter();

    debug!(
        "Namespace search endpoint called for namespace: {}",
        namespace
    );

    Json(json!({"namespace": namespace, "payload": payload}))
}

/// List all namespaces
async fn list_namespaces() -> Json<Value> {
    let span = tracing_utils::server_span("/namespaces", "GET");
    let _guard = span.enter();

    debug!("List namespaces endpoint called");

    Json(json!({"message":"unimplemented"}))
}

/// Get a specific object by ID
async fn get_object_by_id(
    State(state): State<Arc<AppState>>,
    Path(object_id): Path<String>,
) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/objects/{}", object_id), "GET");
    let _guard = span.enter();

    debug!("Get object endpoint called for ID: {}", object_id);

    // Use the get method to retrieve the object (it now handles auto-repair for dummy items)
    match state.db.get(&object_id) {
        Some(record) => {
            // Return the full record with a note if it was auto-repaired
            let was_repaired =
                record.id.contains("dummy_item") && record.metadata.get("auto_repaired").is_some();

            if was_repaired {
                Json(json!({
                    "id": record.id,
                    "metadata": record.metadata,
                    "text": record.text,
                    "text_length": record.text.len(),
                    "note": "This record was automatically repaired due to serialization issues"
                }))
            } else {
                Json(json!({
                    "id": record.id,
                    "metadata": record.metadata,
                    "text": record.text,
                    "text_length": record.text.len()
                }))
            }
        }
        None => {
            // Return error if record doesn't exist or there was a deserialization issue
            Json(json!({
                "error": format!("Object with ID '{}' not found", object_id)
            }))
        }
    }
}

/// List filters for a specific namespace
async fn list_namespace_filters(Path(namespace): Path<String>) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/filters/{}", namespace), "GET");
    let _guard = span.enter();

    debug!(
        "List namespace filters endpoint called for namespace: {}",
        namespace
    );

    Json(json!({"namespace": namespace }))
}

/// Add a file to a namespace
async fn add_file(Path(namespace): Path<String>, Json(payload): Json<Value>) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/add/{}", namespace), "POST");
    let _guard = span.enter();

    debug!("Add file endpoint called for namespace: {}", namespace);

    Json(json!({"namespace": namespace, "payload": payload}))
}

/// List all terms in the index for a specific object
async fn list_object_terms(
    State(state): State<Arc<AppState>>,
    Path(object_id): Path<String>,
) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/objects/{}/terms", object_id), "GET");
    let _guard = span.enter();

    debug!("List terms endpoint called for object ID: {}", object_id);

    // Get a reference to the db
    let db_handle = state.db.clone();

    // Construct the tree name for the object index
    let tree_name = format!("{}:{}", PREFIX_RECORD_INDEX_TREE, object_id);

    // Try to open the tree for the object
    let object_handle = match db_handle.open_tree(&tree_name) {
        Ok(handle) => handle,
        Err(e) => {
            error!("Failed to open tree for object {}: {}", object_id, e);
            return Json(json!({
                "error": format!("Failed to open tree for object {}: {}", object_id, e)
            }));
        }
    };

    // Collect all terms and their positions
    let mut terms = Vec::new();

    // Iterate through all terms in the tree
    let iter = match object_handle.iter() {
        Ok(it) => it,
        Err(e) => {
            error!("Failed to create iterator for object partition: {}", e);
            return Json(json!({
                "error": format!("Failed to create iterator for object partition: {}", e)
            }));
        }
    };
    for item in iter {
        match item {
            Ok((key, value)) => {
                // Try to convert key to string (term)
                if let Ok(term) = std::str::from_utf8(&key) {
                    // Try to deserialize value to positions
                    match rkyv_adapter::deserialize::<Vec<usize>>(&value) {
                        Ok(positions) => {
                            terms.push(json!({
                                "term": term,
                                "positions": positions,
                                "count": positions.len()
                            }));
                        }
                        Err(e) => {
                            // Log error but continue with next term
                            error!("Failed to deserialize positions for term {}: {}", term, e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error iterating terms: {}", e);
            }
        }
    }

    // Sort terms by frequency (count) in descending order
    terms.sort_by(|a, b| {
        let a_count = a.get("count").and_then(|c| c.as_u64()).unwrap_or(0);
        let b_count = b.get("count").and_then(|c| c.as_u64()).unwrap_or(0);
        b_count.cmp(&a_count)
    });

    Json(json!({
        "object_id": object_id,
        "total_terms": terms.len(),
        "terms": terms
    }))
}

/// List all terms across all objects in the database
async fn list_global_terms(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/terms", "GET");
    let _guard = span.enter();

    debug!("Global terms endpoint called");

    // Get a reference to the db
    let db_handle = state.db.clone();

    // We'll try to get the names of all objects
    let mut all_object_trees = Vec::new();

    // We need a helper function to get tree names depending on which backend we're using
    // This will be different depending on which backend is active

    let tree_names_result = db_handle.partition_names();

    let tree_names = tree_names_result
        .into_iter()
        .map(|name| name.into_bytes())
        .collect::<Vec<_>>();

    // Iterate through all trees to find objects
    for name in tree_names {
        if let Ok(name_str) = std::str::from_utf8(&name) {
            if name_str.starts_with(PREFIX_RECORD_INDEX_TREE) {
                // This is an object index tree
                let object_id =
                    name_str.trim_start_matches(&format!("{}:", PREFIX_RECORD_INDEX_TREE));
                all_object_trees.push(object_id.to_string());
            }
        }
    }

    // Create a combined map of all terms
    let mut term_stats: std::collections::HashMap<String, Value> = std::collections::HashMap::new();

    // Iterate through all object trees
    for object_id in all_object_trees {
        let tree_name = format!("{}:{}", PREFIX_RECORD_INDEX_TREE, object_id);

        match db_handle.open_tree(&tree_name) {
            Ok(tree_handle) => {
                // Process each term in the tree
                let term_iter = match tree_handle.iter() {
                    Ok(it) => it,
                    Err(e) => {
                        error!("Failed to create iterator for tree {}: {}", tree_name, e);
                        continue;
                    }
                };
                for item in term_iter {
                    match item {
                        Ok((key, value)) => {
                            if let Ok(term) = std::str::from_utf8(&key) {
                                if let Ok(positions) =
                                    rkyv_adapter::deserialize::<Vec<usize>>(&value)
                                {
                                    // Update stats for this term
                                    if let Some(existing) = term_stats.get_mut(term) {
                                        // Term already exists, update count
                                        let count = existing["doc_count"].as_u64().unwrap_or(0) + 1;
                                        let total_occurrences =
                                            existing["total_occurrences"].as_u64().unwrap_or(0)
                                                + positions.len() as u64;

                                        // Update values
                                        *existing = json!({
                                            "term": term,
                                            "doc_count": count,
                                            "total_occurrences": total_occurrences,
                                            "objects": existing["objects"].as_array().unwrap_or(&Vec::new())
                                                .iter()
                                                .chain(std::iter::once(&json!(object_id)))
                                                .map(|v| v.clone())
                                                .collect::<Vec<_>>()
                                        });
                                    } else {
                                        // New term
                                        term_stats.insert(
                                            term.to_string(),
                                            json!({
                                                "term": term,
                                                "doc_count": 1,
                                                "total_occurrences": positions.len(),
                                                "objects": [object_id]
                                            }),
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error iterating terms: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to open tree {}: {}", tree_name, e);
            }
        }
    }

    // Convert the map to a vector for sorting
    let mut terms: Vec<Value> = term_stats.values().cloned().collect();

    // Sort terms by total occurrences in descending order
    terms.sort_by(|a, b| {
        let a_count = a
            .get("total_occurrences")
            .and_then(|c| c.as_u64())
            .unwrap_or(0);
        let b_count = b
            .get("total_occurrences")
            .and_then(|c| c.as_u64())
            .unwrap_or(0);
        b_count.cmp(&a_count)
    });

    // Limit to top 1000 terms to avoid overwhelming responses
    let terms = if terms.len() > 1000 {
        terms[0..1000].to_vec()
    } else {
        terms
    };

    Json(json!({
        "total_unique_terms": term_stats.len(),
        "terms_shown": terms.len(),
        "terms": terms
    }))
}

/// Create a demo ObjectIndex and add it to the database
pub async fn create_dummy_record(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<DemoIndexRequest>,
) -> Json<Value> {
    let span = tracing_utils::server_span("/demo-index", "POST");
    let _guard = span.enter();

    debug!("Demo index endpoint called with ID: {:?}", payload.id);

    // Generate a timestamp-based ID if none was provided to ensure uniqueness
    let id = payload.id.unwrap_or_else(|| {
        let timestamp = chrono::Utc::now().timestamp();
        format!("dummy_item_{}", timestamp)
    });

    // Create a demo object with lorem ipsum content
    let demo_object = crate::create_dummy_object(Some(id));

    // Create a simple ObjectIndex with the demo object's content
    let mut inverted_index = std::collections::HashMap::new();

    // Split the content into words and create a simple inverted index
    let words: Vec<&str> = demo_object.text.split_whitespace().collect();

    for (position, word) in words.iter().enumerate() {
        let normalized_word = word
            .trim_matches(|c: char| !c.is_alphanumeric())
            .to_lowercase();
        if !normalized_word.is_empty() {
            let positions = inverted_index
                .entry(normalized_word.to_string())
                .or_insert_with(Vec::new);
            positions.push(position);
        }
    }

    // Log the creation of the demo index
    info!(
        "Index parsed with ID: {}, containing {} unique terms",
        demo_object.id,
        inverted_index.len()
    );

    // Keep a copy of the ID for the response
    let demo_id = demo_object.id.clone();
    let word_count = words.len();
    let unique_terms = inverted_index.len();
    let metadata = demo_object.metadata.clone();

    // Create an ObjectIndex instance for the database
    let object_index = ObjectIndex {
        object_id: demo_object.id.clone(),
        field_name: "text".to_string(),
        inverted_index,
    };

    // Get a clone of the shared DB state for the background task
    let db_state = state.db.clone();

    // Add the object to the database in a background task
    tokio::spawn(async move {
        // Log that we're starting the database operations
        info!("Adding demo object to database: {}", demo_object.id);

        // No need for a mutex since only the compactor writes
        // Index the object which will add it to the database
        info!("Indexing demo object: {}", demo_object.id);

        // Queue the object for indexing - the compactor will handle the actual database writes
        db_state.index(object_index);

        // Add the record to the RECORDS tree
        info!("Adding demo object to RECORDS tree: {}", demo_object.id);
        let records_handle = match db_state.open_tree(crate::db::TREE_RECORDS) {
            Ok(handle) => handle,
            Err(e) => {
                error!("Failed to open RECORDS collection: {}", e);
                return;
            }
        };

        // First, check if this ID already exists and remove it if it does
        // This helps prevent any serialization issues with existing corrupted records
        if let Ok(Some(_)) = records_handle.get(&demo_object.id) {
            info!("Removing existing record with ID: {}", demo_object.id);
            if let Err(e) = records_handle.remove(&demo_object.id) {
                error!("Failed to remove existing record: {}", e);
            }
        }

        // Convert to archivable format and serialize
        let archivable = ArchivableObjectRecord::from(&demo_object);
        match rkyv_adapter::serialize(&archivable) {
            Ok(serialized) => {
                // Insert the record into the tree
                let demo_id_clone = demo_object.id.clone();
                if let Err(e) = records_handle.insert(demo_id_clone, serialized.to_vec()) {
                    error!("Failed to insert record into RECORDS collection: {}", e);
                } else {
                    info!(
                        "Successfully added record to RECORDS collection: {}",
                        demo_object.id
                    );
                }
            }
            Err(e) => {
                error!("Failed to serialize record: {}", e);
            }
        }

        // Note: The compactor service will automatically handle compaction on its schedule
        // We don't need to manually trigger it here anymore

        // Log successful completion
        info!(
            "Successfully queued demo object for indexing: {}",
            demo_object.id
        );
    });

    // Return information about the created demo index
    Json(json!({
        "status": "success",
        "message": "Demo ObjectIndex created successfully and added to database",
        "object_id": demo_id,
        "unique_terms": unique_terms,
        "word_count": word_count,
        "metadata": metadata,
        "indexed": true,
        "compacted": true
    }))
}

/// Get all objects stored in the database
async fn list_objects(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/objects", "GET");
    let _guard = span.enter();

    debug!("List objects endpoint called");

    // Get a reference to the db
    let db_handle = state.db.clone();

    // Get the RECORDS tree
    let records_handle = match db_handle.open_tree(TREE_RECORDS) {
        Ok(handle) => handle,
        Err(e) => {
            error!("Failed to open RECORDS collection: {}", e);
            return Json(json!({
                "error": format!("Failed to open RECORDS collection: {}", e)
            }));
        }
    };

    // Collect all records
    let mut objects = Vec::new();

    // Iterate through all records
    let iter = match records_handle.iter() {
        Ok(it) => it,
        Err(e) => {
            error!("Failed to create iterator for RECORDS collection: {}", e);
            return Json(json!({
                "error": format!("Failed to create iterator for RECORDS collection: {}", e)
            }));
        }
    };
    for item in iter {
        match item {
            Ok((key, value)) => {
                // Try to convert key to string
                if let Ok(key_str) = std::str::from_utf8(&key) {
                    // Try to deserialize value to ArchivableObjectRecord and convert to ObjectRecord
                    match rkyv_adapter::deserialize::<ArchivableObjectRecord>(&value) {
                        Ok(archivable) => {
                            let record = ObjectRecord::from(archivable);
                            objects.push(json!({
                                "id": record.id,
                                "metadata": record.metadata,
                                "text_preview": if record.text.len() > 100 {
                                    format!("{}...", &record.text[..100])
                                } else {
                                    record.text
                                }
                            }));
                        }
                        Err(e) => {
                            // Log error but continue with next record
                            error!("Failed to deserialize record {}: {}", key_str, e);

                            // If this is a dummy item with known format issues, try to clean it up
                            if key_str.contains("dummy_item") {
                                info!("Attempting to clean up corrupted dummy item: {}", key_str);

                                // Create a fresh dummy object with the same ID
                                let replacement =
                                    crate::create_dummy_object(Some(key_str.to_string()));

                                // Serialize it
                                // Convert to archivable format for storage
                                let archivable = ArchivableObjectRecord::from(&replacement);
                                match rkyv_adapter::serialize(&archivable) {
                                    Ok(serialized) => {
                                        // Try to replace the corrupted record
                                        if let Err(err) =
                                            records_handle.insert(key_str, serialized.to_vec())
                                        {
                                            error!("Failed to replace corrupted record: {}", err);
                                        } else {
                                            info!(
                                                "Successfully replaced corrupted record: {}",
                                                key_str
                                            );

                                            // Add the replacement to the list
                                            objects.push(json!({
                                                "id": replacement.id,
                                                "metadata": replacement.metadata,
                                                "text_preview": if replacement.text.len() > 100 {
                                                    format!("{}...", &replacement.text[..100])
                                                } else {
                                                    replacement.text
                                                },
                                                "note": "Automatically repaired due to serialization issues"
                                            }));
                                        }
                                    }
                                    Err(err) => {
                                        error!("Failed to serialize replacement record: {}", err);
                                    }
                                }
                            } else {
                                // Add a placeholder for the corrupted record
                                objects.push(json!({
                                    "id": key_str,
                                    "metadata": null,
                                    "text_preview": "[Corrupted record]",
                                    "error": format!("{}", e)
                                }));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Error iterating records: {}", e);
            }
        }
    }

    Json(json!({
        "total": objects.len(),
        "objects": objects
    }))
}

pub async fn start_http_server(http_port: u16, fugu_db: FuguDB) {
    // Create a main span for the HTTP server
    let server_span = tracing::span!(tracing::Level::INFO, "http_server", port = http_port);

    async {
        info!("Starting HTTP server with pre-initialized database");

        // Create the shared state with  FuguDB
        let app_state = Arc::new(AppState {
            db: Arc::new(fugu_db),
        });
        info!("Created shared application state");

        // Create the router with shared state
        let app = Router::new()
            // API routes
            .route("/health", get(health))
            .route("/search", post(search))
            .route("/search/{namespace}", post(search_namespace))
            .route("/namespaces", get(list_namespaces))
            .route("/filters/{namespace}", get(list_namespace_filters))
            .route("/add/{namespace}", post(add_file))
            // Demo index route
            .route("/demo-index", post(create_dummy_record))
            // Database exploration endpoints
            .route("/objects", get(list_objects))
            .route("/objects/{object_id}", get(get_object_by_id))
            .route("/objects/{object_id}/terms", get(list_object_terms))
            .route("/terms", get(list_global_terms))
            // Data ingestion endpoints
            .route("/ingest", post(ingest_object))
            .route("/batch-ingest", post(batch_ingest))
            .route("/ingest-file", post(ingest_file))
            // Query API endpoints
            .route("/query", get(query_endpoints::query_text_get))
            .route("/query/{query}", get(query_endpoints::query_text_path))
            .route("/query", post(query_endpoints::query_json_post))
            .route(
                "/query/advanced",
                post(query_endpoints::query_advanced_post),
            )
            // Add the shared state
            .with_state(app_state);

        debug!("API routes configured with shared state");

        let server_addr = format!("0.0.0.0:{}", http_port);
        info!("Binding HTTP server to {}", server_addr);

        let http_listener = match tokio::net::TcpListener::bind(server_addr.clone()).await {
            Ok(listener) => {
                info!("Successfully bound to {}", server_addr);
                listener
            }
            Err(e) => {
                error!("Failed to bind HTTP server to {}: {}", server_addr, e);
                return;
            }
        };

        info!(
            "HTTP server started on {}. Press Ctrl+C to stop.",
            server_addr,
        );

        // Start the server with graceful shutdown
        axum::serve(http_listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();

        info!("Server shut down gracefully");
    }
    .instrument(server_span)
    .await
}

async fn shutdown_signal() {
    // Create a span for the shutdown signal handler
    let shutdown_span = tracing::span!(tracing::Level::INFO, "shutdown_signal");

    async {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
            debug!("Received Ctrl+C signal");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to install signal handler")
                .recv()
                .await;
            debug!("Received termination signal");
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }

        info!("Shutdown signal received, starting graceful shutdown");
    }
    .instrument(shutdown_span)
    .await
}
