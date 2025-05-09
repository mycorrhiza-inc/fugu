use crate::db::{
    FuguDB, PREFIX_FILTER_INDEX, PREFIX_RECORD_INDEX_TREE, TREE_FILTERS, TREE_GLOBAL_INDEX,
    TREE_RECORDS,
};
use crate::{ObjectIndex, ObjectRecord, tracing_utils, rkyv_adapter};
use crate::object::ArchivableObjectRecord;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde_json::{Value, json};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::hash_map::HashMap;
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

fn index_object(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> Json<Value> {
    let span = tracing_utils::server_span("/index", "POST");
    let _guard = span.enter();

    debug!("index endpoint called with ID: {:?}", payload.data.id);

    let object = payload.data;

    // TODO: check if the object has already been added to the index

    // Create a ObjectIndex with the demo object's content
    let mut inverted_index: HashMap<String, Vec<usize>> = std::collections::HashMap::new();

    // Split the content into words and create a simple inverted index
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

    info!(
        "CreatedObjectIndex with ID: {}, containing {} unique terms",
        object.id,
        inverted_index.len()
    );

    // Keep a copy of the ID for the response
    let metadata = object.metadata.clone();

    // Create an ObjectIndex instance for the database
    let object_index = ObjectIndex {
        object_id: object.id.clone(),
        inverted_index,
    };

    // Get a clone of the shared DB state for the background task
    let db_state = state.db.clone();

    // Add the object to the database in a background task
    tokio::spawn(async move {
        // Log that we're starting the database operations
        info!("Adding demo object to database: {}", object.id);

        // No need for a mutex since only the compactor writes
        // Index the object which will add it to the database
        info!("Indexing demo object: {}", object.id);

        // Queue the object for indexing - the compactor will handle the actual database writes
        db_state.index(object_index);

        // Add the record to the RECORDS tree
        info!("Adding object to RECORDS tree: {}", object.id);
        let records_tree = match db_state.db().open_tree(crate::db::TREE_RECORDS) {
            Ok(tree) => tree,
            Err(e) => {
                error!("Failed to open RECORDS tree: {}", e);
                return;
            }
        };

        // Convert to archivable format and serialize
        let archivable = ArchivableObjectRecord::from(&object);
        match rkyv_adapter::serialize(&archivable) {
            Ok(serialized) => {
                // Insert the record into the tree
                if let Err(e) = records_tree.insert(object.id.as_bytes(), serialized.to_vec()) {
                    error!("Failed to insert record into RECORDS tree: {}", e);
                } else {
                    info!("Successfully added record to RECORDS tree: {}", object.id);
                }
            }
            Err(e) => {
                error!("Failed to serialize record: {}", e);
            }
        }

        // Note: The compactor service will automatically handle compaction on its schedule
        // We don't need to manually trigger it here anymore

        // Log successful completion
        info!("Successfully queued object for indexing: {}", object.id);
    });
    Json(json!({}))
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
            let was_repaired = record.id.contains("dummy_item") &&
                record.metadata.get("auto_repaired").is_some();

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
    let db = state.db.db();

    // Construct the tree name for the object index
    let tree_name = format!("{}:{}", PREFIX_RECORD_INDEX_TREE, object_id);

    // Try to open the tree for the object
    let object_tree = match db.open_tree(tree_name) {
        Ok(tree) => tree,
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
    let iter = object_tree.iter();
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
    let db = state.db.db();

    // We'll try to get the names of all objects
    let mut all_object_trees = Vec::new();

    // Get all tree names - this is not a Result, it returns a Vec directly
    let tree_names = db.tree_names();

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

        match db.open_tree(tree_name.clone()) {
            Ok(tree) => {
                // Process each term in the tree
                for item in tree.iter() {
                    match item {
                        Ok((key, value)) => {
                            if let Ok(term) = std::str::from_utf8(&key) {
                                if let Ok(positions) = rkyv_adapter::deserialize::<Vec<usize>>(&value) {
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
        "Index added with ID: {}, containing {} unique terms",
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
        let records_tree = match db_state.db().open_tree(crate::db::TREE_RECORDS) {
            Ok(tree) => tree,
            Err(e) => {
                error!("Failed to open RECORDS tree: {}", e);
                return;
            }
        };

        // First, check if this ID already exists and remove it if it does
        // This helps prevent any serialization issues with existing corrupted records
        if let Ok(Some(_)) = records_tree.get(demo_object.id.as_bytes()) {
            info!("Removing existing record with ID: {}", demo_object.id);
            if let Err(e) = records_tree.remove(demo_object.id.as_bytes()) {
                error!("Failed to remove existing record: {}", e);
            }
        }

        // Convert to archivable format and serialize
        let archivable = ArchivableObjectRecord::from(&demo_object);
        match rkyv_adapter::serialize(&archivable) {
            Ok(serialized) => {
                // Insert the record into the tree
                if let Err(e) = records_tree.insert(demo_object.id.as_bytes(), serialized.to_vec()) {
                    error!("Failed to insert record into RECORDS tree: {}", e);
                } else {
                    info!("Successfully added record to RECORDS tree: {}", demo_object.id);
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
    let db = state.db.db();

    // Get the RECORDS tree
    let records_tree = match db.open_tree(TREE_RECORDS) {
        Ok(tree) => tree,
        Err(e) => {
            error!("Failed to open RECORDS tree: {}", e);
            return Json(json!({
                "error": format!("Failed to open RECORDS tree: {}", e)
            }));
        }
    };

    // Collect all records
    let mut objects = Vec::new();

    // Iterate through all records
    let iter = records_tree.iter();
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
                                let replacement = crate::create_dummy_object(Some(key_str.to_string()));

                                // Serialize it
                                // Convert to archivable format for storage
                                let archivable = ArchivableObjectRecord::from(&replacement);
                                match rkyv_adapter::serialize(&archivable) {
                                    Ok(serialized) => {
                                        // Try to replace the corrupted record
                                        if let Err(err) = records_tree.insert(key.clone(), serialized.to_vec()) {
                                            error!("Failed to replace corrupted record: {}", err);
                                        } else {
                                            info!("Successfully replaced corrupted record: {}", key_str);

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

