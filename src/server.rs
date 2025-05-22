use crate::db::FuguDB;
use crate::query_endpoints;
use crate::tokeinze::{Token, TokenPosition, TokenType, tokenize, tokenize_into_index};
use crate::{ObjectRecord, tracing_utils};
use anyhow::anyhow;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use fjall::PersistMode;
use futures::future::Join;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize, result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::hash_map::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::thread::JoinHandle;
use tantivy::schema::{Facet, Field};
use tantivy::{Document, IndexWriter, TantivyDocument};
use tokio::signal;
use tokio::sync::Mutex;
use tracing::{Instrument, debug, error, info};

// Define a shared application state for dependency injection
pub struct AppState {
    // Using Arc for shared ownership across handlers, but no Mutex since only the compactor writes
    pub db: FuguDB,
    pub writer: Mutex<IndexWriter>,
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
    data: Vec<ObjectRecord>,
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
async fn ingest_objects(
    State(state): State<Arc<AppState>>,
    Json(mut payload): Json<IndexRequest>,
) -> impl IntoResponse {
    use tokio_stream::StreamExt; // Add this import to bring StreamExt trait into scope

    let span = tracing_utils::server_span("/ingest", "POST");
    let _guard = span.enter();

    debug!(
        "Ingest endpoint called for {:?} objects",
        payload.data.len()
    );

    // Extract and normalize fields
    let objects = payload.data.clone();

    for object in objects {
        // Check if the IDs are valid
        if object.id.clone().is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Object ID cannot be empty",
                    "status": "error"
                })),
            );
        }

        let mut doc = TantivyDocument::new();

        // Process metadata to extract additional fields and create indexes
        let fields = process_additional_fields(&object);
        doc.add_text(state.db.id_field(), object.id);
        doc.add_text(state.db.text_field(), object.text);
        // If we have additional fields, merge them into metadata
        if !is_value_empty(&fields) {
            // Create indexes for each additional field using depth-first traversal
            let facets = create_facet_indexes(&fields, String::new(), &doc);
            for f in facets {
                doc.add_facet(state.db.metadata_field(), f.as_str());
            }
            // Merge additional fields into metadata
        }
        let w = state.writer.lock().await;
        w.add_document(doc);
    }
    let mut w = state.writer.lock().await;
    w.commit().unwrap();

    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
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
        // Return the remaining fields as a new Value
        map.remove("id");
        map.remove("text");
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
fn create_facet_indexes(
    value: &serde_json::Value,
    prefix: String,
    doc: &TantivyDocument,
) -> Vec<String> {
    let mut out = Vec::new();
    match value {
        serde_json::Value::Object(map) => {
            // Process each field in the object
            for (key, val) in map {
                let field_path = if prefix.is_empty() {
                    format!("/{}", key.clone())
                } else {
                    format!("{}/{}", prefix, key)
                };
                let temp = create_facet_indexes(val, field_path, doc);
                out.extend(temp);
            }
        }
        serde_json::Value::Array(arr) => {
            // Process each item in the array
            for (i, item) in arr.iter().enumerate() {
                let temp = create_facet_indexes(item, prefix.clone(), doc);
                out.extend(temp);
            }
        }
        _ => {
            // For primitive values, index the string representation
            let field_str = value.as_str().unwrap().to_string();
            let facet_str = if prefix.is_empty() {
                field_str
            } else {
                format!(
                    "{}/{}",
                    prefix,
                    // urlencoding::Encoded(field_str.clone()).to_string()
                    field_str.clone()
                )
            };
            info!("new facet: {}", facet_str.clone());
            out.push(facet_str);
        }
    }
    return out;
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
        Ok(results) => {
            // Return the full record with a note if it was auto-repaired
            let record = results.get(0).unwrap();
            Json(json!(record.to_json(&state.db.schema())))
        }
        Err(e) => {
            // Return error if record doesn't exist or there was a deserialization issue
            Json(json!({
                "error": format!("error getting object with id {}: {}", object_id, e)
            }))
        }
    }
}

async fn get_filter(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/filters/{}", namespace), "GET");
    let _guard = span.enter();
    debug!("get filter endpoint called for namespace");
    let facets = state.db.clone().get_facets();

    Json(json!({"namespace": namespace }))
}
/// List filters for a specific namespace
async fn list_filters(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/filters/{}", namespace), "GET");
    let _guard = span.enter();

    let facets = state.db.clone().get_facets();
    debug!(
        "List namespace filters endpoint called for namespace: {}",
        namespace
    );

    Json(json!({"namespace": namespace }))
}
/// Get all objects stored in the database
async fn list_objects(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/objects", "GET");
    let _guard = span.enter();

    debug!("List objects endpoint called");

    // Get a reference to the db

    Json(json!({}))
}

pub async fn start_http_server(http_port: u16, fugu_db: FuguDB) {
    // Create a main span for the HTTP server
    let server_span = tracing::span!(tracing::Level::INFO, "http_server", port = http_port);

    async {
        info!("Starting HTTP server with pre-initialized database");

        // Create the shared state with  FuguDB
        let writer = fugu_db.clone().get_index().writer(50_000_000).unwrap();

        let app_state = Arc::new(AppState {
            db: fugu_db,
            writer: Mutex::new(writer),
        });
        info!("Created shared application state");

        // Create the router with shared state
        let app = Router::new()
            // API routes
            .route("/health", get(health))
            .route("/filters/", get(list_filters))
            .route("/filters/{filter}", get(get_filter))
            .route("/ingest", post(ingest_objects))
            // Query API endpoints
            .route("/search", get(query_endpoints::query_text_get))
            .route("/search/{query}", get(query_endpoints::query_text_path))
            .route("/search", post(query_endpoints::query_json_post))
            .route(
                "/query/advanced",
                post(query_endpoints::query_advanced_post),
            )
            // Add the shared state
            .with_state(app_state.clone());

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
            .with_graceful_shutdown(shutdown_signal(app_state))
            .await
            .unwrap();
        info!("Server shut down gracefully");
    }
    .instrument(server_span)
    .await
}

async fn shutdown_signal(app_state: Arc<AppState>) {
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

        // Persist database to disk before shutting down
        info!("Persisting database to disk before shutdown...");
    }
    .instrument(shutdown_span)
    .await
}
