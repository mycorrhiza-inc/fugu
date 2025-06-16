// server.rs
use crate::db::FuguDB;
use crate::query_endpoints;
use crate::{ObjectRecord, tracing_utils};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use rkyv::with;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tantivy::Document;
use tokio::signal;
use tracing::{Instrument, debug, error, info};

// Define a shared application state for dependency injection
pub struct AppState {
    // Using Arc for shared ownership across handlers, but no Mutex since only the compactor writes
    pub db: FuguDB,
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

#[derive(Debug, Serialize, Deserialize)]
struct Pagination {
    page: Option<usize>,
    per_page: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FuguSearchQuery {
    query: String,
    filters: Option<Vec<String>>,
    page: Option<Pagination>,
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
        "filters": payload.filters
    }))
}

/// Ingest objects into the database (now performs upserts)
async fn ingest_objects(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/ingest", "POST");
    let _guard = span.enter();

    info!(
        "Ingest endpoint called for {} objects (performing upserts)",
        payload.data.len()
    );

    let db = state.db.clone();
    match db.ingest(payload.data).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "message": "Objects ingested successfully (upserted)"
            })),
        ),
        Err(e) => {
            error!("Failed to ingest objects: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to ingest objects: {}", e)
                })),
            )
        }
    }
}

/// Upsert multiple objects: delete existing by ID then insert new ones
async fn upsert_objects(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/objects", "PUT");
    let _guard = span.enter();

    info!("Upsert endpoint called for {} objects", payload.data.len());

    let db = state.db.clone();
    match db.upsert(payload.data).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "message": "Objects upserted successfully"
            })),
        ),
        Err(e) => {
            error!("Failed to upsert objects: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to upsert objects: {}", e)
                })),
            )
        }
    }
}

/// Delete a single object by ID
async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path(object_id): Path<String>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span(&format!("/objects/{}", object_id), "DELETE");
    let _guard = span.enter();

    info!("Delete object endpoint called for ID: {}", object_id);

    let db = state.db.clone();
    match db.delete_document(object_id.clone()).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "message": format!("Object with ID '{}' deleted successfully", object_id)
            })),
        ),
        Err(e) => {
            error!("Failed to delete object {}: {}", object_id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to delete object: {}", e)
                })),
            )
        }
    }
}
async fn batch_upsert_objects(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BatchIndexRequest>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/batch/upsert", "POST");
    let _guard = span.enter();

    info!(
        "Batch upsert endpoint called for {} objects",
        payload.objects.len()
    );

    let db = state.db.clone();
    match db.batch_upsert(payload.objects).await {
        Ok(successful_count) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "message": format!("Successfully upserted {} objects", successful_count),
                "upserted_count": successful_count
            })),
        ),
        Err(e) => {
            error!("Failed to batch upsert objects: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to batch upsert objects: {}", e)
                })),
            )
        }
    }
}

// Extract additional fields from JSON payload that aren't id, text, or metadata

// Recursively create indexes for all fields depth-first

/// Search in a specific namespace
// Enhanced server.rs endpoints for namespace facet support

/// Enhanced search endpoint with namespace facet support
async fn search_with_namespace_facets(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<FuguSearchQuery>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/search/namespace", "POST");
    let _guard = span.enter();

    info!(
        "Namespace facet search endpoint called with query: {}",
        payload.query
    );

    let db = state.db.clone();
    let filters = payload.filters.unwrap_or_default();
    let page = payload.page.as_ref().and_then(|p| p.page).unwrap_or(0);
    let per_page = payload.page.as_ref().and_then(|p| p.per_page).unwrap_or(20);

    match db
        .search_with_namespace_facets(&payload.query, &filters, page, per_page)
        .await
    {
        Ok(results) => {
            let response = json!({
                "status": "success",
                "results": results,
                "query": payload.query,
                "filters": filters,
                "total": results.len(),
                "page": page,
                "per_page": per_page
            });
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            error!("Namespace facet search failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Search failed: {}", e)
                })),
            )
        }
    }
}

/// Get facets for a specific namespace
async fn get_namespace_facets(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span(&format!("/namespaces/{}/facets", namespace), "GET");
    let _guard = span.enter();

    info!("Get namespace facets endpoint called for: {}", namespace);

    let db = state.db.clone();
    match db.get_namespace_facets(&namespace) {
        Ok(facets) => {
            let facet_list: Vec<serde_json::Value> = facets
                .iter()
                .map(|(facet, count)| {
                    json!({
                        "path": facet.to_string(),
                        "count": count
                    })
                })
                .collect();

            (
                StatusCode::OK,
                Json(json!({
                    "status": "success",
                    "namespace": namespace,
                    "facets": facet_list
                })),
            )
        }
        Err(e) => {
            error!("Failed to get namespace facets for {}: {}", namespace, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get namespace facets: {}", e)
                })),
            )
        }
    }
}

/// Get all available namespaces
async fn get_available_namespaces(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let span = tracing_utils::server_span("/namespaces", "GET");
    let _guard = span.enter();

    info!("Get available namespaces endpoint called");

    let db = state.db.clone();
    match db.get_available_namespaces() {
        Ok(namespaces) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "namespaces": namespaces
            })),
        ),
        Err(e) => {
            error!("Failed to get available namespaces: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get namespaces: {}", e)
                })),
            )
        }
    }
}

/// Enhanced ingest endpoint that supports namespace facets
async fn ingest_objects_with_namespace_facets(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/ingest/namespace", "POST");
    let _guard = span.enter();

    info!(
        "Enhanced ingest endpoint called for {} objects with namespace facet support",
        payload.data.len()
    );

    // Validate all objects first
    for (i, object) in payload.data.iter().enumerate() {
        if let Err(e) = object.validate() {
            error!("Validation failed for object at index {}: {}", i, e);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "error",
                    "error": format!("Validation failed for object at index {}: {}", i, e)
                })),
            );
        }
    }

    let db = state.db.clone();
    match db.upsert(payload.data).await {
        Ok(_) => {
            info!("Successfully ingested objects with namespace facets");
            (
                StatusCode::OK,
                Json(json!({
                    "status": "success",
                    "message": "Objects ingested successfully with namespace facets"
                })),
            )
        }
        Err(e) => {
            error!("Failed to ingest objects with namespace facets: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to ingest objects: {}", e)
                })),
            )
        }
    }
}

/// Get organization types for a namespace
async fn get_namespace_organizations(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> impl IntoResponse {
    let span =
        tracing_utils::server_span(&format!("/namespaces/{}/organizations", namespace), "GET");
    let _guard = span.enter();

    info!(
        "Get namespace organizations endpoint called for: {}",
        namespace
    );

    let db = state.db.clone();
    let filter_path = format!("/namespace/{}/organization", namespace);

    match db.get_filter_values_at_path(&filter_path) {
        Ok(organizations) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "namespace": namespace,
                "organizations": organizations
            })),
        ),
        Err(e) => {
            error!(
                "Failed to get organizations for namespace {}: {}",
                namespace, e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get organizations: {}", e)
                })),
            )
        }
    }
}

/// Get conversation IDs for a namespace
async fn get_namespace_conversations(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> impl IntoResponse {
    let span =
        tracing_utils::server_span(&format!("/namespaces/{}/conversations", namespace), "GET");
    let _guard = span.enter();

    info!(
        "Get namespace conversations endpoint called for: {}",
        namespace
    );

    let db = state.db.clone();
    let filter_path = format!("/namespace/{}/conversation", namespace);

    match db.get_filter_values_at_path(&filter_path) {
        Ok(conversations) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "namespace": namespace,
                "conversations": conversations
            })),
        ),
        Err(e) => {
            error!(
                "Failed to get conversations for namespace {}: {}",
                namespace, e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get conversations: {}", e)
                })),
            )
        }
    }
}

/// Get data types for a namespace
async fn get_namespace_data_types(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span(&format!("/namespaces/{}/data", namespace), "GET");
    let _guard = span.enter();

    info!(
        "Get namespace data types endpoint called for: {}",
        namespace
    );

    let db = state.db.clone();
    let filter_path = format!("/namespace/{}/data", namespace);

    match db.get_filter_values_at_path(&filter_path) {
        Ok(data_types) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "namespace": namespace,
                "data_types": data_types
            })),
        ),
        Err(e) => {
            error!(
                "Failed to get data types for namespace {}: {}",
                namespace, e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get data types: {}", e)
                })),
            )
        }
    }
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
    debug!("get filter endpoint called for {}", namespace);
    let facets = state
        .db
        .clone()
        .get_facets(Some(format!("/{}", namespace.to_string())))
        .unwrap();

    Json(json!({"filters": facets }))
}
/// List filters for a specific namespace
async fn list_filters(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/filters/"), "GET");
    let _guard = span.enter();

    let facets = state.db.clone().get_facets(None).unwrap();
    info!("List filters endpoint called");
    let res: Vec<Value> = facets
        .iter()
        .map(|f| json!({"value": f.0.to_string()}))
        .collect();

    Json(json!({"filters": res }))
}
/// Get all objects stored in the database
async fn list_objects(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/objects", "GET");
    let _guard = span.enter();

    debug!("List objects endpoint called");

    // Get a reference to the db

    Json(json!({}))
}

async fn sayhi(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/", "GET");
    let _guard = span.enter();

    info!("api endpoint hit");

    // Get a reference to the db

    Json(json!({"message":"hi!"}))
}

#[derive(Debug, Deserialize)]
struct FacetTreeParams {
    max_depth: Option<usize>,
}
// Add this endpoint to your server.rs file
/// Get the complete facet tree up to max_depth - 1
async fn get_facet_tree(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FacetTreeParams>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/facets/tree", "GET");
    let _guard = span.enter();

    info!(
        "Get facet tree endpoint called with max_depth: {:?}",
        params.max_depth
    );

    // Apply max_depth - 1 if specified
    let effective_max_depth = params.max_depth.map(|d| if d > 0 { d - 1 } else { 0 });

    match state.db.get_facet_tree(effective_max_depth) {
        Ok(tree_response) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "data": tree_response
            })),
        ),
        Err(e) => {
            error!("Failed to get facet tree: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get facet tree: {}", e)
                })),
            )
        }
    }
}

/// Get filter paths for a specific namespace
async fn get_namespace_filters(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span(&format!("/filters/namespace/{}", namespace), "GET");
    let _guard = span.enter();

    info!(
        "Get namespace filter paths endpoint called for namespace: {}",
        namespace
    );

    match state.db.get_filter_paths_for_namespace(&namespace) {
        Ok(filter_paths) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "namespace": namespace,
                "filter_paths": filter_paths
            })),
        ),
        Err(e) => {
            error!(
                "Failed to get filter paths for namespace {}: {}",
                namespace, e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get filter paths for namespace: {}", e)
                })),
            )
        }
    }
}

async fn get_all_filters(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let span = tracing_utils::server_span("/filters/all", "GET");
    let _guard = span.enter();

    info!("Get all filter paths endpoint called");

    match state.db.get_all_filter_paths() {
        Ok(filter_paths) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "filter_paths": filter_paths
            })),
        ),
        Err(e) => {
            error!("Failed to get all filter paths: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get filter paths: {}", e)
                })),
            )
        }
    }
}
/// Get filter values at a specific path
async fn get_filter_values_at_path(
    State(state): State<Arc<AppState>>,
    Path(filter_path): Path<String>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span(&format!("/filters/path/{}", filter_path), "GET");
    let _guard = span.enter();

    info!(
        "Get filter values endpoint called for path: {}",
        filter_path
    );

    match state.db.get_filter_values_at_path(&filter_path) {
        Ok(values) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "path": filter_path,
                "values": values
            })),
        ),
        Err(e) => {
            error!(
                "Failed to get filter values for path {}: {}",
                filter_path, e
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to get filter values: {}", e)
                })),
            )
        }
    }
}

// Update your start_http_server function in your server file

pub async fn start_http_server(http_port: u16, fugu_db: FuguDB) {
    let server_span = tracing::span!(tracing::Level::INFO, "http_server", port = http_port);

    async {
        info!("Starting HTTP server with pre-initialized database");

        let app_state = Arc::new(AppState { db: fugu_db });
        info!("Created shared application state");

        // Create the router with shared state
        let app = Router::new()
            // Basic routes
            .route(
                "/",
                get(|| async move { "Hello from Fugu API with Namespace Facets" }),
            )
            .route("/health", get(health))
            // Enhanced namespace routes
            .route("/namespaces", get(get_available_namespaces))
            .route("/namespaces/{namespace}/facets", get(get_namespace_facets))
            .route(
                "/namespaces/{namespace}/organizations",
                get(get_namespace_organizations),
            )
            .route(
                "/namespaces/{namespace}/conversations",
                get(get_namespace_conversations),
            )
            .route(
                "/namespaces/{namespace}/data",
                get(get_namespace_data_types),
            )
            // Filter routes (existing)
            .route("/filters/all", get(get_all_filters))
            .route("/filters/namespace/{namespace}", get(get_namespace_filters))
            .route("/filters/path/{*filter}", get(get_filter_values_at_path))
            // Enhanced ingest and search routes
            .route("/ingest", post(ingest_objects)) // Regular ingest (now with namespace facet support)
            .route(
                "/ingest/namespace",
                post(ingest_objects_with_namespace_facets),
            ) // Explicit namespace facet ingest
            .route("/search/namespace", post(search_with_namespace_facets)) // Enhanced search
            // Existing routes
            .route("/objects", put(upsert_objects))
            .route("/batch/upsert", post(batch_upsert_objects))
            .route("/search", get(query_endpoints::query_text_get))
            .route("/search", post(query_endpoints::query_json_post))
            .route("/search/{query}", get(query_endpoints::query_text_path))
            .route("/objects/{object_id}", get(get_object_by_id))
            .route("/objects/{object_id}", delete(delete_object))
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
            "HTTP server started on {}. Available endpoints:",
            server_addr,
        );
        info!("  GET  /health");
        info!("  GET  /namespaces");
        info!("  GET  /namespaces/{{namespace}}/facets");
        info!("  GET  /namespaces/{{namespace}}/organizations");
        info!("  GET  /namespaces/{{namespace}}/conversations");
        info!("  GET  /namespaces/{{namespace}}/data");
        info!("  POST /ingest (with namespace facet support)");
        info!("  POST /ingest/namespace");
        info!("  POST /search/namespace");
        info!("  GET  /search?q=<query>");
        info!("  POST /search");
        info!("  GET  /objects/{{id}}");

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
