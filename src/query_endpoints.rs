use crate::tracing_utils;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{Instrument, debug, error, info};
use urlencoding::decode;

use crate::server::AppState;

/// Simple text query parameters for GET requests
#[derive(Debug, Deserialize)]
pub struct TextQueryParams {
    query: String,
    #[serde(default)]
    limit: Option<usize>,
}

/// JSON query request body for POST requests
#[derive(Debug, Deserialize)]
pub struct JsonQueryRequest {
    query: String,
    #[serde(rename = "top_k")]
    top_k: Option<usize>,
    filters: Option<Vec<Value>>,
    boost: Option<Vec<Value>>,
}

/// Execute a text query via GET with URL parameters
pub async fn query_text_get(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TextQueryParams>,
) -> impl IntoResponse {
    // Create a span for this endpoint handler
    let span = tracing_utils::server_span("/search/:query", "GET");
    info!("executing query: {}", params.query);
    let result = state.db.simple_search(params.query).await;
    Json(json!({
        "result": result
    }))
}

/// Execute a text query via URL path (URL-encoded)
pub async fn query_text_path(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TextQueryParams>,
) -> impl IntoResponse {
    // Create a span for this endpoint handler
}

/// Execute a JSON query via POST
pub async fn query_json_post(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<JsonQueryRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Create a span for this endpoint handler
    let span = tracing_utils::server_span("/search/", "POST");
    Ok(Json(json!({})))
}

/// Execute a query with advanced options via POST
pub async fn query_advanced_post(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Create a span for this endpoint handler
    let span = tracing_utils::server_span("/api/query/advanced", "POST");
    Ok(Json(json!({})))
}
