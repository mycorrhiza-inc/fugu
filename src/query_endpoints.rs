use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, error, info, Instrument};
use crate::tracing_utils;
use urlencoding::decode;

use crate::query::{QueryConfig, QueryEngine, QueryResults};
use crate::server::AppState;

/// Simple text query parameters for GET requests
#[derive(Debug, Deserialize)]
pub struct TextQueryParams {
    q: String,
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
) -> Result<Json<QueryResults>, (StatusCode, Json<Value>)> {
    // Create a span for this endpoint handler
    let span = tracing_utils::server_span("/api/query", "GET");

    async move {
        info!("Text query endpoint called with query: {}", params.q);

        // Create query engine with default config
        let config = QueryConfig::default();
        let engine = QueryEngine::new(state.db.clone(), config);

        // Execute the query
        match engine.search_text(&params.q, params.limit) {
            Ok(results) => {
                info!(
                    total_hits = results.total_hits,
                    took_ms = results.took_ms,
                    "Query returned {} results in {}ms",
                    results.total_hits,
                    results.took_ms
                );
                Ok(Json(results))
            }
            Err(e) => {
                error!("Query error: {}", e);
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": format!("Query error: {}", e)
                    })),
                ))
            }
        }
    }
    .instrument(span)
    .await
}

/// Execute a text query via URL path (URL-encoded)
pub async fn query_text_path(
    State(state): State<Arc<AppState>>,
    Path(encoded_query): Path<String>,
    Query(params): Query<TextQueryParams>,
) -> Result<Json<QueryResults>, (StatusCode, Json<Value>)> {
    // Create a span for this endpoint handler
    let span = tracing_utils::server_span("/api/query/:query", "GET");

    async move {
        // Decode the URL-encoded query
        let decoded_query = match decode(&encoded_query) {
            Ok(decoded) => decoded.to_string(),
            Err(e) => {
                error!("Failed to decode query: {}", e);
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": format!("Failed to decode query: {}", e)
                    })),
                ));
            }
        };

        info!("Path query endpoint called with query: {}", decoded_query);

        // Create query engine with default config
        let config = QueryConfig::default();
        let engine = QueryEngine::new(state.db.clone(), config);

        // Execute the query
        match engine.search_text(&decoded_query, params.limit) {
            Ok(results) => {
                info!(
                    total_hits = results.total_hits,
                    took_ms = results.took_ms,
                    "Query returned {} results in {}ms",
                    results.total_hits,
                    results.took_ms
                );
                Ok(Json(results))
            }
            Err(e) => {
                error!("Query error: {}", e);
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": format!("Query error: {}", e)
                    })),
                ))
            }
        }
    }
    .instrument(span)
    .await
}

/// Execute a JSON query via POST
pub async fn query_json_post(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<JsonQueryRequest>,
) -> Result<Json<QueryResults>, (StatusCode, Json<Value>)> {
    // Create a span for this endpoint handler
    let span = tracing_utils::server_span("/api/query/json", "POST");

    async move {
        let has_filters = payload.filters.is_some();
        let has_boost = payload.boost.is_some();

        info!(
            query_text = %payload.query,
            top_k = ?payload.top_k,
            has_filters = has_filters,
            has_boost = has_boost,
            "JSON query endpoint called"
        );

        // Create query engine with default config
        let config = QueryConfig::default();
        let engine = QueryEngine::new(state.db.clone(), config);

        // Prepare JSON query string
        let json_query = json!({
            "query": payload.query,
            "top_k": payload.top_k,
            "filters": payload.filters,
            "boost": payload.boost
        });

        // Get top_k value before moving json_query
        let top_k = json_query.get("top_k").and_then(|k| k.as_u64()).map(|v| v as usize);

        // Execute the query
        match engine.search_json(json_query, top_k) {
            Ok(results) => {
                info!(
                    total_hits = results.total_hits,
                    took_ms = results.took_ms,
                    "Query returned {} results in {}ms",
                    results.total_hits,
                    results.took_ms
                );
                Ok(Json(results))
            }
            Err(e) => {
                error!("Query error: {}", e);
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": format!("Query error: {}", e)
                    })),
                ))
            }
        }
    }
    .instrument(span)
    .await
}

/// Execute a query with advanced options via POST
pub async fn query_advanced_post(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Result<Json<QueryResults>, (StatusCode, Json<Value>)> {
    // Create a span for this endpoint handler
    let span = tracing_utils::server_span("/api/query/advanced", "POST");

    async move {
        info!("Advanced query endpoint called");

        // Extract the basic query text
        let query_text = match payload.get("query") {
            Some(q) => {
                if let Some(q_str) = q.as_str() {
                    q_str
                } else {
                    error!("Invalid query format: query must be a string");
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": "Query must be a string"
                        })),
                    ));
                }
            }
            None => {
                error!("Missing required 'query' parameter");
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "Query parameter is required"
                    })),
                ));
            }
        };

        // Extract limit parameter
        let limit = payload
            .get("limit")
            .and_then(|l| l.as_u64())
            .map(|l| l as usize);

        // Create query engine with custom config if provided
        let mut config = QueryConfig::default();
        let mut has_custom_config = false;

        // Apply config overrides if specified
        if let Some(config_obj) = payload.get("config") {
            has_custom_config = true;

            if let Some(highlight) = config_obj.get("highlight_snippets") {
                if let Some(highlight_bool) = highlight.as_bool() {
                    config.highlight_snippets = highlight_bool;
                    debug!("Using custom highlight_snippets: {}", highlight_bool);
                }
            }

            if let Some(threshold) = config_obj.get("min_score_threshold") {
                if let Some(threshold_float) = threshold.as_f64() {
                    config.min_score_threshold = threshold_float;
                    debug!("Using custom min_score_threshold: {}", threshold_float);
                }
            }

            if let Some(k1) = config_obj.get("bm25_k1") {
                if let Some(k1_float) = k1.as_f64() {
                    config.bm25_k1 = k1_float;
                    debug!("Using custom bm25_k1: {}", k1_float);
                }
            }

            if let Some(b) = config_obj.get("bm25_b") {
                if let Some(b_float) = b.as_f64() {
                    config.bm25_b = b_float;
                    debug!("Using custom bm25_b: {}", b_float);
                }
            }
        }

        // Log query information
        let has_filters = payload.get("filters").is_some();
        let has_boost = payload.get("boost").is_some();

        info!(
            query_text = %query_text,
            limit = ?limit,
            has_filters = has_filters,
            has_boost = has_boost,
            has_custom_config = has_custom_config,
            "Advanced query with custom configuration"
        );

        // Store the default limit for use after config is moved
        let default_limit = config.default_limit;

        // Create query engine with the config
        let engine = QueryEngine::new(state.db.clone(), config);

        // Execute the query based on type
        if has_filters || has_boost {
            // JSON query with advanced features
            match engine.search_json(payload.clone(), Some(default_limit)) {
                Ok(results) => {
                    info!(
                        total_hits = results.total_hits,
                        took_ms = results.took_ms,
                        "Advanced JSON query returned {} results in {}ms",
                        results.total_hits,
                        results.took_ms
                    );
                    Ok(Json(results))
                }
                Err(e) => {
                    error!("Query error: {}", e);
                    Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({
                            "error": format!("Query error: {}", e)
                        })),
                    ))
                }
            }
        } else {
            // Simple text query
            match engine.search_text(query_text, limit) {
                Ok(results) => {
                    info!(
                        total_hits = results.total_hits,
                        took_ms = results.took_ms,
                        "Advanced text query returned {} results in {}ms",
                        results.total_hits,
                        results.took_ms
                    );
                    Ok(Json(results))
                }
                Err(e) => {
                    error!("Query error: {}", e);
                    Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({
                            "error": format!("Query error: {}", e)
                        })),
                    ))
                }
            }
        }
    }
    .instrument(span)
    .await
}
