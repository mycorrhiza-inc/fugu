// query_endpoints.rs
use crate::db::FuguSearchResult; // Import from your db module
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
use tracing::{error, info, warn};
use urlencoding::decode;

use crate::server::AppState;

/// Simple text query parameters for GET requests
/// now also accepts ?text=true/false
#[derive(Debug, Deserialize)]
pub struct TextQueryParams {
    q: String, // Changed from "query" to "q" to match standard
    #[serde(default)]
    limit: Option<usize>,
    /// whether to include the `text` of each hit
    #[serde(default)]
    text: Option<bool>,
}

/// Pagination parameters
#[derive(Debug, Deserialize, Serialize)]
pub struct Pagination {
    page: Option<usize>,
    per_page: Option<usize>,
}

/// JSON query request body for POST requests
#[derive(Debug, Deserialize)]
pub struct JsonQueryRequest {
    query: String,
    filters: Option<Vec<String>>,
    page: Option<Pagination>,
    /// whether to include the full text in each hit
    #[serde(default)]
    pub text: Option<bool>,
}

/// helper for POST?text=...
#[derive(Debug, Deserialize)]
pub struct IncludeTextFlag {
    #[serde(default)]
    pub text: Option<bool>,
}

/// Search result item - use an alias to the db type
pub type SearchResultItem = FuguSearchResult;

/// Search response
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    results: Vec<SearchResultItem>,
    total: usize,
    page: usize,
    per_page: usize,
    query: String,
}

/// Execute a text query via GET with URL parameters
pub async fn query_text_get(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TextQueryParams>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/search", "GET");
    let _guard = span.enter();

    info!("GET search query: {}", params.q);
    if params.q.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Query parameter 'q' cannot be empty"
            })),
        );
    }

    let limit = params.limit.unwrap_or(20);
    let include_text = params.text.unwrap_or(false);

    // Perform the search
    match perform_search(&state.db, &params.q, &[], 0, limit).await {
        Ok(response) => {
            // build JSON and strip text if needed
            let mut out = serde_json::to_value(&response).unwrap();
            if !include_text {
                if let Some(arr) = out["results"].as_array_mut() {
                    for item in arr {
                        if let Some(obj) = item.as_object_mut() {
                            obj.remove("text");
                        }
                    }
                }
            }
            info!(
                "Search completed successfully with {} results",
                response.results.len()
            );
            (StatusCode::OK, Json(out))
        }
        Err(err) => {
            error!("Search failed: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Search failed: {}", err)
                })),
            )
        }
    }
}

/// Execute a text query via URL path (URL-encoded)
pub async fn query_text_path(
    State(state): State<Arc<AppState>>,
    Path(encoded_query): Path<String>,
    Query(params): Query<TextQueryParams>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/search/:query", "GET");
    let _guard = span.enter();

    // Decode the URL-encoded query
    let query = match decode(&encoded_query) {
        Ok(decoded) => decoded.to_string(),
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Invalid URL encoding in query"
                })),
            );
        }
    };

    info!("Path search query: {}", query);
    if query.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Query cannot be empty"
            })),
        );
    }

    // Perform the search with default limit
    let include_text = params.text.unwrap_or(false);
    match perform_search(&state.db, &query, &[], 0, 20).await {
        Ok(response) => {
            let mut out = serde_json::to_value(&response).unwrap();
            if !include_text {
                if let Some(arr) = out["results"].as_array_mut() {
                    for item in arr {
                        if let Some(obj) = item.as_object_mut() {
                            obj.remove("text");
                        }
                    }
                }
            }
            info!(
                "Search completed successfully with {} results",
                response.results.len()
            );
            (StatusCode::OK, Json(out))
        }
        Err(err) => {
            error!("Search failed: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Search failed: {}", err)
                })),
            )
        }
    }
}

/// Execute a JSON query via POST
pub async fn query_json_post(
    State(state): State<Arc<AppState>>,
    Query(flag): Query<IncludeTextFlag>,
    Json(mut payload): Json<JsonQueryRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let span = tracing_utils::server_span("/search", "POST");
    let _guard = span.enter();

    info!("POST search query: {}", payload.query);
    if payload.query.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Query cannot be empty"
            })),
        ));
    }

    // resolve text inclusion flags
    let url_text = flag.text.unwrap_or(false);
    let body_text = payload.text.unwrap_or(false);
    let include_text = if flag.text.is_some() {
        url_text
    } else {
        body_text
    };
    let mut developer_message = None;
    if flag.text.is_some() && payload.text.is_some() && url_text != body_text {
        developer_message = Some(
            "url and request body are set to different values; using url:true/false".to_string(),
        );
    }

    let filters = payload.filters.unwrap_or_default();
    let page = payload.page.as_ref().and_then(|p| p.page).unwrap_or(0);
    let per_page = payload.page.as_ref().and_then(|p| p.per_page).unwrap_or(20);

    // Perform the search
    match perform_search(&state.db, &payload.query, &filters, page, per_page).await {
        Ok(response) => {
            let mut out = serde_json::to_value(&response).unwrap();
            if !include_text {
                if let Some(arr) = out["results"].as_array_mut() {
                    for item in arr {
                        if let Some(obj) = item.as_object_mut() {
                            obj.remove("text");
                        }
                    }
                }
            }
            if let Some(msg) = developer_message {
                out.as_object_mut()
                    .unwrap()
                    .insert("developer_message".into(), json!(msg));
            }
            info!(
                "Search completed successfully with {} results",
                response.results.len()
            );
            Ok(Json(out))
        }
        Err(err) => {
            error!("Search failed: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("Search failed: {}", err) })),
            ))
        }
    }
}

/// Execute a query with advanced options via POST
pub async fn query_advanced_post(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let span = tracing_utils::server_span("/query/advanced", "POST");
    let _guard = span.enter();
    info!("Advanced query received: {}", payload);

    // TODO: implement advanced search logic
    Err((
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({ "error": "Advanced search not implemented yet" })),
    ))
}

async fn perform_search(
    db: &crate::db::FuguDB,
    query: &str,
    filters: &[String],
    page: usize,
    per_page: usize,
) -> Result<SearchResponse, Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "Performing search for query: '{}' with {} filters",
        query,
        filters.len()
    );

    // Validate pagination parameters
    let per_page = if per_page == 0 || per_page > 100 {
        20
    } else {
        per_page
    };

    // Perform the search using FuguDB
    match db.search(query, filters, page, per_page).await {
        Ok(search_results) => {
            let results = search_results;
            let total = results.len();
            info!(
                "Search completed successfully with {} results",
                results.len()
            );

            Ok(SearchResponse {
                results,
                total,
                page,
                per_page,
                query: query.to_string(),
            })
        }
        Err(e) => {
            error!("Database search failed: {}", e);
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Search failed: {}", e),
            )))
        }
    }
}

