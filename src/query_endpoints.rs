use crate::tracing_utils;
use crate::db::FuguSearchResult;  // Import from your db module
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
#[derive(Debug, Deserialize)]
pub struct TextQueryParams {
    q: String,  // Changed from "query" to "q" to match standard
    #[serde(default)]
    limit: Option<usize>,
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
            }))
        );
    }

    let limit = params.limit.unwrap_or(20);
    
    // Perform the search
    match perform_search(&state.db, &params.q, &[], 0, limit).await {
        Ok(response) => {
            info!("Search completed successfully with {} results", response.results.len());
            (StatusCode::OK, Json(json!(response)))
        }
        Err(err) => {
            error!("Search failed: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Search failed: {}", err)
                }))
            )
        }
    }
}

/// Execute a text query via URL path (URL-encoded)
pub async fn query_text_path(
    State(state): State<Arc<AppState>>,
    Path(encoded_query): Path<String>,
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
                }))
            );
        }
    };
    
    info!("Path search query: {}", query);
    
    if query.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Query cannot be empty"
            }))
        );
    }

    // Perform the search with default limit
    match perform_search(&state.db, &query, &[], 0, 20).await {
        Ok(response) => {
            info!("Search completed successfully with {} results", response.results.len());
            (StatusCode::OK, Json(json!(response)))
        }
        Err(err) => {
            error!("Search failed: {}", err);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Search failed: {}", err)
                }))
            )
        }
    }
}

/// Execute a JSON query via POST
pub async fn query_json_post(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<JsonQueryRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let span = tracing_utils::server_span("/search", "POST");
    let _guard = span.enter();
    
    info!("POST search query: {}", payload.query);
    
    if payload.query.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Query cannot be empty"
            }))
        ));
    }

    let filters = payload.filters.unwrap_or_default();
    let page = payload.page.as_ref().and_then(|p| p.page).unwrap_or(0);
    let per_page = payload.page.as_ref().and_then(|p| p.per_page).unwrap_or(20);
    
    // Perform the search
    match perform_search(&state.db, &payload.query, &filters, page, per_page).await {
        Ok(response) => {
            info!("Search completed successfully with {} results", response.results.len());
            Ok(Json(json!(response)))
        }
        Err(err) => {
            error!("Search failed: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": format!("Search failed: {}", err)
                }))
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
    
    // Try to parse as JsonQueryRequest first
    match serde_json::from_value::<JsonQueryRequest>(payload.clone()) {
        Ok(query_request) => {
            if query_request.query.is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "Query cannot be empty"
                    }))
                ));
            }

            let filters = query_request.filters.unwrap_or_default();
            let page = query_request.page.as_ref().and_then(|p| p.page).unwrap_or(0);
            let per_page = query_request.page.as_ref().and_then(|p| p.per_page).unwrap_or(20);
            
            // Perform the search
            match perform_search(&state.db, &query_request.query, &filters, page, per_page).await {
                Ok(response) => {
                    info!("Advanced search completed successfully with {} results", response.results.len());
                    Ok(Json(json!(response)))
                }
                Err(err) => {
                    error!("Advanced search failed: {}", err);
                    Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({
                            "error": format!("Search failed: {}", err)
                        }))
                    ))
                }
            }
        }
        Err(_) => {
            // If we can't parse it as our standard format, try to extract just the query
            if let Some(query_str) = payload.get("query").and_then(|v| v.as_str()) {
                if query_str.is_empty() {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": "Query cannot be empty"
                        }))
                    ));
                }

                // Use default values for other parameters
                match perform_search(&state.db, query_str, &[], 0, 20).await {
                    Ok(response) => {
                        info!("Fallback search completed successfully with {} results", response.results.len());
                        Ok(Json(json!(response)))
                    }
                    Err(err) => {
                        error!("Fallback search failed: {}", err);
                        Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({
                                "error": format!("Search failed: {}", err)
                            }))
                        ))
                    }
                }
            } else {
                warn!("Invalid query format received");
                Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "Invalid query format. Expected JSON with 'query' field."
                    }))
                ))
            }
        }
    }
}

/// Perform the actual search logic
async fn perform_search(
    db: &crate::db::FuguDB,
    query: &str,
    filters: &[String],
    page: usize,
    per_page: usize,
) -> Result<SearchResponse, Box<dyn std::error::Error + Send + Sync>> {
    info!("Performing search for query: '{}' with {} filters", query, filters.len());
    
    // Validate pagination parameters
    let per_page = if per_page == 0 || per_page > 100 { 20 } else { per_page };
    
    // Perform the search using FuguDB
    match db.search(query, filters, page, per_page).await {
        Ok(search_results) => {
            let results = search_results; // They're already the right type

            let total = results.len(); // In a real implementation, you'd get total from the search method

            info!("Search completed successfully with {} results", results.len());

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
                format!("Search failed: {}", e)
            )))
        }
    }
}