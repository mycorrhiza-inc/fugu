// src/server/handlers/search.rs - Search endpoint handlers
use crate::server::types::*;
use crate::tracing_utils;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{error, info, warn};
use urlencoding::decode;

use crate::server::server_main::AppState;

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

    // For GET requests without filters, include all data by default
    let filters = Vec::new();

    // Perform the search
    match perform_search(&state.db, &params.q, &filters, 0, limit).await {
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

    // For path-based searches without filters, include all data by default
    let filters = Vec::new();
    let include_text = params.text.unwrap_or(false);

    match perform_search(&state.db, &query, &filters, 0, 20).await {
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

/// Search endpoint returning full facet paths for each result
pub async fn search(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<FuguSearchQuery>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/search", "POST");
    let _guard = span.enter();

    let query = payload.query.clone();
    let filters = payload.filters.clone().unwrap_or_default();
    let page = payload.page.as_ref().and_then(|p| p.page).unwrap_or(0);
    let per_page = payload.page.as_ref().and_then(|p| p.per_page).unwrap_or(20);

    info!(
        "Search endpoint called with query: {} and filters: {:?}",
        query, filters
    );

    match state.db.search(&query, &filters, page, per_page).await {
        Ok(results) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "query": query,
                "filters": filters,
                "page": page,
                "per_page": per_page,
                "total": results.len(),
                "results": results
            })),
        ),
        Err(e) => {
            error!("Search failed: {}", e);
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

/// Execute a JSON query via POST
pub async fn query_json_post(
    State(state): State<Arc<AppState>>,
    Query(flag): Query<IncludeTextFlag>,
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

    // Determine if we should include data objects
    // By default, include data unless specifically targeting conversations/organizations
    let targeting_conv_or_org =
        crate::server::handlers::utils::is_targeting_conversations_or_organizations(&filters);
    let include_data = payload
        .include_data
        .or(flag.include_data)
        .unwrap_or(!targeting_conv_or_org);

    info!(
        "Search targeting conv/org: {}, include_data: {}, filters: {:?}",
        targeting_conv_or_org, include_data, filters
    );

    // The database search method now handles conditional data inclusion automatically
    // based on the filters, so we just pass the filters as-is
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
            // Add metadata about data inclusion
            out.as_object_mut()
                .unwrap()
                .insert("includes_data_objects".into(), json!(include_data));
            out.as_object_mut().unwrap().insert(
                "targeting_conversations_or_organizations".into(),
                json!(targeting_conv_or_org),
            );

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

///// Enhanced search endpoint with namespace facet support
//pub async fn search_with_namespace_facets(
//    State(state): State<Arc<AppState>>,
//    Json(payload): Json<FuguSearchQuery>,
//) -> impl IntoResponse {
//    let span = tracing_utils::server_span("/search/namespace", "POST");
//    let _guard = span.enter();
//
//    info!(
//        "Namespace facet search endpoint called with query: {}",
//        payload.query
//    );
//
//    let db = state.db.clone();
//    let filters = payload.filters.unwrap_or_default();
//    let page = payload.page.as_ref().and_then(|p| p.page).unwrap_or(0);
//    let per_page = payload.page.as_ref().and_then(|p| p.per_page).unwrap_or(20);
//
//    match db
//        .search_with_namespace_facets(&payload.query, &filters, page, per_page)
//        .await
//    {
//        Ok(results) => {
//            let response = json!({
//                "status": "success",
//                "results": results,
//                "query": payload.query,
//                "filters": filters,
//                "total": results.len(),
//                "page": page,
//                "per_page": per_page
//            });
//            (StatusCode::OK, Json(response))
//        }
//        Err(e) => {
//            error!("Namespace facet search failed: {}", e);
//            (
//                StatusCode::INTERNAL_SERVER_ERROR,
//                Json(json!({
//                    "status": "error",
//                    "error": format!("Search failed: {}", e)
//                })),
//            )
//        }
//    }
//}

pub async fn perform_search(
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

    // The database search method now handles conditional data inclusion automatically
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

