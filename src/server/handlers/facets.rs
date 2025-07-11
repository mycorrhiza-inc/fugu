// server/handlers/facets.rs - Facet endpoint handlers
use crate::server::types::FacetTreeParams;
use crate::tracing_utils;
use aide::axum::IntoApiResponse;
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info};

use crate::server::server_main::AppState;

/// Get the complete facet tree up to max_depth - 1
pub async fn get_facet_tree(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FacetTreeParams>,
) -> impl IntoApiResponse {
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
