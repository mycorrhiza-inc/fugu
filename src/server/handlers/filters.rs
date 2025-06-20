// server/handlers/filters.rs - Filter endpoint handlers
use crate::tracing_utils;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::server::server_main::AppState;

/// Get filter for a specific namespace (legacy endpoint)
pub async fn get_filter(
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

/// List all filters
pub async fn list_filters(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/filters", "GET");
    let _guard = span.enter();

    let facets = state.db.clone().get_facets(None).unwrap();
    info!("List filters endpoint called");
    let res: Vec<Value> = facets
        .iter()
        .map(|f| json!({"value": f.0.to_string()}))
        .collect();

    Json(json!({"filters": res }))
}

/// Get all filter paths
pub async fn get_all_filters(State(state): State<Arc<AppState>>) -> impl IntoResponse {
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

/// Get filter paths for a specific namespace
pub async fn get_namespace_filters(
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

/// Get filter values at a specific path
pub async fn get_filter_values_at_path(
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