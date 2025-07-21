// server/handlers/filters.rs - Filter endpoint handlers
use crate::{server::NamespaceUrlComponent, tracing_utils};
use aide::axum::IntoApiResponse;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::server::server_main::AppState;
use aide::transform::TransformOperation;
use schemars::JsonSchema;
use serde::Serialize;

#[derive(Serialize, JsonSchema)]
pub struct FilterResponse {
    filters: Vec<String>,
}

pub fn get_filter_docs(op: TransformOperation) -> TransformOperation {
    op.description("Get filter for a specific namespace (legacy endpoint).")
        .response::<200, Json<FilterResponse>>()
}

pub fn list_filters_docs(op: TransformOperation) -> TransformOperation {
    op.description("List all filters.")
        .response::<200, Json<FilterResponse>>()
}

/// Get filter for a specific namespace (legacy endpoint)
pub async fn get_filter(
    State(state): State<Arc<AppState>>,
    Path(NamespaceUrlComponent { namespace }): Path<NamespaceUrlComponent>,
) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/filters/{}", namespace), "GET");
    let _guard = span.enter();
    debug!("get filter endpoint called for {}", namespace);

    let default_dataset = state.db.get_dataset(&state.db.config().default_namespace)
        .expect("Default dataset not found");
    let facets = default_dataset
                .get_facets(Some(format!("/{}", namespace.to_string())))
        .unwrap();

    Json(json!({"filters": facets }))
}

/// List all filters
pub async fn list_filters(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/filters", "GET");
    let _guard = span.enter();

    let default_dataset = state.db.get_dataset(&state.db.config().default_namespace)
        .expect("Default dataset not found");
    let facets = default_dataset.get_facets(None).unwrap();
    info!("List filters endpoint called");
    let res: Vec<Value> = facets
        .iter()
        .map(|f| json!({"value": f.0.to_string()}))
        .collect();

    Json(json!({"filters": res }))
}

/// Get all filter paths
pub async fn get_all_filters(State(state): State<Arc<AppState>>) -> impl IntoApiResponse {
    let span = tracing_utils::server_span("/filters/all", "GET");
    let _guard = span.enter();

    info!("Get all filter paths endpoint called");

    let default_dataset = match state.db.get_dataset(&state.db.config().default_namespace) {
        Some(dataset) => dataset,
        None => {
            error!("Default dataset not found");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": "Default dataset not found"
                })),
            );
        }
    };

    match default_dataset.as_ref().get_all_filter_paths() {
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
    Path(NamespaceUrlComponent { namespace }): Path<NamespaceUrlComponent>,
) -> impl IntoApiResponse {
    let span = tracing_utils::server_span(&format!("/filters/namespace/{}", namespace), "GET");
    let _guard = span.enter();

    info!(
        "Get namespace filter paths endpoint called for namespace: {}",
        namespace
    );

    let default_dataset = match state.db.get_dataset(&state.db.config().default_namespace) {
        Some(dataset) => dataset,
        None => {
            error!("Default dataset not found");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": "Default dataset not found"
                })),
            );
        }
    };

    match default_dataset.as_ref().get_filter_paths_for_namespace(&namespace) {
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
) -> impl IntoApiResponse {
    let span = tracing_utils::server_span(&format!("/filters/path/{}", filter_path), "GET");
    let _guard = span.enter();

    info!(
        "Get filter values endpoint called for path: {}",
        filter_path
    );

    let default_dataset = match state.db.get_dataset(&state.db.config().default_namespace) {
        Some(dataset) => dataset,
        None => {
            error!("Default dataset not found");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": "Default dataset not found"
                })),
            );
        }
    };

    match default_dataset.as_ref().get_filter_values_at_path(&filter_path) {
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
