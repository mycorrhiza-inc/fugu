// server/handlers/namespaces.rs - Namespace endpoint handlers
use crate::{server::NamespaceUrlComponent, tracing_utils};
use aide::axum::IntoApiResponse;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info};

use crate::server::server_main::AppState;

/// Get all available namespaces
pub async fn get_available_namespaces(State(state): State<Arc<AppState>>) -> impl IntoApiResponse {
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

/// Get facets for a specific namespace
pub async fn get_namespace_facets(
    State(state): State<Arc<AppState>>,
    Path(NamespaceUrlComponent { namespace }): Path<NamespaceUrlComponent>,
) -> impl IntoApiResponse {
    let span = tracing_utils::server_span(&format!("/namespaces/{}/facets", namespace), "GET");
    let _guard = span.enter();

    info!("Get namespace facets endpoint called for: {}", namespace);

    let db = state.db.clone();
    match db.get_namespace_facets(&namespace, "/") {
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

/// Get organization types for a namespace
pub async fn get_namespace_organizations(
    State(state): State<Arc<AppState>>,
    Path(NamespaceUrlComponent { namespace }): Path<NamespaceUrlComponent>,
) -> impl IntoApiResponse {
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
pub async fn get_namespace_conversations(
    State(state): State<Arc<AppState>>,
    Path(NamespaceUrlComponent { namespace }): Path<NamespaceUrlComponent>,
) -> impl IntoApiResponse {
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
pub async fn get_namespace_data_types(
    State(state): State<Arc<AppState>>,
    Path(NamespaceUrlComponent { namespace }): Path<NamespaceUrlComponent>,
) -> impl IntoApiResponse {
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
