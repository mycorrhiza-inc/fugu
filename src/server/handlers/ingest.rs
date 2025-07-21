// server/handlers/ingest.rs - Data ingest endpoint handlers
use crate::server::types::*;
use crate::tracing_utils;
use aide::axum::IntoApiResponse;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::server::server_main::AppState;
use crate::db::document::DocumentOperations;

/// Ingest objects into the database (now performs upserts)
pub async fn ingest_objects(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> impl IntoApiResponse {
    let span = tracing_utils::server_span("/ingest", "POST");
    let _guard = span.enter();

    info!(
        "Ingest endpoint called for {} objects (performing upserts)",
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

    match default_dataset.ingest(payload.data).await {
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

/// Enhanced ingest endpoint that supports namespace facets - now with explicit facets support
pub async fn ingest_objects_with_namespace_facets(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> impl IntoApiResponse {
    let span = tracing_utils::server_span("/ingest/namespace", "POST");
    let _guard = span.enter();

    info!(
        "Enhanced ingest endpoint called for {} objects with namespace facet support",
        payload.data.len()
    );

    // Validate all objects first and check for explicit facets
    let mut explicit_facets_count = 0;
    let mut generated_facets_count = 0;

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

        // Count facet types for logging
        if object.facets.is_some() {
            explicit_facets_count += 1;
        } else {
            generated_facets_count += 1;
        }
    }

    if explicit_facets_count > 0 {
        info!(
            "Received {} objects with explicit facets, {} with generated facets",
            explicit_facets_count, generated_facets_count
        );
    }

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

    match default_dataset.upsert(payload.data).await {
        Ok(_) => {
            info!("Successfully ingested objects with namespace facets");
            (
                StatusCode::OK,
                Json(json!({
                    "status": "success",
                    "message": "Objects ingested successfully with namespace facets",
                    "explicit_facets_count": explicit_facets_count,
                    "generated_facets_count": generated_facets_count
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

/// Batch upsert objects
pub async fn batch_upsert_objects(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BatchIndexRequest>,
) -> impl IntoApiResponse {
    let span = tracing_utils::server_span("/batch/upsert", "POST");
    let _guard = span.enter();

    info!(
        "Batch upsert endpoint called for {} objects",
        payload.objects.len()
    );

    // Validate all objects first
    for (i, object) in payload.objects.iter().enumerate() {
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

    match default_dataset.batch_upsert(payload.objects).await {
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
