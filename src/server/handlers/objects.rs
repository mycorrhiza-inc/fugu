// src/server/handlers/objects.rs - Object CRUD endpoint handlers
use crate::server::types::*;
use crate::tracing_utils;
use aide::axum::IntoApiResponse;
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::ObjectRecord;
use crate::server::server_main::AppState;
use crate::db::document::DocumentOperations;
use aide::transform::TransformOperation;
use tantivy::Document;

pub fn get_object_by_id_docs(op: TransformOperation) -> TransformOperation {
    op.description("Get a specific object by ID.")
        .response::<200, Json<ObjectRecord>>()
}

pub fn list_objects_docs(op: TransformOperation) -> TransformOperation {
    op.description("Get all objects stored in the database.")
        .response::<200, Json<Vec<ObjectRecord>>>()
}

/// Get a specific object by ID
pub async fn get_object_by_id(
    State(state): State<Arc<AppState>>,
    Path(ObjectidUrlComponent { object_id }): Path<ObjectidUrlComponent>,
) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/objects/{}", object_id), "GET");
    let _guard = span.enter();

    debug!("Get object endpoint called for ID: {}", object_id);

    // Use the get method to retrieve the object
    let default_dataset = match state.db.get_dataset(&state.db.config().default_namespace) {
        Some(dataset) => dataset,
        None => {
            return Json(json!({
                "error": "Default dataset not found"
            }));
        }
    };

    match default_dataset.get(&object_id) {
        Ok(results) => {
            if let Some(record) = results.get(0) {
                Json(json!(record.to_json(&default_dataset.docs().schema())))
            } else {
                Json(json!({
                    "error": format!("Object with id {} not found", object_id)
                }))
            }
        }
        Err(e) => Json(json!({
            "error": format!("Error getting object with id {}: {}", object_id, e)
        })),
    }
}

/// Delete a single object by ID
pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path(ObjectidUrlComponent { object_id }): Path<ObjectidUrlComponent>,
) -> impl IntoApiResponse {
    let span = tracing_utils::server_span(&format!("/objects/{}", object_id), "DELETE");
    let _guard = span.enter();

    info!("Delete object endpoint called for ID: {}", object_id);

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

    match default_dataset.delete_document(object_id.clone()).await {
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "message": format!("Object with ID '{}' deleted successfully", object_id)
            })),
        ),
        Err(e) => {
            error!("Failed to delete object {}: {}", object_id, e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to delete object: {}", e)
                })),
            )
        }
    }
}

/// Upsert multiple objects: delete existing by ID then insert new ones
pub async fn upsert_objects(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IndexRequest>,
) -> impl IntoApiResponse {
    let span = tracing_utils::server_span("/objects", "PUT");
    let _guard = span.enter();

    info!("Upsert endpoint called for {} objects", payload.data.len());

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
        Ok(_) => (
            StatusCode::OK,
            Json(json!({
                "status": "success",
                "message": "Objects upserted successfully"
            })),
        ),
        Err(e) => {
            error!("Failed to upsert objects: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to upsert objects: {}", e)
                })),
            )
        }
    }
}

/// Get all objects stored in the database
pub async fn list_objects(State(_state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/objects", "GET");
    let _guard = span.enter();

    debug!("List objects endpoint called");

    // TODO: Implement actual object listing with pagination
    Json(json!({
        "message": "Object listing not yet implemented",
        "suggestion": "Use search endpoints to find objects"
    }))
}
