// src/server/handlers/objects.rs - Object CRUD endpoint handlers
use crate::server::types::*;
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
use tantivy::Document;

/// Get a specific object by ID
pub async fn get_object_by_id(
    State(state): State<Arc<AppState>>,
    Path(object_id): Path<String>,
) -> Json<Value> {
    let span = tracing_utils::server_span(&format!("/objects/{}", object_id), "GET");
    let _guard = span.enter();

    debug!("Get object endpoint called for ID: {}", object_id);

    // Use the get method to retrieve the object
    match state.db.get(&object_id) {
        Ok(results) => {
            if let Some(record) = results.get(0) {
                Json(json!(record.to_json(&state.db.schema())))
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
    Path(object_id): Path<String>,
) -> impl IntoResponse {
    let span = tracing_utils::server_span(&format!("/objects/{}", object_id), "DELETE");
    let _guard = span.enter();

    info!("Delete object endpoint called for ID: {}", object_id);

    let db = state.db.clone();
    match db.delete_document(object_id.clone()).await {
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
) -> impl IntoResponse {
    let span = tracing_utils::server_span("/objects", "PUT");
    let _guard = span.enter();

    info!("Upsert endpoint called for {} objects", payload.data.len());

    let db = state.db.clone();
    match db.upsert(payload.data).await {
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
