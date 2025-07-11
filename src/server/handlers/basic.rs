// server/handlers/basic.rs - Basic endpoint handlers
use crate::{server::FacetTreeParams, tracing_utils};
use aide::transform::TransformOperation;
use axum::{
    Json,
    extract::{Query, State},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, info};

use crate::server::server_main::AppState;

pub fn health_docs(op: TransformOperation) -> TransformOperation {
    op.description("Check the health of the server.")
        .response::<200, String>()
}
/// Health check endpoint
pub async fn health(State(state): State<Arc<AppState>>) -> String {
    let span = tracing_utils::server_span("/health", "GET");
    let _guard = span.enter();
    let num_fields = state.db.schema.num_fields();

    debug!("Health check endpoint called, found {num_fields} fields in db");
    format!("OK, found {num_fields} in db")
}

pub fn sayhi_docs(op: TransformOperation) -> TransformOperation {
    op.description("Server says hi")
        .response::<200, Json<Value>>()
}
/// Basic greeting endpoint
// pub async fn sayhi(State(_state): State<Arc<AppState>>) -> Json<Value> {
pub async fn sayhi(Query(_params): Query<FacetTreeParams>) -> Json<Value> {
    let span = tracing_utils::server_span("/", "GET");
    let _guard = span.enter();

    info!("api endpoint hit");

    Json(json!({"message":"hi!"}))
}

