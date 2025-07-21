// server/handlers/basic.rs - Basic endpoint handlers
use crate::tracing_utils;
use aide::{axum::IntoApiResponse, transform::TransformOperation};
use axum::{
    Json,
    extract::State,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
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
    let num_fields = match state.db.get_dataset(&state.db.config().default_namespace) {
        Some(dataset) => dataset.docs().schema().fields().count(),
        None => 0,
    };

    debug!("Health check endpoint called, found {num_fields} fields in db");
    format!("OK, found {num_fields} in db")
}

pub fn sayhi_docs(op: TransformOperation) -> TransformOperation {
    op.description("Server says hi")
        .response::<200, Json<BasicMessage>>()
}

/// Basic greeting endpoint
#[derive(Clone, Deserialize, Serialize, JsonSchema)]
pub struct BasicMessage {
    message: String,
}

pub async fn sayhi() -> impl IntoApiResponse {
    let span = tracing_utils::server_span("/", "GET");
    let _guard = span.enter();

    info!("api endpoint hit");

    Json(BasicMessage {
        message: "hi".to_owned(),
    })
    // Json(json!({"message":"hi!"}))
}
