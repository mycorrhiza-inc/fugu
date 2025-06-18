// server/handlers/basic.rs - Basic endpoint handlers
use crate::tracing_utils;
use axum::{
    Json,
    extract::State,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, info};

use crate::server::server_main::AppState;

/// Health check endpoint
pub async fn health() -> &'static str {
    let span = tracing_utils::server_span("/health", "GET");
    let _guard = span.enter();

    debug!("Health check endpoint called");
    "OK"
}

/// Basic greeting endpoint
pub async fn sayhi(State(state): State<Arc<AppState>>) -> Json<Value> {
    let span = tracing_utils::server_span("/", "GET");
    let _guard = span.enter();

    info!("api endpoint hit");

    Json(json!({"message":"hi!"}))
}