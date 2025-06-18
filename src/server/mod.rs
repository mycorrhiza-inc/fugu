// src/server/mod.rs - Main server module
pub mod handlers;
pub mod routes;
pub mod types;
pub mod server_main;

// Re-export the main server function and AppState from server_main
pub use server_main::{start_http_server, AppState};

// Re-export key types
pub use types::*;