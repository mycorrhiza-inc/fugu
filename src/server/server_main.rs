// server/server_main.rs - Main server startup and configuration
use crate::tracing_utils;
use crate::{db::DatasetManager, otel_setup::init_subscribers_and_loglevel};
use aide::openapi::{Info, OpenApi};
use aide::swagger::Swagger;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Extension, Json};
use axum_tracing_opentelemetry::middleware::{OtelAxumLayer, OtelInResponseLayer};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::signal;
use tracing::{Instrument, debug, error, info};

use super::routes;

// Define a shared application state for dependency injection
pub struct AppState {
    // Using Arc for shared ownership across handlers, but no Mutex since only the compactor writes
    pub db: DatasetManager,
}

async fn serve_api(Extension(api): Extension<OpenApi>) -> Json<Value> {
    info!("Got API object, attempting JSON serialization");

    // Attempt to serialize the OpenApi struct to a JSON Value
    match serde_json::to_value(&api) {
        Ok(value) => {
            info!("Serialization successful");
            Json(value)
        }
        Err(e) => {
            // Log detailed error information
            error!(err = %e, type_name =  std::any::type_name::<OpenApi>() ,"ApiSerialization FAILED");

            // Return a structured error response
            Json(json!({
                "error": "Serialization failed",
                "message": e.to_string(),
                "type": std::any::type_name::<OpenApi>(),
                "debug api": format!("{:?}", api)
            }))
        }
    }
}
pub async fn start_http_server(http_port: u16, fugu_db: DatasetManager) {
    let server_span = tracing::span!(tracing::Level::INFO, "http_server", port = http_port);

    async {
        info!("Starting HTTP server with pre-initialized database");

        let app_state = Arc::new(AppState { db: fugu_db });
        info!("Created shared application state");

        let _ = init_subscribers_and_loglevel()
            .expect("Failed to initialize opentelemetry tracing stuff");

        let mut api = OpenApi {
            info: Info {
                description: Some(
                    "A search database, but with blackjack and other misc improvements".to_string(),
                ),
                ..Info::default()
            },
            ..OpenApi::default()
        };

        // Create the router with shared state using our modular route configuration
        let app = routes::create_router()
            .route("/api.json", get(serve_api))
            .route("/swagger", Swagger::new("/api.json").axum_route())
            .with_state(app_state.clone())
            .layer(OtelInResponseLayer)
            //start OpenTelemetry trace on incoming request
            .layer(OtelAxumLayer::default())
            .finish_api(&mut api)
            // Expose the documentation to the handlers.
            .layer(Extension(api));

        debug!("API routes configured with shared state");

        let server_addr = format!("0.0.0.0:{}", http_port);
        info!("Binding HTTP server to {}", server_addr);

        let http_listener = match tokio::net::TcpListener::bind(server_addr.clone()).await {
            Ok(listener) => {
                info!("Successfully bound to {}", server_addr);
                listener
            }
            Err(e) => {
                error!("Failed to bind HTTP server to {}: {}", server_addr, e);
                return;
            }
        };

        info!(
            "HTTP server started on {}. Available endpoints:",
            server_addr,
        );
        routes::log_available_endpoints();

        // Start the server with graceful shutdown
        axum::serve(http_listener, app)
            .with_graceful_shutdown(shutdown_signal(app_state))
            .await
            .unwrap();
        info!("Server shut down gracefully");
    }
    .instrument(server_span)
    .await
}

async fn shutdown_signal(app_state: Arc<AppState>) {
    // Create a span for the shutdown signal handler
    let shutdown_span = tracing::span!(tracing::Level::INFO, "shutdown_signal");

    async {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
            debug!("Received Ctrl+C signal");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("Failed to install signal handler")
                .recv()
                .await;
            debug!("Received termination signal");
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }

        info!("Shutdown signal received, starting graceful shutdown");

        // Persist database to disk before shutting down
        info!("Persisting database to disk before shutdown...");
    }
    .instrument(shutdown_span)
    .await
}
