// server/server_main.rs - Main server startup and configuration
use crate::tracing_utils;
use crate::{db::FuguDB, otel_setup::init_subscribers_and_loglevel};
use std::sync::Arc;
use tokio::signal;
use tracing::{Instrument, debug, error, info};

use super::routes;

// Define a shared application state for dependency injection
pub struct AppState {
    // Using Arc for shared ownership across handlers, but no Mutex since only the compactor writes
    pub db: FuguDB,
}

pub async fn start_http_server(http_port: u16, fugu_db: FuguDB) {
    let server_span = tracing::span!(tracing::Level::INFO, "http_server", port = http_port);

    async {
        info!("Starting HTTP server with pre-initialized database");

        let app_state = Arc::new(AppState { db: fugu_db });
        info!("Created shared application state");

        let _ = init_subscribers_and_loglevel()
            .expect("Failed to initialize opentelemetry tracing stuff");

        // Create the router with shared state using our modular route configuration
        let app = routes::create_router()
            .with_state(app_state.clone())
            .layer(OtelInResponseLayer)
            //start OpenTelemetry trace on incoming request
            .layer(OtelAxumLayer::default());

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

