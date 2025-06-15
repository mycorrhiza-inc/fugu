use tracing::{Instrument, debug, info};

// Import our crate modules
use fugu::db::FuguDB;
use fugu::server;
use fugu::tracing_utils;

// Main entry point
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a span for the application initialization
    let init_span = tracing::span!(tracing::Level::INFO, "app_init");
    let _init_guard = init_span.enter();

    // Initialize tracing using our utility module
    if !tracing_utils::init_tracing() {
        eprintln!("Failed to initialize tracing, exiting");
        return Ok(());
    }

    // Check if running in CLI mode or help mode
    let args: Vec<String> = std::env::args().collect();

    // Run in CLI mode if there are any command line arguments
    // (other than the program name itself)
    if args.len() > 1 {
        // Run in CLI mode with its own span
        let cli_mode_span = tracing::span!(tracing::Level::INFO, "cli_mode");
        info!("Running in CLI mode with args: {:?}", &args[1..]);
        return async { fugu::cli::run_cli().await }
            .instrument(cli_mode_span)
            .await;
    } else {
        // Only run server mode if no CLI arguments were provided
        let server_mode_span = tracing::span!(tracing::Level::INFO, "server");
        info!("Running in server mode");
        async { run_server_mode().await }
            .instrument(server_mode_span)
            .await?;
    }

    Ok(())
}

/// Handles all server-specific functionality
async fn run_server_mode() -> Result<(), Box<dyn std::error::Error>> {
    let setup_span = tracing_utils::db_span("setup");
    let _setup_guard = setup_span.enter();

    // Database initialization - properly separated from HTTP server logic
    info!("Initializing database");
    let fdb = FuguDB::new("fugu_db".into());
    info!("Database initialized successfully");

    // With the unified backend, we don't need mailbox or compactor queue anymore
    // These were part of the old implementation before the backend abstraction  

    // We just pass the FuguDB instance directly to the server
    let server_db = fdb.clone();

    debug!("Created shutdown channel...");

    // Start the server and compactor services
    info!("Starting Fugu server...");

    // Create a span for the server runtime
    let runtime_span = tracing::span!(tracing::Level::INFO, "server_runtime");

    // Run the HTTP server and compactor concurrently within the runtime span
    async {
        tokio::select! {
            _ = server::start_http_server(3301, server_db) => {
                info!("HTTP server has shut down");
            },
        }
        info!("Server shutdown complete");
    }
    .instrument(runtime_span)
    .await;

    Ok(())
}
