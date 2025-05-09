use tokio::sync::oneshot;
use tokio::time::{Duration, interval};
use tracing::{Instrument, debug, error, info};

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
        let server_mode_span = tracing::span!(tracing::Level::INFO, "server_mode");
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
    let db = match sled::open("fugu_db") {
        Ok(db) => {
            info!("Successfully opened database");
            db
        }
        Err(e) => {
            error!("Failed to open database: {}", e);
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to open database: {}", e),
            )));
        }
    };

    // Create and initialize the primary FuguDB instance
    let mut fdb = FuguDB::new(db);
    fdb.init_db();
    info!("Database initialized successfully");

    // Create a clone of the mailbox for the HTTP server
    let server_mailbox = fdb.mailbox.clone();

    // Take ownership of the receiver channel (since we only need one receiver)
    let compactor_queue = fdb.to_compact_queue.take();

    // Create an API instance that shares the same mailbox
    let server_db = FuguDB::new_api_instance(fdb.db(), server_mailbox);

    // Update the compactor instance with the queue we took from the original
    if let Some(queue) = compactor_queue {
        fdb = FuguDB::new_compactor_instance(fdb.db(), fdb.mailbox.clone(), queue);
    } else {
        error!("Failed to get compaction queue receiver");
    }

    // Set up compactor shutdown channel
    let (_compactor_shutdown_tx, compactor_shutdown_rx) = oneshot::channel::<()>();
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
            _ = run_compactor(fdb, compactor_shutdown_rx) => {
                info!("Compactor has shut down");
            }
        }
        info!("Server shutdown complete");
    }
    .instrument(runtime_span)
    .await;

    Ok(())
}

/// Run the compactor service in the background
async fn run_compactor(mut db: FuguDB, mut shutdown_recv: oneshot::Receiver<()>) {
    let compactor_span = tracing_utils::compactor_span("service", None);

    async {
        let mut interval_timer = interval(Duration::from_millis(200));
        info!("Compactor service started with 200ms interval");

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    let tick_span = tracing_utils::compactor_span("tick", None);
                    let _guard = tick_span.enter();
                    debug!("Compactor tick");
                    db.compact();
                },
                _ = &mut shutdown_recv => {
                    info!("Compactor received shutdown signal");

                    // Process any remaining items before shutting down
                    info!("Running final compaction cycle before shutdown");
                    let final_span = tracing_utils::compactor_span("final_tick", None);
                    let _final_guard = final_span.enter();
                    db.compact();

                    break;
                }
            }
        }
    }
    .instrument(compactor_span)
    .await
}
