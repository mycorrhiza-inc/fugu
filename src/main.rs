use std::time::Instant;
use tokio::sync::oneshot;
use tokio::time::{Duration, interval};
use tracing::{Instrument, debug, error, info, warn};

// Import our crate modules
use fugu::db::{FuguDB, FuguDBBackend};
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

    let fdb = {
        // Configure fjall with optimized settings
        let config = fjall::Config::new("fugu_db")
            .cache_size(512 * 1024 * 1024) // 512MB cache for better read performance
            .max_write_buffer_size(64 * 1024 * 1024) // 64MB write buffer for better write performance
            .compaction_workers(4) // Use 4 threads for compaction
            .flush_workers(2) // Use 2 threads for flushing
            .manual_journal_persist(false) // Automatic persistence
            .fsync_ms(Some(100)); // Fsync every 100ms for durability with good performance

        let keyspace = match config.open() {
            Ok(keyspace) => {
                info!("Successfully opened fjall keyspace with optimized configuration");
                keyspace
            }
            Err(e) => {
                error!("Failed to open fjall keyspace: {:?}", e);
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to open fjall keyspace: {:?}", e),
                )));
            }
        };
        FuguDB::new(keyspace)
    };

    fdb.init_db();
    info!("Database initialized successfully");

    // With the unified backend, we don't need mailbox or compactor queue anymore
    // These were part of the old implementation before the backend abstraction

    // We just pass the FuguDB instance directly to the server
    let server_db = fdb.clone();

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
    // Compactor implementation with adaptive scheduling for different backends
    // Default to compacting every 60 seconds
    let mut maintenance_interval = interval(Duration::from_secs(60));

    // For full compaction, use a longer interval (every 10 minutes)
    let mut full_compaction_interval = interval(Duration::from_secs(600));

    // Track last full compaction time
    let mut last_full_compaction = Instant::now();

    info!("Starting background compactor service");

    loop {
        tokio::select! {
            _ = maintenance_interval.tick() => {
                let now = Instant::now();
                debug!("Performing periodic maintenance");

                // Use our unified maintenance API for lightweight operations
                if let Err(e) = db.maintenance() {
                    error!("Error during database maintenance: {:?}", e);
                } else {
                    debug!("Periodic maintenance completed in {:?}", now.elapsed());
                }

                // Check our standard trees for targeted maintenance every few cycles
                static mut CYCLE_COUNT: u8 = 0;
                unsafe {
                    CYCLE_COUNT = (CYCLE_COUNT + 1) % 5; // Every 5 cycles
                    if CYCLE_COUNT == 0 {
                        // Perform targeted maintenance on the most important trees
                        let important_trees = [
                            "records",
                            "filters",
                            "global_index"
                        ];

                        for tree_name in &important_trees {
                            debug!("Performing targeted maintenance on {}", tree_name);
                            if let Err(e) = db.compact_tree(tree_name) {
                                warn!("Error during targeted maintenance of {}: {:?}", tree_name, e);
                            }
                        }
                    }
                }
            }

            _ = full_compaction_interval.tick() => {
                let now = Instant::now();
                info!("Starting full database compaction");

                // For both backends, perform a full compaction using our unified API
                if let Err(e) = db.compact() {
                    error!("Error during full database compaction: {:?}", e);
                } else {
                    info!("Full database compaction completed in {:?}", now.elapsed());
                }

                last_full_compaction = Instant::now();
            }

            // Use &mut to avoid moving the receiver in the loop
            _ = &mut shutdown_recv => {
                info!("Received shutdown signal, performing final maintenance before stopping");

                // Do a final compaction before shutting down
                if let Err(e) = db.compact() {
                    error!("Error during final compaction: {:?}", e);
                } else {
                    info!("Final compaction completed successfully");
                }

                // Force persistence for fjall
                if let FuguDBBackend::Fjall(ref keyspace) = db.backend {
                    if let Err(e) = keyspace.persist(fjall::PersistMode::SyncAll) {
                        error!("Error during final fjall persistence: {:?}", e);
                    } else {
                        debug!("Successfully persisted fjall data on shutdown");
                    }
                }

                info!("Compactor service shutdown complete");
                break;
            }
        }
    }
}
