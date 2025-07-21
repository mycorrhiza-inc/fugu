// main.rs
use std::path::PathBuf;
use tracing::{Instrument, debug, info, warn};

// Import our crate modules
use fugu::db::{Dataset, DatasetManager};
use fugu::server;
use fugu::tracing_utils;

// Main entry point
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        info!("Running in server mode");
        run_server_mode().await?;
    }
    Ok(())
}

/// Handles all server-specific functionality
async fn run_server_mode() -> Result<(), Box<dyn std::error::Error>> {
    let setup_span = tracing_utils::db_span("setup");
    let _setup_guard = setup_span.enter();

    // Database initialization with configuration
    info!("Initializing database with configuration");

    let dataset_manager = initialize_dataset_manager().await?;

    info!("Database initialized successfully");
    info!(
        "Available namespaces: {:?}",
        dataset_manager.list_namespaces()
    );

    // Log dataset statistics
    if let Ok(all_stats) = dataset_manager.get_all_stats() {
        for (namespace, stats) in all_stats {
            info!(
                "Namespace '{}': {} total documents across all indexes",
                namespace,
                stats.total_docs()
            );
        }
    }

    // Verify the default dataset exists
    let _default_dataset = dataset_manager
        .get_dataset(&dataset_manager.config().default_namespace)
        .ok_or("Default dataset not found")?;

    debug!("Created shutdown channel...");

    // Start the server
    info!("Starting Fugu server...");

    // Create a span for the server runtime
    let runtime_span = tracing::span!(tracing::Level::INFO, "server_runtime");

    // Run the HTTP server within the runtime span
    async {
        tokio::select! {
            _ = server::start_http_server(3301, dataset_manager.clone()) => {
                info!("HTTP server has shut down");
            },
        }
        info!("Server shutdown complete");
    }
    .instrument(runtime_span)
    .await;

    Ok(())
}

/// Initialize the dataset manager from configuration
async fn initialize_dataset_manager() -> Result<DatasetManager, Box<dyn std::error::Error>> {
    // Try to load from configuration file first
    let config_paths = [
        "server_config.yaml",
        "server_config.yml",
        "config/server_config.yaml",
        "config.yaml",
    ];

    for config_path in &config_paths {
        let path = PathBuf::from(config_path);
        if path.exists() {
            info!("Loading configuration from: {}", config_path);
            match Dataset::setup_from_config(&path) {
                Ok(manager) => {
                    info!("Successfully loaded configuration from {}", config_path);
                    return Ok(manager);
                }
                Err(e) => {
                    warn!("Failed to load config from {}: {}", config_path, e);
                    continue;
                }
            }
        }
    }

    // Fallback to environment variables or defaults
    info!("No configuration file found, using defaults with environment overrides");

    let data_path = std::env::var("FUGU_DATA_PATH").unwrap_or_else(|_| "./data".to_string());

    let default_namespace =
        std::env::var("FUGU_DEFAULT_NAMESPACE").unwrap_or_else(|_| "fugu_db".to_string());

    info!("Using data path: {}", data_path);
    info!("Using default namespace: {}", default_namespace);

    let manager = Dataset::quick_setup(&default_namespace, &data_path)?;

    Ok(manager)
}

/// Alternative initialization function for specific use cases
#[allow(dead_code)]
async fn initialize_with_custom_config() -> Result<DatasetManager, Box<dyn std::error::Error>> {
    use fugu::db::{NamespaceConfig, ServerConfig, ValidationConfig};
    use std::collections::HashMap;

    // Create custom configuration programmatically
    let config = ServerConfig {
        data_path: PathBuf::from("./custom_data"),
        default_namespace: "main_db".to_string(),
        namespaces: vec![
            NamespaceConfig {
                name: "main_db".to_string(),
                data_path: None,
                initialize_on_startup: true,
                schema_overrides: None,
            },
            NamespaceConfig {
                name: "analytics".to_string(),
                data_path: Some(PathBuf::from("./analytics_data")),
                initialize_on_startup: true,
                schema_overrides: None,
            },
            NamespaceConfig {
                name: "temp_storage".to_string(),
                data_path: None,
                initialize_on_startup: false,
                schema_overrides: None,
            },
        ],
        writer_memory_budget: Some(100_000_000), // 100MB
        auto_create_directories: true,
        validation: ValidationConfig {
            require_non_empty_id: true,
            max_text_length: Some(5_000_000), // 5MB
            validate_facet_paths: true,
        },
    };

    let manager = DatasetManager::from_config(config)?;

    Ok(manager)
}

/// Health check function that can be called from server endpoints
pub async fn health_check(
    manager: &DatasetManager,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let stats = manager.get_all_stats()?;
    let namespaces = manager.list_namespaces();

    let health_info = serde_json::json!({
        "status": "healthy",
        "namespaces": namespaces,
        "stats": stats,
        "config": {
            "data_path": manager.config().data_path,
            "default_namespace": manager.config().default_namespace,
            "auto_create_directories": manager.config().auto_create_directories,
        }
    });

    Ok(health_info)
}
