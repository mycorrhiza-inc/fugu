use clap::ArgMatches;
use tracing;
use tracing_subscriber::{fmt, prelude::*};
use tracing_subscriber::fmt::writer::MakeWriterExt;
use crate::fugu::config::new_config_manager;

#[allow(dead_code)]
pub fn cmd_namespace(m: ArgMatches) -> Option<String> {
    m.get_one::<String>("namespace").cloned()
}
pub fn init_logging() {
    // Use ConfigManager to get the proper log directory
    let config = new_config_manager(None);
    let log_dir = config.logs_dir();
    
    eprintln!("Initializing logging system with log directory: {:?}", log_dir);
    
    // Create the directory if it doesn't exist
    match std::fs::create_dir_all(&log_dir) {
        Ok(_) => eprintln!("Log directory created or already exists: {:?}", log_dir),
        Err(e) => eprintln!("Warning: Failed to create log directory: {}", e),
    }
    
    // Create log file path with timestamp for uniqueness
    let timestamp = chrono::Local::now().format("%Y-%m-%d_%H-%M-%S");
    let log_file_name = format!("fugu_{}.log", timestamp);
    let log_file_path = log_dir.join(&log_file_name);
    
    eprintln!("Using log file: {:?}", log_file_path);
    
    // Attempt to open the file with explicit options
    let file = match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&log_file_path) {
        Ok(file) => {
            eprintln!("Logging to {}", log_file_path.display());
            file
        },
        Err(e) => {
            eprintln!("Could not create log file {}: {}", log_file_path.display(), e);
            eprintln!("Falling back to stdout-only logging");
            // Return early with stdout-only logging but still use JSON format
            // Use default filter if RUST_LOG is not set
            let filter_str = std::env::var("RUST_LOG").unwrap_or_else(|_| "info,fugu=trace".to_string());
            let filter = tracing_subscriber::EnvFilter::try_new(&filter_str)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,fugu=trace"));
            
            // Create two layers - one pretty for readability and one JSON for structured logging
            let stdout_pretty = fmt::layer()
                .pretty()
                .with_writer(std::io::stdout.with_max_level(tracing::Level::INFO));
                
            let stdout_json = fmt::layer()
                .json()
                .with_writer(std::io::stdout.with_max_level(tracing::Level::TRACE));
                
            tracing_subscriber::registry()
                .with(filter)
                .with(stdout_pretty)
                .with(stdout_json)
                .init();
            return;
        }
    };

    // Set up filter string for logs - either use RUST_LOG or default to info,fugu=trace
    let filter_str = std::env::var("RUST_LOG").unwrap_or_else(|_| "info,fugu=trace".to_string());
    
    // Layer for pretty-printed stdout logs with filter
    let stdout_layer = fmt::layer()
        .pretty()
        .with_writer(std::io::stdout);
        
    // Layer for JSON logs to file with filter
    let json_file_layer = fmt::layer()
        .json()
        .with_writer(file);

    // Create a filter layer
    let filter_layer = tracing_subscriber::EnvFilter::try_new(&filter_str)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,fugu=trace"));
        
    // Build the subscriber with all layers
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(stdout_layer)
        .with(json_file_layer)
        .init();
}
