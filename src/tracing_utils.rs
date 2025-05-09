use std::fs::{self, File};
use tracing::{Level, Span, info, warn};
use tracing_subscriber::{EnvFilter, filter::LevelFilter, fmt, prelude::*};

/// Initialize the tracing subscriber with console and file outputs.
///
/// This function sets up a comprehensive logging system with:
/// - Console output with ANSI colors
/// - File output in logs/fugu.log
/// - Log filtering based on RUST_LOG env var or INFO level by default
/// - Detailed context (thread IDs, module path, file, line numbers)
///
/// Returns true if initialization was successful, false otherwise.
pub fn init_tracing() -> bool {
    // Initialize tracing with a more comprehensive setup
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        // Parse RUST_LOG environment variable if set
        .from_env_lossy();

    // Create a formatting layer for terminal output
    let stdout_layer = fmt::layer()
        .with_target(true) // Show target module path
        .with_thread_ids(true) // Show thread IDs
        .with_thread_names(true) // Show thread names if available
        .with_file(true) // Show source file
        .with_line_number(true); // Show line numbers

    // Create a log directory if it doesn't exist
    if let Err(e) = fs::create_dir_all("logs") {
        eprintln!("Warning: Failed to create log directory: {}", e);

        // Continue without file logging
        let registry = tracing_subscriber::registry()
            .with(env_filter)
            .with(stdout_layer);

        if let Err(e) = registry.try_init() {
            eprintln!("Error: Failed to initialize tracing: {}", e);
            return false;
        }

        warn!("Tracing initialized with console logging only (file logging failed)");
        return true;
    }

    // Create a file writer for logs
    let log_path = "logs/fugu.log";
    let file = match File::create(log_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Warning: Could not create log file: {}", e);

            // Continue without file logging
            let registry = tracing_subscriber::registry()
                .with(env_filter)
                .with(stdout_layer);

            if let Err(e) = registry.try_init() {
                eprintln!("Error: Failed to initialize tracing: {}", e);
                return false;
            }

            warn!("Tracing initialized with console logging only (file logging failed)");
            return true;
        }
    };

    // Create a file formatting layer
    let file_layer = fmt::layer()
        .with_ansi(false) // No ANSI color codes in files
        .with_target(true) // Include target
        .with_thread_ids(true) // Include thread IDs
        .with_thread_names(true) // Include thread names
        .with_file(true) // Include source file
        .with_line_number(true) // Include line numbers
        .with_writer(file); // Write to file

    // Register the subscriber with all layers
    if let Err(e) = tracing_subscriber::registry()
        .with(env_filter)
        .with(stdout_layer)
        .with(file_layer)
        .try_init()
    {
        eprintln!("Error: Failed to initialize tracing: {}", e);
        return false;
    }

    info!(
        "Tracing initialized with comprehensive logging to console and file: {}",
        log_path
    );
    true
}

/// Create a filter that only logs messages from specific targets
///
/// # Arguments
/// * `targets` - A slice of target strings to include
///
/// # Returns
/// A filter function that can be used with tracing_subscriber
pub fn target_filter<'a>(targets: &'a [&'a str]) -> impl Fn(&tracing::Metadata) -> bool + 'a {
    move |metadata: &tracing::Metadata| -> bool {
        let target = metadata.target();
        targets
            .iter()
            .any(|&allowed_target| target.starts_with(allowed_target))
    }
}

/// Create a JSON formatting layer suitable for structured logging
///
/// This is useful when logs need to be consumed by log aggregation systems
///
/// # Returns
/// A JSON formatting layer
pub fn json_layer() -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> {
    #[cfg(feature = "json")]
    {
        fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(true)
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
    }

    #[cfg(not(feature = "json"))]
    {
        fmt::layer()
            .with_target(true)
            .with_file(true)
            .with_line_number(true)
    }
}

/// Creates a tracing span for the CLI component
///
/// Use this to trace CLI operations with consistent naming
///
/// # Arguments
/// * `command` - The CLI command being executed
///
/// # Returns
/// A new tracing span for the CLI operation
pub fn cli_span(command: &str) -> Span {
    tracing::span!(Level::INFO, "cli", command = command)
}

/// Creates a tracing span for the HTTP server component
///
/// Use this to trace HTTP server operations with consistent naming
///
/// # Arguments
/// * `endpoint` - The API endpoint being handled
/// * `method` - The HTTP method (GET, POST, etc.)
///
/// # Returns
/// A new tracing span for the server operation
pub fn server_span(endpoint: &str, method: &str) -> Span {
    tracing::span!(Level::INFO, "server", endpoint = endpoint, method = method)
}

/// Creates a tracing span for database operations
///
/// Use this to trace database operations with consistent naming
///
/// # Arguments
/// * `operation` - The database operation being performed
///
/// # Returns
/// A new tracing span for the database operation
pub fn db_span(operation: &str) -> Span {
    tracing::span!(Level::INFO, "database", operation = operation)
}

/// Creates a tracing span for the compactor service
///
/// Use this to trace compactor operations with consistent naming
///
/// # Arguments
/// * `operation` - The specific compaction operation
/// * `records` - Optional count of records being processed
///
/// # Returns
/// A new tracing span for the compactor operation
pub fn compactor_span(operation: &str, records: Option<usize>) -> Span {
    if let Some(count) = records {
        tracing::span!(
            Level::INFO,
            "compactor",
            operation = operation,
            records = count
        )
    } else {
        tracing::span!(Level::INFO, "compactor", operation = operation)
    }
}

/// Creates a tracing span for the query engine
///
/// Use this to trace query operations with consistent naming
///
/// # Arguments
/// * `operation` - The specific query operation (e.g., "search_text", "search_json")
/// * `query` - The query text or summary
/// * `additional_fields` - Optional HashMap of additional fields to include in the span
///
/// # Returns
/// A new tracing span for the query operation
pub fn query_span<S>(operation: &str, query: &str, additional_fields: Option<std::collections::HashMap<&str, S>>) -> Span
where
    S: std::fmt::Debug + Send + Sync + 'static,
{
    // Create a basic span with the operation and query
    let mut span = tracing::span!(
        Level::INFO,
        "query",
        operation = operation,
        query = query
    );

    // Add additional fields if provided
    let mut final_span = span;
    if let Some(fields) = additional_fields {
        for (key, value) in fields {
            final_span = final_span.record(key, &tracing::field::debug(value)).clone();
        }
    }

    final_span
}
