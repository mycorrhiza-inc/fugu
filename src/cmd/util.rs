use clap::ArgMatches;
use std::{fs::File};
use tracing_subscriber::{fmt, prelude::*};

pub fn cmd_namespace(m: ArgMatches) -> Option<String> {
    m.get_one::<String>("namespace").cloned()
}
pub fn init_logging() {
    let file = File::create(format!(
        "~/.fugu/tmp/{}.log",
        chrono::Local::now().format("%Y-%m-%d")
    ))
    .expect("Could not create log file");

    // Layer for pretty-printed stdout logs
    let stdout_layer = fmt::layer()
        .pretty() // <-- Pretty formatting
        .with_writer(std::io::stdout);

    // Layer for JSON logs to file
    let json_file_layer = fmt::layer().json().with_writer(file); // <-- Save to file

    // Optional: filter log levels via RUST_LOG

    // Build the subscriber with both layers
    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(json_file_layer)
        .init();
}
