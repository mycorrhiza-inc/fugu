// src/lib.rs
pub mod cli;
pub mod db;

pub mod object;
pub mod otel_setup;
pub mod server;
pub mod tracing_utils;

// Re-export commonly used types
pub use object::ObjectRecord;

// Optional modules
#[cfg(feature = "s3")]
pub mod s3;

// Test modules
#[cfg(test)]
pub mod db_test;
