//! FuguDB - A fast, schema-aware multi-index document database
//!
//! FuguDB provides a flexible database system built on Tantivy with support for:
//! - Multiple specialized indexes per namespace (docs, filter, query)
//! - Schema-aware field access with compile-time safety
//! - YAML-based configuration for different environments
//! - Multi-tenant namespace support
//! - Faceted search and query suggestions

//! Core database functionality

// Core dataset and index management
pub mod core;

// Document operations (upsert, delete, search)
pub mod document;

// Configuration and server startup
pub mod config;

// Usage examples and patterns
//pub mod examples;

// Utility functions  
pub mod utils;

// Search functionality
pub mod search;

// Facet operations  
pub mod facet;

// Re-export main types for easier imports
pub use config::{DatasetManager, NamespaceConfig, ServerConfig, ValidationConfig};
pub use core::{Dataset, IndexType, NamedIndex};
pub use document::DocumentOperations;
pub use schemas::{build_docs_schema, build_filter_index_schema, build_query_index_schema};
pub use crate::db::search::{FuguSearchResult, SearchOptions};

// Schema builders for different index types
pub mod schemas;

// Re-export ObjectRecord from the existing object module
pub use crate::object::ObjectRecord;

// Server modules exist in src/server/, not here

// CLI module exists in src/cli.rs, not here

// Tracing utilities exist in src/tracing_utils.rs, not here

// Re-export commonly used types at the crate root
// (ObjectRecord already re-exported above)

// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Quick setup function for simple use cases
pub async fn quick_start(
    namespace: &str,
    data_path: &str,
) -> Result<DatasetManager, Box<dyn std::error::Error>> {
    Dataset::quick_setup(namespace, data_path).map_err(|e| e.into())
}

/// Setup from configuration file
pub async fn setup_from_config<P: AsRef<std::path::Path>>(
    config_path: P,
) -> Result<DatasetManager, Box<dyn std::error::Error>> {
    Dataset::setup_from_config(config_path).map_err(|e| e.into())
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_full_integration() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;

        // Test quick start
        let manager = quick_start("test", temp_dir.path().to_str().unwrap()).await?;

        // Verify setup
        assert!(!manager.list_namespaces().is_empty());
        assert_eq!(manager.config().default_namespace, "test");

        // Test dataset operations
        let default_dataset = manager.get_dataset("test").unwrap();
        default_dataset.validate_all_schemas()?;

        // Test all index types
        assert_eq!(default_dataset.docs().index_type(), IndexType::Docs);
        assert_eq!(
            default_dataset.filter_index().index_type(),
            IndexType::FilterIndex
        );
        assert_eq!(
            default_dataset.query_index().index_type(),
            IndexType::QueryIndex
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_config_based_setup() -> Result<(), Box<dyn std::error::Error>> {
        use crate::db::config::templates;
        use std::fs;

        let temp_dir = TempDir::new()?;
        let config_path = temp_dir.path().join("test_config.yaml");

        // Write test config
        fs::write(&config_path, templates::basic_yaml_template())?;

        // Setup from config
        let manager = setup_from_config(&config_path).await?;

        // Verify configuration was loaded
        assert_eq!(manager.config().default_namespace, "fugu_db");

        Ok(())
    }
}
