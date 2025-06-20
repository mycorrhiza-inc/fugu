//! Database module for FuguDB
//! 
//! This module provides a full-text search database built on Tantivy
//! with support for faceted search, namespaces, and metadata indexing.

pub mod core;
pub mod search;
pub mod facet;
pub mod document;
pub mod utils;

// Re-export main types and functions
pub use core::FuguDB;
pub use search::{FuguSearchResult, SearchOptions};
pub use facet::{FacetNode, FacetTreeResponse};
pub use document::DocumentOperations;
pub use utils::{create_metadata_facets, process_additional_fields};

// Implement DocumentOperations directly on FuguDB to make methods available
// This ensures the trait methods are available as inherent methods
// impl crate::db::document::DocumentOperations for FuguDB {}

// Re-export commonly used external types
pub use tantivy::{Document as TantivyDocument, Term};
pub use tantivy::schema::Facet;