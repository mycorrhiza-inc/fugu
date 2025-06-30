// src/server/handlers/mod.rs - Handler module exports

pub mod basic;
pub mod facets;
pub mod filters;
pub mod ingest;
pub mod namespaces;
pub mod objects;
pub mod search;
pub mod utils;

// Re-export handlers individually to avoid conflicts
pub use basic::{health, sayhi};
pub use facets::get_facet_tree;
pub use filters::{
    get_all_filters, get_filter, get_filter_values_at_path, get_namespace_filters, list_filters,
};
pub use ingest::{batch_upsert_objects, ingest_objects, ingest_objects_with_namespace_facets};
pub use namespaces::{
    get_available_namespaces, get_namespace_conversations, get_namespace_data_types,
    get_namespace_facets, get_namespace_organizations,
};
pub use objects::{delete_object, get_object_by_id, list_objects, upsert_objects};
pub use search::{perform_search, query_json_post, query_text_get, query_text_path, search};

// Re-export utils but specify which function to avoid conflicts
pub use utils::is_targeting_conversations_or_organizations as utils_is_targeting_conv_org;

