// src/server/handlers/mod.rs - Handler module exports

pub mod basic;
pub mod search;
pub mod ingest;
pub mod filters;
pub mod facets;
pub mod utils;
pub mod objects;
pub mod namespaces;

// Re-export handlers individually to avoid conflicts
pub use basic::{health, sayhi};
pub use search::{
    query_text_get, query_text_path, search, query_json_post, 
    search_with_namespace_facets, perform_search
};
pub use ingest::{
    ingest_objects, ingest_objects_with_namespace_facets, batch_upsert_objects
};
pub use filters::{
    get_filter, list_filters, get_all_filters, 
    get_namespace_filters, get_filter_values_at_path
};
pub use facets::get_facet_tree;
pub use objects::{get_object_by_id, delete_object, upsert_objects, list_objects};
pub use namespaces::{
    get_available_namespaces, get_namespace_facets, 
    get_namespace_organizations, get_namespace_conversations, 
    get_namespace_data_types
};

// Re-export utils but specify which function to avoid conflicts
pub use utils::is_targeting_conversations_or_organizations as utils_is_targeting_conv_org;