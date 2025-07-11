use aide::axum::routing::post_with;
use aide::axum::{
    ApiRouter,
    routing::{delete, get, post, put},
    routing::{get as aide_get, get_with, post as aide_post},
};
// server/routes.rs - Route configuration and setup
use axum::Router;
use tracing::info;

use super::handlers::basic::{health_docs, sayhi_docs};
use super::handlers::search::query_text_get_docs;
use super::handlers::{
    batch_upsert_objects, delete_object, get_all_filters, get_available_namespaces, get_facet_tree,
    get_filter, get_filter_values_at_path, get_namespace_conversations, get_namespace_data_types,
    get_namespace_facets, get_namespace_filters, get_namespace_organizations, get_object_by_id,
    health, ingest_objects, ingest_objects_with_namespace_facets, list_filters, list_objects,
    query_json_post, query_text_get, query_text_path, sayhi, search, upsert_objects,
};
use super::server_main::AppState;

pub fn create_router() -> ApiRouter<std::sync::Arc<AppState>> {
    ApiRouter::new()
        // Basic routes
        .route("/health", get_with(health, health_docs))
        .route("/hi", aide_get(sayhi))
        // Search routes
        .route("/search", aide_get(query_text_get))
        .route("/search", aide_post(search))
        .route("/search/{query}", get(query_text_path))
        .route("/search/json", post(query_json_post))
        // .route("/search/namespace", post(search_with_namespace_facets))
        // Object CRUD routes
        .route("/objects", get(list_objects))
        .route("/objects", put(upsert_objects))
        .route("/objects/{object_id}", get(get_object_by_id))
        .route("/objects/{object_id}", delete(delete_object))
        // Ingest routes
        .route("/ingest", post(ingest_objects))
        .route(
            "/ingest/namespace",
            post(ingest_objects_with_namespace_facets),
        )
        .route("/batch/upsert", post(batch_upsert_objects))
        // Namespace routes
        .route("/namespaces", get(get_available_namespaces))
        .route("/namespaces/{namespace}/facets", get(get_namespace_facets))
        .route(
            "/namespaces/{namespace}/organizations",
            get(get_namespace_organizations),
        )
        .route(
            "/namespaces/{namespace}/conversations",
            get(get_namespace_conversations),
        )
        .route(
            "/namespaces/{namespace}/data",
            get(get_namespace_data_types),
        )
        // Filter routes
        .route("/filters", get(list_filters))
        .route("/filters/all", get(get_all_filters))
        .route("/filters/namespace/{namespace}", get(get_namespace_filters))
        .route("/filters/path/{*filter}", get(get_filter_values_at_path))
        .route("/filters/{namespace}", get(get_filter))
        // Facet tree routes
        .route("/facets/tree", get(get_facet_tree))
}

pub fn log_available_endpoints() {
    info!("Available endpoints:");
    info!("  GET  /");
    info!("  GET  /health");
    info!("  GET  /hi");
    info!("");
    info!("Search endpoints:");
    info!("  GET  /search?q=<query>");
    info!("  GET  /search/:query");
    info!("  POST /search");
    info!("  POST /search/json");
    info!("  POST /search/namespace");
    info!("");
    info!("Object endpoints:");
    info!("  GET  /objects");
    info!("  PUT  /objects");
    info!("  GET  /objects/{{id}}");
    info!("  DELETE /objects/{{id}}");
    info!("");
    info!("Ingest endpoints:");
    info!("  POST /ingest");
    info!("  POST /ingest/namespace");
    info!("  POST /batch/upsert");
    info!("");
    info!("Namespace endpoints:");
    info!("  GET  /namespaces");
    info!("  GET  /namespaces/{{namespace}}/facets");
    info!("  GET  /namespaces/{{namespace}}/organizations");
    info!("  GET  /namespaces/{{namespace}}/conversations");
    info!("  GET  /namespaces/{{namespace}}/data");
    info!("");
    info!("Filter endpoints:");
    info!("  GET  /filters");
    info!("  GET  /filters/all");
    info!("  GET  /filters/namespace/{{namespace}}");
    info!("  GET  /filters/path/{{*filter}}");
    info!("  GET  /filters/{{namespace}}");
    info!("");
    info!("Facet endpoints:");
    info!("  GET  /facets/tree");
}
