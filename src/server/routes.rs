use aide::axum::routing::post_with;
use aide::axum::{
    ApiRouter,
    routing::{delete, get, post, put},
    routing::get_with,
};
// server/routes.rs - Route configuration and setup
use tracing::info;

use super::handlers::basic::{health_docs, sayhi_docs};
use super::handlers::filters::{get_filter_docs, list_filters_docs};
use super::handlers::objects::{get_object_by_id_docs, list_objects_docs};
use super::handlers::search::{
    query_json_post_docs, query_text_get_docs, query_text_path_docs, search_docs,
};
use super::handlers::{
    batch_upsert_objects, delete_object, get_all_filters, get_available_namespaces, get_facet_tree,
    get_filter, get_filter_values_at_path, get_namespace_conversations, get_namespace_data_types,
    get_namespace_facets, get_namespace_filters, get_namespace_organizations, get_object_by_id,
    health, ingest_objects, ingest_objects_with_namespace_facets, list_filters, list_objects,
    query_json_post, query_text_get, query_text_path, sayhi, search_endpoint,
    upsert_objects,
};
use super::server_main::AppState;

pub fn create_router() -> ApiRouter<std::sync::Arc<AppState>> {
    ApiRouter::new()
        // Basic routes
        .api_route("/health", get_with(health, health_docs))
        .api_route("/hi", get_with(sayhi, sayhi_docs))
        // Search routes
        .api_route("/search", get_with(query_text_get, query_text_get_docs))
        .api_route("/search", post_with(search_endpoint, search_docs))
        .api_route(
            "/search/{query}",
            get_with(query_text_path, query_text_path_docs),
        )
        .api_route(
            "/search/json",
            post_with(query_json_post, query_json_post_docs),
        )
        // .api_route("/search/namespace", post(search_with_namespace_facets))
        // Object CRUD routes
        .api_route("/objects", get_with(list_objects, list_objects_docs))
        .api_route("/objects", put(upsert_objects))
        .api_route(
            "/objects/{object_id}",
            get_with(get_object_by_id, get_object_by_id_docs),
        )
        .api_route("/objects/{object_id}", delete(delete_object))
        // Ingest routes
        .api_route("/ingest", post(ingest_objects))
        .api_route(
            "/ingest/namespace",
            post(ingest_objects_with_namespace_facets),
        )
        .api_route("/batch/upsert", post(batch_upsert_objects))
        // Namespace routes
        .api_route("/namespaces", get(get_available_namespaces))
        .api_route("/namespaces/{namespace}/facets", get(get_namespace_facets))
        .api_route(
            "/namespaces/{namespace}/organizations",
            get(get_namespace_organizations),
        )
        .api_route(
            "/namespaces/{namespace}/conversations",
            get(get_namespace_conversations),
        )
        .api_route(
            "/namespaces/{namespace}/data",
            get(get_namespace_data_types),
        )
        // Filter routes
        .api_route("/filters", get_with(list_filters, list_filters_docs))
        // .api_route("/filters/reindex", post())
        .api_route("/filters/all", get(get_all_filters))
        .api_route("/filters/namespace/{namespace}", get(get_namespace_filters))
        .api_route("/filters/path/{*filter}", get(get_filter_values_at_path))
        .api_route(
            "/filters/{namespace}",
            get_with(get_filter, get_filter_docs),
        )
        // Facet tree routes
        .api_route("/facets/tree", get(get_facet_tree))
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
