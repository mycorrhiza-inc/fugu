// server/types.rs - Type definitions for the server
use crate::db::FuguSearchResult;
use crate::ObjectRecord;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A default error response for most API errors.
#[derive(Debug, Serialize)]
pub struct AppError {
    /// An error message.
    pub error: String,

    /// A unique error ID. Not serialized in response.
    #[serde(skip)]
    pub status: StatusCode,

    /// Optional Additional error details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_details: Option<Value>,
}

impl AppError {
    pub fn new(error: &str) -> Self {
        Self {
            error: error.to_string(),
            status: StatusCode::BAD_REQUEST,
            error_details: None,
        }
    }

    pub fn with_status(mut self, status: StatusCode) -> Self {
        self.status = status;
        self
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.error_details = Some(details);
        self
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status;
        let mut res = Json(self).into_response();
        *res.status_mut() = status;
        res
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pagination {
    pub page: Option<usize>,
    pub per_page: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FuguSearchQuery {
    pub query: String,
    pub filters: Option<Vec<String>>,
    pub page: Option<Pagination>,
}

#[derive(Serialize, Deserialize)]
pub struct AddFileQuery {
    pub name: String,
    pub namespace: Option<String>,
    pub body: String,
}

#[derive(Serialize, Deserialize)]
pub struct DemoIndexRequest {
    pub id: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct IndexRequest {
    pub data: Vec<ObjectRecord>,
}

#[derive(Serialize, Deserialize)]
pub struct BatchIndexRequest {
    pub objects: Vec<ObjectRecord>,
}

#[derive(Serialize, Deserialize)]
pub struct FileIngestionRequest {
    pub file_path: String,
    pub id: Option<String>,
    pub namespace: Option<String>,
    pub metadata: Option<Value>,
}

/// JSON query request body for POST requests
#[derive(Debug, Deserialize)]
pub struct JsonQueryRequest {
    pub query: String,
    pub filters: Option<Vec<String>>,
    pub page: Option<Pagination>,
    /// whether to include the full text in each hit
    #[serde(default)]
    pub text: Option<bool>,
    /// whether to include data objects in results
    #[serde(default)]
    pub include_data: Option<bool>,
}

/// helper for POST?text=...
#[derive(Debug, Deserialize)]
pub struct IncludeTextFlag {
    #[serde(default)]
    pub text: Option<bool>,
    #[serde(default)]
    pub include_data: Option<bool>,
}

/// Simple text query parameters for GET requests
#[derive(Debug, Deserialize)]
pub struct TextQueryParams {
    pub q: String, // Changed from "query" to "q" to match standard
    #[serde(default)]
    pub limit: Option<usize>,
    /// whether to include the `text` of each hit
    #[serde(default)]
    pub text: Option<bool>,
    /// whether to include data objects in results (default: true unless targeting conversations/organizations)
    #[serde(default)]
    pub include_data: Option<bool>,
}

/// Search result item - use an alias to the db type
pub type SearchResultItem = FuguSearchResult;

/// Search response
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub results: Vec<SearchResultItem>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
    pub query: String,
}

#[derive(Debug, Deserialize)]
pub struct FacetTreeParams {
    pub max_depth: Option<usize>,
}