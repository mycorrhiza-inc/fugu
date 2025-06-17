// object.rs - Enhanced ObjectRecord with namespace facet support
use chrono::{DateTime as ChronoDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tantivy::schema::*;
use tantivy::{DateTime, Document as TantivyDocument};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRecord {
    pub id: String,
    pub text: String,
    pub metadata: Option<serde_json::Value>,
    pub namespace: Option<String>,

    // New fields for namespace facets
    pub organization: Option<String>,
    pub conversation_id: Option<String>,
    pub data_type: Option<String>,

    // Optional timestamps
    pub date_created: Option<String>,
    pub date_updated: Option<String>,
    pub date_published: Option<String>,
}

impl ObjectRecord {
    /// Creates namespace facets based on the object's namespace and type fields
    pub fn generate_namespace_facets(&self) -> Vec<String> {
        let mut facets = Vec::new();

        if let Some(namespace) = &self.namespace {
            // Add base namespace facet
            facets.push(format!("/namespace/{}", namespace));

            // Add organization facet if present
            if let Some(organization) = &self.organization {
                facets.push(format!("/namespace/{}/organization", namespace));
                facets.push(format!(
                    "/namespace/{}/organization/{}",
                    namespace, organization
                ));
            }

            // Add conversation facet if present
            if let Some(conversation_id) = &self.conversation_id {
                facets.push(format!("/namespace/{}/conversation", namespace));
                facets.push(format!(
                    "/namespace/{}/conversation/{}",
                    namespace, conversation_id
                ));
            }

            // Add data type facet if present
            if let Some(data_type) = &self.data_type {
                facets.push(format!("/namespace/{}/data", namespace));
                facets.push(format!("/namespace/{}/data/{}", namespace, data_type));
            }
        }

        facets
    }

    /// Converts ObjectRecord to JSON for API responses
    pub fn to_json(&self, schema: &Schema) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Validates the object record
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.id.is_empty() {
            return Err(anyhow::anyhow!("Object ID cannot be empty"));
        }

        if self.text.is_empty() {
            return Err(anyhow::anyhow!("Object text cannot be empty"));
        }

        // Validate namespace format if present
        if let Some(namespace) = &self.namespace {
            if namespace.is_empty() || namespace.contains('/') || namespace.contains(' ') {
                return Err(anyhow::anyhow!("Invalid namespace format"));
            }
        }

        Ok(())
    }
}

/// Builds the Tantivy schema for ObjectRecord
pub fn build_object_record_schema(mut schema_builder: SchemaBuilder) -> Schema {
    // Core fields
    schema_builder.add_text_field("id", TEXT | STORED);
    schema_builder.add_text_field("name", TEXT | STORED);
    schema_builder.add_text_field("text", TEXT | STORED);
    schema_builder.add_text_field("namespace", TEXT | STORED);

    // New namespace-aware fields
    schema_builder.add_text_field("organization", TEXT | STORED);
    schema_builder.add_text_field("conversation_id", TEXT | STORED);
    schema_builder.add_text_field("data_type", TEXT | STORED);

    // Facet field for hierarchical filtering
    schema_builder.add_facet_field("facet", INDEXED | STORED);

    // Metadata as JSON object
    schema_builder.add_json_field("metadata", STORED);

    // Date fields
    schema_builder.add_date_field("date_created", INDEXED | STORED);
    schema_builder.add_date_field("date_updated", INDEXED | STORED);
    schema_builder.add_date_field("date_published", INDEXED | STORED);

    schema_builder.build()
}
