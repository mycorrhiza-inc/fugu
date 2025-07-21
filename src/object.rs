// object.rs - Enhanced ObjectRecord with namespace facet support
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tantivy::schema::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
pub struct ObjectRecord {
    pub id: String,
    pub text: String,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
    pub namespace: Option<String>,

    // CRITICAL: Add explicit facets field to match Go SDK
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<String>>,

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
    /// Validates the object record
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.id.is_empty() {
            return Err(anyhow::anyhow!("Object ID cannot be empty"));
        }

        if self.id.len() > 256 {
            return Err(anyhow::anyhow!("Object ID too long (max 256 characters)"));
        }

        if self.text.is_empty() {
            return Err(anyhow::anyhow!("Object text cannot be empty"));
        }

        if self.text.len() > 10000 {
            return Err(anyhow::anyhow!("Text too long (max 10000 characters)"));
        }

        // Validate namespace format if present
        if let Some(namespace) = &self.namespace {
            if namespace.is_empty() || namespace.contains('/') || namespace.contains(' ') {
                return Err(anyhow::anyhow!("Invalid namespace format"));
            }
            if namespace.len() > 128 {
                return Err(anyhow::anyhow!("Namespace too long (max 128 characters)"));
            }
        }

        // Validate facets if present
        if let Some(facets) = &self.facets {
            if facets.len() > 100 {
                return Err(anyhow::anyhow!("Too many facets (max 100 per object)"));
            }

            for (i, facet) in facets.iter().enumerate() {
                if facet.is_empty() {
                    return Err(anyhow::anyhow!("Facet at index {} cannot be empty", i));
                }
                if facet.len() > 512 {
                    return Err(anyhow::anyhow!(
                        "Facet at index {} too long (max 512 characters)",
                        i
                    ));
                }
            }
        }

        Ok(())
    }

    /// Generate namespace facets (for backward compatibility)
    pub fn generate_namespace_facets(&self) -> Vec<String> {
        let mut facets = Vec::new();

        // Add base namespace facet
        if let Some(namespace) = &self.namespace {
            facets.push(format!("/namespace/{}", namespace));

            // Add organization facet
            if let Some(organization) = &self.organization {
                facets.push(format!(
                    "/namespace/{}/organization/{}",
                    namespace, organization
                ));
            }

            // Add conversation facet
            if let Some(conversation_id) = &self.conversation_id {
                facets.push(format!(
                    "/namespace/{}/conversation/{}",
                    namespace, conversation_id
                ));
            }

            // Add data type facet
            if let Some(data_type) = &self.data_type {
                facets.push(format!("/namespace/{}/data/{}", namespace, data_type));
            }
        }

        facets
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

/// builds the schema for facets
pub fn build_facet_index_schema(mut schema_builder: SchemaBuilder) -> Schema {
    // Core fields
    schema_builder.add_text_field("text", TEXT | STORED);
    // field of metadata facet eg `metadata.description`
    schema_builder.add_text_field("field", TEXT | STORED);
    schema_builder.add_text_field("namespace", TEXT | STORED);

    // Date fields
    schema_builder.add_date_field("date_created", INDEXED | STORED);
    schema_builder.add_date_field("date_updated", INDEXED | STORED);
    schema_builder.add_date_field("date_published", INDEXED | STORED);

    schema_builder.build()
}
