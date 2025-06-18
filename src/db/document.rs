//! Document operations for FuguDB (upsert, delete, indexing)

use anyhow::{anyhow, Result};
use tantivy::{DateTime, Term, TantivyDocument};
use tantivy::indexer::UserOperation;
use tracing::{info, warn};

use super::core::FuguDB;
use super::utils::{create_metadata_facets};
use crate::object::ObjectRecord;

/// Trait for document operations
pub trait DocumentOperations {
    async fn upsert(&self, records: Vec<ObjectRecord>) -> Result<()>;
    async fn batch_upsert(&self, records: Vec<ObjectRecord>) -> Result<usize>;
    async fn ingest(&self, records: Vec<ObjectRecord>) -> Result<()>;
    async fn delete_document(&self, doc_id: String) -> Result<()>;
}

impl FuguDB {
    /// Upsert a vector of objects: delete existing by ID then add new
    pub async fn upsert(&self, records: Vec<ObjectRecord>) -> Result<()> {
        let mut w = self.writer().await;
        for object in records {
            if object.id.is_empty() {
                return Err(anyhow!("Object ID cannot be empty"));
            }
            // Delete existing document by ID term
            let term = Term::from_field_text(self.id_field(), &object.id);
            w.delete_term(term);
            // Add new document
            let doc = self.build_tantivy_doc_safe(&object)?;
            w.add_document(doc)?;
        }
        w.commit()?;
        Ok(())
    }

    /// Batch upsert with better error handling
    pub async fn batch_upsert(&self, records: Vec<ObjectRecord>) -> Result<usize> {
        self.upsert(records.clone()).await?;
        Ok(records.len())
    }

    /// Index a vector of objects
    pub async fn ingest(&self, records: Vec<ObjectRecord>) -> Result<()> {
        self.upsert(records).await
    }

    /// Delete a single document by ID
    pub async fn delete_document(&self, doc_id: String) -> Result<()> {
        if doc_id.is_empty() {
            return Err(anyhow!("Document ID cannot be empty"));
        }
        let mut w = self.writer().await;
        let id_field = self.id_field();
        let doc_id_term = Term::from_field_text(id_field, &doc_id);
        w.delete_term(doc_id_term);
        w.commit()?;
        Ok(())
    }

    /// Execute user operations
    async fn execute(&self, ops: Vec<UserOperation>) {
        let w = self.writer().await;
        w.run(ops);
    }

    /// Prepare a delete operation
    fn prep_delete_op(&self, doc_id: String) -> UserOperation {
        let id = self.id_field();
        let doc_id_term = Term::from_field_text(id, &doc_id);
        UserOperation::Delete(doc_id_term)
    }

    /// Construct a Tantivy document from an ObjectRecord
    /// Enhanced build_tantivy_doc_safe with namespace facet generation
    pub fn build_tantivy_doc_safe(&self, object: &ObjectRecord) -> Result<TantivyDocument> {
        let mut doc = TantivyDocument::new();

        // Core fields
        doc.add_text(self.id_field(), &object.id);
        doc.add_text(self.text_field(), &object.text);

        // Add namespace field
        if let Some(namespace) = &object.namespace {
            doc.add_text(self.namespace_field(), namespace);
        }

        // Add new namespace-aware fields
        if let Some(organization) = &object.organization {
            doc.add_text(self.organization_field(), organization);
        }

        if let Some(conversation_id) = &object.conversation_id {
            doc.add_text(self.conversation_id_field(), conversation_id);
        }

        if let Some(data_type) = &object.data_type {
            doc.add_text(self.data_type_field(), data_type);
        }

        // Add metadata JSON if present
        if let Some(metadata) = &object.metadata {
            let json_str = serde_json::to_string(metadata)?;
            doc.add_text(self.metadata_field(), &json_str);
        }

        // Generate and add namespace facets
        let namespace_facets = object.generate_namespace_facets();
        for facet_path in namespace_facets {
            match tantivy::schema::Facet::from_text(&facet_path) {
                Ok(facet) => {
                    doc.add_facet(self.facet_field(), facet);
                    info!("Added namespace facet: {}", facet_path);
                }
                Err(e) => {
                    warn!("Failed to create facet from path '{}': {}", facet_path, e);
                }
            }
        }

        // Process additional metadata facets
        if let Some(metadata) = &object.metadata {
            let additional_facets = create_metadata_facets(metadata, Vec::new());
            for facet_path in additional_facets {
                if let Some(path) = facet_path.first() {
                    let normalized_path = if path.starts_with('/') {
                        path.clone()
                    } else {
                        format!("/metadata/{}", path)
                    };

                    match tantivy::schema::Facet::from_text(&normalized_path) {
                        Ok(facet) => {
                            doc.add_facet(self.facet_field(), facet);
                            info!("Added metadata facet: {}", normalized_path);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to create metadata facet from path '{}': {}",
                                normalized_path, e
                            );
                        }
                    }
                }
            }
        }

        // Add date fields if present
        if let Some(date_str) = &object.date_created {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
                let offset_dt = time::OffsetDateTime::from_unix_timestamp(dt.timestamp())
                    .map_err(|e| anyhow!("Failed to convert timestamp: {}", e))?
                    .replace_nanosecond(dt.timestamp_subsec_nanos())
                    .map_err(|e| anyhow!("Failed to set nanoseconds: {}", e))?;
                doc.add_date(self.date_created(), DateTime::from_utc(offset_dt));
            }
        }

        if let Some(date_str) = &object.date_updated {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
                let offset_dt = time::OffsetDateTime::from_unix_timestamp(dt.timestamp())
                    .map_err(|e| anyhow!("Failed to convert timestamp: {}", e))?
                    .replace_nanosecond(dt.timestamp_subsec_nanos())
                    .map_err(|e| anyhow!("Failed to set nanoseconds: {}", e))?;
                doc.add_date(self.date_updated(), DateTime::from_utc(offset_dt));
            }
        }

        if let Some(date_str) = &object.date_published {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
                let offset_dt = time::OffsetDateTime::from_unix_timestamp(dt.timestamp())
                    .map_err(|e| anyhow!("Failed to convert timestamp: {}", e))?
                    .replace_nanosecond(dt.timestamp_subsec_nanos())
                    .map_err(|e| anyhow!("Failed to set nanoseconds: {}", e))?;
                doc.add_date(self.date_published(), DateTime::from_utc(offset_dt));
            }
        }

        Ok(doc)
    }
}