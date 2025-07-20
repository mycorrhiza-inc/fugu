//! Document operations for FuguDB with schema-aware indexing

use anyhow::{Result, anyhow};
use tantivy::indexer::UserOperation;
use tantivy::{DateTime, TantivyDocument, Term};
use tracing::{info, warn};

use super::core::{IndexType, NamedIndex};
use super::utils::create_metadata_facets;
use crate::db::utils::create_metadata_facets_hashmap;
use crate::object::ObjectRecord;

/// Trait for document operations
pub trait DocumentOperations {
    async fn upsert(&self, records: Vec<ObjectRecord>) -> Result<()>;
    async fn batch_upsert(&self, records: Vec<ObjectRecord>) -> Result<usize>;
    async fn ingest(&self, records: Vec<ObjectRecord>) -> Result<()>;
    async fn delete_document(&self, doc_id: String) -> Result<()>;
}

impl NamedIndex {
    /// Upsert a vector of objects with schema-aware document building
    pub async fn upsert(&self, records: Vec<ObjectRecord>) -> Result<()> {
        // Validate required fields exist
        self.validate_required_fields()?;

        let mut w = self.writer().await;

        for object in records {
            if object.id.is_empty() {
                return Err(anyhow!("Object ID cannot be empty"));
            }

            // Validate the object before processing
            object.validate()?;

            // Delete existing document (only for indexes that support ID-based deletion)
            if self.supports_full_documents() {
                let id_field = self.id_field()?;
                let term = Term::from_field_text(id_field, &object.id);
                w.delete_term(term);
            }

            // Build document based on index type
            match self.index_type {
                IndexType::Docs => {
                    let doc = self.build_full_document(&object)?;
                    w.add_document(doc)?;
                }
                IndexType::QueryIndex => {
                    let docs = self.build_query_suggestion_documents(&object)?;
                    for doc in docs {
                        w.add_document(doc)?;
                    }
                }
                IndexType::FilterIndex => {
                    let docs = self.build_filter_documents(&object)?;
                    for doc in docs {
                        w.add_document(doc)?;
                    }
                }
            }
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

    /// Delete a single document by ID (only works for docs index)
    pub async fn delete_document(&self, doc_id: String) -> Result<()> {
        if doc_id.is_empty() {
            return Err(anyhow!("Document ID cannot be empty"));
        }

        if !self.supports_full_documents() {
            return Err(anyhow!(
                "Delete by ID not supported for {} index type",
                self.index_type_name()
            ));
        }

        let mut w = self.writer().await;
        let id_field = self.id_field()?;
        let doc_id_term = Term::from_field_text(id_field, &doc_id);
        w.delete_term(doc_id_term);
        w.commit()?;
        Ok(())
    }

    /// Execute user operations
    async fn execute(&self, ops: Vec<UserOperation>) -> Result<()> {
        let w = self.writer().await;
        w.run(ops);
        Ok(())
    }

    /// Prepare a delete operation (only for docs index)
    fn prep_delete_op(&self, doc_id: String) -> Result<UserOperation> {
        let id_field = self.id_field()?;
        let doc_id_term = Term::from_field_text(id_field, &doc_id);
        Ok(UserOperation::Delete(doc_id_term))
    }

    /// Build a full document for the docs index
    fn build_full_document(&self, object: &ObjectRecord) -> Result<TantivyDocument> {
        if !matches!(self.index_type, IndexType::Docs) {
            return Err(anyhow!("build_full_document only supported for docs index"));
        }

        let mut doc = TantivyDocument::new();

        // Core fields (required)
        let id_field = self.id_field()?;
        let text_field = self.text_field()?;

        doc.add_text(id_field, &object.id);
        doc.add_text(text_field, &object.text);

        // Optional name field
        if let Some(name_field) = self.name_field() {
            if let Some(name) = &object.name {
                doc.add_text(name_field, name);
            }
        }

        // Add namespace field if it exists and object has namespace
        if let Some(namespace_field) = self.namespace_field() {
            if let Some(namespace) = &object.namespace {
                doc.add_text(namespace_field, namespace);
            }
        }

        // Add new namespace-aware fields
        if let Some(organization_field) = self.organization_field() {
            if let Some(organization) = &object.organization {
                doc.add_text(organization_field, organization);
            }
        }

        if let Some(conversation_id_field) = self.conversation_id_field() {
            if let Some(conversation_id) = &object.conversation_id {
                doc.add_text(conversation_id_field, conversation_id);
            }
        }

        if let Some(data_type_field) = self.data_type_field() {
            if let Some(data_type) = &object.data_type {
                doc.add_text(data_type_field, data_type);
            }
        }

        // Add metadata JSON if present and field exists
        if let Some(metadata_field) = self.metadata_field() {
            if let Some(metadata) = &object.metadata {
                let json_str = serde_json::to_string(metadata)?;
                doc.add_text(metadata_field, &json_str);
            }
        }

        // Process facets if facet field exists (as Facet type in docs index)
        if let Some(facet_field) = self.facet_field() {
            self.add_facets_to_document(&mut doc, facet_field, object)?;
        }

        // Add date fields if present and fields exist in schema
        self.add_date_fields_to_document(&mut doc, object)?;

        Ok(doc)
    }

    /// Build query suggestion documents (just text field)
    fn build_query_suggestion_documents(
        &self,
        object: &ObjectRecord,
    ) -> Result<Vec<TantivyDocument>> {
        if !matches!(self.index_type, IndexType::QueryIndex) {
            return Err(anyhow!(
                "build_query_suggestion_documents only supported for query_index"
            ));
        }

        let text_field = self.text_field()?;
        let mut docs = Vec::new();

        // Add the main text as a query suggestion
        let mut doc = TantivyDocument::new();
        doc.add_text(text_field, &object.text);
        docs.push(doc);

        // Add the name as a query suggestion if present
        if let Some(name) = &object.name {
            let mut doc = TantivyDocument::new();
            doc.add_text(text_field, name);
            docs.push(doc);
        }

        // Extract meaningful phrases from text for additional suggestions
        let suggestions = self.extract_query_suggestions(&object.text);
        for suggestion in suggestions {
            let mut doc = TantivyDocument::new();
            doc.add_text(text_field, &suggestion);
            docs.push(doc);
        }

        Ok(docs)
    }

    /// Build filter documents (facet leaf nodes)
    fn build_filter_documents(&self, object: &ObjectRecord) -> Result<Vec<TantivyDocument>> {
        if !matches!(self.index_type, IndexType::FilterIndex) {
            return Err(anyhow!(
                "build_filter_documents only supported for filter_index"
            ));
        }

        let text_field = self.text_field()?;
        let facet_field = self
            .facet_field()
            .ok_or_else(|| anyhow!("Facet field required for filter_index"))?;

        let mut docs = Vec::new();

        // Get all facet paths for this object
        let all_facets = self.get_all_facet_paths(object)?;

        for facet_path in all_facets {
            // Extract the leaf node (last part of the facet path)
            let leaf_node = facet_path
                .trim_start_matches('/')
                .split('/')
                .last()
                .unwrap_or(&facet_path)
                .to_string();

            let mut doc = TantivyDocument::new();

            // Add leaf node as searchable text
            doc.add_text(text_field, &leaf_node);

            // Add full facet path as searchable text
            doc.add_text(facet_field, &facet_path);

            docs.push(doc);
        }

        Ok(docs)
    }

    /// Get all facet paths for an object (combines explicit facets, namespace facets, and metadata facets)
    fn get_all_facet_paths(&self, object: &ObjectRecord) -> Result<Vec<String>> {
        let mut all_facets = Vec::new();

        // **PRIORITY 1: Process explicit facets field from Go SDK**
        if let Some(facets) = &object.facets {
            for facet_path in facets {
                let normalized_path = if facet_path.starts_with('/') {
                    facet_path.clone()
                } else {
                    format!("/{}", facet_path)
                };
                all_facets.push(normalized_path);
            }
        } else {
            // **FALLBACK: Generate namespace facets if no explicit facets provided**
            let namespace_facets = object.generate_namespace_facets();
            all_facets.extend(namespace_facets);

            // **FALLBACK: Process metadata facets if no explicit facets**
            if let Some(metadata) = &object.metadata {
                let additional_facets = create_metadata_facets_hashmap(metadata, Vec::new());
                for facet_path in additional_facets {
                    if let Some(path) = facet_path.first() {
                        let normalized_path = if path.starts_with('/') {
                            path.clone()
                        } else {
                            format!("/metadata/{}", path)
                        };
                        all_facets.push(normalized_path);
                    }
                }
            }
        }

        Ok(all_facets)
    }

    /// Add facets to document (for docs index - uses Facet type)
    fn add_facets_to_document(
        &self,
        doc: &mut TantivyDocument,
        facet_field: tantivy::schema::Field,
        object: &ObjectRecord,
    ) -> Result<()> {
        let all_facets = self.get_all_facet_paths(object)?;

        for facet_path in all_facets {
            match tantivy::schema::Facet::from_text(&facet_path) {
                Ok(facet) => {
                    doc.add_facet(facet_field, facet);
                    info!("Added facet: {}", facet_path);
                }
                Err(e) => {
                    warn!("Failed to create facet from path '{}': {}", facet_path, e);
                }
            }
        }

        Ok(())
    }

    /// Add date fields to document
    fn add_date_fields_to_document(
        &self,
        doc: &mut TantivyDocument,
        object: &ObjectRecord,
    ) -> Result<()> {
        if let Some(date_created_field) = self.date_created_field() {
            if let Some(date_str) = &object.date_created {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
                    let offset_dt = time::OffsetDateTime::from_unix_timestamp(dt.timestamp())
                        .map_err(|e| anyhow!("Failed to convert timestamp: {}", e))?
                        .replace_nanosecond(dt.timestamp_subsec_nanos())
                        .map_err(|e| anyhow!("Failed to set nanoseconds: {}", e))?;
                    doc.add_date(date_created_field, DateTime::from_utc(offset_dt));
                }
            }
        }

        if let Some(date_updated_field) = self.date_updated_field() {
            if let Some(date_str) = &object.date_updated {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
                    let offset_dt = time::OffsetDateTime::from_unix_timestamp(dt.timestamp())
                        .map_err(|e| anyhow!("Failed to convert timestamp: {}", e))?
                        .replace_nanosecond(dt.timestamp_subsec_nanos())
                        .map_err(|e| anyhow!("Failed to set nanoseconds: {}", e))?;
                    doc.add_date(date_updated_field, DateTime::from_utc(offset_dt));
                }
            }
        }

        if let Some(date_published_field) = self.date_published_field() {
            if let Some(date_str) = &object.date_published {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
                    let offset_dt = time::OffsetDateTime::from_unix_timestamp(dt.timestamp())
                        .map_err(|e| anyhow!("Failed to convert timestamp: {}", e))?
                        .replace_nanosecond(dt.timestamp_subsec_nanos())
                        .map_err(|e| anyhow!("Failed to set nanoseconds: {}", e))?;
                    doc.add_date(date_published_field, DateTime::from_utc(offset_dt));
                }
            }
        }

        Ok(())
    }

    /// Extract meaningful query suggestions from text
    fn extract_query_suggestions(&self, text: &str) -> Vec<String> {
        let mut suggestions = Vec::new();

        // Split into sentences and take first few words as suggestions
        for sentence in text.split(&['.', '!', '?', '\n']) {
            let words: Vec<&str> = sentence.trim().split_whitespace().collect();

            // Add 2-3 word phrases as suggestions
            if words.len() >= 2 {
                let phrase = words[0..std::cmp::min(3, words.len())].join(" ");
                if phrase.len() > 3 && phrase.len() < 50 {
                    suggestions.push(phrase);
                }
            }
        }

        // Limit to avoid too many suggestions
        suggestions.truncate(10);
        suggestions
    }
}

impl DocumentOperations for NamedIndex {
    async fn upsert(&self, records: Vec<ObjectRecord>) -> Result<()> {
        self.upsert(records).await
    }

    async fn batch_upsert(&self, records: Vec<ObjectRecord>) -> Result<usize> {
        self.batch_upsert(records).await
    }

    async fn ingest(&self, records: Vec<ObjectRecord>) -> Result<()> {
        self.ingest(records).await
    }

    async fn delete_document(&self, doc_id: String) -> Result<()> {
        self.delete_document(doc_id).await
    }
}

// Implement DocumentOperations for Dataset to operate on all indexes
impl DocumentOperations for super::core::Dataset {
    async fn upsert(&self, records: Vec<ObjectRecord>) -> Result<()> {
        // Upsert to all three indexes (each will handle the data differently)
        self.docs().upsert(records.clone()).await?;
        self.filter_index().upsert(records.clone()).await?;
        self.query_index().upsert(records).await?;
        Ok(())
    }

    async fn batch_upsert(&self, records: Vec<ObjectRecord>) -> Result<usize> {
        let count = records.len();
        self.upsert(records).await?;
        Ok(count)
    }

    async fn ingest(&self, records: Vec<ObjectRecord>) -> Result<()> {
        self.upsert(records).await
    }

    async fn delete_document(&self, doc_id: String) -> Result<()> {
        // Only delete from docs index (others don't support ID-based deletion)
        self.docs().delete_document(doc_id).await?;

        // For filter and query indexes, we would need to rebuild from the
        // remaining documents or implement a more complex deletion strategy
        warn!(
            "Delete operation only applied to docs index. Filter and query indexes may contain stale data."
        );

        Ok(())
    }
}
