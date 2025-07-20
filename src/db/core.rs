// path: src/db/core.rs
//! Core FuguDB implementation with programmatic field access

use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tantivy::{
    Index, IndexWriter, ReloadPolicy, Searcher,
    schema::{Field, Schema},
};
use tokio::sync::{Mutex, MutexGuard};
use tracing::info;

/// Field name constants for type safety and consistency
pub mod field_names {
    pub const ID: &str = "id";
    pub const TEXT: &str = "text";
    pub const NAME: &str = "name";
    pub const METADATA: &str = "metadata";
    pub const FACET: &str = "facet";
    pub const DATE_PUBLISHED: &str = "date_published";
    pub const DATE_UPDATED: &str = "date_updated";
    pub const DATE_CREATED: &str = "date_created";
    pub const NAMESPACE: &str = "namespace";
    pub const ORGANIZATION: &str = "organization";
    pub const CONVERSATION_ID: &str = "conversation_id";
    pub const DATA_TYPE: &str = "data_type";
}

/// A Dataset manages multiple related indexes for a namespace
pub struct Dataset {
    namespace: String,
    base_path: PathBuf,
    docs: NamedIndex,
    filter_index: NamedIndex,
    query_index: NamedIndex,
}

impl Dataset {
    /// Create a new Dataset with three indexes for the given namespace
    pub fn new(namespace: String, base_path: PathBuf) -> Result<Self> {
        info!("Creating Dataset for namespace: {}", namespace);

        // Create subdirectories for each index
        let docs_path = base_path.join(&namespace).join("docs");
        let filter_path = base_path.join(&namespace).join("filter_index");
        let query_path = base_path.join(&namespace).join("query_index");

        // Ensure all directories exist
        std::fs::create_dir_all(&docs_path)?;
        std::fs::create_dir_all(&filter_path)?;
        std::fs::create_dir_all(&query_path)?;

        // Create the three indexes
        let docs = NamedIndex::new("docs".to_string(), docs_path)?;
        let filter_index = NamedIndex::new("filter_index".to_string(), filter_path)?;
        let query_index = NamedIndex::new("query_index".to_string(), query_path)?;

        Ok(Self {
            namespace,
            base_path,
            docs,
            filter_index,
            query_index,
        })
    }

    /// Get the namespace for this dataset
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get the docs index
    pub fn docs(&self) -> &NamedIndex {
        &self.docs
    }

    /// Get the filter index
    pub fn filter_index(&self) -> &NamedIndex {
        &self.filter_index
    }

    /// Get the query index
    pub fn query_index(&self) -> &NamedIndex {
        &self.query_index
    }

    /// Get a mutable reference to the docs index
    pub fn docs_mut(&mut self) -> &mut NamedIndex {
        &mut self.docs
    }

    /// Get a mutable reference to the filter index
    pub fn filter_index_mut(&mut self) -> &mut NamedIndex {
        &mut self.filter_index
    }

    /// Get a mutable reference to the query index
    pub fn query_index_mut(&mut self) -> &mut NamedIndex {
        &mut self.query_index
    }

    /// Get all indexes as a vector for batch operations
    pub fn all_indexes(&self) -> Vec<&NamedIndex> {
        vec![&self.docs, &self.filter_index, &self.query_index]
    }

    /// Get all indexes as mutable references for batch operations
    pub fn all_indexes_mut(&mut self) -> Vec<&mut NamedIndex> {
        vec![
            &mut self.docs,
            &mut self.filter_index,
            &mut self.query_index,
        ]
    }
}

/// A named index with programmatic field access
pub struct NamedIndex {
    pub(crate) name: String,
    pub(crate) schema: Schema,
    pub(crate) _path: PathBuf,
    pub(crate) index: Index,
    pub(crate) writer: Arc<Mutex<IndexWriter>>,
    // Cache fields for performance
    field_cache: HashMap<String, Field>,
}

impl NamedIndex {
    /// Create a new NamedIndex instance
    pub fn new(name: String, path: PathBuf) -> Result<Self> {
        info!(
            "NamedIndex::new – initializing '{}' with path {:?}",
            name, path
        );

        let schema_builder = Schema::builder();

        // Ensure directory exists
        std::fs::create_dir_all(&path)?;

        let dir = tantivy::directory::MmapDirectory::open(&path)
            .map_err(|e| anyhow!("Failed to open directory {:?}: {}", path, e))?;

        let schema = crate::object::build_object_record_schema(schema_builder);
        let index = Index::open_or_create(dir, schema.clone())
            .map_err(|e| anyhow!("Failed to create index: {}", e))?;

        let writer = index
            .writer(50_000_000)
            .map_err(|e| anyhow!("Failed to create index writer: {}", e))?;

        // Pre-populate field cache
        let mut field_cache = HashMap::new();
        for field in schema.fields() {
            let field_name = schema.get_field_name(field.0).to_string();
            field_cache.insert(field_name, field.0);
        }

        Ok(Self {
            name,
            schema,
            _path: path,
            index,
            writer: Arc::new(Mutex::new(writer)),
            field_cache,
        })
    }

    /// Get the name of this index
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the underlying Tantivy index
    pub fn get_index(&self) -> &Index {
        &self.index
    }

    /// Get the schema
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    /// Get a searcher instance
    pub(crate) fn searcher(&self) -> Result<Searcher> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;
        Ok(reader.searcher())
    }

    /// Get a lock on the index writer
    pub(crate) async fn writer(&self) -> MutexGuard<'_, IndexWriter> {
        self.writer.lock().await
    }

    /// Programmatically get a field by name with error handling
    pub fn get_field(&self, field_name: &str) -> Result<Field> {
        self.field_cache.get(field_name).copied().ok_or_else(|| {
            anyhow!(
                "Field '{}' not found in schema for index '{}'",
                field_name,
                self.name
            )
        })
    }

    /// Check if a field exists in the schema
    pub fn has_field(&self, field_name: &str) -> bool {
        self.field_cache.contains_key(field_name)
    }

    /// Get all available field names
    pub fn field_names(&self) -> Vec<String> {
        self.field_cache.keys().cloned().collect()
    }

    /// Get a field by name, returning None if not found (safe version)
    pub fn try_get_field(&self, field_name: &str) -> Option<Field> {
        self.field_cache.get(field_name).copied()
    }

    // Convenience methods for commonly used fields with proper error handling

    /// Get the ID field (required field)
    pub fn id_field(&self) -> Result<Field> {
        self.get_field(field_names::ID)
    }

    /// Get the text field (required field)
    pub fn text_field(&self) -> Result<Field> {
        self.get_field(field_names::TEXT)
    }

    /// Get the name field (optional field)
    pub fn name_field(&self) -> Option<Field> {
        self.try_get_field(field_names::NAME)
    }

    /// Get the metadata field (optional field)
    pub fn metadata_field(&self) -> Option<Field> {
        self.try_get_field(field_names::METADATA)
    }

    /// Get the facet field (optional field)
    pub fn facet_field(&self) -> Option<Field> {
        self.try_get_field(field_names::FACET)
    }

    /// Get the namespace field (optional field)
    pub fn namespace_field(&self) -> Option<Field> {
        self.try_get_field(field_names::NAMESPACE)
    }

    /// Get the organization field (optional field)
    pub fn organization_field(&self) -> Option<Field> {
        self.try_get_field(field_names::ORGANIZATION)
    }

    /// Get the conversation_id field (optional field)
    pub fn conversation_id_field(&self) -> Option<Field> {
        self.try_get_field(field_names::CONVERSATION_ID)
    }

    /// Get the data_type field (optional field)
    pub fn data_type_field(&self) -> Option<Field> {
        self.try_get_field(field_names::DATA_TYPE)
    }

    /// Get date fields (optional fields)
    pub fn date_created_field(&self) -> Option<Field> {
        self.try_get_field(field_names::DATE_CREATED)
    }

    pub fn date_updated_field(&self) -> Option<Field> {
        self.try_get_field(field_names::DATE_UPDATED)
    }

    pub fn date_published_field(&self) -> Option<Field> {
        self.try_get_field(field_names::DATE_PUBLISHED)
    }

    /// Validate that required fields exist in the schema
    pub fn validate_required_fields(&self) -> Result<()> {
        let required_fields = [field_names::ID, field_names::TEXT];

        for field_name in &required_fields {
            if !self.has_field(field_name) {
                return Err(anyhow!(
                    "Required field '{}' not found in schema for index '{}'",
                    field_name,
                    self.name
                ));
            }
        }

        Ok(())
    }

    /// Get schema information for debugging
    pub fn schema_info(&self) -> HashMap<String, String> {
        let mut info = HashMap::new();

        for (field_name, field) in &self.field_cache {
            let field_entry = self.schema.get_field_entry(*field);
            info.insert(
                field_name.clone(),
                format!("{:?}", field_entry.field_type()),
            );
        }

        info
    }
}
