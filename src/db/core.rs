//! Core FuguDB implementation with schema-aware field access

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

use crate::db::schemas::{build_docs_schema, build_filter_index_schema, build_query_index_schema};

// Include generated schemas and types
// include!(concat!(env!("OUT_DIR"), "/generated_schemas.rs"));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    Docs,        // Full documents with all fields
    FilterIndex, // Facet leaf nodes for filtering
    QueryIndex,  // Text suggestions for autocomplete
}

impl IndexType {
    pub fn build_schema(&self) -> Schema {
        let schema_builder = tantivy::schema::SchemaBuilder::new();
        match self {
            IndexType::Docs => build_docs_schema(schema_builder),
            IndexType::FilterIndex => build_filter_index_schema(schema_builder),
            IndexType::QueryIndex => build_query_index_schema(schema_builder),
        }
    }
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

        // Create the three indexes with appropriate schemas
        let docs = NamedIndex::new("docs".to_string(), docs_path, IndexType::Docs)?;
        let filter_index = NamedIndex::new(
            "filter_index".to_string(),
            filter_path,
            IndexType::FilterIndex,
        )?;
        let query_index =
            NamedIndex::new("query_index".to_string(), query_path, IndexType::QueryIndex)?;

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

    /// Create multiple datasets for different namespaces
    pub fn create_multiple(
        namespaces: Vec<String>,
        base_path: PathBuf,
    ) -> Result<HashMap<String, Dataset>> {
        let mut datasets = HashMap::new();

        for namespace in namespaces {
            let dataset = Dataset::new(namespace.clone(), base_path.clone())?;
            datasets.insert(namespace, dataset);
        }

        Ok(datasets)
    }

    /// Get dataset statistics
    pub fn stats(&self) -> Result<DatasetStats> {
        let docs_count = self.docs().get_index().reader()?.searcher().num_docs();
        let filter_count = self
            .filter_index()
            .get_index()
            .reader()?
            .searcher()
            .num_docs();
        let query_count = self
            .query_index()
            .get_index()
            .reader()?
            .searcher()
            .num_docs();

        Ok(DatasetStats {
            namespace: self.namespace.clone(),
            docs_count,
            filter_count,
            query_count,
        })
    }

    /// Validate all indexes have the required fields
    pub fn validate_all_schemas(&self) -> Result<()> {
        self.docs().validate_required_fields()?;
        self.filter_index().validate_required_fields()?;
        self.query_index().validate_required_fields()?;
        Ok(())
    }

    /// Get schema information for all indexes
    pub fn schema_info(&self) -> HashMap<String, HashMap<String, String>> {
        let mut info = HashMap::new();
        info.insert("docs".to_string(), self.docs().schema_info());
        info.insert(
            "filter_index".to_string(),
            self.filter_index().schema_info(),
        );
        info.insert("query_index".to_string(), self.query_index().schema_info());
        info
    }
}

#[derive(Debug, Clone)]
pub struct DatasetStats {
    pub namespace: String,
    pub docs_count: u64,
    pub filter_count: u64,
    pub query_count: u64,
}

impl DatasetStats {
    pub fn total_docs(&self) -> u64 {
        self.docs_count + self.filter_count + self.query_count
    }
}

/// A named index with schema-aware field access
pub struct NamedIndex {
    pub(crate) name: String,
    pub(crate) schema: Schema,
    pub(crate) index_type: IndexType,
    pub(crate) _path: PathBuf,
    pub(crate) index: Index,
    pub(crate) writer: Arc<Mutex<IndexWriter>>,
    // Cache fields for performance
    field_cache: HashMap<String, Field>,
}

impl NamedIndex {
    /// Create a new NamedIndex instance with a specific schema type
    pub fn new(name: String, path: PathBuf, index_type: IndexType) -> Result<Self> {
        info!(
            "NamedIndex::new – initializing '{}' ({:?}) with path {:?}",
            name, index_type, path
        );

        // Ensure directory exists
        std::fs::create_dir_all(&path)?;

        let dir = tantivy::directory::MmapDirectory::open(&path)
            .map_err(|e| anyhow!("Failed to open directory {:?}: {}", path, e))?;

        // Build schema based on index type
        let schema = index_type.build_schema();

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
            index_type,
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

    /// Get the index type
    pub fn index_type(&self) -> IndexType {
        self.index_type
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
                "Field '{}' not found in schema for {} index '{}'",
                field_name,
                self.index_type_name(),
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

    /// Get the index type as a string for debugging
    pub fn index_type_name(&self) -> &'static str {
        match self.index_type {
            IndexType::Docs => "docs",
            IndexType::FilterIndex => "filter_index",
            IndexType::QueryIndex => "query_index",
        }
    }

    /// Schema-aware field accessors based on index type

    /// Get the text field (available in all schemas)
    pub fn text_field(&self) -> Result<Field> {
        self.get_field("text")
    }

    /// Get the ID field (only available in docs schema)
    pub fn id_field(&self) -> Result<Field> {
        match self.index_type {
            IndexType::Docs => self.get_field("id"),
            _ => Err(anyhow!(
                "ID field not available in {} index",
                self.index_type_name()
            )),
        }
    }

    /// Get the facet field (docs and filter_index schemas)
    pub fn facet_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("facet"), // Facet type
            IndexType::FilterIndex => self.try_get_field("facet"), // Text type for searchable facet paths
            IndexType::QueryIndex => None,
        }
    }

    /// Get optional fields that only exist in docs schema
    pub fn name_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("name"),
            _ => None,
        }
    }

    pub fn metadata_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("metadata"),
            _ => None,
        }
    }

    pub fn namespace_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("namespace"),
            _ => None,
        }
    }

    pub fn organization_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("organization"),
            _ => None,
        }
    }

    pub fn conversation_id_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("conversation_id"),
            _ => None,
        }
    }

    pub fn data_type_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("data_type"),
            _ => None,
        }
    }

    /// Get date fields (only available in docs schema)
    pub fn date_created_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("date_created"),
            _ => None,
        }
    }

    pub fn date_updated_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("date_updated"),
            _ => None,
        }
    }

    pub fn date_published_field(&self) -> Option<Field> {
        match self.index_type {
            IndexType::Docs => self.try_get_field("date_published"),
            _ => None,
        }
    }

    /// Validate that required fields exist for this index type
    pub fn validate_required_fields(&self) -> Result<()> {
        // All indexes must have text field
        if !self.has_field("text") {
            return Err(anyhow!(
                "Required field 'text' not found in {} index '{}'",
                self.index_type_name(),
                self.name
            ));
        }

        // Docs index must have ID field
        if matches!(self.index_type, IndexType::Docs) && !self.has_field("id") {
            return Err(anyhow!(
                "Required field 'id' not found in docs index '{}'",
                self.name
            ));
        }

        // Filter index must have facet field (as text)
        if matches!(self.index_type, IndexType::FilterIndex) && !self.has_field("facet") {
            return Err(anyhow!(
                "Required field 'facet' not found in filter_index '{}'",
                self.name
            ));
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

    /// Check if this index supports a specific operation based on its schema
    pub fn supports_full_documents(&self) -> bool {
        matches!(self.index_type, IndexType::Docs)
    }

    pub fn supports_facet_filtering(&self) -> bool {
        matches!(self.index_type, IndexType::Docs | IndexType::FilterIndex)
    }

    pub fn supports_query_suggestions(&self) -> bool {
        matches!(self.index_type, IndexType::QueryIndex)
    }
}
