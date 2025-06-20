//! Core FuguDB implementation with basic setup and field accessors

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use tantivy::{Index, IndexWriter, ReloadPolicy, Searcher, schema::Schema};
use tracing::info;
use anyhow::Result;

use crate::object::ObjectRecord;

/// Main database struct for FuguDB
#[derive(Clone)]
pub struct FuguDB {
    pub(crate) schema: Schema,
    pub(crate) _path: PathBuf,
    pub(crate) index: Index,
    pub(crate) writer: Arc<Mutex<IndexWriter>>,
}

impl FuguDB {
    /// Create a new FuguDB instance
    pub fn new(path: PathBuf) -> Self {
        info!("FuguDB::new – initializing with path {:?}", path);
        
        let schema_builder = Schema::builder();
        
        // Ensure directory exists
        let _ = std::fs::create_dir_all(&path);
        
        let dir = tantivy::directory::MmapDirectory::open(&path).unwrap();
        let schema = crate::object::build_object_record_schema(schema_builder);
        let index = Index::open_or_create(dir, schema.clone()).unwrap();
        let writer = index.writer(50_000_000).unwrap();
        
        Self {
            schema,
            _path: path,
            index,
            writer: Arc::new(Mutex::new(writer)),
        }
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
}

// Field accessor methods
impl FuguDB {
    pub fn id_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("id").unwrap()
    }

    pub fn text_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("text").unwrap()
    }

    pub fn name_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("name").unwrap()
    }

    pub fn metadata_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("metadata").unwrap()
    }

    pub fn facet_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("facet").unwrap()
    }

    pub fn date_published(&self) -> tantivy::schema::Field {
        self.schema.get_field("date_published").unwrap()
    }

    pub fn date_updated(&self) -> tantivy::schema::Field {
        self.schema.get_field("date_created").unwrap()
    }

    pub fn date_created(&self) -> tantivy::schema::Field {
        self.schema.get_field("date_updated").unwrap()
    }

    pub fn namespace_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("namespace").unwrap()
    }

    pub fn organization_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("organization").unwrap()
    }

    pub fn conversation_id_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("conversation_id").unwrap()
    }

    pub fn data_type_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("data_type").unwrap()
    }
}