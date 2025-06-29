//! Core FuguDB implementation with basic setup and field accessors

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tantivy::{Index, IndexWriter, ReloadPolicy, Searcher, schema::Schema};
use tokio::sync::{Mutex, MutexGuard};
use tracing::info;

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

// Add to Cargo.toml: paste = "1.0"
use paste::paste;

macro_rules! generate_field_accessors {
    ($($field:ident),*) => {
        impl FuguDB {
            $(
                paste! {
                    pub fn [<$field _field>](&self) -> tantivy::schema::Field {
                        self.schema.get_field(stringify!($field)).unwrap()
                    }
                }
            )*
        }
    };
}
// Generate field accessors using the macro
generate_field_accessors!(
    id,
    text,
    name,
    metadata,
    facet,
    date_published,
    date_updated,
    date_created,
    namespace,
    organization,
    conversation_id,
    data_type
);
