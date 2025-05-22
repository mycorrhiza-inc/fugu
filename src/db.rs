use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{Json, http::StatusCode, response::IntoResponse};
use rkyv::string;
use serde_json::{Value as SerdeValue, json};

use anyhow::{Result, anyhow};
use std::fs;
use tantivy::indexer::UserOperation;
use tantivy::schema::Value;
use tokio::sync::{Mutex, MutexGuard};

use rkyv::ser::writer;
use tantivy::collector::{FacetCollector, TopDocs};
use tantivy::query::{AllQuery, QueryParser, TermQuery};
use tantivy::{DateTime, Directory, Searcher, schema::*};
use tantivy::{Index, IndexWriter, ReloadPolicy};
use tracing::{Instrument, debug, error, info};

use crate::object::ObjectRecord;

// Database handle abstraction
#[derive(Clone)]
pub enum FuguDBBackend {
    Fjall(fjall::Keyspace),
}

// Tree handle abstraction
#[derive(Clone)]
pub enum TreeHandle {
    Fjall(fjall::PartitionHandle),
}

// Batch operation abstraction
#[derive(Clone)]
pub enum BatchOperation {
    Fjall(Arc<fjall::Batch>), // Use Arc to provide Clone for fjall::Batch
}

#[derive(Clone)]
pub struct FuguDB {
    schema: Schema,
    path: PathBuf,
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
}

impl FuguDB {
    /// Initialize the database by creating necessary trees/partitions
    pub fn new(path: PathBuf) -> Self {
        let schema_builder = Schema::builder();
        fs::create_dir_all(&path);
        let dir = tantivy::directory::MmapDirectory::open(&path).unwrap();
        let schema = crate::object::build_object_record_schema(schema_builder);
        let index = Index::open_or_create(dir, schema.clone()).unwrap();
        let writer = index.writer(50_000_000).unwrap();
        Self {
            schema,
            path,
            index,
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    /// Direct get method for convenience (assumes TREE_RECORDS)
    /// Returns the deserialized ObjectRecord if found and successfully deserialized
    pub fn get(&self, id: &str) -> anyhow::Result<Vec<TantivyDocument>> {
        let searcher = self.searcher()?;
        let query_parser = QueryParser::for_index(&self.index, vec![self.id_field()]);
        let query = query_parser.parse_query(id)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;
        let (_score, doc_address) = top_docs.get(0).unwrap();
        let retrieved_doc: TantivyDocument = searcher.doc(*doc_address).unwrap();
        Ok(vec![retrieved_doc])
    }

    pub fn get_index(&self) -> &Index {
        &self.index
    }
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

    pub fn schema(&self) -> tantivy::schema::Schema {
        self.schema.clone()
    }

    fn searcher(&self) -> anyhow::Result<Searcher> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;
        Ok(reader.searcher())
    }

    pub async fn simple_search(&self, q: String) -> Vec<SerdeValue> {
        let index = self.get_index();
        let text = self.text_field();
        let searcher = self.searcher().unwrap();
        let query_parser = QueryParser::for_index(index, vec![text]);
        let query = query_parser.parse_query(&q).unwrap();
        let top_docs = searcher.search(&query, &TopDocs::with_limit(50)).unwrap();

        let mut d = Vec::new();
        for (score, doc_address) in top_docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address).unwrap();
            let a = serde_json::from_str(retrieved_doc.to_json(&self.schema()).as_str()).unwrap();
            let s = serde_json::Number::from_f64(score as f64).unwrap();
            // set the score per doc
            let o = match a {
                SerdeValue::Object(m) => {
                    let mut m = m.clone();
                    m.insert("score".to_string(), SerdeValue::Number(s));
                    SerdeValue::Object(m)
                }
                v => v.clone(),
            };
            d.push(o);
        }
        return d;
    }

    async fn search_query(query: String) {}
    async fn filter_search(&self, query: String, filters: Vec<String>) {
        let facet = self.facet_field();
        let facets: Vec<Facet> = filters
            .iter()
            .map(|f| Facet::from_text(f).unwrap())
            .collect();
        let terms: Vec<Term> = facets
            .iter()
            .map(|key| Term::from_facet(facet.clone(), key))
            .collect();
        let searcher = self.searcher().unwrap();
        let mut facet_collector = FacetCollector::for_field("metadata");
    }

    pub fn list_facet(self, from_level: String) -> anyhow::Result<Vec<(Facet, u64)>> {
        let searcher = self.searcher()?;
        let facet = Facet::from(from_level.as_str());
        let facet_term = Term::from_facet(self.metadata_field(), &facet);
        let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet("/");
        let facet_counts = searcher.search(&facet_term_query, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get(from_level.as_str()).collect();
        let mut out: Vec<(Facet, u64)> = Vec::new();
        for f in facets {
            out.push((f.0.clone(), f.1))
        }
        return Ok(out);
    }

    async fn writer(&self) -> MutexGuard<'_, IndexWriter> {
        self.writer.lock().await
    }
    async fn execute(&self, ops: Vec<UserOperation>) {
        let w = self.writer().await;
        w.run(ops);
    }

    fn prep_delete_op(&self, doc_id: String) -> UserOperation {
        let ops: Vec<UserOperation> = Vec::new();
        let id = self.id_field();
        let doc_id_term = Term::from_field_text(id, &doc_id);
        UserOperation::Delete(doc_id_term)
    }

    pub fn delete_document(&self, doc_id: String) {
        let ops = vec![self.prep_delete_op(doc_id)];
        self.execute(ops);
    }

    pub fn get_facets(self) -> anyhow::Result<Vec<(Facet, u64)>> {
        let searcher = self.searcher()?;
        let facet = Facet::from("/");
        let facet_term = Term::from_facet(self.metadata_field(), &facet);
        let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
        let mut facet_collector = FacetCollector::for_field("metadata");
        facet_collector.add_facet("/");
        let facet_counts = searcher.search(&facet_term_query, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get("/").collect();
        let mut out: Vec<(Facet, u64)> = Vec::new();
        for f in facets {
            out.push((f.0.clone(), f.1))
        }
        return Ok(out);
    }

    /// Index a vector of objects
    pub async fn ingest(&self, records: Vec<ObjectRecord>) -> Result<(), impl IntoResponse> {
        let mut w = self.writer().await;
        for object in records {
            // Check if the IDs are valid
            if object.id.clone().is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "Object ID cannot be empty",
                        "status": "error"
                    })),
                ));
            }

            let mut doc = TantivyDocument::parse_json(
                &self.schema,
                serde_json::to_string(&object).unwrap().as_str(),
            )
            .unwrap();

            // Process metadata to extract additional fields and create indexes
            let fields = process_additional_fields(&object);
            // If we have additional fields, merge them into metadata
            if !is_value_empty(&fields) {
                // Create indexes for each additional field using depth-first traversal
                let facets = create_facet_indexes(&fields, String::new(), &doc);
                for f in facets {
                    doc.add_facet(self.facet_field(), f.as_str());
                }
                // Merge additional fields into metadata
            }
            w.add_document(doc);
        }
        w.commit().unwrap();
        Ok(())
    }
}

fn create_facet_indexes(
    value: &serde_json::Value,
    prefix: String,
    doc: &TantivyDocument,
) -> Vec<String> {
    let mut out = Vec::new();
    match value {
        serde_json::Value::Object(map) => {
            // Process each field in the object
            for (key, val) in map {
                let field_path = if prefix.is_empty() {
                    format!("/{}", key.clone())
                } else {
                    format!("{}/{}", prefix, key)
                };
                let temp = create_facet_indexes(val, field_path, doc);
                out.extend(temp);
            }
        }
        serde_json::Value::Array(arr) => {
            // Process each item in the array
            for (i, item) in arr.iter().enumerate() {
                let temp = create_facet_indexes(item, prefix.clone(), doc);
                out.extend(temp);
            }
        }
        _ => {
            // For primitive values, index the string representation
            let field_str = value.as_str().unwrap().to_string();
            let facet_str = if prefix.is_empty() {
                field_str
            } else {
                format!(
                    "{}/{}",
                    prefix,
                    // urlencoding::Encoded(field_str.clone()).to_string()
                    field_str.clone()
                )
            };
            info!("new facet: {}", facet_str.clone());
            out.push(facet_str);
        }
    }
    return out;
}

fn process_additional_fields(object_record: &ObjectRecord) -> serde_json::Value {
    // Deserialize the entire object to a Value
    let serialized = serde_json::to_string(object_record).unwrap_or_default();
    let mut value: serde_json::Value =
        serde_json::from_str(&serialized).unwrap_or(serde_json::Value::Null);

    if let serde_json::Value::Object(ref mut map) = value {
        // Return the remaining fields as a new Value
        map.remove("id");
        map.remove("text");
        serde_json::Value::Object(map.clone())
    } else {
        serde_json::Value::Object(serde_json::Map::new())
    }
}

fn is_value_empty(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => true,
        serde_json::Value::Bool(false) => false, // false is not empty
        serde_json::Value::Bool(true) => false,  // false is not empty
        serde_json::Value::Number(n) => n.as_f64().map(|x| x == 0.0).unwrap_or(false), // 0 is not empty
        serde_json::Value::String(s) => s.is_empty(),
        serde_json::Value::Array(arr) => arr.is_empty(),
        serde_json::Value::Object(obj) => obj.is_empty(),
    }
}
