use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use axum::{Json, http::StatusCode, response::IntoResponse};
use serde_json::{Value as SerdeValue, json};

use anyhow::{Result, anyhow};
use std::fs;
use tantivy::columnar::DocId;
use tantivy::indexer::UserOperation;
use tantivy::schema::Value;
use tokio::sync::{Mutex, MutexGuard};
use tracing_subscriber::filter;

use tantivy::collector::{FacetCollector, TopDocs};
use tantivy::query::{AllQuery, BooleanQuery, Occur, QueryParser, TermQuery};
use tantivy::{DateTime, Directory, DocAddress, Score, Searcher, SegmentReader, schema::*};
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
    _path: PathBuf,
    index: Index,
    writer: Arc<Mutex<IndexWriter>>,
}

impl FuguDB {
    pub fn new(path: PathBuf) -> Self {
        let schema_builder = Schema::builder();
        match fs::create_dir_all(&path) {
            Ok(_) => {}
            Err(_) => {}
        };
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

    async fn filter_search(
        &self,
        query: String,
        filters: Vec<String>,
        max: Option<usize>,
    ) -> Result<()> {
        // validate data
        let facets: Vec<Facet> = filters.iter().map(|f| Facet::from(f)).collect();

        let limit = match max {
            Some(m) => m,
            None => 10,
        };

        // build query
        let facet_field = self.facet_field();

        let searcher = self.searcher()?;

        let qp = QueryParser::for_index(&self.index, vec![self.text_field(), self.name_field()]);
        let string_query = qp.parse_query(&query).unwrap();
        let facet_query = Box::new(BooleanQuery::new_multiterms_query(
            facets
                .iter()
                .map(|key| Term::from_facet(facet_field, key))
                .collect(),
        ));
        let query = BooleanQuery::new(vec![
            (Occur::Must, string_query),
            (Occur::Must, facet_query),
        ]);
        let top_docs_by_custom_score =
            // Call TopDocs with a custom tweak score
            TopDocs::with_limit(limit).tweak_score(move |segment_reader: &SegmentReader| {
                let filter_reader = segment_reader.facet_reader("facets").unwrap();
                let facet_dict = filter_reader.facet_dict();

                let query_ords: HashSet<u64> = facets
                    .iter()
                    .filter_map(|key| facet_dict.term_ord(key.encoded_str()).unwrap())
                    .collect();

                move |doc: DocId, original_score: Score| {
                    // Update the original score with a tweaked score
                    let missing_filters = filter_reader
                        .facet_ords(doc)
                        .filter(|ord| !query_ords.contains(ord))
                        .count();
                    let tweak = 1.0 / 4_f32.powi(missing_filters as i32);

                    original_score * tweak
                }
            });
        let top_docs = searcher.search(&query, &top_docs_by_custom_score)?;
        Ok(())
    }

    pub fn list_facet(self, from_level: String) -> anyhow::Result<Vec<(Facet, u64)>> {
        let searcher = self.searcher()?;
        let facet = Facet::from(from_level.as_str());
        let facet_term = Term::from_facet(self.facet_field(), &facet);
        let facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(from_level.as_str());
        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get(from_level.as_str()).collect();
        info!("found {} facets", facets.len());
        let mut out: Vec<(Facet, u64)> = Vec::new();
        for f in facets {
            info!("found facet {} with {} sub facets", f.0.clone(), f.1);
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

    pub fn get_facets(self, namespace: Option<String>) -> anyhow::Result<Vec<(Facet, u64)>> {
        let root = match namespace {
            Some(n) => n,
            None => "/".to_string(),
        };
        info!("getting facets for: {}", root);
        self.list_facet(root)
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
                let facets = create_facet_indexes(&fields, Vec::new(), &doc);
                for f in facets {
                    doc.add_facet(self.facet_field(), Facet::from_path(f));
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
    mut prefix: Vec<String>,
    doc: &TantivyDocument,
) -> Vec<Vec<String>> {
    let mut out: Vec<Vec<String>> = Vec::new();
    match value {
        serde_json::Value::Object(map) => {
            // Process each field in the object
            for (key, val) in map {
                let mut p = prefix.clone();
                p.push(key.to_string());
                let temp = create_facet_indexes(val, p.clone(), doc);
                out.extend(temp);
            }
        }
        serde_json::Value::Array(arr) => {
            // Process each item in the array
            for (_i, item) in arr.iter().enumerate() {
                let temp = create_facet_indexes(item, prefix.clone(), doc);
                out.extend(temp);
            }
        }
        _ => {
            // For primitive values, index the string representation
            let field_str = value.as_str().unwrap().to_string();
            prefix.push(field_str);
            info!("new facet: {}", prefix.join("/"));
            out.push(prefix);
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
