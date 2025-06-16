// db.rs
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
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
use tantivy::schema::OwnedValue;
use tantivy::{DateTime, Directory, DocAddress, Score, Searcher, SegmentReader, schema::*};
use tantivy::{Index, IndexWriter, ReloadPolicy};
use tracing::{Instrument, debug, error, info, warn};

use crate::object::ObjectRecord;

#[derive(Debug, Serialize, Deserialize)]
pub struct FacetNode {
    pub name: String,
    pub path: String,
    pub count: u64,
    pub children: BTreeMap<String, FacetNode>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FacetTreeResponse {
    pub tree: BTreeMap<String, FacetNode>,
    pub max_depth: usize,
    pub total_facets: usize,
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
#[derive(Debug, Clone, Serialize)] // Add Serialize for JSON responses
pub struct FuguSearchResult {
    pub id: String,
    pub score: f32,
    pub text: String,
    pub metadata: Option<serde_json::Value>,
    pub facets: Option<Vec<String>>,
}
impl FuguDB {
    pub fn new(path: PathBuf) -> Self {
        info!("FuguDB::new – initializing with path {:?}", path);
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
        info!("FuguDB::get – querying id={}", id);
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
    // New field accessors for namespace-aware fields
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
    /// Perform a search query
    pub async fn search(
        &self,
        query: &str,
        filters: &[String],
        page: usize,
        per_page: usize,
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "FuguDB::search – query=\'{}\', filters={:?}, page={}, per_page={}",
            query, filters, page, per_page
        );
        info!(
            "Searching for query: '{}' with {} filters, page: {}, per_page: {}",
            query,
            filters.len(),
            page,
            per_page
        );

        // Get the searcher
        let reader = self.index.reader()?;
        let searcher = reader.searcher();

        // Get the text field for searching
        let text_field = self
            .schema
            .get_field("text")
            .map_err(|e| format!("Text field not found in schema: {}", e))?;

        // Create query parser
        let query_parser = QueryParser::for_index(&self.index, vec![text_field]);

        // Parse the query
        let parsed_query = match query_parser.parse_query(query) {
            Ok(q) => q,
            Err(e) => {
                warn!("Failed to parse query '{}': {}", query, e);
                // Fallback to a term query or phrase query
                let escaped_query = query.replace(['(', ')', '[', ']', '{', '}', '"'], "");
                query_parser.parse_query(&escaped_query)?
            }
        };

        debug!("Parsed query successfully");

        // Calculate offset for pagination
        let offset = page * per_page;
        let limit = per_page + offset; // Get extra docs to handle pagination

        // Execute the search
        let top_docs = searcher.search(&parsed_query, &TopDocs::with_limit(limit))?;

        debug!("Found {} total documents", top_docs.len());

        // Apply pagination and convert results
        let results: Vec<FuguSearchResult> = top_docs
            .into_iter()
            .skip(offset)
            .take(per_page)
            .map(|(score, doc_address)| match searcher.doc(doc_address) {
                Ok(doc) => self.convert_doc_to_search_result(doc, score),
                Err(e) => {
                    error!("Failed to retrieve document: {}", e);
                    FuguSearchResult {
                        id: "error".to_string(),
                        score: 0.0,
                        text: "Error retrieving document".to_string(),
                        metadata: None,
                        facets: None,
                    }
                }
            })
            .collect();

        info!("Search completed, returning {} results", results.len());
        Ok(results)
    }
    /// Enhanced search with namespace facet filtering
    pub async fn search_with_namespace_facets(
        &self,
        query: &str,
        namespace_filters: &[String], // e.g., ["namespace/NYPUC/organization", "namespace/DOE/data"]
        page: usize,
        per_page: usize,
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Searching with namespace facets: query='{}', facets={:?}",
            query, namespace_filters
        );

        let reader = self.index.reader()?;
        let searcher = reader.searcher();

        // Build the main text query
        let text_field = self.text_field();
        let query_parser = QueryParser::for_index(&self.index, vec![text_field]);
        let text_query = match query_parser.parse_query(query) {
            Ok(q) => q,
            Err(e) => {
                warn!("Failed to parse query '{}': {}", query, e);
                let escaped_query = query.replace(['(', ')', '[', ']', '{', '}', '"'], "");
                query_parser.parse_query(&escaped_query)?
            }
        };

        // Build facet filters
        if !namespace_filters.is_empty() {
            let facet_field = self.facet_field();
            let mut facet_terms = Vec::new();

            for filter in namespace_filters {
                let facet_path = if filter.starts_with('/') {
                    filter.clone()
                } else {
                    format!("/{}", filter)
                };

                match tantivy::schema::Facet::from_text(&facet_path) {
                    Ok(facet) => {
                        let term = Term::from_facet(facet_field, &facet);
                        facet_terms.push(term);
                    }
                    Err(e) => {
                        warn!("Invalid facet path '{}': {}", facet_path, e);
                    }
                }
            }

            if !facet_terms.is_empty() {
                // Combine text query with facet filters using BooleanQuery
                let facet_query = Box::new(BooleanQuery::new_multiterms_query(facet_terms));
                let combined_query =
                    BooleanQuery::new(vec![(Occur::Must, text_query), (Occur::Must, facet_query)]);

                let offset = page * per_page;
                let limit = per_page + offset;
                let top_docs = searcher.search(&combined_query, &TopDocs::with_limit(limit))?;

                let results: Vec<FuguSearchResult> = top_docs
                    .into_iter()
                    .skip(offset)
                    .take(per_page)
                    .map(|(score, doc_address)| match searcher.doc(doc_address) {
                        Ok(doc) => self.convert_doc_to_search_result(doc, score),
                        Err(e) => {
                            error!("Failed to retrieve document: {}", e);
                            FuguSearchResult {
                                id: "error".to_string(),
                                score: 0.0,
                                text: "Error retrieving document".to_string(),
                                metadata: None,
                                facets: None,
                            }
                        }
                    })
                    .collect();

                return Ok(results);
            }
        }

        // Fallback to regular search if no valid facet filters
        self.search(query, &[], page, per_page).await
    }

    /// Get facets for a specific namespace
    pub fn get_namespace_facets(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<(tantivy::schema::Facet, u64)>> {
        let searcher = self.searcher()?;
        let namespace_root = format!("/namespace/{}", namespace);

        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(&namespace_root);

        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&tantivy::schema::Facet, u64)> =
            facet_counts.get(&namespace_root).collect();

        let mut result = Vec::new();
        for (facet, count) in facets {
            result.push((facet.clone(), count));
        }

        Ok(result)
    }

    /// Get all available namespaces
    pub fn get_available_namespaces(&self) -> anyhow::Result<Vec<String>> {
        let searcher = self.searcher()?;
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet("/namespace");

        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&tantivy::schema::Facet, u64)> = facet_counts.get("/namespace").collect();

        let mut namespaces = Vec::new();
        for (facet, _count) in facets {
            let facet_str = facet.to_string();
            // Extract namespace from "/namespace/NYPUC" format
            if let Some(namespace) = facet_str.strip_prefix("/namespace/") {
                if !namespace.contains('/') {
                    // Only top-level namespaces
                    namespaces.push(namespace.to_string());
                }
            }
        }

        namespaces.sort();
        namespaces.dedup();
        Ok(namespaces)
    }
    /// Simple search method for basic queries
    pub async fn simple_search(
        &self,
        query: String,
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        self.search(&query, &[], 0, 20).await
    }

    /// Convert a Tantivy document to a search result
    fn convert_doc_to_search_result(&self, doc: TantivyDocument, score: f32) -> FuguSearchResult {
        // Get field references - handle errors gracefully
        let id = if let Ok(id_field) = self.schema.get_field("id") {
            doc.get_first(id_field)
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string()
        } else {
            "unknown".to_string()
        };

        let text = if let Ok(text_field) = self.schema.get_field("text") {
            doc.get_first(text_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        } else {
            "".to_string()
        };

        // Extract metadata if present
        let metadata = if let Ok(metadata_field) = self.schema.get_field("metadata") {
            doc.get_first(metadata_field)
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str(s).ok())
        } else {
            None
        };

        // Extract namespace as facet if present
        let facets = if let Ok(namespace_field) = self.schema.get_field("namespace") {
            doc.get_first(namespace_field)
                .and_then(|v| v.as_str())
                .map(|ns| vec![ns.to_string()])
        } else {
            None
        };

        FuguSearchResult {
            id,
            score,
            text,
            metadata,
            facets,
        }
    }

    /// Search with more advanced options (for future expansion)
    pub async fn advanced_search(
        &self,
        query: &str,
        filters: &[String],
        page: usize,
        per_page: usize,
        _boost_fields: Option<&[String]>, // For future use
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        // For now, just delegate to the regular search
        // In the future, you can implement field boosting, complex filters, etc.
        self.search(query, filters, page, per_page).await
    }

    // pub async fn simple_search(&self, q: String) -> Vec<SerdeValue> {
    //     let index = self.get_index();
    //     let text = self.text_field();
    //     let searcher = self.searcher().unwrap();
    //     let query_parser = QueryParser::for_index(index, vec![text]);
    //     let query = query_parser.parse_query(&q).unwrap();
    //     let top_docs = searcher.search(&query, &TopDocs::with_limit(50)).unwrap();

    //     let mut d = Vec::new();
    //     for (score, doc_address) in top_docs {
    //         let retrieved_doc: TantivyDocument = searcher.doc(doc_address).unwrap();
    //         let a = serde_json::from_str(retrieved_doc.to_json(&self.schema()).as_str()).unwrap();
    //         let s = serde_json::Number::from_f64(score as f64).unwrap();
    //         // set the score per doc
    //         let o = match a {
    //             SerdeValue::Object(m) => {
    //                 let mut m = m.clone();
    //                 m.insert("score".to_string(), SerdeValue::Number(s));
    //                 SerdeValue::Object(m)
    //             }
    //             v => v.clone(),
    //         };
    //         d.push(o);
    //     }
    //     return d;
    // }

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

    pub fn get_facets(self, namespace: Option<String>) -> anyhow::Result<Vec<(Facet, u64)>> {
        let root = match namespace {
            Some(n) => n,
            None => "/".to_string(),
        };
        info!("getting facets for: {}", root);
        self.list_facet(root)
    }
    fn collect_facets_recursive(
        &self,
        searcher: &Searcher,
        facet_path: &str,
        current_depth: usize,
        max_depth: Option<usize>,
        all_facets: &mut Vec<(Facet, u64)>,
    ) -> anyhow::Result<()> {
        // Check if we've reached max depth
        if let Some(max_d) = max_depth {
            if current_depth >= max_d {
                return Ok(());
            }
        }

        // Create facet collector for this level
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(facet_path);

        // Search and collect facets at this level
        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets_at_level: Vec<(&Facet, u64)> = facet_counts.get(facet_path).collect();

        // Add these facets to our collection and recurse
        for (facet, count) in facets_at_level {
            all_facets.push((facet.clone(), count));

            // Recursively collect children of this facet
            let facet_str = facet.to_string();
            self.collect_facets_recursive(
                searcher,
                &facet_str,
                current_depth + 1,
                max_depth,
                all_facets,
            )?;
        }

        Ok(())
    }

    /// Construct a Tantivy document from an ObjectRecord
    /// Safe version of build_tantivy_doc that returns Result
    /// Enhanced build_tantivy_doc_safe with namespace facet generation
    fn build_tantivy_doc_safe(&self, object: &ObjectRecord) -> anyhow::Result<TantivyDocument> {
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

        // Add metadata if present
        if let Some(metadata) = &object.metadata {
            let object_map: std::collections::BTreeMap<String, OwnedValue> = match metadata {
                serde_json::Value::Object(map) => map
                    .iter()
                    .map(|(k, v)| (k.clone(), OwnedValue::from(v.clone())))
                    .collect(),
                _ => std::collections::BTreeMap::new(),
            };
            doc.add_object(self.metadata_field(), object_map);
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
        // FIXED: Add date fields with proper DateTime conversion
        if let Some(date_str) = &object.date_created {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(date_str) {
                // Convert to OffsetDateTime and then to tantivy DateTime
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

    /// Upsert a vector of objects: delete existing by ID then add new
    pub async fn upsert(&self, records: Vec<ObjectRecord>) -> anyhow::Result<()> {
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
    pub async fn batch_upsert(&self, records: Vec<ObjectRecord>) -> anyhow::Result<usize> {
        // Delegate to regular upsert since they now do the same thing
        self.upsert(records.clone()).await?;
        Ok(records.len())
    }

    /// Index a vector of objects
    pub async fn ingest(&self, records: Vec<ObjectRecord>) -> anyhow::Result<()> {
        // Delegate to upsert since all ingests should be upserts
        self.upsert(records).await
    }

    /// Delete a single document by ID
    pub async fn delete_document(&self, doc_id: String) -> anyhow::Result<()> {
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
}
impl FuguDB {
    /// Get all facets and build a tree structure up to max_depth - 1
    pub fn get_facet_tree(&self, max_depth: Option<usize>) -> anyhow::Result<FacetTreeResponse> {
        info!("Getting facet tree with max_depth: {:?}", max_depth);

        let searcher = self.searcher()?;
        let mut all_facets = Vec::new();

        // Start by collecting facets from the root
        self.collect_facets_recursive(&searcher, "/", 0, max_depth, &mut all_facets)?;

        let mut tree: BTreeMap<String, FacetNode> = BTreeMap::new();
        let mut actual_max_depth = 0;
        let total_facets = all_facets.len();

        info!("Found {} total facets", total_facets);

        for (facet, count) in all_facets {
            let facet_str = facet.to_string();
            info!("Processing facet: {} with count: {}", facet_str, count);

            // Skip root facet "/"
            if facet_str == "/" {
                continue;
            }

            // Split the facet path into components, removing empty strings
            let components: Vec<&str> = facet_str.split('/').filter(|s| !s.is_empty()).collect();

            let depth = components.len();
            actual_max_depth = actual_max_depth.max(depth);

            // Apply max_depth filter if specified
            if let Some(max_d) = max_depth {
                if depth >= max_d {
                    continue;
                }
            }

            // Build the tree path
            let mut current_map = &mut tree;
            let mut current_path = String::new();

            for (i, component) in components.iter().enumerate() {
                current_path.push('/');
                current_path.push_str(component);

                let is_leaf = i == components.len() - 1;

                current_map
                    .entry(component.to_string())
                    .or_insert_with(|| FacetNode {
                        name: component.to_string(),
                        path: current_path.clone(),
                        count: if is_leaf { count } else { 0 },
                        children: BTreeMap::new(),
                    });

                // If this is the leaf node, update its count
                if is_leaf {
                    if let Some(node) = current_map.get_mut(*component) {
                        node.count = count;
                    }
                } else {
                    // Move to the next level
                    current_map = &mut current_map.get_mut(*component).unwrap().children;
                }
            }
        }

        // Update parent counts by summing children
        fn update_parent_counts(node: &mut FacetNode) -> u64 {
            if node.children.is_empty() {
                return node.count;
            }

            let mut total = node.count;
            for child in node.children.values_mut() {
                total += update_parent_counts(child);
            }
            node.count = total;
            total
        }

        for node in tree.values_mut() {
            update_parent_counts(node);
        }

        Ok(FacetTreeResponse {
            tree,
            max_depth: actual_max_depth,
            total_facets,
        })
    }
}
impl FuguDB {
    /// Get all parent paths that have leaf children, plus their leaf values
    pub fn get_all_filter_paths(&self) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
        let tree_response = self.get_facet_tree(None)?;
        let mut filter_paths = BTreeMap::new();

        fn collect_parent_leaf_paths(
            node: &FacetNode,
            results: &mut BTreeMap<String, Vec<String>>,
        ) {
            // If this node has children, check if any are leaves
            if !node.children.is_empty() {
                let mut leaf_values = Vec::new();
                let mut has_leaf_children = false;

                for (child_name, child_node) in &node.children {
                    if child_node.children.is_empty() {
                        // This is a leaf
                        leaf_values.push(child_name.clone());
                        has_leaf_children = true;
                    }
                }

                // If this node has leaf children, add it as a filter path
                if has_leaf_children {
                    results.insert(node.path.clone(), leaf_values);
                }

                // Recursively process children
                for child_node in node.children.values() {
                    collect_parent_leaf_paths(child_node, results);
                }
            }
        }

        for root_node in tree_response.tree.values() {
            collect_parent_leaf_paths(root_node, &mut filter_paths);
        }

        Ok(filter_paths)
    }

    /// Get filter paths for documents that have a specific namespace facet
    pub fn get_filter_paths_for_namespace(
        &self,
        namespace: &str,
    ) -> anyhow::Result<BTreeMap<String, Vec<String>>> {
        let searcher = self.searcher()?;

        // Create a query to filter by namespace facet
        let namespace_facet = Facet::from_text(&format!("/namespace/{}", namespace))?;
        let namespace_term = Term::from_facet(self.facet_field(), &namespace_facet);
        let namespace_query = TermQuery::new(namespace_term, IndexRecordOption::Basic);

        // Get all documents with this namespace
        let top_docs = searcher.search(&namespace_query, &TopDocs::with_limit(10000))?;

        // Collect all facets from these documents
        let mut namespace_facets = std::collections::HashMap::new();

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;

            // Collect facets from this document
            let facet_field = self.facet_field();
            let doc_facets: Vec<Facet> = doc
                .get_all(facet_field)
                .filter_map(|field_value| {
                    if let Some(facet_str) = field_value.as_facet() {
                        Facet::from_text(facet_str).ok()
                    } else {
                        None
                    }
                })
                .collect();

            // Process the collected facets
            for facet in doc_facets {
                let facet_str = facet.to_string();
                // Skip namespace facets themselves
                if !facet_str.starts_with("/namespace/") {
                    *namespace_facets.entry(facet).or_insert(0) += 1;
                }
            }
        }

        // Build tree from these facets
        let mut filter_paths = BTreeMap::new();
        let mut facet_tree = BTreeMap::new();

        // First, build the tree structure
        for (facet, count) in namespace_facets {
            let facet_str = facet.to_string();
            if facet_str == "/" {
                continue;
            }

            let components: Vec<&str> = facet_str.split('/').filter(|s| !s.is_empty()).collect();
            let mut current_map = &mut facet_tree;
            let mut current_path = String::new();

            for (i, component) in components.iter().enumerate() {
                current_path.push('/');
                current_path.push_str(component);

                let is_leaf = i == components.len() - 1;

                current_map
                    .entry(component.to_string())
                    .or_insert_with(|| FacetNode {
                        name: component.to_string(),
                        path: current_path.clone(),
                        count: if is_leaf { count } else { 0 },
                        children: BTreeMap::new(),
                    });

                if !is_leaf {
                    current_map = &mut current_map
                        .get_mut(&component.to_string())
                        .unwrap()
                        .children;
                }
            }
        }

        // Now extract parent-leaf relationships
        fn collect_namespace_parent_leaf_paths(
            node: &FacetNode,
            results: &mut BTreeMap<String, Vec<String>>,
        ) {
            if !node.children.is_empty() {
                let mut leaf_values = Vec::new();
                let mut has_leaf_children = false;

                for (child_name, child_node) in &node.children {
                    if child_node.children.is_empty() {
                        leaf_values.push(child_name.clone());
                        has_leaf_children = true;
                    }
                }

                if has_leaf_children {
                    results.insert(node.path.clone(), leaf_values);
                }

                for child_node in node.children.values() {
                    collect_namespace_parent_leaf_paths(child_node, results);
                }
            }
        }

        for root_node in facet_tree.values() {
            collect_namespace_parent_leaf_paths(root_node, &mut filter_paths);
        }

        Ok(filter_paths)
    }

    /// Get the values (children) at a specific filter path
    pub fn get_filter_values_at_path(&self, filter_path: &str) -> anyhow::Result<Vec<String>> {
        let searcher = self.searcher()?;
        let mut facet_collector = FacetCollector::for_field("facet");

        // Ensure the path starts with /
        let normalized_path = if filter_path.starts_with('/') {
            filter_path.to_string()
        } else {
            format!("/{}", filter_path)
        };

        facet_collector.add_facet(&normalized_path);
        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;

        let facets_at_path: Vec<(&Facet, u64)> = facet_counts.get(&normalized_path).collect();

        let mut values = Vec::new();
        for (facet, _count) in facets_at_path {
            let facet_str = facet.to_string();
            // Extract just the last component of the path
            if let Some(last_component) = facet_str.split('/').last() {
                if !last_component.is_empty() {
                    values.push(last_component.to_string());
                }
            }
        }

        values.sort();
        Ok(values)
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
// Helper function to create metadata facets
fn create_metadata_facets(value: &serde_json::Value, mut prefix: Vec<String>) -> Vec<Vec<String>> {
    let mut facets = Vec::new();

    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                let mut new_prefix = prefix.clone();
                new_prefix.push(key.clone());
                let sub_facets = create_metadata_facets(val, new_prefix);
                facets.extend(sub_facets);
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                let sub_facets = create_metadata_facets(item, prefix.clone());
                facets.extend(sub_facets);
            }
        }
        _ => {
            // For primitive values, create a facet path
            if let Some(value_str) = value.as_str() {
                if !value_str.is_empty() {
                    prefix.push(value_str.to_string());
                    facets.push(prefix);
                }
            }
        }
    }

    facets
}
