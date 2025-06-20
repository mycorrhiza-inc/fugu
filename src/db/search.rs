//! Search functionality for FuguDB

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tantivy::collector::TopDocs;
use tantivy::columnar::DocId;
use tantivy::query::{AllQuery, BooleanQuery, Occur, QueryParser, TermQuery};
use tantivy::schema::Facet;
use tantivy::schema::Value;
use tantivy::schema::document::CompactDocValue;
use tantivy::{Score, Searcher, SegmentReader, TantivyDocument, Term};
use tracing::{debug, error, info, warn};

use super::core::FuguDB;

/// Search result returned by FuguDB queries
#[derive(Debug, Clone, Serialize)]
pub struct FuguSearchResult {
    pub id: String,
    pub score: f32,
    pub text: String,
    pub metadata: Option<serde_json::Value>,
    pub facets: Option<Vec<String>>,
}

/// Search configuration options
#[derive(Debug, Clone)]
pub struct SearchOptions {
    pub page: usize,
    pub per_page: usize,
    pub filters: Vec<String>,
    pub boost_fields: Option<Vec<String>>,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            page: 0,
            per_page: 20,
            filters: Vec::new(),
            boost_fields: None,
        }
    }
}

impl FuguDB {
    /// Simple search method for basic queries (includes all data types by default)
    pub async fn simple_search(
        &self,
        query: String,
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        self.search(&query, &[], 0, 20).await
    }

    /// Perform a search query with conditional data inclusion
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

        let reader = self.index.reader()?;
        let searcher = reader.searcher();

        let text_field = self.text_field();
        let name_field = self.name_field();

        let query_parser = QueryParser::for_index(&self.index, vec![text_field, name_field]);

        let parsed_query = match query_parser.parse_query(query) {
            Ok(q) => q,
            Err(e) => {
                warn!("Failed to parse query '{}': {}", query, e);
                let escaped_query = query.replace(['(', ')', '[', ']', '{', '}', '"'], "");
                query_parser.parse_query(&escaped_query)?
            }
        };

        debug!("Parsed query successfully");

        let offset = page * per_page;
        let limit = per_page + offset;

        let query_with_facets: Box<dyn tantivy::query::Query> = if !filters.is_empty() {
            let facet_field = self.facet_field();
            let mut facet_terms = Vec::new();

            let targeting_conv_or_org = self.is_targeting_conversations_or_organizations(filters);

            for filter in filters {
                let facet_path = if filter.starts_with('/') {
                    filter.clone()
                } else {
                    format!("/{}", filter)
                };
                if let Ok(facet) = Facet::from_text(&facet_path) {
                    facet_terms.push(Term::from_facet(facet_field, &facet));
                }
            }

            if !facet_terms.is_empty() {
                let facet_query = Box::new(BooleanQuery::new_multiterms_query(facet_terms));
                Box::new(BooleanQuery::new(vec![
                    (Occur::Must, parsed_query),
                    (Occur::Must, facet_query),
                ]))
            } else {
                parsed_query
            }
        } else {
            parsed_query
        };

        let top_docs = searcher.search(&query_with_facets, &TopDocs::with_limit(limit))?;

        debug!("Found {} total documents", top_docs.len());

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
        namespace_filters: &[String],
        page: usize,
        per_page: usize,
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Searching with namespace facets: query='{}', facets={:?}",
            query, namespace_filters
        );

        let reader = self.index.reader()?;
        let searcher = reader.searcher();

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

        if !namespace_filters.is_empty() {
            let facet_field = self.facet_field();
            let mut facet_terms = Vec::new();

            for filter in namespace_filters {
                let facet_path = if filter.starts_with('/') {
                    filter.clone()
                } else {
                    format!("/{}", filter)
                };

                match Facet::from_text(&facet_path) {
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

        self.search(query, &[], page, per_page).await
    }

    /// Search with advanced options
    pub async fn advanced_search(
        &self,
        query: &str,
        options: SearchOptions,
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        self.search(query, &options.filters, options.page, options.per_page)
            .await
    }

    /// Direct get method for convenience
    pub fn get(&self, id: &str) -> Result<Vec<TantivyDocument>> {
        info!("FuguDB::get – querying id={}", id);
        let searcher = self.searcher()?;
        let query_parser = QueryParser::for_index(&self.index, vec![self.id_field()]);
        let query = query_parser.parse_query(id)?;
        let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;

        if let Some((_score, doc_address)) = top_docs.first() {
            let retrieved_doc: TantivyDocument = searcher.doc(*doc_address)?;
            Ok(vec![retrieved_doc])
        } else {
            Ok(vec![])
        }
    }

    /// Filter search with advanced scoring
    pub async fn filter_search(
        &self,
        query: String,
        filters: Vec<String>,
        max: Option<usize>,
    ) -> Result<()> {
        let facets: Vec<Facet> = filters.iter().map(|f| Facet::from(f)).collect();
        let limit = max.unwrap_or(10);
        let facet_field = self.facet_field();
        let searcher = self.searcher()?;

        let qp = QueryParser::for_index(&self.index, vec![self.text_field(), self.name_field()]);
        let string_query = qp.parse_query(&query)?;
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
            TopDocs::with_limit(limit).tweak_score(move |segment_reader: &SegmentReader| {
                let filter_reader = segment_reader.facet_reader("facets").unwrap();
                let facet_dict = filter_reader.facet_dict();

                let query_ords: HashSet<u64> = facets
                    .iter()
                    .filter_map(|key| facet_dict.term_ord(key.encoded_str()).unwrap())
                    .collect();

                move |doc: DocId, original_score: Score| {
                    let missing_filters = filter_reader
                        .facet_ords(doc)
                        .filter(|ord| !query_ords.contains(ord))
                        .count();
                    let tweak = 1.0 / 4_f32.powi(missing_filters as i32);
                    original_score * tweak
                }
            });

        let _top_docs = searcher.search(&query, &top_docs_by_custom_score)?;
        Ok(())
    }

    /// Check if filters target conversations or organizations
    fn is_targeting_conversations_or_organizations(&self, filters: &[String]) -> bool {
        filters.iter().any(|filter| {
            let normalized = if filter.starts_with('/') {
                filter.clone()
            } else {
                format!("/{}", filter)
            };
            normalized.contains("/conversation") || normalized.contains("/organization")
        })
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

        // Extract all facets for the document
        let facets = if let Ok(facet_field) = self.schema.get_field("facet") {
            let facet_vals: Vec<String> = doc
                .get_all(facet_field)
                .filter_map(|value| {
                    value.as_facet().map(|s| {
                        let raw = s.to_string();
                        raw.replace('\u{0000}', "/")
                    })
                })
                .collect();
            if !facet_vals.is_empty() {
                Some(facet_vals)
            } else {
                None
            }
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
}
