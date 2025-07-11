// path: src/db/search.rs
//! Search functionality for FuguDB

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tantivy::collector::TopDocs;
use tantivy::columnar::DocId;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, TermQuery};

use tantivy::schema::Facet;
use tantivy::schema::Value;
use tantivy::schema::document::CompactDocValue;
use tantivy::{Score, Searcher, SegmentReader, TantivyDocument, Term};
use tracing::{debug, error, info, warn};

use super::core::FuguDB;

/// Search result returned by FuguDB queries
#[derive(Debug, Clone, Serialize, JsonSchema)]
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
/// Represents a parsed facet filter with optional operators
#[derive(Debug, Clone)]
pub struct FacetFilter {
    pub path: String,
    pub operator: FilterOperator,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterOperator {
    Equals,   // Exact match
    Prefix,   // Prefix match (e.g., /namespace/*)
    Contains, // Contains substring
    Exists,   // Facet exists (regardless of value)
    Wildcard, // Wildcard match (*text* - any facet containing text)
}

impl FuguDB {
    /// Simple search method for basic queries (includes all data types by default)
    pub async fn simple_search(
        &self,
        query: String,
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        self.search(&query, &[], 0, 20).await
    }

    pub async fn search(
        &self,
        query: &str,
        filters: &[String],
        page: usize,
        per_page: usize,
    ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "FuguDB::search – query='{}', filters={:?}, page={}, per_page={}",
            query, filters, page, per_page
        );

        let reader = self.index.reader()?;
        let searcher = reader.searcher();

        // Parse filters to check for wildcard patterns
        let parsed_filters = self.parse_filters(filters);
        let has_wildcard = parsed_filters
            .iter()
            .any(|f| f.operator == FilterOperator::Wildcard);
        let wildcard_patterns: Vec<String> = parsed_filters
            .iter()
            .filter(|f| f.operator == FilterOperator::Wildcard)
            .map(|f| f.path.to_lowercase())
            .collect();

        // Separate wildcard filters from others
        let non_wildcard_filters: Vec<String> = filters
            .iter()
            .filter(|f| !f.starts_with('*') || !f.ends_with('*'))
            .cloned()
            .collect();

        // Parse the text query
        let text_field = self.text_field();
        let name_field = self.name_field();
        let fields_to_search = vec![text_field, name_field];

        let query_parser = QueryParser::for_index(&self.index, fields_to_search);

        // Build the text query
        let text_query = if query.trim().is_empty() {
            Box::new(AllQuery) as Box<dyn Query>
        } else {
            match query_parser.parse_query(query) {
                Ok(q) => q,
                Err(e) => {
                    warn!("Failed to parse query '{}': {}", query, e);
                    // Fallback: escape special characters and retry
                    let escaped_query = escape_query_string(query);
                    query_parser.parse_query(&escaped_query)?
                }
            }
        };

        debug!("Parsed text query successfully");

        // Build the facet query if non-wildcard filters are provided
        let base_query: Box<dyn Query> = if !non_wildcard_filters.is_empty() {
            let facet_query = self.build_facet_query(&non_wildcard_filters)?;

            // Combine text and facet queries
            if query.trim().is_empty() {
                // If no text query, just use facet query
                facet_query
            } else {
                // Combine both queries with AND
                Box::new(BooleanQuery::new(vec![
                    (Occur::Must, text_query),
                    (Occur::Must, facet_query),
                ]))
            }
        } else if query.trim().is_empty() && has_wildcard {
            // If only wildcard filters, search all documents
            Box::new(AllQuery)
        } else {
            text_query
        };

        // Execute search with larger limit if we have wildcard filters (for post-filtering)
        let offset = page * per_page;
        let search_limit = if has_wildcard {
            // Get more results for wildcard filtering
            (offset + per_page) * 10 // 10x to ensure we have enough after filtering
        } else {
            offset + per_page
        };

        let top_docs = searcher.search(&base_query, &TopDocs::with_limit(search_limit))?;
        debug!(
            "Found {} total documents before wildcard filtering",
            top_docs.len()
        );

        // Convert and filter results
        let mut results: Vec<FuguSearchResult> = Vec::new();
        let mut processed = 0;

        for (score, doc_address) in top_docs {
            match searcher.doc(doc_address) {
                Ok(doc) => {
                    let result = self.convert_doc_to_search_result(doc, score);

                    // Apply wildcard filtering if needed
                    if has_wildcard {
                        if let Some(facets) = &result.facets {
                            // Check if any facet contains any of the wildcard patterns
                            let matches = facets.iter().any(|facet| {
                                let facet_lower = facet.to_lowercase();
                                wildcard_patterns
                                    .iter()
                                    .any(|pattern| facet_lower.contains(pattern))
                            });

                            if matches {
                                results.push(result);
                            }
                        }
                    } else {
                        results.push(result);
                    }

                    processed += 1;

                    // Stop if we have enough results
                    if results.len() >= offset + per_page {
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to retrieve document: {}", e);
                }
            }
        }

        // Apply pagination to the filtered results
        let paginated_results: Vec<FuguSearchResult> =
            results.into_iter().skip(offset).take(per_page).collect();

        info!(
            "Search completed, returning {} results",
            paginated_results.len()
        );
        Ok(paginated_results)
    }

    /// Build a facet query from filter strings
    fn build_facet_query(&self, filters: &[String]) -> Result<Box<dyn Query>> {
        let facet_field = self.facet_field();
        let parsed_filters = self.parse_filters(filters);

        // Group filters by their behavior
        let mut exact_terms = Vec::new();
        let mut prefix_queries = Vec::new();

        for filter in parsed_filters {
            match filter.operator {
                FilterOperator::Equals => {
                    // Exact facet match
                    if let Ok(facet) = Facet::from_text(&filter.path) {
                        exact_terms.push(Term::from_facet(facet_field, &facet));
                    }
                }
                FilterOperator::Prefix => {
                    // Prefix matching - we'll need to implement a custom query
                    // For now, collect these for special handling
                    prefix_queries.push(filter.path.clone());
                }
                FilterOperator::Wildcard => {
                    // Wildcard filters are handled separately in the search method
                    // Skip them here
                    continue;
                }
                FilterOperator::Contains | FilterOperator::Exists => {
                    // These would require more complex handling
                    // For now, treat as exact match
                    if let Ok(facet) = Facet::from_text(&filter.path) {
                        exact_terms.push(Term::from_facet(facet_field, &facet));
                    }
                }
            }
        }

        // Build the query
        if exact_terms.is_empty() && prefix_queries.is_empty() {
            // No valid filters
            return Ok(Box::new(AllQuery));
        }

        let mut queries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        // Add exact term queries
        if !exact_terms.is_empty() {
            // Create OR query for all exact terms
            let term_query = Box::new(BooleanQuery::new_multiterms_query(exact_terms));
            queries.push((Occur::Should, term_query));
        }

        // Handle prefix queries (simplified for now)
        for prefix in prefix_queries {
            // In a real implementation, you'd want to use a prefix query
            // For now, we'll create exact matches for the prefix
            if let Ok(facet) = Facet::from_text(&prefix) {
                let term = Term::from_facet(facet_field, &facet);
                let term_query = Box::new(TermQuery::new(term, Default::default()));
                queries.push((Occur::Should, term_query));
            }
        }

        // If we have multiple queries, combine them
        if queries.len() == 1 {
            Ok(queries.into_iter().next().unwrap().1)
        } else {
            Ok(Box::new(BooleanQuery::new(queries)))
        }
    }

    /// Parse filter strings into structured FacetFilter objects
    fn parse_filters(&self, filters: &[String]) -> Vec<FacetFilter> {
        filters
            .iter()
            .map(|filter| {
                let normalized = normalize_facet_path(filter);

                // Check for operators
                if normalized.ends_with("/*") {
                    // Prefix match
                    FacetFilter {
                        path: normalized[..normalized.len() - 2].to_string(),
                        operator: FilterOperator::Prefix,
                        value: None,
                    }
                } else if normalized.contains("=") {
                    // Key-value match
                    let parts: Vec<&str> = normalized.splitn(2, '=').collect();
                    FacetFilter {
                        path: parts[0].to_string(),
                        operator: FilterOperator::Equals,
                        value: Some(parts[1].to_string()),
                    }
                } else {
                    // Default to exact match
                    FacetFilter {
                        path: normalized,
                        operator: FilterOperator::Equals,
                        value: None,
                    }
                }
            })
            .collect()
    }

    /// Search with facet scoring boost
    // pub async fn search_with_facet_boost(
    //     &self,
    //     query: &str,
    //     filters: &[String],
    //     boost_facets: &[String],
    //     page: usize,
    //     per_page: usize,
    // ) -> Result<Vec<FuguSearchResult>, Box<dyn std::error::Error + Send + Sync>> {
    //     info!(
    //         "Search with facet boost: query='{}', boost_facets={:?}",
    //         query, boost_facets
    //     );
    //
    //     let reader = self.index.reader()?;
    //     let searcher = reader.searcher();
    //
    //     // Build the base query
    //     let text_field = self.text_field();
    //     let name_field = self.name_field();
    //     let query_parser = QueryParser::for_index(&self.index, vec![text_field, name_field]);
    //
    //     let text_query = if query.trim().is_empty() {
    //         Box::new(AllQuery) as Box<dyn Query>
    //     } else {
    //         query_parser.parse_query(query)?
    //     };
    //
    //     // Build facet query if filters provided
    //     let base_query: Box<dyn Query> = if !filters.is_empty() {
    //         let facet_query = self.build_facet_query(filters)?;
    //         Box::new(BooleanQuery::new(vec![
    //             (Occur::Must, text_query),
    //             (Occur::Must, facet_query),
    //         ]))
    //     } else {
    //         text_query
    //     };
    //
    //     // Create boost facets set for scoring
    //     let boost_facet_set: HashSet<String> = boost_facets
    //         .iter()
    //         .map(|f| normalize_facet_path(f))
    //         .collect();
    //
    //     let offset = page * per_page;
    //     let limit = per_page + offset;
    //
    //     // Custom scoring based on facet presence
    //     let top_docs =
    //         TopDocs::with_limit(limit).tweak_score(move |segment_reader: &SegmentReader| {
    //             let facet_reader = segment_reader.facet_reader(self.facet_field_name()).ok();
    //
    //             move |doc: DocId, original_score: Score| {
    //                 if let Some(reader) = &facet_reader {
    //                     // Count matching boost facets
    //                     let matching_boosts = reader
    //                         .facet_ords(doc)
    //                         .filter_map(|ord| {
    //                             reader
    //                                 .facet_dict()
    //                                 .get_term(ord)
    //                                 .ok()
    //                                 .and_then(|bytes| std::str::from_utf8(bytes).ok())
    //                                 .map(|s| s.replace('\u{0000}', "/"))
    //                         })
    //                         .filter(|facet| boost_facet_set.contains(facet))
    //                         .count();
    //
    //                     // Apply boost: 1.5x for each matching facet
    //                     original_score * (1.5_f32).powi(matching_boosts as i32)
    //                 } else {
    //                     original_score
    //                 }
    //             }
    //         });
    //
    //     let results = searcher.search(&base_query, &top_docs)?;
    //
    //     // Convert results
    //     let search_results: Vec<FuguSearchResult> = results
    //         .into_iter()
    //         .skip(offset)
    //         .take(per_page)
    //         .map(|(score, doc_address)| match searcher.doc(doc_address) {
    //             Ok(doc) => self.convert_doc_to_search_result(doc, score),
    //             Err(e) => {
    //                 error!("Failed to retrieve document: {}", e);
    //                 FuguSearchResult {
    //                     id: "error".to_string(),
    //                     score: 0.0,
    //                     text: "Error retrieving document".to_string(),
    //                     metadata: None,
    //                     facets: None,
    //                 }
    //             }
    //         })
    //         .collect();
    //
    //     Ok(search_results)
    // }

    ///// Get facet statistics for search results
    //pub async fn search_with_facet_counts(
    //    &self,
    //    query: &str,
    //    filters: &[String],
    //    page: usize,
    //    per_page: usize,
    //) -> Result<
    //    (Vec<FuguSearchResult>, HashMap<String, usize>),
    //    Box<dyn std::error::Error + Send + Sync>,
    //> {
    //    // First, get the search results
    //    let results = self.search(query, filters, page, per_page).await?;
    //
    //    // Then, collect facet counts from the results
    //    let mut facet_counts: HashMap<String, usize> = HashMap::new();
    //
    //    for result in &results {
    //        if let Some(facets) = &result.facets {
    //            for facet in facets {
    //                *facet_counts.entry(facet.clone()).or_insert(0) += 1;
    //            }
    //        }
    //    }
    //
    //    Ok((results, facet_counts))
    //}

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

/// Normalize a facet path to ensure consistent formatting
fn normalize_facet_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}

/// Escape special characters in query strings
fn escape_query_string(query: &str) -> String {
    query.replace(
        [
            '(', ')', '[', ']', '{', '}', '"', ':', '+', '-', '!', '~', '*', '?', '\\', '^',
        ],
        "",
    )
}
