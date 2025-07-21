// path: src/db/facet.rs
//! Facet operations and tree building for FuguDB

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tantivy::collector::FacetCollector;
use tantivy::query::{AllQuery, TermQuery};
use tantivy::schema::*;
use tantivy::schema::{Facet, IndexRecordOption};
use tantivy::{TantivyDocument, Term};
use tracing::info;

use super::core::Dataset;

/// A node in the facet tree structure
#[derive(Debug, Serialize, Deserialize)]
pub struct FacetNode {
    pub name: String,
    pub path: String,
    pub count: u64,
    pub children: BTreeMap<String, FacetNode>,
}

/// Response containing the complete facet tree
#[derive(Debug, Serialize, Deserialize)]
pub struct FacetTreeResponse {
    pub tree: BTreeMap<String, FacetNode>,
    pub max_depth: usize,
    pub total_facets: usize,
}

impl Dataset {
    /// Get facets for a specific namespace
    pub fn get_namespace_facets(&self, namespace: &str) -> Result<Vec<(Facet, u64)>> {
        let searcher = self.docs().searcher()?;
        let namespace_root = format!("/namespace/{}", namespace);

        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(&namespace_root);

        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get(&namespace_root).collect();

        let mut result = Vec::new();
        for (facet, count) in facets {
            result.push((facet.clone(), count));
        }

        Ok(result)
    }

    /// Get all available namespaces
    pub fn get_available_namespaces(&self) -> Result<Vec<String>> {
        let searcher = self.docs().searcher()?;
        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet("/namespace");

        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get("/namespace").collect();

        let mut namespaces = Vec::new();
        for (facet, _count) in facets {
            let facet_str = facet.to_string();
            if let Some(namespace) = facet_str.strip_prefix("/namespace/") {
                if !namespace.contains('/') {
                    namespaces.push(namespace.to_string());
                }
            }
        }

        namespaces.sort();
        namespaces.dedup();
        Ok(namespaces)
    }

    /// List facets at a specific level
    pub fn list_facet(&self, from_level: String) -> Result<Vec<(Facet, u64)>> {
        let searcher = self.docs().searcher()?;
        let facet = Facet::from(from_level.as_str());
        let facet_term = Term::from_facet(self.docs().facet_field().unwrap(), &facet);
        let _facet_term_query = TermQuery::new(facet_term, IndexRecordOption::Basic);

        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(from_level.as_str());

        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get(from_level.as_str()).collect();

        info!("found {} facets", facets.len());
        let mut out: Vec<(Facet, u64)> = Vec::new();
        for f in facets {
            info!("found facet {} with {} sub facets", f.0, f.1);
            out.push((f.0.clone(), f.1));
        }
        Ok(out)
    }

    /// Get facets with optional namespace filtering
    pub fn get_facets(&self, namespace: Option<String>) -> Result<Vec<(Facet, u64)>> {
        let root = namespace.unwrap_or_else(|| "/".to_string());
        info!("getting facets for: {}", root);
        self.list_facet(root)
    }

    /// Get facets at a specific prefix/root path
    pub fn get_facets_at(&self, prefix: &str) -> Result<Vec<(Facet, u64)>> {
        info!("getting facets at prefix: {}", prefix);
        self.list_facet(prefix.to_string())
    }

    /// Get all facets and build a tree structure up to max_depth
    pub fn get_facet_tree(&self, max_depth: Option<usize>) -> Result<FacetTreeResponse> {
        info!("Getting facet tree with max_depth: {:?}", max_depth);

        let searcher = self.docs().searcher()?;
        let mut all_facets = Vec::new();

        self.collect_facets_recursive(&searcher, "/", 0, max_depth, &mut all_facets)?;

        let mut tree: BTreeMap<String, FacetNode> = BTreeMap::new();
        let mut actual_max_depth = 0;
        let total_facets = all_facets.len();

        info!("Found {} total facets", total_facets);

        for (facet, count) in all_facets {
            let facet_str = facet.to_string();
            info!("Processing facet: {} with count: {}", facet_str, count);

            if facet_str == "/" {
                continue;
            }

            let components: Vec<&str> = facet_str.split('/').filter(|s| !s.is_empty()).collect();
            let depth = components.len();
            actual_max_depth = actual_max_depth.max(depth);

            if let Some(max_d) = max_depth {
                if depth >= max_d {
                    continue;
                }
            }

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

                if is_leaf {
                    if let Some(node) = current_map.get_mut(*component) {
                        node.count = count;
                    }
                } else {
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

    /// Recursively collect facets from the index
    fn collect_facets_recursive(
        &self,
        searcher: &tantivy::Searcher,
        facet_path: &str,
        current_depth: usize,
        max_depth: Option<usize>,
        all_facets: &mut Vec<(Facet, u64)>,
    ) -> Result<()> {
        if let Some(max_d) = max_depth {
            if current_depth >= max_d {
                return Ok(());
            }
        }

        let mut facet_collector = FacetCollector::for_field("facet");
        facet_collector.add_facet(facet_path);

        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets_at_level: Vec<(&Facet, u64)> = facet_counts.get(facet_path).collect();

        for (facet, count) in facets_at_level {
            all_facets.push((facet.clone(), count));

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

    /// Get all parent paths that have leaf children, plus their leaf values
    pub fn get_all_filter_paths(&self) -> Result<BTreeMap<String, Vec<String>>> {
        let tree_response = self.get_facet_tree(None)?;
        let mut filter_paths = BTreeMap::new();

        fn collect_parent_leaf_paths(
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
    ) -> Result<BTreeMap<String, Vec<String>>> {
        let searcher = self.filter_index().searcher()?;

        let namespace_facet = Facet::from_text(&format!("/namespace/{}", namespace))?;
        // For filter index, facet is a text field, not a facet field
        let facet_field = self.filter_index().get_field("facet")?;
        let namespace_term =
            Term::from_field_text(facet_field, &format!("/namespace/{}", namespace));
        let namespace_query = TermQuery::new(namespace_term, IndexRecordOption::Basic);

        let top_docs = searcher.search(
            &namespace_query,
            &tantivy::collector::TopDocs::with_limit(10000),
        )?;
        // let top_docs = searcher.search(&namespace_query, &TopDocs::with_limit(10000))?;

        let mut namespace_facets = std::collections::HashMap::new();

        for (_score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher.doc(doc_address)?;

            let facet_field = self.docs().facet_field().unwrap();
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

            for facet in doc_facets {
                let facet_str = facet.to_string();
                if !facet_str.starts_with("/namespace/") {
                    *namespace_facets.entry(facet).or_insert(0) += 1;
                }
            }
        }

        let mut filter_paths = BTreeMap::new();
        let mut facet_tree = BTreeMap::new();

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
    pub fn get_filter_values_at_path(&self, filter_path: &str) -> Result<Vec<String>> {
        let searcher = self.filter_index().searcher()?;
        let facet_field = self.filter_index().get_field("facet")?;

        let normalized_path = if filter_path.starts_with('/') {
            filter_path.to_string()
        } else {
            format!("/{}", filter_path)
        };

        // Use the facet hierarchy field for proper facet queries
        let facet_hierarchy_field = self.filter_index().get_field("facet_hierarchy")?;
        
        let mut facet_collector = FacetCollector::for_field("facet_hierarchy");
        facet_collector.add_facet(&normalized_path);

        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get(&normalized_path).collect();
        
        let mut values = std::collections::HashSet::new();

        for (facet, _count) in facets {
            let facet_str = facet.to_string();
            if let Some(child_path) = facet_str.strip_prefix(&format!("{}/", normalized_path)) {
                // Only get immediate children (no nested paths)
                if !child_path.contains('/') && !child_path.is_empty() {
                    values.insert(child_path.to_string());
                }
            }
        }

        let mut result: Vec<String> = values.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Search for facets that start with a given facet prefix and match query text
    /// Used for frontend filter searches
    pub fn search_facet(&self, facet_prefix: &str, query_text: Option<&str>) -> Result<Vec<(Facet, u64)>> {
        let searcher = self.filter_index().searcher()?;
        
        let normalized_prefix = if facet_prefix.starts_with('/') {
            facet_prefix.to_string()
        } else {
            format!("/{}", facet_prefix)
        };

        // Use FacetCollector to get facets that start with the given prefix
        let mut facet_collector = FacetCollector::for_field("facet_hierarchy");
        facet_collector.add_facet(&normalized_prefix);

        let facet_counts = searcher.search(&AllQuery, &facet_collector)?;
        let facets: Vec<(&Facet, u64)> = facet_counts.get(&normalized_prefix).collect();
        
        let mut result = Vec::new();
        
        for (facet, count) in facets {
            let facet_str = facet.to_string();
            
            // If query_text is provided, filter facets that contain the query text
            if let Some(query) = query_text {
                if !facet_str.to_lowercase().contains(&query.to_lowercase()) {
                    continue;
                }
            }
            
            result.push((facet.clone(), count));
        }

        // Sort by facet path for consistent results
        result.sort_by(|a, b| a.0.to_string().cmp(&b.0.to_string()));
        
        Ok(result)
    }
}
