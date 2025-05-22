use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use std::fs;

use rkyv::ser::writer;
use tantivy::collector::{FacetCollector, TopDocs};
use tantivy::query::{QueryParser, TermQuery};
use tantivy::{Directory, Searcher, schema::*};
use tantivy::{Index, IndexWriter, ReloadPolicy};

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
}

impl FuguDB {
    /// Initialize the database by creating necessary trees/partitions
    pub fn new(path: PathBuf) -> Self {
        let schema_builder = Schema::builder();
        fs::create_dir_all(&path);
        let dir = tantivy::directory::MmapDirectory::open(&path).unwrap();
        let schema = crate::object::build_object_record_schema(schema_builder);
        let index = Index::open_or_create(dir, schema.clone()).unwrap();
        Self {
            schema,
            path,
            index, //writer: Arc::new(writer),
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

    pub fn get_index(self) -> Index {
        self.index
    }
    pub fn id_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("id").unwrap()
    }

    pub fn metadata_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("metadata").unwrap()
    }
    pub fn text_field(&self) -> tantivy::schema::Field {
        self.schema.get_field("text").unwrap()
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

    /// Index a single object
    pub fn ingest(&self, record: crate::object::ObjectRecord) {}

    /// Index multiple objects in batch
    pub async fn batch_ingest(self, objects: Vec<ObjectRecord>) {
        let schema = self.schema;
        let id = schema.get_field("id").unwrap();
        let text = schema.get_field("title").unwrap();
        let date_created = schema.get_field("date_created").unwrap();
        let date_updated = schema.get_field("date_updated").unwrap();
        let metadata = schema.get_field("metadata").unwrap();
        for object in objects {
            let doc = TantivyDocument::new();
        }
    }
}
