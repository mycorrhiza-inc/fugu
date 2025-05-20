use rkyv::{Archive, Deserialize, Serialize};
use crate::rkyv_adapter;
use sled::Tree;
use tokio::task;

// These types already implement Archive and other rkyv traits
type Timestamp = u64;
type DocId = u64;

fn encode_ts(ts: Timestamp) -> [u8; 8] {
    ts.to_be_bytes()
}

#[derive(Clone)]
pub struct TimeIndex {
    tree: Tree,
}

impl TimeIndex {
    pub fn new(db: &sled::Db, name: &str) -> sled::Result<Self> {
        Ok(Self {
            tree: db.open_tree(name)?,
        })
    }

    pub async fn insert(&self, timestamp: Timestamp, doc_id: DocId) -> sled::Result<()> {
        let tree = self.tree.clone();
        task::spawn_blocking(move || {
            let key = encode_ts(timestamp);
            let updated = match tree.get(&key)? {
                Some(val) => {
                    let mut ids: Vec<DocId> = rkyv_adapter::deserialize(&val)?;
                    ids.push(doc_id);
                    rkyv::to_bytes::<rkyv::rancor::Error>(&ids)?.to_vec()
                }
                None => rkyv::to_bytes::<rkyv::rancor::Error>(&vec![doc_id])?.to_vec(),
            };
            tree.insert(key, updated)?;
            Ok(())
        })
        .await
        .unwrap()
    }

    pub async fn range_query(&self, start: Timestamp, end: Timestamp) -> sled::Result<Vec<DocId>> {
        let tree = self.tree.clone();
        task::spawn_blocking(move || {
            let start_key = encode_ts(start);
            let end_key = encode_ts(end);
            let mut results = Vec::new();

            for res in tree.range(start_key..end_key) {
                let (_, val) = res?;
                let ids: Vec<DocId> = rkyv_adapter::deserialize(&val)?;
                results.extend(ids);
            }

            Ok(results)
        })
        .await
        .unwrap()
    }
}
