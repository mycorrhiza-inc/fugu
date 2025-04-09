use crate::fugu::index::{InvertedIndex, Token};
use crate::fugu::wal::{WALCMD, WALOP};
use serde_json::json;
use sled;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tracing_subscriber::fmt::time;

pub enum NodeJob {}

pub struct Node {
    namespace: String,
    frequency: u16, // how often to check
    wal_chan: tokio::sync::mpsc::Sender<WALCMD>,
    shutdown_rx: tokio::sync::oneshot::Receiver<bool>, // receiver for shutdown signal
    shutdown_tx: tokio::sync::oneshot::Sender<bool>,   // send here to shutdown node
    job_queue: Vec<NodeJob>,
    inverted_index: Option<InvertedIndex>,
}

impl Node {
    fn new(namespace: String, wal_chan: mpsc::Sender<WALCMD>) -> Self {
        let (tx, rx) = tokio::sync::oneshot::channel::<bool>();
        Node {
            namespace,
            wal_chan,
            frequency: 1000, //check every second
            shutdown_tx: tx,
            shutdown_rx: rx,
            job_queue: vec![],
            inverted_index: None,
        }
    }
    async fn init_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let index_path = format!("{}_index", self.namespace);
        let index = InvertedIndex::new(&index_path, self.wal_chan.clone()).await;
        self.inverted_index = Some(index);
        Ok(())
    }

    async fn index_term(&self, term_token: Token) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(index) = &self.inverted_index {
            index.add_term(term_token).await?;
        }
        Ok(())
    }
    // fn new_file(&self) {}
    async fn index_file(&self, path: PathBuf) {
        let _ = json!({});
    }
    pub async fn walog(&self, msg: WALOP) -> Result<(), mpsc::error::SendError<WALCMD>> {
        let msg_clone = msg.clone();
        match self.wal_chan.send(msg.into()).await {
            Ok(_) => {
                println!("{:?}", msg_clone);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    fn delete_file(&self) {}
    async fn load_index(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let index_path = format!("{}_index", self.namespace);
        let index = InvertedIndex::new(&index_path, self.wal_chan.clone()).await;
        self.inverted_index = Some(index);
        Ok(())
    }
}

pub fn new(namespace: String, wal_chan: mpsc::Sender<WALCMD>) -> Node {
    Node::new(namespace, wal_chan)
}
