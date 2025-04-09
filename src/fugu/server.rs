use std::path::PathBuf;

use rkyv;
use tokio::sync::mpsc;

use crate::fugu::wal::{WAL, WALCMD};

pub struct FuguServer {
    path: PathBuf,
    wal: WAL,
    stop: bool,
    wal_sender: tokio::sync::mpsc::Sender<WALCMD>,
    wal_receiver: tokio::sync::mpsc::Receiver<WALCMD>,
}

impl FuguServer {
    pub fn new(path: PathBuf) -> Self {
        // first,
        //  - check if the
        //
        let (tx, rx): (mpsc::Sender<WALCMD>, mpsc::Receiver<WALCMD>) = mpsc::channel(1000);
        let wal = WAL::open(path.clone());
        Self {
            path,
            wal,
            stop: false,
            wal_sender: tx,
            wal_receiver: rx,
        }
    }
    pub fn get_wal_sender(&self) -> mpsc::Sender<WALCMD> {
        self.wal_sender.clone()
    }
    async fn dump_wal(&self) -> Result<String, rkyv::rancor::Error> {
        Ok(self.wal.dump()?)
    }

    async fn wal_listen(&mut self) {
        loop {
            if self.stop {
                break;
            }
            if let Some(msg) = self.wal_receiver.recv().await {
                match msg {
                    WALCMD::Put { .. } | WALCMD::Delete { .. } | WALCMD::Patch { .. } => {
                        match self.wal.push(msg.into()) {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                    WALCMD::DumpWAL { response } => {
                        if let Ok(dump) = self.dump_wal().await {
                            let _ = response.send(dump);
                        }
                    }
                }
            }
        }
    }

    pub async fn up(&mut self) {
        self.wal_listen().await;
    }
    pub async fn down(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("this is where we'll make sure that everything is saved correctly");
        self.stop = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fugu::wal;
    use crate::fugu::{node, node::Node};

    #[tokio::test]
    pub async fn run_wal_text() -> Result<(), Box<dyn std::error::Error>> {
        let wal_path = PathBuf::from("./test_wal.bin");
        let mut server = FuguServer::new(wal_path);
        let sender = server.get_wal_sender();
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel::<bool>();

        let server_handle = tokio::spawn(async move {
            let _ = server.up();
            // Wait for shutdown signal
            let _ = rx_shutdown.await;
            let _ = server.down().await;
        });

        let mut nodes: Vec<Node> = Vec::new();
        let mut handles = Vec::new();

        // Create 5 nodes
        for i in 0..5 {
            let node = node::new(format!("node_{}/", i), sender.clone());
            nodes.push(node);
        }

        // Spawn concurrent tasks for each node
        for (i, node) in nodes.into_iter().enumerate() {
            let handle = tokio::spawn(async move {
                // Each node does random operations
                for j in 1..10 {
                    let random = rand::random::<u64>() % 500;
                    // Random delay between operations (0-500ms)
                    tokio::time::sleep(tokio::time::Duration::from_millis(random)).await;

                    // Randomly choose between put and delete
                    if rand::random_bool(0.7) {
                        // 70% chance of put, 30% chance of delete
                        let key = format!("node_{}_key_{}", i, j);
                        let value = format!("value_from_node_{}_op_{}", i, j).into_bytes();
                        let _ = node.walog(wal::WALOP::Put { key, value }).await;
                    } else {
                        // Delete a random previous key
                        let prev_key = format!("node_{}_key_{}", i, rand::random::<u32>() % j);
                        let _ = node.walog(wal::WALOP::Delete { key: prev_key }).await;
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all node operations to complete
        for handle in handles {
            handle.await?;
        }

        // Sleep briefly to ensure all operations are processed
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        let _ = tx_shutdown.send(true);

        let _ = server_handle.await?;

        Ok(())
    }
}
