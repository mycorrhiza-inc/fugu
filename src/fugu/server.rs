use std::path::PathBuf;

use rkyv;
use tokio::sync::mpsc;

use crate::fugu::wal::{WAL, WALCMD};

#[derive(Clone)]
pub struct FuguServer {
    path: PathBuf,
    wal: WAL,
    stop: bool,
    wal_sender: tokio::sync::mpsc::Sender<WALCMD>,
    #[allow(dead_code)]
    wal_receiver_count: usize, // We can't clone the receiver, so just track the count
}

impl FuguServer {
    pub fn new(path: PathBuf) -> Self {
        // Create channel for WAL commands
        let (tx, mut rx): (mpsc::Sender<WALCMD>, mpsc::Receiver<WALCMD>) = mpsc::channel(1000);
        
        // Store the receiver in a static variable to allow Clone implementation
        let wal = WAL::open(path.clone());
        
        // Spawn a task to process WAL messages with proper shutdown handling
        let _sender_clone = tx.clone();
        let path_clone = path.clone();
        let (_shutdown_tx, mut shutdown_rx) = mpsc::channel::<bool>(1);
        
        tokio::spawn(async move {
            let mut wal = WAL::open(path_clone.clone());
            let stop = false;
            
            loop {
                tokio::select! {
                    // Process WAL message
                    Some(msg) = rx.recv() => {
                        match msg {
                            WALCMD::Put { .. } | WALCMD::Delete { .. } | WALCMD::Patch { .. } => {
                                match wal.push(msg.into()) {
                                    Ok(_) => {}
                                    Err(e) => { eprintln!("WAL push error: {:?}", e); }
                                }
                            }
                            WALCMD::DumpWAL { response } => {
                                if let Ok(dump) = wal.dump() {
                                    let _ = response.send(dump);
                                }
                            }
                        }
                    },
                    // Shutdown message
                    Some(_) = shutdown_rx.recv() => {
                        println!("WAL processor received shutdown signal");
                        break;
                    },
                    // Channel closed (server dropped)
                    else => {
                        println!("WAL processor channel closed");
                        break;
                    }
                }
                
                if stop {
                    break;
                }
            }
            println!("WAL processor shutdown complete");
        });
        
        Self {
            path,
            wal,
            stop: false,
            wal_sender: tx,
            wal_receiver_count: 1,
        }
    }
    pub fn get_wal_sender(&self) -> mpsc::Sender<WALCMD> {
        self.wal_sender.clone()
    }
    async fn dump_wal(&self) -> Result<String, rkyv::rancor::Error> {
        Ok(self.wal.dump()?)
    }

    // The wal_listen logic is now handled by the tokio task spawned in new()
    
    pub async fn up(&mut self) {
        // Just wait for shutdown, real processing is done in the spawned task
        while !self.stop {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
    pub async fn down(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Shutting down server and saving all data");
        self.stop = true;
        
        // Send shutdown signal to WAL processor task with timeout
        if let Err(_) = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            self.wal_sender.send(WALCMD::DumpWAL { 
                response: tokio::sync::oneshot::channel().0 
            })
        ).await {
            println!("Warning: Timeout sending final WAL command");
        }
        
        // The actual flushing of data is handled by the node's unload_index method
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
            let node = node::new(format!("node_{}/", i), None, sender.clone());
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
