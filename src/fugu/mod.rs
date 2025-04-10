use std::path::PathBuf;

pub mod server;
use server::FuguServer;

pub fn new(path: PathBuf) -> FuguServer {
    FuguServer::new(path)
}

pub mod node;
mod test;
pub mod wal;
pub mod index;
pub mod grpc;
#[cfg(test)]
mod test_grpc;
#[cfg(test)]
mod test_client;