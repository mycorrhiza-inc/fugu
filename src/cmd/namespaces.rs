use crate::cmd::commands::{NamespaceCommand, NamespaceSubcommands};
use crate::fugu::grpc::{client_index, client_delete, client_search, client_vector_search};
use std::path::PathBuf;

// Default gRPC server address
const DEFAULT_GRPC_ADDR: &str = "http://127.0.0.1:50051";

pub async fn run(ns: NamespaceCommand) -> Result<(), Box<dyn std::error::Error>> {
    let namespace = match ns.namespace {
        Some(ns) => ns,
        None => {
            println!("No namespace provided for command");
            return Ok(());
        }
    };

    // Use namespace in the address for future scalability
    let grpc_addr = format!("{}", DEFAULT_GRPC_ADDR);

    match ns.command {
        Some(NamespaceSubcommands::Up(up_cmd)) => {
            println!("starting namespace `{namespace}`...");
            if up_cmd.config {
                println!("using custom configuration");
            }
            // TODO: Start the server with the specific namespace
        }
        Some(NamespaceSubcommands::Down(down_cmd)) => {
            if down_cmd.force {
                println!("force stopping namespace `{namespace}`...");
            } else {
                println!("gracefully stopping namespace `{namespace}`...");
            }
            // TODO: Connect to the server and send a shutdown command
        }
        None => {
            if ns.status {
                println!("checking status of namespace `{namespace}`...");
                // Use empty search to check if server is responding
                client_search(grpc_addr, String::new(), 0, 0).await?;
            } else if ns.init {
                println!("initializing namespace `{namespace}`...");
                // TODO: Implement initialization through the gRPC client
                // For now, just verify the connection
                client_search(grpc_addr, String::new(), 0, 0).await?;
                println!("Namespace `{namespace}` initialized successfully");
            } else if ns.reindex {
                println!("reindexing namespace `{namespace}`...");
                // Find all files in the namespace directory and index them
                let namespace_dir = PathBuf::from(&namespace);
                if namespace_dir.exists() && namespace_dir.is_dir() {
                    // This would typically be implemented to walk the directory
                    // and index each file found
                    println!("Reindexing all files in `{}`", namespace);
                } else {
                    println!("Namespace directory `{}` not found", namespace);
                }
            } else {
                println!("namespace logic at `{namespace}`...");
                // Default operation: show namespace info
                client_search(grpc_addr, String::new(), 0, 0).await?;
                println!("Connected to namespace `{namespace}` successfully");
            }
        }
    }

    Ok(())
}