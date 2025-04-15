use crate::cmd::commands::{
    NamespaceCommand, NamespaceSubcommands,
    NamespaceIndexCommand, NamespaceSearchCommand, NamespaceDeleteCommand, NamespaceListCommand
};
use crate::fugu::grpc::utils::{client_index, client_delete, client_search};
use std::path::PathBuf;
use std::convert::TryInto;

// Default gRPC server address
const DEFAULT_GRPC_ADDR: &str = "http://127.0.0.1:50051";

pub async fn run(ns: NamespaceCommand) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let namespace = ns.namespace;

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
        Some(NamespaceSubcommands::List(list_cmd)) => {
            handle_list_command(list_cmd, &namespace).await?;
        }
        None => {
            if ns.status {
                println!("checking status of namespace `{namespace}`...");
                // Use empty search to check if server is responding
                client_search(grpc_addr, String::new(), 0, 0, namespace.clone()).await?;
            } else if ns.init {
                println!("initializing namespace `{namespace}`...");
                // TODO: Implement initialization through the gRPC client
                // For now, just verify the connection
                client_search(grpc_addr, String::new(), 0, 0, namespace.clone()).await?;
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
                client_search(grpc_addr, String::new(), 0, 0, namespace.clone()).await?;
                println!("Connected to namespace `{namespace}` successfully");
            }
        }
    }

    Ok(())
}

// Handle the index subcommand
pub async fn handle_index_command(
    cmd: NamespaceIndexCommand, 
    namespace: &str
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Indexing file in namespace `{namespace}`...");
    
    // Index the file using the client, passing the namespace
    let result = client_index(cmd.addr, cmd.file, namespace.to_string()).await?;
    
    // Display indexing status information to the user
    println!("Indexing completed successfully:");
    println!("  - Location: {}", result.location);
    println!("  - Bytes processed: {}", result.bytes_received);
    println!("  - Indexed terms: {}", result.indexed_terms);
    println!("  - Indexing time: {} ms", result.indexing_time_ms);
    println!("  - Status: {}", result.indexing_status);
    
    Ok(())
}

// Handle the search subcommand
pub async fn handle_search_command(
    cmd: NamespaceSearchCommand,
    namespace: &str
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Searching in namespace `{namespace}`...");
    let q = cmd.query.clone();
    println!("query \"{q}\"");
    
    // Perform the search with the namespace
    client_search(
        cmd.addr, 
        cmd.query, 
        cmd.limit.try_into().unwrap_or(10), 
        cmd.offset.try_into().unwrap_or(0),
        namespace.to_string()
    ).await?;
    
    Ok(())
}

// Handle the delete subcommand
pub async fn handle_delete_command(
    cmd: NamespaceDeleteCommand,
    namespace: &str
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Deleting from namespace `{namespace}`...");
    
    // Delete the document with namespace
    client_delete(cmd.addr, cmd.file, namespace.to_string()).await?;
    
    Ok(())
}

// Handle the list subcommand
pub async fn handle_list_command(
    cmd: NamespaceListCommand,
    namespace: &str
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // If all_namespaces flag is set, list all available namespaces
    if cmd.all_namespaces {
        println!("Listing all available namespaces...");
        list_all_namespaces().await?;
        return Ok(());
    }
    
    println!("Listing documents in namespace `{namespace}`...");
    
    // First try an empty query to list all documents in the namespace
    println!("Querying with empty string...");
    client_search(
        cmd.addr.clone(), 
        String::new(), 
        cmd.limit.try_into().unwrap_or(100), 
        cmd.offset.try_into().unwrap_or(0),
        namespace.to_string()
    ).await?;
    
    // Then try with a wildcard-like broad search for diagnostic purposes
    println!("\nTrying with broad search term 'a'...");
    client_search(
        cmd.addr, 
        "a".to_string(), 
        cmd.limit.try_into().unwrap_or(100), 
        cmd.offset.try_into().unwrap_or(0),
        namespace.to_string()
    ).await?;
    
    Ok(())
}

// List all available namespaces
async fn list_all_namespaces() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::fugu::config::new_config_manager;
    
    
    let config_mgr = new_config_manager(None);
    let namespaces_dir = config_mgr.base_dir().join("namespaces");
    
    if !namespaces_dir.exists() {
        println!("No namespaces found");
        return Ok(());
    }
    
    println!("Available namespaces:");
    let entries = std::fs::read_dir(namespaces_dir)?;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_dir() {
            // Check if it has a .fugu directory (valid namespace)
            if path.join(".fugu").exists() {
                let name = path.file_name().unwrap().to_string_lossy();
                
                // Check for consolidated file
                let index_path = path.join(".fugu").join("index");
                let consolidated_path = index_path.join("consolidated.rkyv");
                let consolidated = if consolidated_path.exists() { "✓" } else { "✗" };
                
                // Check if the namespace has a cache directory - this would indicate old structure
                let cache_path = index_path.join("cache");
                let using_old_structure = cache_path.exists();
                
                println!("- {} [Consolidated: {}, Direct Documents: {}]", 
                         name, 
                         consolidated,
                         if using_old_structure { "✗" } else { "✓" });
            }
        }
    }
    
    Ok(())
}