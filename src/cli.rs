use std::time::{SystemTime, UNIX_EPOCH};

use crate::tracing_utils;
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde_json::{Value, json};
use std::error::Error;
use tracing::{Instrument, debug, info};

// Base CLI struct
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Server URL
    #[arg(short, long, default_value = "http://localhost:3301")]
    server: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Namespace operations
    Namespace {
        #[command(subcommand)]
        action: NamespaceActions,
    },

    /// Create a dummy item with a demo ObjectIndex
    DummyItem {
        /// Optional custom ID for the dummy item
        #[arg(short, long)]
        id: Option<String>,

        /// Optional suffix to append to the dummy item ID
        #[arg()]
        suffix: Option<String>,
    },

    /// Get an object by ID
    GetObject {
        /// ID of the object to retrieve
        #[arg(required = true)]
        id: String,
    },
}

#[derive(Subcommand)]
enum NamespaceActions {
    /// List all namespaces
    List,

    /// Add a new namespace
    Add {
        /// Name of the namespace
        #[arg(required = true)]
        name: String,
    },

    /// Delete a namespace
    Delete {
        /// Name of the namespace
        #[arg(required = true)]
        name: String,
    },

    /// Get filters for a namespace
    Filters {
        /// Name of the namespace
        #[arg(required = true)]
        name: String,
    },

    /// Search within a namespace
    Search {
        /// Name of the namespace
        #[arg(required = true)]
        name: String,

        /// Search query
        #[arg(short, long, required = true)]
        query: String,
    },

    /// Add a file to a namespace
    AddFile {
        /// Name of the namespace
        #[arg(required = true)]
        name: String,

        /// File name
        #[arg(short, long, required = true)]
        file_name: String,

        /// File content
        #[arg(short, long, required = true)]
        content: String,
    },
}

/// Main CLI execution function
pub async fn run_cli() -> Result<(), Box<dyn Error>> {
    // Create a main CLI span
    let cli_main_span = tracing::span!(tracing::Level::INFO, "cli_main");

    async {
        debug!("CLI execution started");
        let cli = Cli::parse();
        let client = Client::new();

        debug!("Using server URL: {}", cli.server);

        match &cli.command {
            Commands::Namespace { action } => {
                let namespace_span = tracing::span!(tracing::Level::INFO, "namespace_command");

                async {
                    debug!("Executing namespace command");
                    match action {
                        NamespaceActions::List => {
                            debug!("List namespaces action");
                            list_namespaces(&client, &cli.server).await?;
                        }
                        NamespaceActions::Add { name } => {
                            debug!("Add namespace action: {}", name);
                            add_namespace(&client, &cli.server, name).await?;
                        }
                        NamespaceActions::Delete { name } => {
                            debug!("Delete namespace action: {}", name);
                            delete_namespace(&client, &cli.server, name).await?;
                        }
                        NamespaceActions::Filters { name } => {
                            debug!("Get filters action for namespace: {}", name);
                            get_namespace_filters(&client, &cli.server, name).await?;
                        }
                        NamespaceActions::Search { name, query } => {
                            debug!("Search action in namespace: {} with query: {}", name, query);
                            search_namespace(&client, &cli.server, name, query).await?;
                        }
                        NamespaceActions::AddFile {
                            name,
                            file_name,
                            content,
                        } => {
                            debug!(
                                "Add file action to namespace: {}, file: {}",
                                name, file_name
                            );
                            add_file(&client, &cli.server, name, file_name, content).await?;
                        }
                    }

                    Ok::<(), Box<dyn Error>>(())
                }
                .instrument(namespace_span)
                .await?
            }
            Commands::DummyItem { id, suffix } => {
                let dummy_span = tracing::span!(tracing::Level::INFO, "dummy_item_command");

                async {
                    if let Some(custom_id) = id {
                        info!("Executing dummy item command with custom ID: {}", custom_id);
                        println!("Using custom ID: {}", custom_id);
                    } else {
                        info!("Executing dummy item command with default ID");
                        println!("Using default ID: dummy_item");
                    }

                    if let Some(s) = &suffix {
                        info!("Using suffix: {}", s);
                        println!("Using suffix: {}", s);
                    }

                    create_dummy_item(&client, &cli.server, id, suffix).await?;
                    Ok::<(), Box<dyn Error>>(())
                }
                .instrument(dummy_span)
                .await?
            }
            Commands::GetObject { id } => {
                let get_span = tracing::span!(tracing::Level::INFO, "get_object_command");

                async {
                    info!("Executing get object command for ID: {}", id);
                    get_object(&client, &cli.server, id).await?;
                    Ok::<(), Box<dyn Error>>(())
                }
                .instrument(get_span)
                .await?
            }
        }

        info!("CLI execution completed successfully");
        Ok(())
    }
    .instrument(cli_main_span)
    .await
}

async fn list_namespaces(client: &Client, server: &str) -> Result<(), Box<dyn Error>> {
    let span = tracing_utils::cli_span("list_namespaces");

    async {
        debug!("Sending request to list namespaces");
        let response = match client.get(format!("{}/namespaces", server)).send().await {
            Ok(resp) => resp,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        let json_response = match response.json::<Value>().await {
            Ok(json) => json,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        debug!("Response received from server");
        match serde_json::to_string_pretty(&json_response) {
            Ok(pretty_json) => println!("{}", pretty_json),
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        }
        Ok(())
    }
    .instrument(span)
    .await
}

async fn add_namespace(client: &Client, server: &str, name: &str) -> Result<(), Box<dyn Error>> {
    let span = tracing_utils::cli_span("add_namespace");

    async {
        // This is currently not implemented in the server API based on what we've seen
        // For now, we'll create a placeholder implementation
        println!(
            "Note: The server API doesn't currently have a direct endpoint for adding namespaces."
        );
        println!("This operation may not be fully supported by the server.");

        debug!("Sending request to add namespace: {}", name);
        // This is a placeholder implementation that assumes the server will eventually support this
        let response = match client
            .post(format!("{}/namespaces", server))
            .json(&json!({"name": name}))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        let json_response = match response.json::<Value>().await {
            Ok(json) => json,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        debug!("Response received from server");
        match serde_json::to_string_pretty(&json_response) {
            Ok(pretty_json) => println!("{}", pretty_json),
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        }
        Ok(())
    }
    .instrument(span)
    .await
}

async fn delete_namespace(client: &Client, server: &str, name: &str) -> Result<(), Box<dyn Error>> {
    let span = tracing_utils::cli_span("delete_namespace");

    async {
        // This is currently not implemented in the server API based on what we've seen
        // For now, we'll create a placeholder implementation
        println!(
            "Note: The server API doesn't currently have a direct endpoint for deleting namespaces."
        );
        println!("This operation may not be fully supported by the server.");

        debug!("Sending request to delete namespace: {}", name);
        // This is a placeholder implementation that assumes the server will eventually support this
        let response = match client
            .delete(format!("{}/namespaces/{}", server, name))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        let json_response = match response.json::<Value>().await {
            Ok(json) => json,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        debug!("Response received from server");
        match serde_json::to_string_pretty(&json_response) {
            Ok(pretty_json) => println!("{}", pretty_json),
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        }
        Ok(())
    }
    .instrument(span)
    .await
}

async fn get_namespace_filters(
    client: &Client,
    server: &str,
    name: &str,
) -> Result<(), Box<dyn Error>> {
    let span = tracing_utils::cli_span("get_namespace_filters");

    async {
        debug!("Sending request to get filters for namespace: {}", name);
        let response = match client
            .get(format!("{}/filters/{}", server, name))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        let json_response = match response.json::<Value>().await {
            Ok(json) => json,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        debug!("Response received from server");
        match serde_json::to_string_pretty(&json_response) {
            Ok(pretty_json) => println!("{}", pretty_json),
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        }
        Ok(())
    }
    .instrument(span)
    .await
}

async fn search_namespace(
    client: &Client,
    server: &str,
    name: &str,
    query: &str,
) -> Result<(), Box<dyn Error>> {
    let span = tracing_utils::cli_span("search_namespace");

    async {
        debug!(
            "Sending request to search namespace: {} with query: {}",
            name, query
        );
        let response = match client
            .post(format!("{}/search/{}", server, name))
            .json(&json!({"query": query}))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        let json_response = match response.json::<Value>().await {
            Ok(json) => json,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        debug!("Response received from server");
        match serde_json::to_string_pretty(&json_response) {
            Ok(pretty_json) => println!("{}", pretty_json),
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        }
        Ok(())
    }
    .instrument(span)
    .await
}

async fn add_file(
    client: &Client,
    server: &str,
    namespace: &str,
    file_name: &str,
    content: &str,
) -> Result<(), Box<dyn Error>> {
    let span = tracing_utils::cli_span("add_file");

    async {
        debug!(
            "Sending request to add file: {} to namespace: {}",
            file_name, namespace
        );
        let response = match client
            .post(format!("{}/add/{}", server, namespace))
            .json(&json!({
                "name": file_name,
                "body": content
            }))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        let json_response = match response.json::<Value>().await {
            Ok(json) => json,
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        };

        debug!("Response received from server");
        match serde_json::to_string_pretty(&json_response) {
            Ok(pretty_json) => println!("{}", pretty_json),
            Err(e) => return Err(Box::new(e) as Box<dyn Error>),
        }
        Ok(())
    }
    .instrument(span)
    .await
}

/// Create a dummy item with a demo ObjectIndex
async fn create_dummy_item(
    client: &Client,
    server: &str,
    id: &Option<String>,
    suffix: &Option<String>,
) -> Result<(), Box<dyn Error>> {
    let span = tracing_utils::cli_span("create_dummy_item");

    async {
        // Check if server is available before sending main request
        info!("Checking server availability at {}", server);
        println!("Checking if server is available at: {}", server);

        let health_check = client
            .get(format!("{}/health", server))
            .send()
            .await;

        match health_check {
            Err(e) => {
                let error_msg = format!("Failed to connect to server at {}: {}", server, e);
                info!("Server connection error: {}", e);
                println!("Error: {}", error_msg);
                println!("The server may not be running. Please start the server with 'cargo run' before executing CLI commands.");
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, error_msg)) as Box<dyn Error>);
            },
            Ok(response) if !response.status().is_success() => {
                let error_msg = format!("Server at {} is not healthy (status code: {})", server, response.status());
                info!("Server health check failed with status: {}", response.status());
                println!("Error: {}", error_msg);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_msg)) as Box<dyn Error>);
            },
            Ok(response) => {
                info!("Server is available and healthy");
                println!("Server is available and healthy. Response: {}", response.status());
            }
        }
        let start = SystemTime::now();
        let timestamp = start.duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();


        let object_id = if let Some(custom_id) = id {
            custom_id.clone()
        } else if let Some(sfx) = suffix {
            format!("dummy_item_{}_{}", timestamp, sfx)
        } else {
            format!("dummy_item_{}", timestamp)
        };

        info!("Creating dummy item with ID: {}", object_id);
        debug!("Sending request to create dummy item with ID: {}", object_id);

        // Send request to the server endpoint
        let response = match client
            .post(format!("{}/demo-index", server))
            .json(&json!({
                "id": object_id
            }))
            .send()
            .await {
                Ok(resp) => resp,
                Err(e) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn Error>)
            };

        let json_response = match response.json::<Value>().await {
            Ok(json) => json,
            Err(e) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn Error>)
        };

        debug!("Response received from server");
        match serde_json::to_string_pretty(&json_response) {
            Ok(pretty_json) => println!("{}", pretty_json),
            Err(e) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn Error>)
        }
        info!("Successfully created dummy item with ID: {}", object_id);
        println!("Successfully created dummy item with demo ObjectIndex");


        Ok(())
    }
    .instrument(span)
    .await
    .map_err(|e: Box<dyn Error>| e)
}

/// Get an object by ID from the server
async fn get_object(client: &Client, server: &str, object_id: &str) -> Result<(), Box<dyn Error>> {
    let span = tracing_utils::cli_span("get_object");

    async {
        // Check if server is available before sending main request
        debug!("Checking server availability at {}", server);

        let health_check = client
            .get(format!("{}/health", server))
            .send()
            .await;

        match health_check {
            Err(e) => {
                let error_msg = format!("Failed to connect to server at {}: {}", server, e);
                info!("Server connection error: {}", e);
                println!("Error: {}", error_msg);
                println!("The server may not be running. Please start the server with 'cargo run' before executing CLI commands.");
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, error_msg)) as Box<dyn Error>);
            },
            Ok(response) if !response.status().is_success() => {
                let error_msg = format!("Server at {} is not healthy (status code: {})", server, response.status());
                info!("Server health check failed with status: {}", response.status());
                println!("Error: {}", error_msg);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, error_msg)) as Box<dyn Error>);
            },
            Ok(_) => {
                debug!("Server is available and healthy");
            }
        }

        info!("Retrieving object with ID: {}", object_id);
        debug!("Sending request to get object with ID: {}", object_id);

        // Send request to the server endpoint
        let response = match client
            .get(format!("{}/objects/{}", server, object_id))
            .send()
            .await {
                Ok(resp) => resp,
                Err(e) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn Error>)
            };

        let json_response = match response.json::<Value>().await {
            Ok(json) => json,
            Err(e) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn Error>)
        };

        debug!("Response received from server");
        match serde_json::to_string_pretty(&json_response) {
            Ok(pretty_json) => println!("{}", pretty_json),
            Err(e) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) as Box<dyn Error>)
        }

        // Check if there was an error in the response
        if json_response.get("error").is_some() {
            let error_msg = json_response.get("error").and_then(|e| e.as_str()).unwrap_or("Unknown error");
            info!("Failed to get object: {}", error_msg);
            println!("Failed to get object: {}", error_msg);
        } else {
            info!("Successfully retrieved object with ID: {}", object_id);
        }

        Ok(())
    }
    .instrument(span)
    .await
    .map_err(|e: Box<dyn Error>| e)
}
