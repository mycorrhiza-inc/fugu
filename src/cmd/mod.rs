use clap::{Parser, Subcommand};
use daemonize::Daemonize;
use std::fs::OpenOptions;
use std::path::PathBuf;

pub mod commands;
mod init;
mod namespaces;
pub mod util;

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new fugu instance
    Init(commands::InitCommand),

    /// Status of the fugu instance at the given namespace
    Status(commands::StatusCommand),

    /// Namespace operations
    Namespace(commands::NamespaceCommand),

    /// Index a document in a namespace
    Index(commands::NamespaceIndexCommand),

    /// Legacy search command
    NamespaceSearch(commands::NamespaceSearchCommand),
    
    /// Direct search command
    Search(commands::SearchCommand),

    /// Delete a document from a namespace
    Delete(commands::NamespaceDeleteCommand),

    /// Add a file to a namespace and update its index
    Add(commands::AddCommand),

    /// Gracefully shuts down the fugu process
    Down(commands::DownCommand),

    /// Starts the parent fugu process
    Up(commands::UpCommand),
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Namespace to operate on
    #[arg(global = true)]
    namespace: Option<String>,

    /// Sets a custom config file
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

pub async fn start() {
    // Initialize logging
    util::init_logging();
    
    let cli = Cli::parse();

    // Handle subcommands
    match cli.command {
        Some(Commands::Init(args)) => {
            println!("in init command");
            let _ = init::run(args).await;
            ()
        }
        Some(Commands::Namespace(args)) => {
            let _ = namespaces::run(args).await;
        }
        Some(Commands::Index(args)) => {
            // Handle index command with default namespace if none provided
            let namespace = cli.namespace.unwrap_or_else(|| "default".to_string());
            let _ = namespaces::handle_index_command(args, &namespace).await;
        }
        Some(Commands::NamespaceSearch(args)) => {
            // Use namespace from args first, then from global flag, defaulting to "default"
            let namespace = args.namespace.clone().unwrap_or_else(|| {
                cli.namespace.clone().unwrap_or_else(|| "default".to_string())
            });
            let _ = namespaces::handle_search_command(args, &namespace).await;
        }
        Some(Commands::Search(args)) => {
            // Use namespace from args first, then from global flag, defaulting to "default"
            let namespace = args.namespace.clone().unwrap_or_else(|| {
                cli.namespace.clone().unwrap_or_else(|| "default".to_string())
            });
            // Convert the new search command to the existing structure to reuse logic
            let search_args = commands::NamespaceSearchCommand {
                query: args.query,
                namespace: Some(namespace.clone()),
                limit: args.limit,
                offset: args.offset,
                addr: args.addr,
            };
            let _ = namespaces::handle_search_command(search_args, &namespace).await;
        }
        Some(Commands::Delete(args)) => {
            // Handle delete command with default namespace if none provided
            let namespace = cli.namespace.unwrap_or_else(|| "default".to_string());
            let _ = namespaces::handle_delete_command(args, &namespace).await;
        }
        Some(Commands::Add(args)) => {
            println!("Adding file to namespace `{}`...", args.namespace);
            
            // Check file size to determine if we should use streaming
            let file_path = std::path::PathBuf::from(&args.file_path);
            if let Ok(file_metadata) = std::fs::metadata(&file_path) {
                let file_size = file_metadata.len();
                
                // Define threshold for large files (10MB)
                const LARGE_FILE_THRESHOLD: u64 = 10 * 1024 * 1024;
                
                if file_size > LARGE_FILE_THRESHOLD {
                    println!("File is large ({} bytes). Using streaming for better performance.", file_size);
                    let _ = crate::fugu::grpc::client_stream_index(
                        args.addr, 
                        args.file_path, 
                        Some(args.namespace),
                        None  // Use default chunk size
                    ).await;
                } else {
                    // For smaller files, convert to a namespace index command to reuse logic
                    let index_args = commands::NamespaceIndexCommand {
                        file: args.file_path,
                        addr: args.addr,
                    };
                    let _ = namespaces::handle_index_command(index_args, &args.namespace).await;
                }
            } else {
                // If we can't get file size, fall back to regular indexing
                let index_args = commands::NamespaceIndexCommand {
                    file: args.file_path,
                    addr: args.addr,
                };
                let _ = namespaces::handle_index_command(index_args, &args.namespace).await;
            }
        }
        Some(Commands::Up(args)) => {
            // Use the ConfigManager to get the proper path
            let config_manager = crate::fugu::config::new_config_manager(None);
            let path = config_manager.base_dir().to_path_buf();

            // Use the port parameter to build the server address
            let addr = format!("127.0.0.1:{}", args.port);

            if args.daemon {
                println!("Starting the fugu node as a daemon...");

                // Set up log file
                let stdout = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&args.log_file)
                    .unwrap();

                let stderr = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&args.log_file)
                    .unwrap();

                // Set up the daemon
                let daemonize = Daemonize::new()
                    .pid_file(&args.pid_file)
                    .working_directory(".")
                    // Removed user and group settings that were causing issues
                    .stdout(stdout)
                    .stderr(stderr);

                match daemonize.start() {
                    Ok(_) => {
                        // We're now in the daemon process
                        // Create a new PID file after daemonizing
                        // This ensures we don't inherit a stale file descriptor
                        let _ = std::fs::remove_file(&args.pid_file);
                        let pid = std::process::id();
                        let _ = std::fs::write(&args.pid_file, pid.to_string());
                        
                        // Use a single-threaded runtime to avoid file descriptor issues
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .unwrap();
                            
                        rt.block_on(async {
                            // Create channels for shutdown
                            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                            
                            // Spawn a task to handle termination signals (SIGTERM)
                            let shutdown_handle = tokio::spawn(async move {
                                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                                    .expect("Failed to install SIGTERM handler")
                                    .recv()
                                    .await;
                                eprintln!("Received SIGTERM, shutting down...");
                                let _ = shutdown_tx.send(());
                            });
                            
                            // We need another channel pair for the interrupt signal
                            let (int_shutdown_tx, int_shutdown_rx) = tokio::sync::oneshot::channel();
                            
                            // Spawn a task to handle interrupt signals (SIGINT)
                            tokio::spawn(async move {
                                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                                    .expect("Failed to install SIGINT handler")
                                    .recv()
                                    .await;
                                eprintln!("Received SIGINT, shutting down...");
                                let _ = int_shutdown_tx.send(());
                                let _ = shutdown_handle.abort(); // Abort the SIGTERM handler
                            });
                            
                            // Combine both shutdown signals
                            let combined_shutdown = async {
                                tokio::select! {
                                    _ = shutdown_rx => {},
                                    _ = int_shutdown_rx => {},
                                }
                            };
                            
                            // Create a new oneshot channel for the combined signal
                            let (combined_tx, combined_rx) = tokio::sync::oneshot::channel();
                            
                            // Spawn a task to handle the combined shutdown signal
                            tokio::spawn(async move {
                                combined_shutdown.await;
                                let _ = combined_tx.send(());
                            });
                            
                            if let Err(e) =
                                crate::fugu::grpc::start_grpc_server(path, addr.clone(), None, Some(combined_rx)).await
                            {
                                eprintln!("Server error: {}", e);
                            }
                        });
                    }
                    Err(e) => eprintln!("Error starting daemon: {}", e),
                }
            } else {
                println!("Starting the fugu node in foreground...");

                // Create channels for ready signal and shutdown
                let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

                // Spawn in a background task so it doesn't block
                let _server_handle = tokio::spawn(async move {
                    if let Err(e) = crate::fugu::grpc::start_grpc_server(
                        path,
                        addr.clone(),
                        Some(ready_tx),
                        Some(shutdown_rx),
                    )
                    .await
                    {
                        eprintln!("Server error: {}", e);
                    }
                });

                // Wait for the server to signal it's ready
                let _ = ready_rx.await;

                // We don't await the handle here, as we want the server to run in the background
                println!("Fugu node started successfully in foreground mode");
                
                if let Some(timeout_secs) = args.timeout {
                    println!("Server will automatically exit after {} seconds", timeout_secs);
                    
                    // Create a timeout future
                    let timeout_fut = tokio::time::sleep(tokio::time::Duration::from_secs(timeout_secs));
                    
                    // Create a Ctrl-C future
                    let ctrl_c_fut = tokio::signal::ctrl_c();
                    
                    // Wait for either timeout or Ctrl-C
                    tokio::select! {
                        _ = timeout_fut => {
                            println!("Server timeout reached, shutting down...");
                        }
                        _ = ctrl_c_fut => {
                            println!("Received Ctrl-C, shutting down server...");
                        }
                    }
                } else {
                    println!("Press Ctrl-C to terminate the server");
                    
                    // Block the main thread to keep the program running until Ctrl-C
                    tokio::signal::ctrl_c()
                        .await
                        .expect("Failed to listen for ctrl-c event");
                        
                    println!("Received Ctrl-C, shutting down server...");
                }
                
                // Send shutdown signal to gracefully shutdown the server
                let _ = shutdown_tx.send(());

                // Give server a moment to clean up
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                println!("Server shutdown complete");
            }
        }
        Some(Commands::Down(args)) => {
            // Check if there's a PID file
            if std::path::Path::new(&args.pid_file).exists() {
                // Read the PID file
                match std::fs::read_to_string(&args.pid_file) {
                    Ok(pid_str) => {
                        match pid_str.trim().parse::<i32>() {
                            Ok(pid) => {
                                println!("Found daemon process with PID: {}", pid);

                                // Send signal to the process
                                #[cfg(unix)]
                                {
                                    use std::process::Command;

                                    let signal = if args.force { "SIGKILL" } else { "SIGTERM" };
                                    println!("Sending {} to PID {}", signal, pid);

                                    let output = Command::new("kill")
                                        .arg(if args.force { "-9" } else { "-15" })
                                        .arg(pid.to_string())
                                        .output();

                                    match output {
                                        Ok(output) => {
                                            if output.status.success() {
                                                println!("Signal sent successfully");
                                                // Remove the PID file
                                                if let Err(e) = std::fs::remove_file(&args.pid_file)
                                                {
                                                    eprintln!("Failed to remove PID file: {}", e);
                                                }
                                            } else {
                                                eprintln!("Failed to send signal: {:?}", output);
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to execute kill command: {}", e)
                                        }
                                    }
                                }

                                #[cfg(not(unix))]
                                {
                                    eprintln!("Daemon control is only supported on Unix systems");
                                }
                            }
                            Err(e) => eprintln!("Invalid PID in file: {}", e),
                        }
                    }
                    Err(e) => eprintln!("Failed to read PID file: {}", e),
                }
            } else if args.force {
                println!("No PID file found at {}, but force flag was set, trying to find and kill the process...",  args.force);
                // Here we could potentially try to find the process by name or port
                // For now, just print a message
                println!("Note: To find and kill the process manually, try:");
                println!("  ps aux | grep fugu");
                println!("  kill <PID>");
            } else {
                println!(
                    "No PID file found at {}. Is the daemon running?",
                    args.pid_file
                );
            }
        }
        Some(Commands::Status(args)) => {
            println!("Checking status of the fugu service...");

            // Check for daemon status
            if std::path::Path::new(&args.pid_file).exists() {
                match std::fs::read_to_string(&args.pid_file) {
                    Ok(pid_str) => {
                        match pid_str.trim().parse::<i32>() {
                            Ok(pid) => {
                                println!("Found daemon process with PID: {}", pid);

                                // Check if the process is still running
                                #[cfg(unix)]
                                {
                                    use std::process::Command;

                                    let output =
                                        Command::new("ps").arg("-p").arg(pid.to_string()).output();

                                    match output {
                                        Ok(output) => {
                                            if output.status.success()
                                                && String::from_utf8_lossy(&output.stdout)
                                                    .contains(&pid.to_string())
                                            {
                                                println!("Daemon is running (PID: {})", pid);
                                                println!("PID file: {}", args.pid_file);

                                                // Try to get more information about the server
                                                println!("\nServer details:");
                                                // In a real app, you might call the gRPC server for stats
                                                println!("  Default server address: 0.0.0.0:50051");
                                            } else {
                                                println!("Warning: PID file exists but process is not running");
                                                println!(
                                                    "You may want to remove the stale PID file: {}",
                                                    args.pid_file
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("Failed to check process status: {}", e)
                                        }
                                    }
                                }

                                #[cfg(not(unix))]
                                {
                                    println!(
                                        "Process status checking is only supported on Unix systems"
                                    );
                                    println!("PID file exists at: {}", args.pid_file);
                                }
                            }
                            Err(e) => eprintln!("Invalid PID in file: {}", e),
                        }
                    }
                    Err(e) => eprintln!("Failed to read PID file: {}", e),
                }
            } else {
                println!(
                    "No PID file found at {}. Daemon is not running.",
                    args.pid_file
                );

                // Check if there's any fugu process running
                #[cfg(unix)]
                {
                    use std::process::Command;

                    let output = Command::new("pgrep").arg("-f").arg("fugu").output();

                    match output {
                        Ok(output) => {
                            if output.status.success() {
                                let pids = String::from_utf8_lossy(&output.stdout);
                                println!("Found fugu processes running: {}", pids.trim());
                                println!(
                                    "But no PID file exists, these might be foreground instances."
                                );
                            } else {
                                println!("No fugu processes found. Service is not running.");
                            }
                        }
                        Err(e) => eprintln!("Failed to check for fugu processes: {}", e),
                    }
                }

                #[cfg(not(unix))]
                {
                    println!("Process status checking is only supported on Unix systems");
                }
            }
        }
        // Some(Commands::Config(args)) => {
        //     if let Some(path) = args.path {
        //         println!("Using config at: {}", path.display());
        //     }
        // }
        None => {
            println!("No subcommand provided");
        }
    }
}
