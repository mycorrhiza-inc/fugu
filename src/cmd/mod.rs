use clap::{Parser, Subcommand};
use daemonize::Daemonize;
use std::fs::{File, OpenOptions};
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

    /// Gracefully shuts down the fugu process
    Down(commands::DownCommand),

    /// Starts the parent fugu process
    Up(commands::UpCommand),
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Namespace to operate on
    namespace: Option<String>,

    /// Sets a custom config file
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

pub async fn start() {
    let cli = Cli::parse();

    // Handle global namespace first
    if let Some(namespace) = cli.namespace.as_ref() {
        println!("operating on namespace: {namespace}");
        println!("Nothing to do. Exiting");
        return;
    }

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
        Some(Commands::Up(args)) => {
            // Path for this instance - in a real app, this might come from config
            let path = PathBuf::from("./data");

            // Default gRPC server address
            let addr = "0.0.0.0:50051".to_string();

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
                    .user("nobody")
                    .group("daemon")
                    .stdout(stdout)
                    .stderr(stderr);

                match daemonize.start() {
                    Ok(_) => {
                        // We're now in the daemon process
                        // Set up a runtime for the daemon
                        let rt = tokio::runtime::Runtime::new().unwrap();
                        rt.block_on(async {
                            if let Err(e) = crate::fugu::grpc::start_grpc_server(path, addr, None).await {
                                eprintln!("Server error: {}", e);
                            }
                        });
                    }
                    Err(e) => eprintln!("Error starting daemon: {}", e),
                }
            } else {
                println!("Starting the fugu node in foreground...");

                // Create a channel to signal when the server is ready
                let (tx, rx) = tokio::sync::oneshot::channel();

                // Spawn in a background task so it doesn't block
                let server_handle = tokio::spawn(async move {
                    if let Err(e) = crate::fugu::grpc::start_grpc_server(path, addr, Some(tx)).await {
                        eprintln!("Server error: {}", e);
                    }
                });

                // Wait for the server to signal it's ready
                let _ = rx.await;
                
                // We don't await the handle here, as we want the server to run in the background
                println!("Fugu node started successfully in foreground mode");
                println!("Press Ctrl-C to terminate the server");

                // Block the main thread to keep the program running until Ctrl-C
                tokio::signal::ctrl_c()
                    .await
                    .expect("Failed to listen for ctrl-c event");
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
