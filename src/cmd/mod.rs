use clap::{Parser, Subcommand};

pub mod commands;
mod init;
mod namespaces;
pub mod util;

use std::path::PathBuf;

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
        Some(Commands::Up(_)) => {
            println!("starting the fugu node");
        }
        Some(Commands::Down(args)) => {
            if args.force {
                println!("Force shutting down fugu...");
            } else {
                println!("Gracefully shutting down fugu...");
            }
        }
        Some(Commands::Status(_)) => {
            println!("status of the fugu service")
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
