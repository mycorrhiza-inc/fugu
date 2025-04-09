use clap::{value_parser, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "init", about = "initializes the fugu service")]
pub struct InitCommand {
    /// config path to initialize with
    // #[arg(short = 'c', long = "config", required = false, value_parser = value_parser!(PathBuf))]
    // pub config: PathBuf,

    /// forced reindexing
    #[arg(short = 'f', long = "force", required = false)]
    pub force: bool,
}

// #[derive(Parser)]
// #[command(name = "config")]
// pub struct ConfigCommand {
//     /// Alternate config file path
//     #[arg(short, long, value_name = "FILE")]
//     pub path: Option<PathBuf>,
// }

#[derive(Parser)]
#[command(name = "status")]
pub struct StatusCommand {
    /// Namespace to operate on
    pub namespace: Option<String>,
}

#[derive(Parser)]
#[command(name = "down")]
pub struct DownCommand {
    /// Forcefully shuts down the fugu server
    #[arg(short, long)]
    pub force: bool,
}

#[derive(Parser)]
#[command(name = "up")]
pub struct UpCommand {
    /// Start with start the fugu server
    #[arg(short, long)]
    pub config: bool,
}

// NAMESPACES
#[derive(Parser)]
#[command(
    name = "namespace",
    about = "Load fugu instance at the given namespace"
)]
pub struct NamespaceCommand {
    /// Namespace to operate on
    #[arg(required = true)]
    pub namespace: Option<String>,

    /// Check the status of the node at a given namespace
    #[arg(short, long)]
    pub status: bool,

    /// Rebuild the index at a given namespace
    #[arg(short, long)]
    pub reindex: bool,

    /// Initialize a given namespace
    #[arg(short, long)]
    pub init: bool,

    #[command(subcommand)]
    pub command: Option<NamespaceSubcommands>,
}

#[derive(Parser)]
#[command(name = "down")]
pub struct NamespaceDownCommand {
    /// Force shutdown the namespace
    #[arg(short, long)]
    pub force: bool,
}

#[derive(Parser)]
#[command(name = "up")]
pub struct NamespaceUpCommand {
    /// Start with specific configuration
    pub namespace: String,

    /// Config for the server
    #[arg(short, long)]
    pub config: bool,
}

#[derive(Subcommand)]
pub enum NamespaceSubcommands {
    /// Start the namespace
    Up(NamespaceUpCommand),
    /// Stop the namespace
    Down(NamespaceDownCommand),
}
