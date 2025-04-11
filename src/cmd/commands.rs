use clap::{Parser, Subcommand};

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
    
    /// Path to the PID file (for daemon mode)
    #[arg(long, default_value = "/tmp/fugu.pid")]
    pub pid_file: String,
}

#[derive(Parser)]
#[command(name = "down")]
pub struct DownCommand {
    /// Forcefully shuts down the fugu server
    #[arg(short, long)]
    pub force: bool,
    
    /// Path to the PID file (for daemon mode)
    #[arg(long, default_value = "/tmp/fugu.pid")]
    pub pid_file: String,
}

#[derive(Parser)]
#[command(name = "up")]
pub struct UpCommand {
    /// Start with start the fugu server
    #[arg(short, long)]
    pub config: bool,
    
    /// Run server as a daemon in the background
    #[arg(short, long)]
    pub daemon: bool,
    
    /// Path to the PID file when running as a daemon
    #[arg(long, default_value = "/tmp/fugu.pid")]
    pub pid_file: String,
    
    /// Path to the log file when running as a daemon
    #[arg(long, default_value = "/tmp/fugu.log")]
    pub log_file: String,
    
    /// Timeout in seconds before server automatically exits (useful for tests)
    #[arg(long)]
    pub timeout: Option<u64>,
}

// NAMESPACES
#[derive(Parser)]
#[command(
    name = "namespace",
    about = "Load fugu instance at the given namespace"
)]
pub struct NamespaceCommand {
    /// Namespace to operate on
    pub namespace: String,

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

#[derive(Parser)]
#[command(name = "index")]
pub struct NamespaceIndexCommand {
    /// File to index
    #[arg(short, long, required = true)]
    pub file: String,
    
    /// Server address
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    pub addr: String,
}

#[derive(Parser)]
#[command(name = "search")]
pub struct NamespaceSearchCommand {
    /// Search query
    #[arg(short, long, required = true)]
    pub query: String,
    
    /// Maximum results to return
    #[arg(short, long, default_value = "10")]
    pub limit: u32,
    
    /// Results offset
    #[arg(short, long, default_value = "0")]
    pub offset: u32,
    
    /// Server address
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    pub addr: String,
}

#[derive(Parser)]
#[command(name = "delete")]
pub struct NamespaceDeleteCommand {
    /// Location to delete
    #[arg(short, long, required = true)]
    pub location: String,
    
    /// Server address
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    pub addr: String,
}
