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

#[derive(Parser)]
#[command(name = "search", about = "search for content")]
pub struct SearchCommand {
    /// Search query
    // #[arg(last = true)]
    pub query: String,
    
    /// Namespace to search in (required)
    #[arg(short, long, required = true)]
    pub namespace: String,
    
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
    /// Namespace to operate on (required)
    pub namespace: String,
    
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
    
    /// Port to use for the server
    #[arg(short, long, default_value = "50051")]
    pub port: u16,
    
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
    /// List all documents in the namespace
    List(NamespaceListCommand),
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
#[command(name = "add", about = "Add a file to a namespace and update its index")]
pub struct AddCommand {
    /// Namespace to add the file to
    #[arg(short, long, required = true)]
    pub namespace: String,
    
    /// File path to add
    pub file_path: String,
    
    /// Server address
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    pub addr: String,
}

#[derive(Parser)]
#[command(name = "search")]
pub struct NamespaceSearchCommand {
    /// Search query
    #[arg(last = true)]
    pub query: String,
    
    /// Namespace to search in (required)
    #[arg(short, long, required = true)]
    pub namespace: String,
    
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
    /// File to delete
    pub file: String,
    
    /// Server address
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    pub addr: String,
}

#[derive(Parser)]
#[command(name = "list", about = "List all documents in the namespace")]
pub struct NamespaceListCommand {
    /// Maximum results to return
    #[arg(short, long, default_value = "100")]
    pub limit: u32,
    
    /// Results offset
    #[arg(short, long, default_value = "0")]
    pub offset: u32,
    
    /// Server address
    #[arg(short, long, default_value = "http://127.0.0.1:50051")]
    pub addr: String,
    
    /// List all available namespaces
    #[arg(short, long)]
    pub all_namespaces: bool,
}
