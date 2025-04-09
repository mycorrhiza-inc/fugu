use crate::cmd::commands::{NamespaceCommand, NamespaceSubcommands};

pub async fn run(ns: NamespaceCommand) -> Result<(), Box<dyn std::error::Error>> {
    let namespace = match ns.namespace {
        Some(ns) => ns,
        None => {
            println!("No namespace provided for command");
            return Ok(());
        }
    };

    match ns.command {
        Some(NamespaceSubcommands::Up(up_cmd)) => {
            println!("starting namespace `{namespace}`...");
            if up_cmd.config {
                println!("using custom configuration");
            }
            // TODO: Implement up logic
        }
        Some(NamespaceSubcommands::Down(down_cmd)) => {
            if down_cmd.force {
                println!("force stopping namespace `{namespace}`...");
            } else {
                println!("gracefully stopping namespace `{namespace}`...");
            }
            // TODO: Implement down logic
        }
        None => {
            if ns.status {
                println!("checking status of namespace `{namespace}`...");
                // TODO: Implement status check logic
                /*  RPC client stuff:
                    -- 
                 */ 
            } else if ns.init {
                println!("initializing namespace `{namespace}`...");
                // TODO: Implement init logic
                /*  RPC client stuff:
                    -- 
                 */ 
            } else if ns.reindex {
                println!("reindexing namespace `{namespace}`...");
                // TODO: Implement reindex logic
                /*  RPC client stuff:
                    - send a reindex 
                 */ 
            } else {
                println!("namespace logic at `{namespace}`...");
                // TODO: Implement initialization logic
                /*
                    -- 
                 */ 
            }
        }
    }

    Ok(())
}