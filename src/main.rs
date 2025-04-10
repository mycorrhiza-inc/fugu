use rkyv::rancor::Error;
use tokio::sync::oneshot;
use tokio::signal;

mod cmd;
mod fugu;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Set up a channel to signal shutdown
    let (tx, _rx) = oneshot::channel::<()>();
    
    // Handle Ctrl-C signal
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl-C");
        println!("\nReceived Ctrl-C, shutting down...");
        let _ = tx.send(());
    });
    
    // Start the command processing
    cmd::start().await;
    
    Ok(())
}
