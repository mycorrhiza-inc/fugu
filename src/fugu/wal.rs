/// Write-Ahead Log (WAL) implementation for ensuring data durability
///
/// The WAL provides:
/// - Persistence of operations before they're applied to indexes
/// - Recovery capability in case of crashes
/// - Atomic operations for consistent state
/// - Fast, multithreaded append-only operations
/// - Namespace isolation with a dedicated WAL per namespace
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write, Error as IoError, ErrorKind};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicU64, Ordering}};
use tokio::sync::RwLock;
use std::time::Duration;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::sync::{Semaphore, oneshot};
use tokio::time::sleep;
use tracing::{info, warn, error};

/// Maximum number of concurrent writers to the WAL
const MAX_CONCURRENT_WRITERS: usize = 8;

/// Size of buffer for batching WAL operations (bytes)
const BUFFER_SIZE: usize = 1024 * 1024; // 1MB

/// How frequently to flush buffers (milliseconds)
const FLUSH_INTERVAL_MS: u64 = 100;

/// WAL operations that can be recorded in the log
///
/// These operations represent the possible mutations to the system:
/// - Put: Insert or update a key-value pair
/// - Delete: Remove a key-value pair
/// - Patch: Partially update a key-value pair
#[derive(Archive, Debug, Deserialize, Serialize, Clone)]
pub enum WALOP {
    /// Insert or update a key-value pair
    Put { key: String, value: Vec<u8> },
    /// Remove a key by its identifier
    Delete { key: String },
    /// Partially update an existing key-value pair
    Patch { key: String, value: Vec<u8> },
}

/// Commands that can be sent through the WAL channel
///
/// These commands represent both:
/// - Operations that modify data (Put, Delete, Patch)
/// - Management operations (DumpWAL, FlushWAL)
#[derive(Debug)]
pub enum WALCMD {
    /// Insert or update a key-value pair
    Put {
        /// The key to insert or update
        key: String,
        /// The value to store
        value: Vec<u8>,
        /// The namespace for this operation
        namespace: String,
    },
    /// Delete a key-value pair
    Delete {
        /// The key to delete
        key: String,
        /// The namespace for this operation
        namespace: String,
    },
    /// Partially update an existing key-value pair
    Patch {
        /// The key to update
        key: String,
        /// The updated value
        value: Vec<u8>,
        /// The namespace for this operation
        namespace: String,
    },
    /// Dump the WAL contents for debugging or inspection
    DumpWAL {
        /// Channel to send the WAL dump response - moved out of the enum
        response: oneshot::Sender<String>,
        /// Optional namespace to dump (None for all namespaces)
        namespace: Option<String>,
    },
    /// Flush a specific namespace's WAL to disk
    FlushWAL {
        /// The namespace to flush
        namespace: String,
        /// Channel to send the flush completion response - moved out of the enum
        response: Option<oneshot::Sender<io::Result<()>>>,
    },
}

/// Conversion from WAL commands to WAL operations
impl WALCMD {
    /// Converts a WAL command to a WAL operation
    ///
    /// # Returns
    ///
    /// The WAL operation with namespace metadata stripped
    #[allow(dead_code)]
    pub fn to_op(&self) -> Option<WALOP> {
        match self {
            WALCMD::Put { key, value, namespace: _ } => 
                Some(WALOP::Put { key: key.clone(), value: value.clone() }),
            WALCMD::Delete { key, namespace: _ } => 
                Some(WALOP::Delete { key: key.clone() }),
            WALCMD::Patch { key, value, namespace: _ } => 
                Some(WALOP::Patch { key: key.clone(), value: value.clone() }),
            WALCMD::DumpWAL { response: _, namespace: _ } => None,
            WALCMD::FlushWAL { namespace: _, response: _ } => None,
        }
    }
    
    /// Gets the namespace this command belongs to
    #[allow(dead_code)]
    pub fn namespace(&self) -> Option<&str> {
        match self {
            WALCMD::Put { namespace, .. } => Some(namespace),
            WALCMD::Delete { namespace, .. } => Some(namespace),
            WALCMD::Patch { namespace, .. } => Some(namespace),
            WALCMD::DumpWAL { namespace, .. } => namespace.as_deref(),
            WALCMD::FlushWAL { namespace, .. } => Some(namespace),
        }
    }
}

/// Result type for WAL operations 
type WALResult<T> = Result<T, io::Error>;

/// Represents a batch of operations to be written to the WAL
struct WALBatch {
    /// Operations in this batch
    operations: Vec<WALOP>,
    /// Size of the batch in bytes (cached for performance)
    size: usize,
}

impl WALBatch {
    /// Creates a new empty batch
    fn new() -> Self {
        Self {
            operations: Vec::new(),
            size: 0,
        }
    }

    /// Adds an operation to the batch
    fn add(&mut self, op: WALOP) -> usize {
        // Estimate the serialized size of the operation
        let estimated_size = match &op {
            WALOP::Put { key, value } => key.len() + value.len() + 16,
            WALOP::Delete { key } => key.len() + 8,
            WALOP::Patch { key, value } => key.len() + value.len() + 16,
        };

        self.operations.push(op);
        self.size += estimated_size;
        self.size
    }

    /// Serializes the batch for writing to disk
    fn serialize(&self) -> Result<Vec<u8>, io::Error> {
        match rkyv::to_bytes::<rkyv::rancor::Panic>(&self.operations) {
            Ok(aligned) => Ok(aligned.to_vec()),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, format!("Serialization error: {:?}", e)))
        }
    }

    /// Checks if the batch is empty
    fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Clears the batch
    #[allow(dead_code)]
    fn clear(&mut self) {
        self.operations.clear();
        self.size = 0;
    }
}

/// Write-Ahead Log (WAL) writer responsible for appending operations to the log file
struct WALWriter {
    /// Path to the WAL file
    path: PathBuf,
    /// Current batch of operations waiting to be written
    batch: Mutex<WALBatch>,
    /// Flag to indicate if a flush operation is in progress
    flushing: AtomicBool,
    /// Semaphore to limit concurrent write operations
    write_semaphore: Semaphore,
    /// Total number of operations processed
    ops_count: AtomicU64,
    /// Flag to signal shutdown
    shutdown: AtomicBool,
}

impl WALWriter {
    /// Creates a new WAL writer
    fn new(path: PathBuf) -> Self {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                let _ = fs::create_dir_all(parent);
            }
        }

        Self {
            path,
            batch: Mutex::new(WALBatch::new()),
            flushing: AtomicBool::new(false),
            write_semaphore: Semaphore::new(MAX_CONCURRENT_WRITERS),
            ops_count: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
        }
    }

    /// Adds an operation to the batch, triggering a flush if the batch is full
    async fn append(&self, op: WALOP) -> WALResult<()> {
        // Increment the operations counter
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        // Add the operation to the current batch
        let should_flush = {
            let mut batch = self.batch.lock().unwrap();
            let new_size = batch.add(op);
            new_size >= BUFFER_SIZE
        };

        // If the batch is full or close to full, trigger a flush
        if should_flush {
            self.flush(false).await?;
        }

        Ok(())
    }

    /// Flushes the current batch to disk
    async fn flush(&self, force: bool) -> WALResult<()> {
        // If a flush is already in progress and this isn't a forced flush, just return
        if !force && self.flushing.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Set the flushing flag to true
        self.flushing.store(true, Ordering::Relaxed);

        // Take the current batch and replace it with an empty one
        let batch_to_flush = {
            let mut batch = self.batch.lock().unwrap();
            if batch.is_empty() {
                // Nothing to flush
                self.flushing.store(false, Ordering::Relaxed);
                return Ok(());
            }
            
            // Swap with a new empty batch
            let mut new_batch = WALBatch::new();
            std::mem::swap(&mut *batch, &mut new_batch);
            new_batch
        };

        // Acquire a permit from the semaphore for this write operation
        let permit = match self.write_semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                self.flushing.store(false, Ordering::Relaxed);
                return Err(io::Error::new(io::ErrorKind::Other, "Failed to acquire semaphore permit"));
            }
        };

        // Serialize the batch
        let serialized = match batch_to_flush.serialize() {
            Ok(bytes) => bytes,
            Err(e) => {
                self.flushing.store(false, Ordering::Relaxed);
                return Err(e);
            }
        };

        // Open the file for appending
        // Clone the data we need for the blocking task
        let file_path = self.path.clone();
        let serialized_clone = serialized.clone();
        
        // Spawn a blocking task to perform filesystem I/O
        let result = tokio::task::spawn_blocking(move || -> io::Result<()> {
            // Open the file in append mode
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)?;
            
            // Write the batch and flush to disk
            file.write_all(&serialized_clone)?;
            file.flush()?;
            
            Ok(())
        }).await;
        
        // Release the permit after the task completes
        drop(permit);

        // Handle any errors
        if let Err(e) = result {
            self.flushing.store(false, Ordering::Relaxed);
            return Err(io::Error::new(ErrorKind::Other, format!("Spawn error: {}", e)));
        }

        let write_result = result.unwrap();
        if let Err(e) = write_result {
            self.flushing.store(false, Ordering::Relaxed);
            return Err(e);
        }

        // Reset the flushing flag
        self.flushing.store(false, Ordering::Relaxed);
        
        Ok(())
    }

    /// Starts the background flush task
    #[allow(dead_code)]
    async fn start_background_flush(writer: Arc<Self>) {
        // Spawn a task that periodically flushes the WAL
        tokio::spawn(async move {
            while !writer.shutdown.load(Ordering::Relaxed) {
                // Sleep for the flush interval
                sleep(Duration::from_millis(FLUSH_INTERVAL_MS)).await;
                
                // Try to flush, ignoring errors (they'll be logged)
                if let Err(e) = writer.flush(false).await {
                    warn!("Background WAL flush error: {:?}", e);
                }
            }
            
            // Final flush on shutdown
            if let Err(e) = writer.flush(true).await {
                error!("Final WAL flush error on shutdown: {:?}", e);
            }
            
            info!("WAL background flush task terminated");
        });
    }

    /// Signals the writer to shut down
    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
    
    /// Reads all operations from the WAL file
    fn read_all(&self) -> Result<Vec<WALOP>, IoError> {
        // If the file doesn't exist yet, return an empty vector
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        
        // Read the file
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) => {
                warn!("Error reading WAL file: {:?}", e);
                return Ok(Vec::new());
            }
        };
        
        // If empty file, return empty vector
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        
        // Deserialize the operations
        let archived = match rkyv::from_bytes::<Vec<WALOP>, rkyv::rancor::Panic>(&bytes) {
            Ok(archived) => archived,
            Err(e) => {
                return Err(IoError::new(ErrorKind::InvalidData, format!("WAL file corrupted: {:?}", e)));
            }
        };
        
        Ok(archived)
    }
    
    /// Returns the path to this WAL file
    fn get_path(&self) -> &PathBuf {
        &self.path
    }
}

/// Write-Ahead Log (WAL) manager that maintains separate WALs for each namespace
///
/// The WAL system:
/// - Maintains a separate WAL file for each namespace
/// - Provides atomic, durable operations
/// - Ensures crash recovery 
/// - Handles concurrent updates efficiently
#[derive(Clone)]
pub struct WAL {
    /// Base directory for WAL storage
    base_path: PathBuf,
    /// Map of namespace to WAL writers
    writers: Arc<RwLock<HashMap<String, Arc<WALWriter>>>>,
    /// Map of namespace to recent operations for fast access 
    recent_ops: Arc<RwLock<HashMap<String, Arc<Mutex<Vec<WALOP>>>>>>,
    /// Maximum number of recent operations to keep in memory per namespace
    max_recent_ops: usize,
    /// Flag to indicate if the WAL system is shutting down
    shutdown_flag: Arc<AtomicBool>,
}

impl WAL {
    /// Opens or creates a new WAL system at the specified base path
    ///
    /// # Arguments
    ///
    /// * `base_path` - Directory where WAL files will be stored
    ///
    /// # Returns
    ///
    /// A new WAL instance
    pub fn open(base_path: PathBuf) -> WAL {
        // Ensure the base directory exists
        if !base_path.exists() {
            let _ = fs::create_dir_all(&base_path);
        }
        
        let wal = WAL {
            base_path,
            writers: Arc::new(RwLock::new(HashMap::new())),
            recent_ops: Arc::new(RwLock::new(HashMap::new())),
            max_recent_ops: 1000,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        };
        
        // Start background task for periodic flush across all namespaces
        if tokio::runtime::Handle::try_current().is_ok() {
            let wal_clone = wal.clone();
            tokio::spawn(async move {
                wal_clone.start_background_flush().await;
            });
        }
        
        wal
    }
    
    /// Gets the WAL file path for a namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace identifier
    ///
    /// # Returns
    ///
    /// Path to the namespace's WAL file
    fn get_namespace_path(&self, namespace: &str) -> PathBuf {
        self.base_path.join(format!("{}_wal.bin", namespace))
    }
    
    /// Gets or creates a writer for the specified namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace identifier
    ///
    /// # Returns
    ///
    /// Arc-wrapped WAL writer for the namespace
    async fn get_writer(&self, namespace: &str) -> Arc<WALWriter> {
        // First check if we already have a writer for this namespace
        {
            let readers = self.writers.read().await;
            if let Some(writer) = readers.get(namespace) {
                return writer.clone();
            }
        }
        
        // If not, create a new writer
        let mut writers = self.writers.write().await;
        
        // Double-check after acquiring write lock
        if let Some(writer) = writers.get(namespace) {
            return writer.clone();
        }
        
        // Create the WAL path for this namespace
        let wal_path = self.get_namespace_path(namespace);
        
        // Create and initialize the writer
        let writer = Arc::new(WALWriter::new(wal_path));
        
        // Store the writer in our map
        writers.insert(namespace.to_string(), writer.clone());
        
        // Return the new writer
        writer
    }
    
    /// Gets or creates the recent operations vector for a namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace identifier
    ///
    /// # Returns
    ///
    /// Arc-wrapped mutex containing recent operations
    async fn get_recent_ops(&self, namespace: &str) -> Arc<Mutex<Vec<WALOP>>> {
        // First check if we already have recent ops for this namespace
        {
            let readers = self.recent_ops.read().await;
            if let Some(recent) = readers.get(namespace) {
                return recent.clone();
            }
        }
        
        // If not, create a new recent ops container
        let mut writers = self.recent_ops.write().await;
        
        // Double-check after acquiring write lock
        if let Some(recent) = writers.get(namespace) {
            return recent.clone();
        }
        
        // Create the recent ops container
        let recent_ops = Arc::new(Mutex::new(Vec::with_capacity(self.max_recent_ops)));
        
        // Store in our map
        writers.insert(namespace.to_string(), recent_ops.clone());
        
        // Return the new container
        recent_ops
    }

    /// Starts the background flush task for all namespaces
    async fn start_background_flush(&self) {
        let wal_ref = self.clone();
        
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            // Sleep for the flush interval
            sleep(Duration::from_millis(FLUSH_INTERVAL_MS)).await;
            
            // Get all current writers
            let writers = {
                let writers_lock = wal_ref.writers.read().await;
                writers_lock.clone()
            };
            
            // Flush each writer
            for (namespace, writer) in writers.iter() {
                if let Err(e) = writer.flush(false).await {
                    warn!(namespace=%namespace, error=%e, "Background WAL flush error");
                }
            }
        }
        
        // Final flush on shutdown
        let writers = {
            let writers_lock = wal_ref.writers.read().await;
            writers_lock.clone()
        };
        
        for (namespace, writer) in writers.iter() {
            if let Err(e) = writer.flush(true).await {
                error!(namespace=%namespace, error=%e, "Final WAL flush error on shutdown");
            }
        }
        
        info!("WAL background flush task terminated");
    }

    /// Processes a WAL command by routing to the appropriate namespace
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command to process
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    pub async fn process_command(&self, cmd: WALCMD) -> Result<Option<String>, IoError> {
        // If we're shutting down, reject new operations
        if self.shutdown_flag.load(Ordering::Relaxed) {
            return Err(IoError::new(ErrorKind::Other, "WAL system is shutting down"));
        }
        
        match cmd {
            WALCMD::Put { namespace, key, value } => {
                // Get the writer for this namespace
                let writer = self.get_writer(&namespace).await;
                
                // Get the recent ops container
                let recent_ops = self.get_recent_ops(&namespace).await;
                
                // Create the operation
                let op = WALOP::Put { key: key.clone(), value: value.clone() };
                
                // Add to recent operations
                {
                    let mut recent = recent_ops.lock().unwrap();
                    
                    // If at capacity, remove oldest entry
                    if recent.len() >= self.max_recent_ops {
                        recent.remove(0);
                    }
                    
                    recent.push(op.clone());
                }
                
                // Append to the WAL
                writer.append(op).await?;
                
                Ok(None)
            },
            WALCMD::Delete { namespace, key } => {
                // Get the writer for this namespace
                let writer = self.get_writer(&namespace).await;
                
                // Get the recent ops container
                let recent_ops = self.get_recent_ops(&namespace).await;
                
                // Create the operation
                let op = WALOP::Delete { key: key.clone() };
                
                // Add to recent operations
                {
                    let mut recent = recent_ops.lock().unwrap();
                    
                    // If at capacity, remove oldest entry
                    if recent.len() >= self.max_recent_ops {
                        recent.remove(0);
                    }
                    
                    recent.push(op.clone());
                }
                
                // Append to the WAL
                writer.append(op).await?;
                
                Ok(None)
            },
            WALCMD::Patch { namespace, key, value } => {
                // Get the writer for this namespace
                let writer = self.get_writer(&namespace).await;
                
                // Get the recent ops container
                let recent_ops = self.get_recent_ops(&namespace).await;
                
                // Create the operation
                let op = WALOP::Patch { key: key.clone(), value: value.clone() };
                
                // Add to recent operations
                {
                    let mut recent = recent_ops.lock().unwrap();
                    
                    // If at capacity, remove oldest entry
                    if recent.len() >= self.max_recent_ops {
                        recent.remove(0);
                    }
                    
                    recent.push(op.clone());
                }
                
                // Append to the WAL
                writer.append(op).await?;
                
                Ok(None)
            },
            WALCMD::DumpWAL { response, namespace } => {
                // Format for dump output
                let mut output = String::new();
                
                if let Some(ns) = namespace {
                    // Dump a specific namespace
                    let dump = self.dump_namespace(&ns).await?;
                    output.push_str(&dump);
                } else {
                    // Dump all namespaces
                    output.push_str("WAL system dump across all namespaces:\n");
                    output.push_str("=====================================\n\n");
                    
                    // Get all namespaces
                    let namespaces = {
                        let writers = self.writers.read().await;
                        writers.keys().cloned().collect::<Vec<_>>()
                    };
                    
                    for ns in namespaces {
                        output.push_str(&format!("Namespace: {}\n", ns));
                        output.push_str(&format!("{}\n\n", self.dump_namespace(&ns).await?));
                    }
                }
                
                // Send the response using the owned sender
                let _ = response.send(output);
                Ok(None)
            },
            WALCMD::FlushWAL { namespace, response } => {
                // Get the writer for this namespace
                let writer = self.get_writer(&namespace).await;
                
                // Flush the WAL
                let result = writer.flush(true).await;
                
                // Handle the response
                if let Some(response_tx) = response {
                    // Clone the result if we need to send it
                    let result_to_send = match &result {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            // Create a new error with the same message
                            let error_msg = format!("{}", e);
                            Err(io::Error::new(e.kind(), error_msg))
                        }
                    };
                    
                    // Send the response with the owned sender
                    let _ = response_tx.send(result_to_send);
                }
                
                // Propagate the original result
                result?;
                Ok(None)
            }
        }
    }
    
    /// Dumps the WAL contents for a specific namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to dump
    ///
    /// # Returns
    ///
    /// String containing the WAL dump
    async fn dump_namespace(&self, namespace: &str) -> Result<String, IoError> {
        // Get the writer for this namespace if it exists
        let writer_opt = {
            let writers = self.writers.read().await;
            writers.get(namespace).cloned()
        };
        
        if let Some(writer) = writer_opt {
            // Read all operations from disk
            let ops = writer.read_all()?;
            
            // Format the operations
            let mut output = String::new();
            output.push_str(&format!("WAL file: {:?}\n", writer.get_path()));
            
            for (i, op) in ops.iter().enumerate() {
                output.push_str(&format!("{}. {:?}\n", i + 1, op));
            }
            
            // Append recent operations from memory if not already included
            let recent_ops_opt = {
                let recent_map = self.recent_ops.read().await;
                recent_map.get(namespace).cloned()
            };
            
            let mut recent_count = 0;
            
            if let Some(ref recent_ops) = recent_ops_opt {
                output.push_str("\nRecent operations in memory:\n");
                
                let recent = match recent_ops.lock() {
                    Ok(guard) => {
                        recent_count = guard.len();
                        guard
                    },
                    Err(_) => return Err(IoError::new(ErrorKind::Other, "Failed to lock recent_ops mutex"))
                };
                
                for (i, op) in recent.iter().enumerate() {
                    output.push_str(&format!("M{}. {:?}\n", i + 1, op));
                }
            }
            
            // Include statistics
            output.push_str(&format!("\nTotal operations on disk: {}\n", ops.len()));
            
            output.push_str(&format!("Recent operations in memory: {}\n", recent_count));
            output.push_str(&format!("Operations processed since startup: {}\n", 
                writer.ops_count.load(Ordering::Relaxed)));
            
            Ok(output)
        } else {
            Ok(format!("No WAL found for namespace: {}\n", namespace))
        }
    }

    /// Appends an operation to the WAL for a specific namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for this operation
    /// * `msg` - The operation to append
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    #[allow(dead_code)]
    pub async fn push(&self, namespace: &str, msg: WALOP) -> Result<(), IoError> {
        // Get the writer for this namespace
        let writer = self.get_writer(namespace).await;
        
        // Get the recent ops container
        let recent_ops = self.get_recent_ops(namespace).await;
        
        // Add to recent operations
        {
            let mut recent = recent_ops.lock().unwrap();
            
            // If at capacity, remove oldest entry
            if recent.len() >= self.max_recent_ops {
                recent.remove(0);
            }
            
            recent.push(msg.clone());
        }
        
        // Append to the WAL
        writer.append(msg).await
    }

    /// Flushes the WAL for a specific namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to flush
    ///
    /// # Returns
    ///
    /// Result indicating success or error
    #[allow(dead_code)]
    pub async fn flush_namespace(&self, namespace: &str) -> Result<(), IoError> {
        // Get the writer for this namespace
        let writer = self.get_writer(namespace).await;
        
        // Flush the WAL
        writer.flush(true).await
    }

    /// Reads all operations from the WAL for a specific namespace
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace to read
    ///
    /// # Returns
    ///
    /// Vector of operations or error
    #[allow(dead_code)]
    pub async fn read_namespace(&self, namespace: &str) -> Result<Vec<WALOP>, IoError> {
        // Get the writer for this namespace
        let writer = self.get_writer(namespace).await;
        
        // Read all operations
        writer.read_all()
    }

    /// Shuts down the WAL system, ensuring all data is flushed
    pub async fn shutdown(&self) {
        // Signal that we're shutting down
        self.shutdown_flag.store(true, Ordering::Relaxed);
        
        // Get all writers
        let writers = {
            let writers_lock = self.writers.read().await;
            writers_lock.clone()
        };
        
        // Shutdown each writer
        for (namespace, writer) in writers.iter() {
            // Signal the writer to shut down
            writer.shutdown();
            
            // Force a final flush
            if let Err(e) = writer.flush(true).await {
                error!(namespace=%namespace, error=%e, "Error during WAL shutdown flush");
            }
        }
        
        info!("WAL system shutdown complete");
    }
}

impl Drop for WAL {
    fn drop(&mut self) {
        // Set shutdown flag - can't do async operations in Drop
        self.shutdown_flag.store(true, Ordering::Relaxed);
        
        // Log that we're being dropped with timestamp to help debug issues
        info!("WAL system being dropped at {:?}", std::time::Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use tempfile::tempdir;
    
    // Helper to create a test WAL system in a temp directory
    async fn create_test_wal() -> (WAL, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let wal = WAL::open(temp_dir.path().to_path_buf());
        (wal, temp_dir)
    }
    
    #[tokio::test]
    async fn test_wal_basic_operations() {
        // Create a WAL system in a temporary directory
        let (wal, _temp_dir) = create_test_wal().await;
        
        // Define test namespace
        let namespace = "test_namespace";
        
        // Add operations to the WAL
        wal.push(namespace, WALOP::Put { 
            key: "key1".to_string(), 
            value: b"value1".to_vec() 
        }).await.unwrap();
        
        wal.push(namespace, WALOP::Put { 
            key: "key2".to_string(), 
            value: b"value2".to_vec() 
        }).await.unwrap();
        
        wal.push(namespace, WALOP::Delete { 
            key: "key1".to_string() 
        }).await.unwrap();
        
        // Flush to disk
        wal.flush_namespace(namespace).await.unwrap();
        
        // Check that operations were recorded
        let ops = wal.read_namespace(namespace).await.unwrap();
        assert_eq!(ops.len(), 3, "Should have 3 operations");
        
        if let WALOP::Put { key, value } = &ops[0] {
            assert_eq!(key, "key1");
            assert_eq!(value, b"value1");
        } else {
            panic!("Expected Put operation");
        }
        
        if let WALOP::Delete { key } = &ops[2] {
            assert_eq!(key, "key1");
        } else {
            panic!("Expected Delete operation");
        }
    }
    
 
    #[tokio::test]
    async fn test_performance() {
        // Create a WAL system in a temporary directory
        let (wal, _temp_dir) = create_test_wal().await;
        
        // Define timestamp-based test namespace
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let namespace = format!("tests/{}/perf", timestamp);

        // Number of operations for performance test - smaller for test
        const NUM_OPS: usize = 100;
        
        // Create and add test operations
        let mut ops = Vec::with_capacity(NUM_OPS);
        for i in 0..NUM_OPS {
            let key = format!("perf_key_{}", i);
            let value = vec![i as u8; 16]; // 16 bytes of data
            ops.push(WALOP::Put { key, value });
        }
        
        // Measure time to append all operations
        let start = Instant::now();
        
        // Add all operations to the WAL
        for op in ops {
            wal.push(&namespace, op).await.unwrap();
        }
        
        // Flush to ensure all data is written
        wal.flush_namespace(&namespace).await.unwrap();
        
        let elapsed = start.elapsed();
        let ops_per_sec = NUM_OPS as f64 / elapsed.as_secs_f64();
        
        // Verify all operations were recorded
        let recorded_ops = wal.read_namespace(&namespace).await.unwrap();
        
        info!("Tokio WAL Performance: {} ops in {:?} ({:.2} ops/sec) - read back {} ops", 
            NUM_OPS, elapsed, ops_per_sec, recorded_ops.len());
            
        assert_eq!(recorded_ops.len(), NUM_OPS, "All operations should be stored");
    }
    
    #[tokio::test]
    async fn test_wal_command_processing() {
        // Create a WAL system in a temporary directory
        let (wal, _temp_dir) = create_test_wal().await;
        
        // Define timestamp-based test namespace
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let namespace = format!("tests/{}/cmd", timestamp);
        
        // Create and process WAL commands
        let cmd1 = WALCMD::Put { 
            key: "cmd_key1".to_string(),
            value: b"cmd_value1".to_vec(),
            namespace: namespace.to_string(),
        };
        
        let cmd2 = WALCMD::Delete {
            key: "cmd_key1".to_string(),
            namespace: namespace.to_string(),
        };
        
        // Process commands
        wal.process_command(cmd1).await.unwrap();
        wal.process_command(cmd2).await.unwrap();
        
        // Flush the namespace
        wal.flush_namespace(&namespace).await.unwrap();
        
        // Verify commands were processed
        let ops = wal.read_namespace(&namespace).await.unwrap();
        assert_eq!(ops.len(), 2, "Should have 2 operations");
        
        if let WALOP::Put { key, value } = &ops[0] {
            assert_eq!(key, "cmd_key1");
            assert_eq!(value, b"cmd_value1");
        } else {
            panic!("Expected Put operation");
        }
        
        if let WALOP::Delete { key } = &ops[1] {
            assert_eq!(key, "cmd_key1");
        } else {
            panic!("Expected Delete operation");
        }
    }
    
    #[tokio::test]
    async fn test_dump_wal() {
        // Create a WAL system in a temporary directory
        let (wal, _temp_dir) = create_test_wal().await;
        
        // Define test namespace
        let namespace = "dump_test";
        
        // Add operations to the WAL
        wal.push(namespace, WALOP::Put { 
            key: "dump_key1".to_string(), 
            value: b"dump_value1".to_vec() 
        }).await.unwrap();
        
        wal.push(namespace, WALOP::Delete { 
            key: "dump_key1".to_string() 
        }).await.unwrap();
        
        // Flush to disk
        wal.flush_namespace(namespace).await.unwrap();
        
        // Create a channel for dump response
        let (tx, rx) = oneshot::channel();
        
        // Create dump command
        let cmd = WALCMD::DumpWAL {
            response: tx,
            namespace: Some(namespace.to_string()),
        };
        
        // Process the dump command
        wal.process_command(cmd).await.unwrap();
        
        // Receive the dump response
        let dump = rx.await.unwrap();
        
        // Check that the dump contains expected information
        assert!(dump.contains("WAL file:"));
        assert!(dump.contains("dump_key1"));
        assert!(dump.contains("Total operations on disk: 2"));
        
        // Now test dump all namespaces
        // Add a second namespace
        let namespace2 = "dump_test2";
        wal.push(namespace2, WALOP::Put { 
            key: "ns2_key".to_string(), 
            value: b"ns2_value".to_vec() 
        }).await.unwrap();
        
        wal.flush_namespace(namespace2).await.unwrap();
        
        // Create a channel for dump response
        let (tx, rx) = oneshot::channel();
        
        // Create dump command for all namespaces
        let cmd = WALCMD::DumpWAL {
            response: tx,
            namespace: None,
        };
        
        // Process the dump command
        wal.process_command(cmd).await.unwrap();
        
        // Receive the dump response
        let dump = rx.await.unwrap();
        
        // Check that the dump contains both namespaces
        assert!(dump.contains("Namespace: dump_test"));
        assert!(dump.contains("Namespace: dump_test2"));
    }
    
    #[tokio::test]
    async fn test_concurrent_namespaces() {
        // Create a WAL system in a temporary directory
        let (wal, _temp_dir) = create_test_wal().await;
        
        // Create multiple namespaces with different data
        const NUM_NAMESPACES: usize = 5;
        const OPS_PER_NAMESPACE: usize = 10;
        
        // Spawn tasks to add operations to different namespaces concurrently
        let mut tasks = Vec::new();
        
        for ns_id in 0..NUM_NAMESPACES {
            let namespace = format!("concurrent_ns_{}", ns_id);
            let wal_clone = wal.clone();
            
            let task = tokio::spawn(async move {
                for op_id in 0..OPS_PER_NAMESPACE {
                    let key = format!("key_{}_{}", ns_id, op_id);
                    let value = format!("value_{}_{}", ns_id, op_id).into_bytes();
                    
                    wal_clone.push(&namespace, WALOP::Put { key, value }).await.unwrap();
                }
                
                // Flush this namespace
                wal_clone.flush_namespace(&namespace).await.unwrap();
                
                namespace
            });
            
            tasks.push(task);
        }
        
        // Wait for all tasks to complete
        let mut namespaces = Vec::new();
        for task in tasks {
            namespaces.push(task.await.unwrap());
        }
        
        // Verify that each namespace has the correct operations
        for namespace in namespaces {
            let ops = wal.read_namespace(&namespace).await.unwrap();
            
            assert_eq!(ops.len(), OPS_PER_NAMESPACE, 
                "Namespace {} should have {} operations", namespace, OPS_PER_NAMESPACE);
            
            // Parse namespace ID from the name
            let ns_id = namespace.split('_').last().unwrap().parse::<usize>().unwrap();
            
            for (op_id, op) in ops.iter().enumerate() {
                if let WALOP::Put { key, value } = op {
                    let expected_key = format!("key_{}_{}", ns_id, op_id);
                    let expected_value = format!("value_{}_{}", ns_id, op_id).into_bytes();
                    
                    assert_eq!(key, &expected_key, "Key mismatch in namespace {}", namespace);
                    assert_eq!(value, &expected_value, "Value mismatch in namespace {}", namespace);
                } else {
                    panic!("Expected Put operation in namespace {}", namespace);
                }
            }
        }
    }
}