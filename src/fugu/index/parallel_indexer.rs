/// CRDT-based parallel indexing for large files
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::RwLock;
use tokio::time::Instant;
use crdts::{CmRDT, CvRDT, GCounter};
use num_traits::ToPrimitive;
use tracing::info;

use crate::fugu::node::Node;
use crate::fugu::grpc::BoxError;

/// ParallelIndexer provides efficient parallel indexing of large files
/// using Conflict-free Replicated Data Types (CRDTs) for concurrent term counting
pub struct ParallelIndexer;

impl ParallelIndexer {
    /// Creates a new parallel indexer
    pub fn new() -> Self {
        Self
    }

    /// Parallel indexing for large files using CRDTs
    ///
    /// This method:
    /// - Splits a large file into chunks
    /// - Processes each chunk in parallel using tokio tasks
    /// - Merges results using CRDTs (Conflict-free Replicated Data Types)
    /// - Provides significant performance improvements for large files
    ///
    /// # Arguments
    ///
    /// * `node` - The node to index the file in
    /// * `file_path` - Path to the file to index
    /// * `file_name` - Name of the file
    ///
    /// # Returns
    ///
    /// Result with Duration indicating indexing time or an error
    pub async fn index_file(
        &self, 
        node: &Node, 
        file_path: &PathBuf, 
        file_name: &str
    ) -> Result<std::time::Duration, BoxError> {
        use crate::fugu::index::Tokenizer;
        
        // Start timing
        let start_time = Instant::now();
        
        // Create a new CRDT GCounter for tracking term frequencies
        // This will hold the final merged counter from all workers
        // Since we don't need to modify the counter during processing (each worker has its own),
        // we can use RwLock instead of Mutex for better concurrency
        let term_counter = Arc::new(RwLock::new(GCounter::<String>::new()));
        
        // Use a thread-safe concurrent HashMap for term positions
        // We'll use one HashMap per worker task and merge them at the end to prevent contention
        let term_positions_map = Arc::new(RwLock::new(Vec::<HashMap<String, Vec<u64>>>::new()));
        
        // Open the file for reading
        let file = tokio::fs::File::open(file_path).await?;
        let file_size = file.metadata().await?.len();
        
        // Determine chunk size and number of chunks
        // Balance between having enough chunks for parallelism but not too many
        let num_cores = num_cpus::get() as u64;
        let ideal_chunks = num_cores * 2; // Use 2x CPU cores for better parallelism
        let chunk_size = std::cmp::max(file_size / ideal_chunks, 8192); // Min 8KB chunks
        let num_chunks = (file_size + chunk_size - 1) / chunk_size; // Ceiling division
        
        // Pre-allocate empty HashMaps for each worker
        {
            let mut positions_map = term_positions_map.write().await;
            for _ in 0..num_chunks {
                positions_map.push(HashMap::<String, Vec<u64>>::new());
            }
        }
        
        info!(
            file_size=%file_size, 
            chunk_size=%chunk_size, 
            num_chunks=%num_chunks,
            "Starting parallel indexing"
        );
        
        // Create a tokenizer
        let tokenizer = crate::fugu::index::WhitespaceTokenizer;
        
        // Create a vector to store all process chunk tasks
        let mut tasks = Vec::new();
        
        // Process each chunk in parallel
        for chunk_idx in 0..num_chunks {
            // Clone references for this task
            let file_path_task = file_path.clone();
            let doc_id = file_name.to_string();
            // term_positions_map no longer needed in each task as we're returning results
            let worker_idx = chunk_idx as usize; // Use chunk index as worker index
            
            // Spawn a task to process this chunk
            let task = tokio::spawn(async move {
                // Calculate chunk boundaries
                let start_pos = chunk_idx * chunk_size;
                let end_pos = std::cmp::min((chunk_idx + 1) * chunk_size, file_size);
                let chunk_actual_size = end_pos - start_pos;
                
                // Open file for this chunk
                let mut chunk_file = tokio::fs::File::open(&file_path_task).await?;
                
                // Seek to the start position of this chunk
                use std::io::SeekFrom;
                chunk_file.seek(SeekFrom::Start(start_pos)).await?;
                
                // Read the chunk
                let mut buffer = vec![0u8; chunk_actual_size as usize];
                chunk_file.read_exact(&mut buffer).await?;
                
                // Convert to string with lossy UTF-8 handling (for binary files)
                let chunk_text = String::from_utf8_lossy(&buffer).to_string();
                
                // Tokenize the chunk
                let tokens = tokenizer.tokenize(&chunk_text, &doc_id);
                
                // Create a worker-specific counter for this chunk
                // Using CRDT's correctly: each worker has its own counter
                let mut worker_counter = GCounter::<String>::new();
                
                // Create a local HashMap for this worker's term positions
                let mut local_positions = HashMap::<String, Vec<u64>>::new();
                
                // Process tokens and update worker's counter and local term positions
                for token in tokens {
                    // Update worker-specific counter using CRDT operations
                    let op = worker_counter.inc(token.term.clone());
                    worker_counter.apply(op);
                    
                    // Calculate global position by adding chunk offset
                    let global_position = start_pos + token.position;
                    
                    // Update local term positions
                    local_positions
                        .entry(token.term.clone())
                        .or_insert_with(Vec::new)
                        .push(global_position);
                }
                
                // Return both the counter and positions to avoid needing to write to shared state during processing
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((worker_counter, local_positions, worker_idx))
            });
            
            tasks.push(task);
        }
        
        // Wait for all tasks to complete and collect their results
        let mut worker_counters = Vec::new();
        // Create a merged map of all term positions
        let mut merged_positions = HashMap::<String, Vec<u64>>::new();
        
        for task in tasks {
            match task.await {
                Ok(Ok((worker_counter, worker_positions, _worker_idx))) => {
                    // Collect each worker's counter
                    worker_counters.push(worker_counter);
                    
                    // Merge this worker's positions into the merged map
                    for (term, positions) in worker_positions {
                        merged_positions
                            .entry(term)
                            .or_insert_with(Vec::new)
                            .extend_from_slice(&positions);
                    }
                },
                Ok(Err(e)) => {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Worker task error: {}", e)
                    )));
                },
                Err(e) => {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other, 
                        format!("Task join error: {}", e)
                    )));
                }
            }
        }
        
        // Merge all worker counters into the main counter
        // This is the key CRDT operation - merging distributed counters correctly
        {
            let mut main_counter = term_counter.write().await;
            for worker_counter in worker_counters {
                // Use the CRDT merge operation to combine counters
                main_counter.merge(worker_counter);
            }
        }
        
        // Get the final term counter
        let counter = term_counter.read().await;
        
        // Create tokens for indexing based on the merged data
        // This makes one token for each term in each document, with all its positions
        let tokens_to_index = merged_positions
            .iter()
            .map(|(term, term_positions)| {
                // Sort positions to ensure they're in the right order and remove duplicates
                let mut sorted_positions = term_positions.clone();
                sorted_positions.sort();
                sorted_positions.dedup();
                
                // Get the total frequency from the CRDT counter using the read method
                let frequency = counter.read().to_u32().unwrap_or(0);
                
                (term.clone(), sorted_positions, frequency)
            })
            .collect::<Vec<_>>();
        
        // Create token batches for indexing
        // We handle this differently since we need to add the full position list at once
        let tokens_len = tokens_to_index.len();
        for (term, positions, _frequency) in &tokens_to_index {
            // Use the index directly to add terms with all their positions
            // Access the inverted index to add terms
            if let Some(index) = node.get_index() {
                // Add term with all its positions in one operation
                let doc_id = file_name.to_string();
                
                // Create a token for this term
                // (We're inserting a small representative token with position 0,
                // but we'll actually use the full positions list in the index impl)
                let token = crate::fugu::index::Token {
                    term: term.clone(),
                    doc_id: doc_id.clone(),
                    position: 0, // Using 0 as a placeholder
                };
                
                // Index the term with all its positions
                // Convert the error to ensure it implements Send + Sync
                index.add_term_with_positions(token, positions.clone()).await.map_err(|e| {
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to add term with positions: {}", e)
                    )) as BoxError
                })?;
            }
        }
        
        // Get the elapsed time
        let elapsed = start_time.elapsed();
        info!(
            file=%file_name, 
            elapsed=?elapsed, 
            tokens=%tokens_len,
            workers=%num_chunks,
            "Completed threadsafe parallel indexing"
        );
        
        Ok(elapsed)
    }
}