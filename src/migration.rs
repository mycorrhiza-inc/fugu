use crate::db::{FuguDB, PREFIX_RECORD_INDEX_TREE, TREE_FILTERS, TREE_GLOBAL_INDEX, TREE_RECORDS};
use crate::tracing_utils;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Migration options
pub struct MigrationOptions {
    /// Source sled database path
    pub source_path: String,
    /// Destination fjall database path
    pub destination_path: String,
    /// Whether to verify data after migration
    pub verify: bool,
    /// Whether to delete the source database after successful migration
    pub delete_source_after_migration: bool,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            source_path: "fugu_db".to_string(),
            destination_path: "fugu_db_fjall".to_string(),
            verify: true,
            delete_source_after_migration: false,
        }
    }
}

/// Struct to hold migration statistics
pub struct MigrationStats {
    pub total_trees: usize,
    pub total_keys: usize,
    pub migrated_keys: usize,
    pub skipped_keys: usize,
    pub errors: usize,
    pub duration_ms: u128,
}

/// Migrates data from a sled database to a fjall database
pub fn migrate_sled_to_fjall(options: MigrationOptions) -> Result<MigrationStats, String> {
    let span = tracing_utils::db_span("migrate_sled_to_fjall");
    let _enter = span.enter();

    info!("Starting migration from sled to fjall");
    info!("Source path: {}", options.source_path);
    info!("Destination path: {}", options.destination_path);

    // Start the timer
    let start_time = Instant::now();

    // Initialize statistics
    let mut stats = MigrationStats {
        total_trees: 0,
        total_keys: 0,
        migrated_keys: 0,
        skipped_keys: 0,
        errors: 0,
        duration_ms: 0,
    };

    // Open the source sled database
    let sled_db = match sled::open(&options.source_path) {
        Ok(db) => db,
        Err(e) => {
            error!("Failed to open source sled database: {}", e);
            return Err(format!("Failed to open source sled database: {}", e));
        }
    };

    // Open the destination fjall database
    let fjall_keyspace = match fjall::Config::new(&options.destination_path).open() {
        Ok(keyspace) => keyspace,
        Err(e) => {
            error!("Failed to open destination fjall keyspace: {:?}", e);
            return Err(format!("Failed to open destination fjall keyspace: {:?}", e));
        }
    };

    // Create FuguDB instances for both databases
    let sled_fugu = FuguDB::new(sled_db);
    let fjall_fugu = FuguDB::new(fjall_keyspace);

    // Initialize the fjall database
    fjall_fugu.init_db();

    // Get the list of all trees from the sled database
    let tree_names = match sled_fugu.db().tree_names() {
        Ok(names) => names,
        Err(e) => {
            error!("Failed to get tree names from sled database: {}", e);
            return Err(format!("Failed to get tree names from sled database: {}", e));
        }
    };

    stats.total_trees = tree_names.len();
    info!("Found {} trees to migrate", stats.total_trees);

    // Process each tree
    for tree_name in tree_names {
        let tree_name_str = match std::str::from_utf8(&tree_name) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to convert tree name to string: {}", e);
                stats.errors += 1;
                continue;
            }
        };

        info!("Migrating tree: {}", tree_name_str);

        // Open the source tree
        let source_tree = match sled_fugu.db().open_tree(&tree_name) {
            Ok(tree) => tree,
            Err(e) => {
                error!("Failed to open source tree {}: {}", tree_name_str, e);
                stats.errors += 1;
                continue;
            }
        };

        // Open the destination partition
        let dest_partition = match fjall_fugu.keyspace().open_partition(tree_name_str, fjall::PartitionCreateOptions::default()) {
            Ok(partition) => partition,
            Err(e) => {
                error!("Failed to open destination partition {}: {:?}", tree_name_str, e);
                stats.errors += 1;
                continue;
            }
        };

        // Track the number of keys in this tree
        let mut tree_keys = 0;

        // Batch operations for better performance
        let mut batch = fjall::Batch::default();
        let mut batch_size = 0;
        let batch_max_size = 1000; // Process in batches of 1000 keys

        // Iterate through all key-value pairs in the tree
        for item in source_tree.iter() {
            match item {
                Ok((key, value)) => {
                    tree_keys += 1;
                    stats.total_keys += 1;

                    // Convert key to string if possible (for better logging)
                    let key_str = match std::str::from_utf8(&key) {
                        Ok(s) => s.to_string(),
                        Err(_) => format!("<binary key of length {}>", key.len()),
                    };

                    // Add to batch
                    batch.insert(&dest_partition, key.to_vec(), value.to_vec());
                    batch_size += 1;

                    // If batch is full, commit it
                    if batch_size >= batch_max_size {
                        match fjall_fugu.keyspace().commit_batch(batch) {
                            Ok(_) => {
                                stats.migrated_keys += batch_size;
                                debug!("Committed batch of {} keys", batch_size);
                            }
                            Err(e) => {
                                error!("Failed to commit batch: {:?}", e);
                                stats.errors += batch_size;
                            }
                        }
                        // Create a new batch
                        batch = fjall::Batch::default();
                        batch_size = 0;
                    }
                }
                Err(e) => {
                    error!("Error reading item from tree {}: {}", tree_name_str, e);
                    stats.errors += 1;
                }
            }
        }

        // Commit any remaining items
        if batch_size > 0 {
            match fjall_fugu.keyspace().commit_batch(batch) {
                Ok(_) => {
                    stats.migrated_keys += batch_size;
                    debug!("Committed final batch of {} keys", batch_size);
                }
                Err(e) => {
                    error!("Failed to commit final batch: {:?}", e);
                    stats.errors += batch_size;
                }
            }
        }

        info!("Migrated {} keys from tree {}", tree_keys, tree_name_str);
    }

    // Ensure durability
    if let Err(e) = fjall_fugu.keyspace().persist(fjall::PersistMode::SyncAll) {
        error!("Failed to persist fjall database: {:?}", e);
    }

    // Calculate duration
    stats.duration_ms = start_time.elapsed().as_millis();
    
    info!("Migration completed in {} ms", stats.duration_ms);
    info!("Migrated {} keys across {} trees", stats.migrated_keys, stats.total_trees);
    
    if stats.errors > 0 {
        warn!("There were {} errors during migration", stats.errors);
    }

    // If verification is requested, verify the data
    if options.verify {
        info!("Verifying migrated data...");
        let verified = verify_migration(&sled_fugu, &fjall_fugu);
        
        if !verified {
            error!("Verification failed! The migrated data does not match the source data.");
            return Err("Verification failed".to_string());
        }
        
        info!("Verification successful. All data was migrated correctly.");
    }

    // If delete_source_after_migration is true, delete the source database
    if options.delete_source_after_migration {
        info!("Deleting source database at {}", options.source_path);
        if let Err(e) = std::fs::remove_dir_all(&options.source_path) {
            error!("Failed to delete source database: {}", e);
        }
    }

    Ok(stats)
}

/// Verify that the migration was successful by comparing the data in both databases
fn verify_migration(sled_db: &FuguDB, fjall_db: &FuguDB) -> bool {
    let span = tracing_utils::db_span("verify_migration");
    let _enter = span.enter();

    let mut verification_success = true;

    // Verify the standard trees first
    let standard_trees = [TREE_RECORDS, TREE_FILTERS, TREE_GLOBAL_INDEX];
    
    for tree_name in standard_trees.iter() {
        info!("Verifying standard tree: {}", tree_name);
        
        if !verify_tree(sled_db, fjall_db, tree_name) {
            verification_success = false;
            error!("Verification failed for tree: {}", tree_name);
        }
    }

    // Verify any index trees (PREFIX_RECORD_INDEX_TREE:object_id)
    let sled_tree_names = match sled_db.db().tree_names() {
        Ok(names) => names,
        Err(e) => {
            error!("Failed to get tree names from sled database: {}", e);
            return false;
        }
    };

    for tree_name in sled_tree_names {
        let tree_name_str = match std::str::from_utf8(&tree_name) {
            Ok(s) => s,
            Err(_) => continue, // Skip binary tree names
        };

        // Skip standard trees as we already checked them
        if standard_trees.contains(&tree_name_str) {
            continue;
        }

        // Check if this is an index tree
        if tree_name_str.starts_with(PREFIX_RECORD_INDEX_TREE) {
            info!("Verifying index tree: {}", tree_name_str);
            
            if !verify_tree(sled_db, fjall_db, tree_name_str) {
                verification_success = false;
                error!("Verification failed for index tree: {}", tree_name_str);
            }
        }
    }

    verification_success
}

/// Verify that a specific tree was migrated correctly
fn verify_tree(sled_db: &FuguDB, fjall_db: &FuguDB, tree_name: &str) -> bool {
    let mut verification_success = true;

    // Open the source tree
    let source_tree = match sled_db.db().open_tree(tree_name) {
        Ok(tree) => tree,
        Err(e) => {
            error!("Failed to open source tree {}: {}", tree_name, e);
            return false;
        }
    };

    // Open the destination partition
    let dest_partition = match fjall_db.keyspace().open_partition(tree_name, fjall::PartitionCreateOptions::default()) {
        Ok(partition) => partition,
        Err(e) => {
            error!("Failed to open destination partition {}: {:?}", tree_name, e);
            return false;
        }
    };

    // Count keys in both
    let sled_count = source_tree.iter().count();
    let fjall_count = match dest_partition.iter() {
        Ok(iter) => iter.count(),
        Err(e) => {
            error!("Failed to iterate over fjall partition: {:?}", e);
            return false;
        }
    };

    if sled_count != fjall_count {
        error!("Key count mismatch for {}: sled={}, fjall={}", tree_name, sled_count, fjall_count);
        verification_success = false;
    }

    // Check that each key-value pair was migrated correctly
    let mut checked = 0;
    for item in source_tree.iter() {
        match item {
            Ok((key, sled_value)) => {
                checked += 1;

                // Convert key to bytes or string for fjall
                let key_bytes = key.to_vec();
                let key_str = match std::str::from_utf8(&key) {
                    Ok(s) => s,
                    Err(_) => {
                        error!("Invalid UTF-8 key in tree {}", tree_name);
                        verification_success = false;
                        continue;
                    }
                };

                // Get the corresponding value from fjall
                let fjall_value = match dest_partition.get(key_str) {
                    Ok(Some(value)) => value,
                    Ok(None) => {
                        error!("Key missing in fjall partition: {:?}", key_str);
                        verification_success = false;
                        continue;
                    }
                    Err(e) => {
                        error!("Error getting key from fjall: {:?}", e);
                        verification_success = false;
                        continue;
                    }
                };

                // Compare the values
                if sled_value.as_ref() != fjall_value.as_ref() {
                    error!("Value mismatch for key {:?}", key_str);
                    verification_success = false;
                }

                // Log progress periodically
                if checked % 1000 == 0 {
                    debug!("Verified {} key-value pairs in tree {}", checked, tree_name);
                }
            }
            Err(e) => {
                error!("Error reading item from tree {}: {}", tree_name, e);
                verification_success = false;
            }
        }
    }

    info!("Verified {} key-value pairs in tree {}", checked, tree_name);
    verification_success
}

/// Creates a CLI command for migrating data from sled to fjall
pub fn migrate_cmd(
    source_path: &str,
    destination_path: &str,
    verify: bool,
    delete_source: bool,
) -> Result<(), String> {
    let options = MigrationOptions {
        source_path: source_path.to_string(),
        destination_path: destination_path.to_string(),
        verify,
        delete_source_after_migration: delete_source,
    };

    match migrate_sled_to_fjall(options) {
        Ok(stats) => {
            info!("Migration completed successfully!");
            info!(
                "Migrated {} keys from {} trees in {} ms",
                stats.migrated_keys, stats.total_trees, stats.duration_ms
            );
            if stats.errors > 0 {
                warn!("There were {} errors during migration", stats.errors);
            }
            Ok(())
        }
        Err(e) => {
            error!("Migration failed: {}", e);
            Err(e)
        }
    }
}