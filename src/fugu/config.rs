/// Configuration management for Fugu
///
/// This module provides:
/// - Default configuration paths
/// - Directory creation and validation
/// - Path resolution for various Fugu components
use std::path::{Path, PathBuf};
use std::fs;
use tracing::{debug, warn};
use std::io;

/// ConfigManager handles all configuration paths for Fugu
///
/// It ensures:
/// - Directories exist and are readable/writable
/// - Paths are resolved consistently
/// - Default locations are used when not specified
#[derive(Clone, Debug)]
pub struct ConfigManager {
    /// Base directory for all Fugu data
    base_dir: PathBuf,
}

impl ConfigManager {
    /// Creates a new ConfigManager with the specified base directory
    ///
    /// # Arguments
    ///
    /// * `custom_path` - Optional custom base directory path
    ///
    /// # Returns
    ///
    /// A new ConfigManager instance
    pub fn new(custom_path: Option<PathBuf>) -> Self {
        let base_dir = match custom_path {
            Some(path) => path,
            None => {
                // Use ~/.fugu as default location
                let home_dir = std::env::var("HOME").unwrap_or_else(|_| String::from("/tmp"));
                PathBuf::from(home_dir).join(".fugu")
            }
        };
        
        let config = Self { base_dir };
        
        // Ensure the base directory exists
        config.ensure_base_dir().unwrap_or_else(|e| {
            warn!("Failed to create base directory: {}", e);
        });
        
        config
    }
    
    /// Returns the base directory path
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
    
    /// Returns the logs directory path
    pub fn logs_dir(&self) -> PathBuf {
        self.base_dir.join("logs")
    }
    
    /// Converts a namespace string to a path
    /// This function parses a namespace string like "a/b/c" into a proper path
    fn namespace_to_path(&self, namespace: &str) -> PathBuf {
        let namespace_path = PathBuf::from(namespace);
        if namespace_path.is_absolute() {
            namespace_path
        } else {
            // For relative namespaces, store them under the base "namespaces" directory
            self.base_dir.join("namespaces").join(namespace)
        }
    }
    
    /// Returns the path for namespace data
    pub fn namespace_dir(&self, namespace: &str) -> PathBuf {
        self.namespace_to_path(namespace)
    }
    
    /// Returns the WAL path for a specific namespace
    #[allow(dead_code)]
    pub fn namespace_wal_path(&self, namespace: &str) -> PathBuf {
        self.namespace_dir(namespace).join(".fugu").join("wal.bin")
    }
    
    /// Returns the index path for a specific namespace
    /// 
    /// This is where the consolidated index is stored for faster loading/unloading
    pub fn namespace_index_path(&self, namespace: &str) -> PathBuf {
        self.namespace_dir(namespace).join(".fugu").join("index")
    }
    
    /// Returns the temp directory path
    #[allow(dead_code)]
    pub fn temp_dir(&self) -> PathBuf {
        self.base_dir.join("tmp")
    }
    
    /// Ensures the base directory exists
    fn ensure_base_dir(&self) -> std::io::Result<()> {
        if !self.base_dir.exists() {
            debug!("Creating base directory: {:?}", self.base_dir);
            fs::create_dir_all(&self.base_dir)?;
        }
        
        // Create standard subdirectories
        let dirs = [
            self.base_dir.join("logs"),
            self.base_dir.join("namespaces"),
            self.base_dir.join("tmp"),
        ];
        
        for dir in &dirs {
            if !dir.exists() {
                debug!("Creating directory: {:?}", dir);
                fs::create_dir_all(dir)?;
            }
        }
        
        // Check if we need to migrate old namespace data
        self.check_and_migrate_old_namespaces()?;
        
        Ok(())
    }
    
    /// Migrates data from old namespace structure to new .fugu structure
    fn check_and_migrate_old_namespaces(&self) -> std::io::Result<()> {
        let namespaces_dir = self.base_dir.join("namespaces");
        if !namespaces_dir.exists() {
            return Ok(());
        }
        
        // Scan for namespaces with old structure
        if let Ok(entries) = fs::read_dir(&namespaces_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_dir() {
                        // Check if this has the old structure (index directory directly under namespace)
                        let old_index_path = path.join("index");
                        if old_index_path.exists() && old_index_path.is_dir() {
                            // This is an old-style namespace directory
                            let namespace_name = path.file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown");
                                
                            debug!("Migrating old namespace structure for: {}", namespace_name);
                            
                            // Create the new .fugu directory
                            let new_fugu_dir = path.join(".fugu");
                            fs::create_dir_all(&new_fugu_dir)?;
                            
                            // Move the index directory to the new location
                            let new_index_path = new_fugu_dir.join("index");
                            if !new_index_path.exists() {
                                debug!("Moving index from {:?} to {:?}", old_index_path, new_index_path);
                                
                                // Use rename if possible (fast), otherwise copy and delete (slower but works across filesystems)
                                if let Err(_) = fs::rename(&old_index_path, &new_index_path) {
                                    warn!("Failed to rename index directory, will try copy+delete instead");
                                    copy_dir_all(&old_index_path, &new_index_path)?;
                                    fs::remove_dir_all(&old_index_path)?;
                                }
                            }
                            
                            // Check for old WAL file
                            let old_wal_path = path.join("wal.bin");
                            if old_wal_path.exists() {
                                let new_wal_path = new_fugu_dir.join("wal.bin");
                                debug!("Moving WAL from {:?} to {:?}", old_wal_path, new_wal_path);
                                
                                if let Err(_) = fs::rename(&old_wal_path, &new_wal_path) {
                                    warn!("Failed to rename WAL file, will try copy+delete instead");
                                    fs::copy(&old_wal_path, &new_wal_path)?;
                                    fs::remove_file(&old_wal_path)?;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Ensures a namespace directory exists including its .fugu subdirectory
    pub fn ensure_namespace_dir(&self, namespace: &str) -> std::io::Result<()> {
        let ns_dir = self.namespace_dir(namespace);
        if !ns_dir.exists() {
            debug!("Creating namespace directory: {:?}", ns_dir);
            fs::create_dir_all(&ns_dir)?;
        }
        
        // Ensure the .fugu directory exists within the namespace
        let fugu_dir = ns_dir.join(".fugu");
        if !fugu_dir.exists() {
            debug!("Creating .fugu directory: {:?}", fugu_dir);
            fs::create_dir_all(&fugu_dir)?;
        }
        
        Ok(())
    }
}

/// Creates a new ConfigManager with optional custom path
///
/// # Arguments
///
/// * `custom_path` - Optional custom base directory path
///
/// # Returns
///
/// A new ConfigManager instance
pub fn new_config_manager(custom_path: Option<PathBuf>) -> ConfigManager {
    ConfigManager::new(custom_path)
}

/// Recursively copy a directory and all its contents
///
/// This function is used during namespace migration when rename fails
/// (which can happen when moving across different filesystems)
fn copy_dir_all(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        
        if file_type.is_dir() {
            copy_dir_all(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    
    Ok(())
}
