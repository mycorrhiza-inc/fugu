/// Configuration management for Fugu
///
/// This module provides:
/// - Default configuration paths
/// - Directory creation and validation
/// - Path resolution for various Fugu components
use std::path::{Path, PathBuf};
use std::fs;
use tracing::{debug, warn};

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
    
    /// Returns the path for namespace data
    pub fn namespace_dir(&self, namespace: &str) -> PathBuf {
        self.base_dir.join("namespaces").join(namespace)
    }
    
    /// Returns the WAL path for a specific namespace
    #[allow(dead_code)]
    pub fn namespace_wal_path(&self, namespace: &str) -> PathBuf {
        self.namespace_dir(namespace).join("wal.bin")
    }
    
    /// Returns the index path for a specific namespace
    pub fn namespace_index_path(&self, namespace: &str) -> PathBuf {
        self.namespace_dir(namespace).join("index")
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
        
        Ok(())
    }
    
    /// Ensures a namespace directory exists
    pub fn ensure_namespace_dir(&self, namespace: &str) -> std::io::Result<()> {
        let ns_dir = self.namespace_dir(namespace);
        if !ns_dir.exists() {
            debug!("Creating namespace directory: {:?}", ns_dir);
            fs::create_dir_all(&ns_dir)?;
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
