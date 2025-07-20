//! Dataset configuration and initialization for server startup

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info, warn};

use super::core::{Dataset, IndexType, NamedIndex};

/// Server configuration for FuguDB
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    /// Base path for all data storage
    pub data_path: PathBuf,

    /// Default namespace to use if none specified
    pub default_namespace: String,

    /// Pre-configured namespaces to initialize on startup
    pub namespaces: Vec<NamespaceConfig>,

    /// Index writer memory budget in bytes
    pub writer_memory_budget: Option<usize>,

    /// Whether to create directories automatically
    pub auto_create_directories: bool,

    /// Validation settings
    pub validation: ValidationConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NamespaceConfig {
    /// Namespace name
    pub name: String,

    /// Override the default data path for this namespace
    pub data_path: Option<PathBuf>,

    /// Whether to initialize this namespace on startup
    pub initialize_on_startup: bool,

    /// Custom schema overrides for this namespace
    pub schema_overrides: Option<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ValidationConfig {
    /// Require ID field to be non-empty
    pub require_non_empty_id: bool,

    /// Maximum text field length
    pub max_text_length: Option<usize>,

    /// Whether to validate facet paths
    pub validate_facet_paths: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("./data"),
            default_namespace: "default".to_string(),
            namespaces: vec![NamespaceConfig {
                name: "default".to_string(),
                data_path: None,
                initialize_on_startup: true,
                schema_overrides: None,
            }],
            writer_memory_budget: Some(50_000_000), // 50MB default
            auto_create_directories: true,
            validation: ValidationConfig::default(),
        }
    }
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            require_non_empty_id: true,
            max_text_length: Some(1_000_000), // 1MB max text
            validate_facet_paths: true,
        }
    }
}

/// Dataset manager that handles multiple namespaces and configuration
#[derive(Debug)]
pub struct DatasetManager {
    config: ServerConfig,
    datasets: HashMap<String, Dataset>,
}

impl DatasetManager {
    /// Create a new DatasetManager from configuration
    pub fn from_config(config: ServerConfig) -> Result<Self> {
        info!("Initializing DatasetManager with config: {:?}", config);

        let mut manager = Self {
            config,
            datasets: HashMap::new(),
        };

        // Initialize configured namespaces
        manager.initialize_configured_namespaces()?;

        Ok(manager)
    }

    /// Load configuration from file and create DatasetManager
    pub fn from_config_file<P: AsRef<std::path::Path>>(config_path: P) -> Result<Self> {
        let config_content = std::fs::read_to_string(config_path.as_ref()).map_err(|e| {
            anyhow!(
                "Failed to read config file {:?}: {}",
                config_path.as_ref(),
                e
            )
        })?;

        let config: ServerConfig = if config_path
            .as_ref()
            .extension()
            .and_then(|ext| ext.to_str())
            == Some("yaml")
            || config_path
                .as_ref()
                .extension()
                .and_then(|ext| ext.to_str())
                == Some("yml")
        {
            serde_yaml::from_str(&config_content)
                .map_err(|e| anyhow!("Failed to parse YAML config: {}", e))?
        } else {
            serde_json::from_str(&config_content)
                .map_err(|e| anyhow!("Failed to parse JSON config: {}", e))?
        };

        Self::from_config(config)
    }

    /// Create DatasetManager with default configuration
    pub fn with_defaults<P: AsRef<std::path::Path>>(
        data_path: P,
        default_namespace: &str,
    ) -> Result<Self> {
        let mut config = ServerConfig::default();
        config.data_path = data_path.as_ref().to_path_buf();
        config.default_namespace = default_namespace.to_string();
        config.namespaces = vec![NamespaceConfig {
            name: default_namespace.to_string(),
            data_path: None,
            initialize_on_startup: true,
            schema_overrides: None,
        }];

        Self::from_config(config)
    }

    /// Initialize all namespaces marked for startup initialization
    fn initialize_configured_namespaces(&mut self) -> Result<()> {
        for namespace_config in &self.config.namespaces {
            if namespace_config.initialize_on_startup {
                info!("Initializing namespace: {}", namespace_config.name);
                self.create_namespace(&namespace_config.name, Some(namespace_config))?;
            }
        }
        Ok(())
    }

    /// Create or get a dataset for a namespace
    pub fn get_or_create_dataset(&mut self, namespace: &str) -> Result<&Dataset> {
        if !self.datasets.contains_key(namespace) {
            self.create_namespace(namespace, None)?;
        }

        Ok(self
            .datasets
            .get(namespace)
            .ok_or_else(|| anyhow!("Failed to create dataset for namespace: {}", namespace))?)
    }

    /// Get an existing dataset
    pub fn get_dataset(&self, namespace: &str) -> Option<&Dataset> {
        self.datasets.get(namespace)
    }

    /// Create a new namespace
    fn create_namespace(
        &mut self,
        namespace: &str,
        config: Option<&NamespaceConfig>,
    ) -> Result<()> {
        let base_path = if let Some(config) = config {
            config
                .data_path
                .clone()
                .unwrap_or_else(|| self.config.data_path.clone())
        } else {
            self.config.data_path.clone()
        };

        if self.config.auto_create_directories {
            std::fs::create_dir_all(&base_path)
                .map_err(|e| anyhow!("Failed to create directory {:?}: {}", base_path, e))?;
        }

        let dataset = Dataset::new(namespace.to_string(), base_path)?;

        // Validate the dataset
        dataset.validate_all_schemas().map_err(|e| {
            anyhow!(
                "Schema validation failed for namespace {}: {}",
                namespace,
                e
            )
        })?;

        info!("Successfully created dataset for namespace: {}", namespace);
        self.datasets.insert(namespace.to_string(), dataset);

        Ok(())
    }

    /// Get the default namespace dataset
    pub fn default_dataset(&mut self) -> Result<&Dataset> {
        let default_ns = self.config.default_namespace.clone();
        self.get_or_create_dataset(&default_ns)
    }

    /// List all available namespaces
    pub fn list_namespaces(&self) -> Vec<String> {
        self.datasets.keys().cloned().collect()
    }

    /// Get configuration
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Get dataset statistics for all namespaces
    pub fn get_all_stats(
        &self,
    ) -> Result<HashMap<String, super::dataset_usage_example::DatasetStats>> {
        let mut all_stats = HashMap::new();

        for (namespace, dataset) in &self.datasets {
            match dataset.stats() {
                Ok(stats) => {
                    all_stats.insert(namespace.clone(), stats);
                }
                Err(e) => {
                    warn!("Failed to get stats for namespace {}: {}", namespace, e);
                }
            }
        }

        Ok(all_stats)
    }

    /// Validate configuration
    pub fn validate_config(&self) -> Result<()> {
        // Check data path exists or can be created
        if !self.config.data_path.exists() && !self.config.auto_create_directories {
            return Err(anyhow!(
                "Data path {:?} does not exist and auto_create_directories is false",
                self.config.data_path
            ));
        }

        // Validate namespace names
        for namespace_config in &self.config.namespaces {
            if namespace_config.name.is_empty() {
                return Err(anyhow!("Empty namespace name found in configuration"));
            }

            if namespace_config
                .name
                .contains(['/', '\\', ':', '*', '?', '"', '<', '>', '|'])
            {
                return Err(anyhow!(
                    "Invalid characters in namespace name: {}",
                    namespace_config.name
                ));
            }
        }

        // Check for duplicate namespace names
        let mut seen_namespaces = std::collections::HashSet::new();
        for namespace_config in &self.config.namespaces {
            if !seen_namespaces.insert(&namespace_config.name) {
                return Err(anyhow!(
                    "Duplicate namespace name: {}",
                    namespace_config.name
                ));
            }
        }

        Ok(())
    }
}

impl Dataset {
    /// Create a Dataset with server startup configuration
    pub fn from_server_config(namespace: String, config: &ServerConfig) -> Result<Self> {
        let base_path = config.data_path.clone();

        if config.auto_create_directories {
            let namespace_path = base_path.join(&namespace);
            std::fs::create_dir_all(&namespace_path).map_err(|e| {
                anyhow!(
                    "Failed to create namespace directory {:?}: {}",
                    namespace_path,
                    e
                )
            })?;
        }

        info!(
            "Creating dataset for namespace '{}' with server config",
            namespace
        );
        let dataset = Self::new(namespace, base_path)?;

        // Apply validation rules
        if config.validation.require_non_empty_id {
            debug!("ID validation enabled for namespace");
        }

        dataset.validate_all_schemas()?;

        Ok(dataset)
    }

    /// Quick setup method for simple server startup
    pub fn quick_setup<P: AsRef<std::path::Path>>(
        namespace: &str,
        data_path: P,
    ) -> Result<DatasetManager> {
        info!(
            "Quick setup for namespace '{}' at path {:?}",
            namespace,
            data_path.as_ref()
        );

        DatasetManager::with_defaults(data_path, namespace)
    }

    /// Setup from configuration file
    pub fn setup_from_config<P: AsRef<std::path::Path>>(config_path: P) -> Result<DatasetManager> {
        info!("Setting up from config file: {:?}", config_path.as_ref());

        DatasetManager::from_config_file(config_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_dataset_manager_creation() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let manager = DatasetManager::with_defaults(temp_dir.path(), "test_namespace")?;

        assert_eq!(manager.list_namespaces(), vec!["test_namespace"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_config_validation() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut config = ServerConfig::default();
        config.data_path = temp_dir.path().to_path_buf();

        let manager = DatasetManager::from_config(config)?;
        manager.validate_config()?;

        Ok(())
    }
}
