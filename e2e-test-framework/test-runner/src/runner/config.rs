use serde::{Deserialize, Serialize, Serializer};

// The ServiceConfig is what is loaded from the Service config File. It can contain the configurations for multiple Change Script Players,
// as well as default values that are used when a Change Script Player doesn't specify a value.
#[derive(Debug, Deserialize, Serialize)]
pub struct ServiceConfig {
    #[serde(default)]
    pub source_defaults: SourceConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub test_repos: Vec<TestRepoConfig>,
}

impl ServiceConfig {
    pub fn from_file_path(config_file_path: &str) -> anyhow::Result<Self> {
        // Validate that the file exists and if not return an error.
        if !std::path::Path::new(config_file_path).exists() {
            anyhow::bail!("Service Config file not found: {}", config_file_path);
        }

        // Read the file content into a string.
        let config_file_json = std::fs::read_to_string(config_file_path)?;

        // Parse the string into a ServiceConfig struct.
        let service_config_file = serde_json::from_str(&config_file_json)?;

        Ok(service_config_file)
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        ServiceConfig {
            source_defaults: SourceConfig::default(),
            test_repos: Vec::new(),
            sources: Vec::new(),
        }
    }
}

// The SourceConfig is what is loaded from the Service config file or passed in to the Web API 
// to create a new Change Script Player.
#[derive(Debug, Deserialize, Serialize)]
pub struct SourceConfig {
    pub test_repo_id: Option<String>,
    pub test_id: Option<String>,
    pub test_run_id: Option<String>,
    pub source_id: Option<String>,
    pub proxy: Option<ProxyConfig>,
    pub reactivator: Option<ReactivatorConfig>,
}

impl Default for SourceConfig {
    fn default() -> Self {
        SourceConfig {
            test_repo_id: None,
            test_id: None,
            test_run_id: None,
            source_id: None,
            proxy: None,
            reactivator: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProxyConfig {
    pub time_mode: Option<String>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            time_mode: Some("Recorded".to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReactivatorConfig {
    pub dispatchers: Option<Vec<SourceChangeDispatcherConfig>>,
    pub ignore_scripted_pause_commands: Option<bool>,
    pub spacing_mode: Option<String>,
    pub start_immediately: Option<bool>,
    pub time_mode: Option<String>,
}

impl Default for ReactivatorConfig {
    fn default() -> Self {
        ReactivatorConfig {
            dispatchers: None,
            ignore_scripted_pause_commands: Some(false),
            spacing_mode: Some("Recorded".to_string()),
            start_immediately: Some(true),
            time_mode: Some("Recorded".to_string()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceChangeDispatcherConfig {
    Console(ConsoleSourceChangeDispatcherConfig),
    Dapr(DaprSourceChangeDispatcherConfig),
    JsonlFile(JsonlFileSourceChangeDispatcherConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleSourceChangeDispatcherConfig {
    pub date_time_format: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaprSourceChangeDispatcherConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pubsub_name: Option<String>,
    pub pubsub_topic: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileSourceChangeDispatcherConfig {
    pub folder_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TestRepoConfig {
    AzureStorageBlob {
        #[serde(flatten)]
        common_config: CommonTestRepoConfig,
        #[serde(flatten)]
        unique_config: AzureStorageBlobTestRepoConfig,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonTestRepoConfig {
    #[serde(default = "is_false")]
    pub force_cache_refresh: bool,
    pub id: String,
}
fn is_false() -> bool { false }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AzureStorageBlobTestRepoConfig {
    pub account_name: String,
    #[serde(serialize_with = "mask_secret")]
    pub access_key: String,
    pub container: String,
    pub root_path: String,
}
pub fn mask_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("******")
}