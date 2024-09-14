use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Deserialize, Serialize)]
pub struct TestRunnerConfig {
    #[serde(default = "default_data_store_path")]
    pub data_store_path: String,
    #[serde(default = "prune_data_store_path")]
    pub prune_data_store_path: bool,
    #[serde(default)]
    pub source_defaults: SourceConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub test_repos: Vec<TestRepoConfig>,
}
fn default_data_store_path() -> String { "./test_runner_data".to_string() }
fn prune_data_store_path() -> bool { false }

impl Default for TestRunnerConfig {
    fn default() -> Self {
        TestRunnerConfig {
            data_store_path: default_data_store_path(),
            prune_data_store_path: false,
            source_defaults: SourceConfig::default(),
            test_repos: Vec::new(),
            sources: Vec::new(),
        }
    }
}

// The SourceConfig is what is loaded from the TestRunner config file or passed in to the Web API 
// to create a new Source.
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