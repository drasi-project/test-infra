use serde::{Deserialize, Serialize};

use test_data_store::TestDataStoreConfig;

#[derive(Debug, Deserialize, Serialize)]
pub struct TestRunnerConfig {
    #[serde(default)]
    pub data_store: TestDataStoreConfig,
    #[serde(default)]
    pub source_defaults: SourceConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default = "default_start_reactivators_together")]
    pub start_reactivators_together: bool,
}
fn default_start_reactivators_together() -> bool { true }

impl Default for TestRunnerConfig {
    fn default() -> Self {
        TestRunnerConfig {
            data_store: TestDataStoreConfig::default(),
            source_defaults: SourceConfig::default(),
            sources: Vec::new(),
            start_reactivators_together: default_start_reactivators_together(),
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