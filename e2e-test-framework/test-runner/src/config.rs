use std::fmt;

use serde::{Deserialize, Serialize};
use test_data_store::test_run_storage::{ParseTestRunSourceIdError, TestRunSourceId};

#[derive(Debug, Deserialize, Serialize)]
pub struct TestRunnerConfig {
    #[serde(default)]
    pub source_defaults: SourceConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
}

impl Default for TestRunnerConfig {
    fn default() -> Self {
        TestRunnerConfig {
            source_defaults: SourceConfig::default(),
            sources: Vec::new(),
        }
    }
}

// The SourceConfig is what is loaded from the TestRunner config file or passed in to the Web API 
// to create a new Source.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceConfig {
    pub test_repo_id: Option<String>,
    pub test_id: Option<String>,
    pub test_run_id: Option<String>,
    pub test_source_id: Option<String>,
    pub proxy: Option<ProxyConfig>,
    pub reactivator: Option<ReactivatorConfig>,
}

impl Default for SourceConfig {
    fn default() -> Self {
        SourceConfig {
            test_repo_id: None,
            test_id: None,
            test_run_id: None,
            test_source_id: None,
            proxy: None,
            reactivator: None,
        }
    }
}

impl TryFrom<&SourceConfig> for TestRunSourceId {
    type Error = ParseTestRunSourceIdError;

    fn try_from(value: &SourceConfig) -> Result<Self, Self::Error> {
        let test_repo_id = value.test_repo_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_repo_id".to_string()))?;
        let test_run_id = value.test_run_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_run_id".to_string()))?;
        let test_id = value.test_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_id".to_string()))?;
        let test_source_id = value.test_source_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_source_id".to_string()))?;

        Ok(TestRunSourceId::new(test_run_id, test_repo_id, test_id, test_source_id))
    }
}

impl fmt::Display for SourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SourceConfig: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_source_id: {:?}, proxy: {:?}, reactivator: {:?}", 
            self.test_repo_id, self.test_id, self.test_run_id, self.test_source_id, self.proxy, self.reactivator)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
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