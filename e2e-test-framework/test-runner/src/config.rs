use std::fmt;

use serde::{Deserialize, Serialize};
use test_data_store::{
    test_repo_storage::test_metadata::{SpacingMode, TimeMode}, 
    test_run_storage::{ParseTestRunSourceIdError, TestRunSourceId}
};

#[derive(Debug, Deserialize, Serialize)]
pub struct TestRunnerConfig {
    #[serde(default)]
    pub source_defaults: TestRunSourceConfig,
    #[serde(default)]
    pub sources: Vec<TestRunSourceConfig>,
}

impl Default for TestRunnerConfig {
    fn default() -> Self {
        TestRunnerConfig {
            source_defaults: TestRunSourceConfig::default(),
            sources: Vec::new(),
        }
    }
}

// The TestRunSourceConfig is what is loaded from the TestRunner config file or passed in to the Web API 
// to create a new Source.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestRunSourceConfig {
    pub test_repo_id: Option<String>,
    pub test_id: Option<String>,
    pub test_run_id: Option<String>,
    pub test_source_id: Option<String>,
    pub bootstrap_data_generator: Option<BootstrapDataGeneratorConfig>,
    pub source_change_generator: Option<SourceChangeGeneratorConfig>,
}

impl Default for TestRunSourceConfig {
    fn default() -> Self {
        TestRunSourceConfig {
            test_repo_id: None,
            test_id: None,
            test_run_id: None,
            test_source_id: None,
            bootstrap_data_generator: None,
            source_change_generator: None,
        }
    }
}

impl TryFrom<&TestRunSourceConfig> for TestRunSourceId {
    type Error = ParseTestRunSourceIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        let test_repo_id = value.test_repo_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_repo_id".to_string()))?;
        let test_run_id = value.test_run_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_run_id".to_string()))?;
        let test_id = value.test_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_id".to_string()))?;
        let test_source_id = value.test_source_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_source_id".to_string()))?;

        Ok(TestRunSourceId::new(test_repo_id, test_id, test_run_id, test_source_id))
    }
}

impl fmt::Display for TestRunSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestRunSourceConfig: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_source_id: {:?}, bootstrap_data_generator: {:?}, source_change_generator: {:?}", 
            self.test_repo_id, self.test_id, self.test_run_id, self.test_source_id, self.bootstrap_data_generator, self.source_change_generator)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BootstrapDataGeneratorConfig {
    pub time_mode: Option<TimeMode>,
}

impl Default for BootstrapDataGeneratorConfig {
    fn default() -> Self {
        BootstrapDataGeneratorConfig {
            time_mode: Some(TimeMode::Recorded),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceChangeGeneratorConfig {
    pub dispatchers: Option<Vec<SourceChangeDispatcherConfig>>,
    pub ignore_scripted_pause_commands: Option<bool>,
    pub spacing_mode: Option<SpacingMode>,
    pub start_immediately: Option<bool>,
    pub time_mode: Option<TimeMode>,
}

impl Default for SourceChangeGeneratorConfig {
    fn default() -> Self {
        SourceChangeGeneratorConfig {
            dispatchers: None,
            ignore_scripted_pause_commands: Some(false),
            spacing_mode: Some(SpacingMode::Recorded),
            start_immediately: Some(true),
            time_mode: Some(TimeMode::Recorded),
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