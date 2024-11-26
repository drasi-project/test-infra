use std::fmt;

use serde::{Deserialize, Serialize};
use test_data_store::{
    test_repo_storage::models::{SpacingMode, TimeMode}, 
    test_run_storage::{ParseTestRunIdError, ParseTestRunSourceIdError, TestRunId, TestRunSourceId}
};

#[derive(Debug, Deserialize, Serialize)]
pub struct TestRunnerConfig {
    #[serde(default)]
    pub sources: Vec<TestRunSourceConfig>,
}

impl Default for TestRunnerConfig {
    fn default() -> Self {
        TestRunnerConfig {
            sources: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestRunSourceConfig {
    pub test_repo_id: String,
    pub test_id: String,
    #[serde(default = "random_test_run_id")]
    pub test_run_id: String,
    pub test_source_id: String,
    #[serde(default = "is_true")]
    pub start_immediately: bool,    
    pub bootstrap_data_generator: Option<BootstrapDataGeneratorConfig>,
    pub source_change_generator: Option<SourceChangeGeneratorConfig>,
}
fn is_true() -> bool { false }
fn random_test_run_id() -> String { chrono::Utc::now().format("%Y%m%d%H%M%S").to_string() }

impl TryFrom<&TestRunSourceConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        Ok(TestRunId {
            test_repo_id: value.test_repo_id.clone(),
            test_id: value.test_id.clone(),
            test_run_id: value.test_run_id.clone(),
        })
    }
}

impl TryFrom<&TestRunSourceConfig> for TestRunSourceId {
    type Error = ParseTestRunSourceIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        match TestRunId::try_from(value) {
            Ok(test_run_id) => {
                Ok(TestRunSourceId::new(&test_run_id, &value.test_source_id.clone()))
            }
            Err(e) => return Err(ParseTestRunSourceIdError::InvalidValues(e.to_string())),
        }
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
    #[serde(default)]
    pub time_mode: Option<TimeMode>,
}

// impl BootstrapDataGeneratorConfig {
//     pub async fn new_with_defaults( config: Option<Self>,  defaults: Option<Self>) -> anyhow::Result<Option<Self>> {

//         let merged = match config {
//             Some(cfg) => {
//                 match defaults {
//                     Some(def) => {
//                         Self {
//                             time_mode: cfg.time_mode.or(def.time_mode).or(Some(TimeMode::default())),
//                         }
//                     },
//                     None => cfg
//                 }
//             }
//             None => {
//                 match defaults {
//                     Some(def) => def,
//                     None => {
//                         return Ok(None);
//                     }
//                 }
//             }
//         };

//         Ok(Some(merged))
//     }
// }


#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SourceChangeGeneratorConfig {
    Script {
        #[serde(flatten)]
        common_config: CommonSourceChangeGeneratorConfig,
        #[serde(flatten)]
        unique_config: ScriptSourceChangeGeneratorConfig,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonSourceChangeGeneratorConfig {
    #[serde(default)]
    pub dispatchers: Vec<SourceChangeDispatcherConfig>,
    #[serde(default)]
    pub spacing_mode: SpacingMode,
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptSourceChangeGeneratorConfig {
    #[serde(default = "is_false")]
    pub ignore_scripted_pause_commands: bool,
}
fn is_false() -> bool { false }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
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

