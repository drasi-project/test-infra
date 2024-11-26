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
    pub bootstrap_data_generator: Option<BootstrapDataGeneratorConfig>,
    pub source_change_generator: Option<SourceChangeGeneratorConfig>,
}
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

// The TestRunSourceConfig is what is loaded from the TestRunner config file or passed in to the Web API 
// to create a new Source.
// #[derive(Clone, Debug, Deserialize, Serialize)]
// pub struct TestRunSourceConfig {
//     pub test_repo_id: Option<String>,
//     pub test_id: Option<String>,
//     pub test_run_id: Option<String>,
//     pub test_source_id: Option<String>,
//     pub bootstrap_data_generator: Option<BootstrapDataGeneratorConfig>,
//     pub source_change_generator: Option<SourceChangeGeneratorConfig>,
// }

// impl Default for TestRunSourceConfig {
//     fn default() -> Self {
//         TestRunSourceConfig {
//             test_repo_id: None,
//             test_id: None,
//             test_run_id: None,
//             test_source_id: None,
//             bootstrap_data_generator: None,
//             source_change_generator: None,
//         }
//     }
// }

// impl TestRunSourceConfig {
//     pub async fn new( config: Self, defaults: Self ) -> anyhow::Result<Self> {

//         let mut merged = Self {
//             test_repo_id: config.test_repo_id.or(defaults.test_repo_id),
//             test_id: config.test_id.or(defaults.test_id),
//             test_run_id: config.test_run_id.or(defaults.test_run_id),
//             test_source_id: config.test_source_id.or(defaults.test_source_id),
//             bootstrap_data_generator: BootstrapDataGeneratorConfig::new_with_defaults(config.bootstrap_data_generator, defaults.bootstrap_data_generator).await?,
//             source_change_generator: SourceChangeGeneratorConfig::new_with_defaults(config.source_change_generator, defaults.source_change_generator).await?,
//         };

//         // Validate the merged TestRunSourceConfig
//         if merged.test_repo_id.is_none() {
//             anyhow::bail!("No test_repo_id provided and no default value found.");
//         } else if merged.test_id.is_none() {
//             anyhow::bail!("No test_id provided and no default value found.");
//         } else if merged.test_source_id.is_none() {
//             anyhow::bail!("No test_source_id provided and no default value found.");
//         };

//         // Fill in missing test_run_id
//         if merged.test_run_id.is_none() {
//             merged.test_run_id = Some(chrono::Utc::now().format("%Y%m%d%H%M%S").to_string());
//         }

//         Ok(merged)
//     }
// }

// impl TryFrom<&TestRunSourceConfig> for TestRunId {
//     type Error = ParseTestRunIdError;

//     fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
//         let test_repo_id = value.test_repo_id.as_ref().ok_or_else(|| ParseTestRunIdError::InvalidValues("test_repo_id".to_string()))?;
//         let test_id = value.test_id.as_ref().ok_or_else(|| ParseTestRunIdError::InvalidValues("test_id".to_string()))?;
//         let test_run_id = value.test_run_id.as_ref().ok_or_else(|| ParseTestRunIdError::InvalidValues("test_run_id".to_string()))?;

//         Ok(TestRunId::new(test_repo_id, test_id, test_run_id))
//     }
// }

// impl TryFrom<&TestRunSourceConfig> for TestRunSourceId {
//     type Error = ParseTestRunSourceIdError;

//     fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
//         match TestRunId::try_from(value) {
//             Ok(test_run_id) => {
//                 let test_source_id = value.test_source_id.as_ref().ok_or_else(|| ParseTestRunSourceIdError::InvalidValues("test_source_id".to_string()))?;
//                 Ok(TestRunSourceId::new(&test_run_id, test_source_id))
//             }
//             Err(e) => return Err(ParseTestRunSourceIdError::InvalidValues(e.to_string())),
//         }
//     }
// }

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

// impl Default for BootstrapDataGeneratorConfig {
//     fn default() -> Self {
//         BootstrapDataGeneratorConfig {
//             time_mode: Some(TimeMode::default()),
//         }
//     }
// }

impl BootstrapDataGeneratorConfig {
    pub async fn new_with_defaults( config: Option<Self>,  defaults: Option<Self>) -> anyhow::Result<Option<Self>> {

        let merged = match config {
            Some(cfg) => {
                match defaults {
                    Some(def) => {
                        Self {
                            time_mode: cfg.time_mode.or(def.time_mode).or(Some(TimeMode::default())),
                        }
                    },
                    None => cfg
                }
            }
            None => {
                match defaults {
                    Some(def) => def,
                    None => {
                        return Ok(None);
                    }
                }
            }
        };

        Ok(Some(merged))
    }
}


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
    #[serde(default = "is_true")]
    pub start_immediately: bool,
    #[serde(default)]
    pub time_mode: TimeMode,
}
fn is_true() -> bool { true }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptSourceChangeGeneratorConfig {
    #[serde(default = "is_false")]
    pub ignore_scripted_pause_commands: bool,
}
fn is_false() -> bool { false }

// #[derive(Clone, Debug, Deserialize, Serialize)]
// pub struct SourceChangeGeneratorConfig {
//     pub dispatchers: Option<Vec<SourceChangeDispatcherConfig>>,
//     pub ignore_scripted_pause_commands: Option<bool>,
//     pub spacing_mode: Option<SpacingMode>,
//     pub start_immediately: Option<bool>,
//     pub time_mode: Option<TimeMode>,
// }

// impl Default for SourceChangeGeneratorConfig {
//     fn default() -> Self {
//         SourceChangeGeneratorConfig {
//             dispatchers: None,
//             ignore_scripted_pause_commands: Some(false),
//             spacing_mode: Some(SpacingMode::default()),
//             start_immediately: Some(true),
//             time_mode: Some(TimeMode::default()),
//         }
//     }
// }

// impl SourceChangeGeneratorConfig {
//     pub async fn new_with_defaults( config: Option<Self>, defaults: Option<Self> ) -> anyhow::Result<Option<Self>> {

//         let mut merged = match config {
//             Some(cfg) => {
//                 match defaults {
//                     Some(def) => {
//                         Self {
//                             dispatchers: cfg.dispatchers.or(def.dispatchers),
//                             ignore_scripted_pause_commands: cfg.ignore_scripted_pause_commands.or(def.ignore_scripted_pause_commands),
//                             spacing_mode: cfg.spacing_mode.or(def.spacing_mode),
//                             start_immediately: cfg.start_immediately.or(def.start_immediately),
//                             time_mode: cfg.time_mode.or(def.time_mode),
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

//         // Fill in missing values
//         if merged.dispatchers.is_none() {
//             merged.dispatchers = Some(Vec::new());
//         };
//         if merged.ignore_scripted_pause_commands.is_none() {
//             merged.ignore_scripted_pause_commands = Some(false);
//         };
//         if merged.spacing_mode.is_none() {
//             merged.spacing_mode = Some(SpacingMode::default());
//         };
//         if merged.start_immediately.is_none() {
//             merged.start_immediately = Some(true);
//         };
//         if merged.time_mode.is_none() {
//             merged.time_mode = Some(TimeMode::default());
//         };

//         Ok(Some(merged))
//     }
// }

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

