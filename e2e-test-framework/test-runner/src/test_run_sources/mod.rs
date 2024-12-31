use std::{collections::HashSet, fmt};

use derive_more::Debug;
use serde::{Deserialize, Serialize};
use test_data_store::{test_repo_storage::{models::{BootstrapDataGeneratorDefinition, QueryId, SourceChangeGeneratorDefinition, SpacingMode, TestSourceDefinition}, TestSourceStorage}, test_run_storage::{ParseTestRunIdError, ParseTestRunSourceIdError, TestRunId, TestRunSourceId, TestRunSourceStorage}};

use crate::{bootstrap_data_generators::{create_bootstrap_data_generator, BootstrapData, BootstrapDataGenerator}, source_change_generators::{create_source_change_generator, SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState}};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum TestRunSourceConfig {
    Inline {
        start_immediately: Option<bool>,    
        test_id: String,
        test_repo_id: String,
        test_run_id: Option<String>,
        #[serde(flatten)]
        test_source_definition: TestSourceDefinition,
    },
    Repo {
        start_immediately: Option<bool>,    
        test_id: String,
        test_repo_id: String,
        test_run_id: Option<String>,
        test_run_overrides: Option<TestSourceDefinition>,
        test_source_id: String,
    },
}

impl TryFrom<&TestRunSourceConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        match value {
            TestRunSourceConfig::Inline { test_id, test_run_id, .. } => {
                Ok(TestRunId::new(
                    "local", 
                    test_id, 
                    test_run_id
                        .as_deref()
                        .unwrap_or(&chrono::Utc::now().format("%Y%m%d%H%M%S").to_string())))
            },
            TestRunSourceConfig::Repo { test_repo_id, test_id, test_run_id, .. } => {
                Ok(TestRunId::new(
                    test_repo_id, 
                    test_id, 
                    test_run_id
                        .as_deref()
                        .unwrap_or(&chrono::Utc::now().format("%Y%m%d%H%M%S").to_string())))
            }
        }
    }
}

impl TryFrom<&TestRunSourceConfig> for TestRunSourceId {
    type Error = ParseTestRunSourceIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        match TestRunId::try_from(value) {
            Ok(test_run_id) => {
                match value {
                    TestRunSourceConfig::Inline { test_source_definition, .. } => {
                        Ok(TestRunSourceId::new(&test_run_id, &test_source_definition.test_source_id))
                    },
                    TestRunSourceConfig::Repo { test_source_id, .. } => {
                        Ok(TestRunSourceId::new(&test_run_id, test_source_id))
                    }
                }
            }
            Err(e) => return Err(ParseTestRunSourceIdError::InvalidValues(e.to_string())),
        }
    }
}

impl fmt::Display for TestRunSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TestRunSourceConfig::Inline { test_id, test_run_id, test_source_definition, .. } => {
                write!(f, "TestRunSourceDefinition: Inline: test_id: {:?}, test_run_id: {:?}, test_source_id: {:?}", test_id, test_run_id, test_source_definition.test_source_id)
            },
            TestRunSourceConfig::Repo { test_repo_id, test_id, test_run_id, test_source_id, .. } => {
                write!(f, "TestRunSourceDefinition: Repo: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_source_id: {:?}", test_repo_id, test_id, test_run_id, test_source_id)
            }
        }
    }
}


#[derive(Clone, Debug)]
pub struct TestRunSourceDefinition {
    pub bootstrap_data_generator_def: Option<BootstrapDataGeneratorDefinition>,
    pub id: TestRunSourceId,
    pub source_change_generator_def: Option<SourceChangeGeneratorDefinition>,
    pub start_immediately: bool,    
    pub subscribers: Vec<QueryId>,
}

impl TestRunSourceDefinition {
    pub fn new( test_run_source_config: TestRunSourceConfig, test_source_definition: Option<TestSourceDefinition> ) -> anyhow::Result<Self> {
        let id = TestRunSourceId::try_from(&test_run_source_config)?;

        match (&test_run_source_config, test_source_definition) {
            (TestRunSourceConfig::Inline { start_immediately, test_source_definition, .. }, _) => {
                Ok(Self {
                    bootstrap_data_generator_def: test_source_definition.bootstrap_data_generator_def.clone(),
                    id,
                    source_change_generator_def: test_source_definition.source_change_generator_def.clone(),
                    start_immediately: start_immediately.unwrap_or(false), 
                    subscribers: test_source_definition.subscribers.clone(),
                })
            },
            (TestRunSourceConfig::Repo { start_immediately, test_run_overrides, .. }, Some(test_source_definition)) => {

                // If test_run_overrides is Some, use the values from test_run_overrides if they are available, 
                // otherwise use the values from if they are available.
                let bootstrap_data_generator_def = match test_run_overrides {
                    Some(overrides) => match &overrides.bootstrap_data_generator_def {
                        Some(definition) => Some(definition.clone()),
                        None => test_source_definition.bootstrap_data_generator_def.clone(),
                    },
                    None => test_source_definition.bootstrap_data_generator_def.clone(),
                };

                let source_change_dispatchers_def = match test_run_overrides {
                    Some(overrides) => match &overrides.source_change_generator_def {
                        Some(definition) => Some(definition.clone()),
                        None => test_source_definition.source_change_generator_def.clone(),
                    },
                    None => test_source_definition.source_change_generator_def.clone(),
                };

                let subscribers = match test_run_overrides {
                    Some(overrides) => match &overrides.subscribers.len() {
                        0 => test_source_definition.subscribers.clone(),
                        _ => overrides.subscribers.clone(),
                    },
                    None => test_source_definition.subscribers.clone(),
                };

                Ok(Self {
                    bootstrap_data_generator_def,
                    id,
                    source_change_generator_def: source_change_dispatchers_def,
                    start_immediately: start_immediately.unwrap_or(false),
                    subscribers,
                })
            },
            (TestRunSourceConfig::Repo { .. }, None) => {
                anyhow::bail!("Attempt to create TestRunSourceDefinition from repo without definition {:?}", test_run_source_config);
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TestRunSourceState {
    pub id: TestRunSourceId,
    pub source_change_generator: SourceChangeGeneratorState,
    pub start_immediately: bool,
}

#[derive(Debug)]
pub struct TestRunSource {
    #[debug(skip)]
    pub bootstrap_data_generator: Option<Box<dyn BootstrapDataGenerator + Send + Sync>>,
    pub id: TestRunSourceId,
    #[debug(skip)]
    pub source_change_generator: Option<Box<dyn SourceChangeGenerator + Send + Sync>>,    
    pub start_immediately: bool,
    pub subscribers: Vec<QueryId>
}

impl TestRunSource {
    pub async fn new(
        definition: TestRunSourceDefinition,
        input_storage: TestSourceStorage, 
        output_storage: TestRunSourceStorage
    ) -> anyhow::Result<Self> {

        let bootstrap_data_generator = create_bootstrap_data_generator(
            definition.id.clone(),
            definition.bootstrap_data_generator_def,
            input_storage.clone(),
            output_storage.clone()
        ).await?;

        let source_change_generator = create_source_change_generator(
            definition.id.clone(),
            definition.source_change_generator_def,
            input_storage,
            output_storage
        ).await?;
    
        Ok(Self { 
            id: definition.id.clone(),
            start_immediately: definition.start_immediately,
            bootstrap_data_generator, 
            source_change_generator,
            subscribers: definition.subscribers,
        })
    }

    pub async fn get_bootstrap_data_for_query(&self, query_id: QueryId, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        log::debug!("Query ID: {:?}, Node Labels: {:?}, Rel Labels: {:?}", query_id, node_labels, rel_labels);

        // If the QueryId is in the subscribers list, return the BootstrapData.
        if self.bootstrap_data_generator.is_some() && self.subscribers.contains(&query_id) {
            self.bootstrap_data_generator.as_ref().unwrap().get_data(node_labels, rel_labels).await
        } else {
            Ok(BootstrapData::new())
        }
    }

    pub async fn get_state(&self) -> anyhow::Result<TestRunSourceState> {

        Ok(TestRunSourceState {
            id: self.id.clone(),
            source_change_generator: self.get_source_change_generator_state().await?,
            start_immediately: self.start_immediately,
        })
    }

    pub async fn get_source_change_generator_state(&self) -> anyhow::Result<SourceChangeGeneratorState> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.get_state().await?;
                Ok(response.state)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn pause_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.pause().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn skip_source_change_generator(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.skip(skips, spacing_mode).await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn start_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.start().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn step_source_change_generator(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.step(steps, spacing_mode).await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn stop_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.stop().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }
}