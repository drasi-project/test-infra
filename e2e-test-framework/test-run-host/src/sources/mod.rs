use std::{collections::HashSet, fmt};

use derive_more::Debug;
use serde::{Deserialize, Serialize};

use bootstrap_data_generators::{create_bootstrap_data_generator, BootstrapData, BootstrapDataGenerator};
use source_change_generators::{create_source_change_generator, SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState};
use test_data_store::{test_repo_storage::{models::{BootstrapDataGeneratorDefinition, QueryId, SourceChangeDispatcherDefinition, SourceChangeGeneratorDefinition, SpacingMode, TestSourceDefinition, TimeMode}, TestSourceStorage}, test_run_storage::{ParseTestRunIdError, ParseTestRunSourceIdError, TestRunId, TestRunSourceId, TestRunSourceStorage}};

pub mod bootstrap_data_generators;
pub mod source_change_generators;
pub mod source_change_dispatchers;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunSourceOverrides {
    pub bootstrap_data_generator: Option<TestRunBootstrapDataGeneratorOverrides>,
    pub source_change_dispatchers: Option<Vec<SourceChangeDispatcherDefinition>>,
    pub source_change_generator: Option<TestRunSourceChangeGeneratorOverrides>,
    pub subscribers: Option<Vec<QueryId>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunBootstrapDataGeneratorOverrides {
    #[serde(default)]
    pub time_mode: Option<TimeMode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunSourceChangeGeneratorOverrides {
    pub spacing_mode: Option<SpacingMode>,
    pub time_mode: Option<TimeMode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunSourceConfig {
    pub start_immediately: Option<bool>,    
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: Option<String>,
    pub test_run_overrides: Option<TestRunSourceOverrides>,
    pub test_source_id: String,
}

impl TryFrom<&TestRunSourceConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        Ok(TestRunId::new(
            &value.test_repo_id, 
            &value.test_id, 
            value.test_run_id
                .as_deref()
                .unwrap_or(&chrono::Utc::now().format("%Y%m%d%H%M%S").to_string())))
    }
}

impl TryFrom<&TestRunSourceConfig> for TestRunSourceId {
    type Error = ParseTestRunSourceIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        match TestRunId::try_from(value) {
            Ok(test_run_id) => {
                Ok(TestRunSourceId::new(&test_run_id, &value.test_source_id))
            }
            Err(e) => return Err(ParseTestRunSourceIdError::InvalidValues(e.to_string())),
        }
    }
}

impl fmt::Display for TestRunSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestRunSourceDefinition: Repo: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_source_id: {:?}", 
            self.test_repo_id, self.test_id, self.test_run_id, self.test_source_id)
    }
}


#[derive(Clone, Debug)]
pub struct TestRunSourceDefinition {
    pub bootstrap_data_generator_def: Option<BootstrapDataGeneratorDefinition>,
    pub id: TestRunSourceId,
    pub source_change_dispatcher_defs: Vec<SourceChangeDispatcherDefinition>,
    pub source_change_generator_def: Option<SourceChangeGeneratorDefinition>,
    pub start_immediately: bool,    
    pub subscribers: Vec<QueryId>,
}

impl TestRunSourceDefinition {
    pub fn new( test_run_source_config: TestRunSourceConfig, test_source_definition: TestSourceDefinition) -> anyhow::Result<Self> {
            
        let mut def = Self {
            bootstrap_data_generator_def: test_source_definition.bootstrap_data_generator_def.clone(),
            id: TestRunSourceId::try_from(&test_run_source_config)?,
            source_change_dispatcher_defs: test_source_definition.source_change_dispatcher_defs.clone(),
            source_change_generator_def: test_source_definition.source_change_generator_def.clone(),
            start_immediately: test_run_source_config.start_immediately.unwrap_or(false),
            subscribers: test_source_definition.subscribers.clone(),
        };

        if let Some(overrides) = test_run_source_config.test_run_overrides {
            if let Some(bdg_overrides) = overrides.bootstrap_data_generator {
                match &mut def.bootstrap_data_generator_def {
                    Some(BootstrapDataGeneratorDefinition::Script { common_config, .. }) => {
                        if let Some(time_mode) = bdg_overrides.time_mode {
                            common_config.time_mode = time_mode.clone();
                        }
                    },
                    None => {}
                }
            }

            if let Some(scg_overrides) = overrides.source_change_generator {
                match &mut def.source_change_generator_def {
                    Some(SourceChangeGeneratorDefinition::Script { common_config, .. }) => {
                        if let Some(spacing_mode) = scg_overrides.spacing_mode {
                            common_config.spacing_mode = spacing_mode.clone();
                        }
                        if let Some(time_mode) = scg_overrides.time_mode {
                            common_config.time_mode = time_mode.clone();
                        }
                    },
                    None => {}
                }
            }
            
            if let Some(dispatchers) = overrides.source_change_dispatchers {
                def.source_change_dispatcher_defs = dispatchers;
            }

            if let Some(subscribers) = overrides.subscribers {
                def.subscribers = subscribers;
            }
        };

        Ok(def)
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
            output_storage,
            definition.source_change_dispatcher_defs
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

    pub async fn reset_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.reset().await?;
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