use std::{collections::HashSet, fmt};

use derive_more::Debug;
use serde::{Deserialize, Serialize};
use test_data_store::{test_repo_storage::{models::{BootstrapDataGeneratorDefinition, QueryId, SourceChangeGeneratorDefinition, SourceDefinition}, TestSourceStorage}, test_run_storage::{ParseTestRunIdError, ParseTestRunSourceIdError, TestRunId, TestRunSourceId, TestRunSourceStorage}};

use crate::{bootstrap_data_generators::{create_bootstrap_data_generator, BootstrapData, BootstrapDataGenerator}, source_change_generators::{create_source_change_generator, SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState}};


#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestRunSourceConfig {
    pub test_repo_id: String,
    pub test_id: String,
    #[serde(default = "random_test_run_id")]
    pub test_run_id: String,
    pub test_source_id: String,
    #[serde(default = "is_false")]
    pub start_immediately: bool,    
    pub bootstrap_data_generator: Option<BootstrapDataGeneratorDefinition>,
    pub source_change_generator: Option<SourceChangeGeneratorDefinition>,
}
fn is_false() -> bool { false }
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

#[derive(Debug, Serialize)]
pub struct TestRunSourceState {
    pub id: TestRunSourceId,
    pub source_change_generator: SourceChangeGeneratorState,
}

#[derive(Debug)]
pub struct TestRunSource {
    pub id: TestRunSourceId,
    pub start_immediately: bool,
    #[debug(skip)]
    pub bootstrap_data_generator: Option<Box<dyn BootstrapDataGenerator + Send + Sync>>,
    #[debug(skip)]
    pub source_change_generator: Option<Box<dyn SourceChangeGenerator + Send + Sync>>,    
    pub subscribers: Vec<QueryId>
}

impl TestRunSource {
    pub async fn new(
        test_source_definition: SourceDefinition,
        test_run_source_config: TestRunSourceConfig,
        test_data_store: TestSourceStorage, 
        result_store: TestRunSourceStorage
    ) -> anyhow::Result<Self> {

        let id = TestRunSourceId::try_from(&test_run_source_config)?;

        let SourceDefinition {
            bootstrap_data_generator: bootstrap_data_generator_definition,
            source_change_generator: source_change_generator_definition,
            subscribers,
            ..
        } = test_source_definition;

        let TestRunSourceConfig { 
            bootstrap_data_generator: bootstrap_data_generator_overrides, 
            source_change_generator: source_change_generator_overrides,             
            .. 
        } = test_run_source_config;

        let bootstrap_data_generator = create_bootstrap_data_generator(
            id.clone(),
            bootstrap_data_generator_definition,
            bootstrap_data_generator_overrides,
            test_data_store.clone(),
            result_store.clone()
        ).await?;

        let source_change_generator = create_source_change_generator(
            id.clone(),
            source_change_generator_definition,
            source_change_generator_overrides,
            test_data_store,
            result_store
        ).await?;
    
        Ok(Self { 
            id, 
            start_immediately: test_run_source_config.start_immediately,
            bootstrap_data_generator, 
            source_change_generator,
            subscribers
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

    pub async fn skip_source_change_generator(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.skip(skips).await?;
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

    pub async fn step_source_change_generator(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.step(steps).await?;
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