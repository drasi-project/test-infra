use std::fmt;

use derive_more::Debug;
use reaction_observer::{ReactionObserver, ReactionObserverCommandResponse, ReactionObserverState};
use serde::{Deserialize, Serialize};

use test_data_store::{test_repo_storage::models::TestReactionDefinition, test_run_storage::{ParseTestRunIdError, ParseTestRunReactionIdError, TestRunId, TestRunReactionId, TestRunReactionStorage}};

mod reaction_collector;
mod reaction_dispatchers;
pub mod reaction_observer;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunReactionConfig {
    pub start_immediately: Option<bool>,    
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: Option<String>,
    pub test_reaction_id: String,
}

impl TryFrom<&TestRunReactionConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunReactionConfig) -> Result<Self, Self::Error> {
        Ok(TestRunId::new(
            &value.test_repo_id, 
            &value.test_id, 
            value.test_run_id
                .as_deref()
                .unwrap_or(&chrono::Utc::now().format("%Y%m%d%H%M%S").to_string())))
    }
}

impl TryFrom<&TestRunReactionConfig> for TestRunReactionId {
    type Error = ParseTestRunReactionIdError;

    fn try_from(value: &TestRunReactionConfig) -> Result<Self, Self::Error> {
        match TestRunId::try_from(value) {
            Ok(test_run_id) => {
                Ok(TestRunReactionId::new(&test_run_id, &value.test_reaction_id))
            }
            Err(e) => return Err(ParseTestRunReactionIdError::InvalidValues(e.to_string())),
        }
    }
}

impl fmt::Display for TestRunReactionConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestRunSourceDefinition: Repo: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_reaction_id: {:?}", 
            self.test_repo_id, self.test_id, self.test_run_id, self.test_reaction_id)
    }
}

#[derive(Clone, Debug)]
pub struct TestRunReactionDefinition {
    pub id: TestRunReactionId,
    pub start_immediately: bool,    
    pub test_reaction_definition: TestReactionDefinition,
}

impl TestRunReactionDefinition {
    pub fn new( test_run_reaction_config: TestRunReactionConfig, test_reaction_definition: TestReactionDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            id: TestRunReactionId::try_from(&test_run_reaction_config)?,
            start_immediately: test_run_reaction_config.start_immediately.unwrap_or(false),
            test_reaction_definition,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct TestRunReactionState {
    pub id: TestRunReactionId,
    pub reaction_observer: ReactionObserverState,
    pub start_immediately: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunReaction {
    #[debug(skip)]
    pub id: TestRunReactionId,
    pub reaction_observer: Option<ReactionObserver>,
    pub start_immediately: bool,
}

impl TestRunReaction {
    pub async fn new(
        definition: TestRunReactionDefinition,
        _output_storage: TestRunReactionStorage
    ) -> anyhow::Result<Self> {

        let reaction_observer = None;

        Ok(Self { 
            id: definition.id.clone(),
            reaction_observer,
            start_immediately: definition.start_immediately,
        })
    }

    pub async fn get_state(&self) -> anyhow::Result<TestRunReactionState> {

        Ok(TestRunReactionState {
            id: self.id.clone(),
            reaction_observer: self.get_reaction_observer_state().await?,
            start_immediately: self.start_immediately,
        })
    }

    pub async fn get_reaction_observer_state(&self) -> anyhow::Result<ReactionObserverState> {
        match &self.reaction_observer {
            Some(observer) => {
                let response = observer.get_state().await?;
                Ok(response.state)
            },
            None => {
                anyhow::bail!("SourceChangeobserver not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn pause_reaction_observer(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        match &self.reaction_observer {
            Some(observer) => {
                let response = observer.pause().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeobserver not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn reset_reaction_observer(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        match &self.reaction_observer {
            Some(observer) => {
                let response = observer.reset().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeobserver not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }    

    pub async fn start_reaction_observer(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        match &self.reaction_observer {
            Some(observer) => {
                let response = observer.start().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeobserver not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn stop_reaction_observer(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        match &self.reaction_observer {
            Some(observer) => {
                let response = observer.stop().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeobserver not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }
}