use async_trait::async_trait;

use change_script_player::ScriptSourceChangeGenerator;
use serde::Serialize;
use test_data_store::{test_repo_storage::{models::SourceChangeGeneratorDefinition, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};
use tokio::sync::oneshot;

pub mod change_script_player;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeGeneratorError {
    // NotConfigured
}

// Enum of SourceChangeGenerator status.
// Running --start--> <ignore>
// Running --skip--> <ignore>
// Running --step--> <ignore>
// Running --pause--> Paused
// Running --stop--> Stopped
// Running --finish_script--> Finished

// Skipping --start--> <ignore>
// Skipping --skip--> <ignore>
// Skipping --step--> <ignore>
// Skipping --pause--> Paused
// Skipping --stop--> Stopped
// Skipping --finish_script--> Finished

// Stepping --start--> <ignore>
// Stepping --skip--> <ignore>
// Stepping --step--> <ignore>
// Stepping --pause--> Paused
// Stepping --stop--> Stopped
// Stepping --finish_script--> Finished

// Paused --start--> Running
// Paused --skip--> Skipping
// Paused --step--> Stepping
// Paused --pause--> <ignore>
// Paused --stop--> Stopped

// Stopped --*--> <ignore>
// Finished --*--> <ignore>
// Error --*--> <ignore>
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SourceChangeGeneratorStatus {
    Running,
    Skipping,
    Stepping,
    Paused,
    Stopped,
    Finished,
    Error
}

impl SourceChangeGeneratorStatus {
    pub fn is_active(&self) -> bool {
        match self {
            SourceChangeGeneratorStatus::Running => true,
            SourceChangeGeneratorStatus::Skipping => true,
            SourceChangeGeneratorStatus::Stepping => true,
            _ => false,
        }
    }
}

impl Serialize for SourceChangeGeneratorStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            SourceChangeGeneratorStatus::Running => serializer.serialize_str("Running"),
            SourceChangeGeneratorStatus::Stepping => serializer.serialize_str("Stepping"),
            SourceChangeGeneratorStatus::Skipping => serializer.serialize_str("Skipping"),
            SourceChangeGeneratorStatus::Paused => serializer.serialize_str("Paused"),
            SourceChangeGeneratorStatus::Stopped => serializer.serialize_str("Stopped"),
            SourceChangeGeneratorStatus::Finished => serializer.serialize_str("Finished"),
            SourceChangeGeneratorStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Debug)]
pub enum SourceChangeGeneratorAction {
    GetState,
    Pause,
    Skip(u64),
    Start,
    Step(u64),
    Stop,
}

#[derive(Debug,)]
pub struct SourceChangeGeneratorCommand {
    pub action: SourceChangeGeneratorAction,
    pub response_tx: Option<oneshot::Sender<SourceChangeGeneratorCommandResponse>>,
}

#[derive(Debug)]
pub struct SourceChangeGeneratorCommandResponse {
    pub result: anyhow::Result<()>,
    pub state: SourceChangeGeneratorState,
}

#[derive(Debug, Serialize)]
pub struct SourceChangeGeneratorState {    
    status: SourceChangeGeneratorStatus,
}

#[async_trait]
pub trait SourceChangeGenerator : Send + Sync {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn skip(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn step(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
}

#[async_trait]
impl SourceChangeGenerator for Box<dyn SourceChangeGenerator + Send + Sync> {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).get_state().await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).pause().await
    }

    async fn skip(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).skip(skips).await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).start().await
    }

    async fn step(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).step(steps).await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).stop().await
    }
}

pub async fn create_source_change_generator(
    id: TestRunSourceId, 
    definition: Option<SourceChangeGeneratorDefinition>,
    input_storage: TestSourceStorage, 
    output_storage: TestRunSourceStorage
) -> anyhow::Result<Option<Box<dyn SourceChangeGenerator + Send + Sync>>> {
    match definition {
        None => Ok(None),
        Some(SourceChangeGeneratorDefinition::Script{common_config, unique_config}) => {
            Ok(Some(Box::new(ScriptSourceChangeGenerator::new(
                id, 
                common_config, 
                unique_config, 
                input_storage, 
                output_storage).await?)))
        }
    }
}