use async_trait::async_trait;

use change_script_source_change_generator::ChangeScriptPlayer;
use test_data_store::{test_repo_storage::TestSourceStorage, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

use crate::config::SourceChangeGeneratorConfig;

pub mod change_script_source_change_generator;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeGeneratorError {
}

#[derive(Debug)]
pub struct SourceChangeGeneratorCommandResponse {
    pub result: anyhow::Result<()>,
    pub state: SourceChangeGeneratorState,
}

#[derive(Debug)]
pub struct SourceChangeGeneratorState {    
}

#[async_trait]
pub trait SourceChangeGenerator : Send + Sync {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn step(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn skip(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
}

#[async_trait]
impl SourceChangeGenerator for Box<dyn SourceChangeGenerator + Send + Sync> {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).get_state().await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).start().await
    }

    async fn step(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).step(steps).await
    }

    async fn skip(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).skip(skips).await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).pause().await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).stop().await
    }
}

pub async fn create_source_change_generator(
    id: TestRunSourceId, 
    config: Option<SourceChangeGeneratorConfig>,
    dataset: TestSourceStorage, 
    storage: TestRunSourceStorage
) -> anyhow::Result<Option<Box<dyn SourceChangeGenerator + Send + Sync>>> {
    match config {
        None => Ok(None),
        Some(SourceChangeGeneratorConfig::Script{common_config, unique_config}) => {
            Ok(Some(Box::new(ChangeScriptPlayer::new(
                id, 
                common_config, 
                unique_config, 
                dataset, 
                storage).await?)))
        }
    }
}