// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use serde::Serialize;
use test_data_store::{test_repo_storage::{models::{SourceChangeDispatcherDefinition, SourceChangeGeneratorDefinition, SpacingMode}, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};
use tokio::sync::oneshot;

use script_source_change_generator::ScriptSourceChangeGenerator;

pub mod script_source_change_generator;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeGeneratorError {
    // NotConfigured
}

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
        matches!(self, SourceChangeGeneratorStatus::Running | SourceChangeGeneratorStatus::Skipping | SourceChangeGeneratorStatus::Stepping | SourceChangeGeneratorStatus::Paused)
    }

    pub fn is_processing(&self) -> bool {
        matches!(self, SourceChangeGeneratorStatus::Running | SourceChangeGeneratorStatus::Skipping | SourceChangeGeneratorStatus::Stepping)
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
    Skip{skips: u64, spacing_mode: Option<SpacingMode>},
    Start,
    Step{steps: u64, spacing_mode: Option<SpacingMode>},
    Stop,
}

#[derive(Debug)]
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
    pub state: serde_json::Value,
    pub status: SourceChangeGeneratorStatus,
}

#[async_trait]
pub trait SourceChangeGenerator : Send + Sync + std::fmt::Debug {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn reset(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn skip(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn step(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
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

    async fn reset(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).reset().await
    }

    async fn skip(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).skip(skips, spacing_mode).await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).start().await
    }

    async fn step(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).step(steps, spacing_mode).await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).stop().await
    }
}

pub async fn create_source_change_generator(
    id: TestRunSourceId, 
    definition: Option<SourceChangeGeneratorDefinition>,
    input_storage: TestSourceStorage, 
    output_storage: TestRunSourceStorage,
    dispatchers: Vec<SourceChangeDispatcherDefinition>,
) -> anyhow::Result<Option<Box<dyn SourceChangeGenerator + Send + Sync>>> {
    match definition {
        None => Ok(None),
        Some(SourceChangeGeneratorDefinition::Script(definition)) => {
            Ok(Some(Box::new(ScriptSourceChangeGenerator::new(
                id, 
                definition, 
                input_storage, 
                output_storage,
                dispatchers,
            ).await?) as Box<dyn SourceChangeGenerator + Send + Sync> ))
        }
    }
}