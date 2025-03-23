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

use std::collections::HashSet;

use async_trait::async_trait;

use building_hierarchy::BuildingHierarchyDataGenerator;
use test_data_store::{test_repo_storage::{models::{ModelDataGeneratorDefinition, SourceChangeDispatcherDefinition, SpacingMode}, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

use super::{bootstrap_data_generators::{BootstrapData, BootstrapDataGenerator}, source_change_generators::{SourceChangeGenerator, SourceChangeGeneratorCommandResponse}};

pub mod building_hierarchy;
pub mod domain_model_graph;

#[async_trait]
pub trait ModelDataGenerator : SourceChangeGenerator + BootstrapDataGenerator + Send + Sync + std::fmt::Debug {}

#[async_trait]
impl ModelDataGenerator for Box<dyn ModelDataGenerator + Send + Sync> {}

#[async_trait]
impl BootstrapDataGenerator for Box<dyn ModelDataGenerator + Send + Sync> {
    async fn get_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        (**self).get_data(node_labels, rel_labels).await
    }
}

#[async_trait]
impl SourceChangeGenerator for Box<dyn ModelDataGenerator + Send + Sync> {
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

pub async fn create_model_data_generator(
    id: TestRunSourceId, 
    definition: Option<ModelDataGeneratorDefinition>,
    input_storage: TestSourceStorage, 
    output_storage: TestRunSourceStorage,
    dispatchers: Vec<SourceChangeDispatcherDefinition>,
) -> anyhow::Result<Option<Box<dyn ModelDataGenerator + Send + Sync>>> {
    match definition {
        None => Ok(None),
        Some(ModelDataGeneratorDefinition::BuildingHierarchy(definition)) => {
            Ok(Some(Box::new(BuildingHierarchyDataGenerator::new(
                id, 
                definition,
                input_storage, 
                output_storage,
                dispatchers,
            ).await?) as Box<dyn ModelDataGenerator + Send + Sync>))
        }
    }
}