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
use derive_more::Debug;

use test_data_store::{test_repo_storage::{models::{BuildingEnvironmentModelTestSourceDefinition, QueryId, SourceChangeDispatcherDefinition, SpacingMode}, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

use crate::sources::{bootstrap_data_generators::BootstrapData, source_change_generators::{SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState}, TestRunSource, TestRunSourceConfig, TestRunSourceState};

#[derive(Clone, Debug)]
pub struct BuildingEnvironmentModelTestRunSourceDefinition {
    pub id: TestRunSourceId,
    pub source_change_dispatcher_defs: Vec<SourceChangeDispatcherDefinition>,
    pub subscribers: Vec<QueryId>,
}

impl BuildingEnvironmentModelTestRunSourceDefinition {
    pub fn new( cfg: &TestRunSourceConfig, def: &BuildingEnvironmentModelTestSourceDefinition) -> anyhow::Result<Self> {
            
        let mut def = Self {
            id: TestRunSourceId::try_from(cfg)?,
            source_change_dispatcher_defs: def.common.source_change_dispatcher_defs.clone(),
            subscribers: def.common.subscribers.clone(),
        };

        if let Some(overrides) = &cfg.test_run_overrides {
            if let Some(dispatchers) = &overrides.source_change_dispatchers {
                def.source_change_dispatcher_defs = dispatchers.clone();
            }

            if let Some(subscribers) = &overrides.subscribers {
                def.subscribers = subscribers.clone();
            }
        };

        Ok(def)
    }
}

#[derive(Debug)]
pub struct BuildingEnvironmentModelTestRunSource {
    pub id: TestRunSourceId,
}

impl BuildingEnvironmentModelTestRunSource {
    pub async fn new(
        cfg: &TestRunSourceConfig,
        def: &BuildingEnvironmentModelTestSourceDefinition,
        _input_storage: TestSourceStorage, 
        _output_storage: TestRunSourceStorage      
    ) -> anyhow::Result<Self> {

        let definition = BuildingEnvironmentModelTestRunSourceDefinition::new(cfg, def)?;

        let trs = Self { 
            id: definition.id.clone(),
        };

        Ok(trs)
    }
}

#[async_trait]
impl TestRunSource for BuildingEnvironmentModelTestRunSource {
    async fn get_bootstrap_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        log::debug!("Node Labels: {:?}, Rel Labels: {:?}", node_labels, rel_labels);

        anyhow::bail!("Bootstrap data generation is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }

    async fn get_state(&self) -> anyhow::Result<TestRunSourceState> {

        anyhow::bail!("State generation is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }

    async fn get_source_change_generator_state(&self) -> anyhow::Result<SourceChangeGeneratorState> {
        anyhow::bail!("Source change generator state generation is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }

    async fn pause_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        anyhow::bail!("Pausing source change generator is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }

    async fn reset_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        anyhow::bail!("Resetting source change generator is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }    

    async fn skip_source_change_generator(&self, _skips: u64, _spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        anyhow::bail!("Skipping source change generator is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }

    async fn start_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        anyhow::bail!("Starting source change generator is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }

    async fn step_source_change_generator(&self, _steps: u64, _spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        anyhow::bail!("Stepping source change generator is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }

    async fn stop_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        anyhow::bail!("Stopping source change generator is not supported for BuildingEnvironmentTestRunSource: {:?}", &self.id);
    }
}