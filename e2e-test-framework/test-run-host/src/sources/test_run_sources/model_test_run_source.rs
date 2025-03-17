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

use rand::Rng;
use std::collections::HashSet;

use async_trait::async_trait;
use derive_more::Debug;

use test_data_store::{test_repo_storage::{models::{ModelDataGeneratorDefinition, ModelTestSourceDefinition, QueryId, SourceChangeDispatcherDefinition, SpacingMode}, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

use crate::sources::{bootstrap_data_generators::BootstrapData, model_data_generators::{create_model_data_generator, ModelDataGenerator}, source_change_generators::{ SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState}, SourceStartMode, TestRunSource, TestRunSourceConfig, TestRunSourceState};

// use super::{building_graph::BuildingGraph, domain_model_graph::DomainModelGraph};

#[derive(Clone, Debug)]
pub struct ModelTestRunSourceSettings {
    pub id: TestRunSourceId,
    pub seed: u64,
    pub source_change_dispatcher_defs: Vec<SourceChangeDispatcherDefinition>,
    pub model_data_generator_def: Option<ModelDataGeneratorDefinition>,
    pub start_mode: SourceStartMode,    
    pub subscribers: Vec<QueryId>,
}

impl ModelTestRunSourceSettings {
    pub fn new( cfg: &TestRunSourceConfig, def: &ModelTestSourceDefinition) -> anyhow::Result<Self> {
            
        let mut settings = Self {
            id: TestRunSourceId::try_from(cfg)?,
            seed: def.seed.unwrap_or(rand::thread_rng().gen()),
            source_change_dispatcher_defs: def.common.source_change_dispatcher_defs.clone(),
            model_data_generator_def: def.model_data_generator_def.clone(),
            start_mode: cfg.start_mode.clone().unwrap_or_default(),
            subscribers: def.common.subscribers.clone(),
        };

        if let Some(overrides) = &cfg.test_run_overrides {
            if let Some(mdg_overrides) = &overrides.model_data_generator {
                match &mut settings.model_data_generator_def {
                    Some(ModelDataGeneratorDefinition::BuildingEnvironment(mdg_def)) => {
                        if let Some(spacing_mode) = &mdg_overrides.spacing_mode {
                            mdg_def.common.spacing_mode = spacing_mode.clone();
                        }
                        if let Some(time_mode) = &mdg_overrides.time_mode {
                            mdg_def.common.time_mode = time_mode.clone();
                        }
                    },
                    None => {}
                }
            }
            
            if let Some(dispatchers) = &overrides.source_change_dispatchers {
                settings.source_change_dispatcher_defs = dispatchers.clone();
            }

            if let Some(subscribers) = &overrides.subscribers {
                settings.subscribers = subscribers.clone();
            }
        };

        Ok(settings)
    }
}

#[derive(Debug)]
pub struct ModelTestRunSource {
    pub id: TestRunSourceId,
    pub model_data_generator: Option<Box<dyn ModelDataGenerator + Send + Sync>>,    
    pub start_mode: SourceStartMode,
    pub subscribers: Vec<QueryId>
}

impl ModelTestRunSource {
    pub async fn new(
        cfg: &TestRunSourceConfig,
        def: &ModelTestSourceDefinition,
        input_storage: TestSourceStorage, 
        output_storage: TestRunSourceStorage      
    ) -> anyhow::Result<Self> {

        let definition = ModelTestRunSourceSettings::new(cfg, def)?;

        let model_data_generator = create_model_data_generator(
            definition.id.clone(),
            definition.model_data_generator_def,
            input_storage,
            output_storage,
            definition.source_change_dispatcher_defs
        ).await?;

        let trs = Self { 
            id: definition.id.clone(),
            model_data_generator,
            start_mode: definition.start_mode,
            subscribers: definition.subscribers,
        };

        if trs.start_mode == SourceStartMode::Auto {
            trs.start_source_change_generator().await?;
        }

        Ok(trs)
    }
}


#[async_trait]
impl TestRunSource for ModelTestRunSource {
    async fn get_bootstrap_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        log::debug!("Node Labels: {:?}, Rel Labels: {:?}", node_labels, rel_labels);

        let bootstrap_data = if self.model_data_generator.is_some() {
            self.model_data_generator.as_ref().unwrap().get_data(node_labels, rel_labels).await
        } else {
            Ok(BootstrapData::new())
        };

        if self.start_mode == SourceStartMode::Bootstrap {
            self.start_source_change_generator().await?;
        };

        bootstrap_data
    }

    async fn get_state(&self) -> anyhow::Result<TestRunSourceState> {

        Ok(TestRunSourceState {
            id: self.id.clone(),
            source_change_generator: self.get_source_change_generator_state().await?,
            start_mode: self.start_mode.clone(),
        })
    }

    async fn get_source_change_generator_state(&self) -> anyhow::Result<SourceChangeGeneratorState> {
        match &self.model_data_generator {
            Some(generator) => {
                let response = generator.get_state().await?;
                Ok(response.state)
            },
            None => {
                anyhow::bail!("ModelGenerator not configured for ModelTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn pause_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.model_data_generator {
            Some(generator) => {
                let response = generator.pause().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("ModelGenerator not configured for ModelTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn reset_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.model_data_generator {
            Some(generator) => {
                let response = generator.reset().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("ModelGenerator not configured for ModelTestRunSource: {:?}", &self.id);
            }
        }
    }    

    async fn skip_source_change_generator(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.model_data_generator {
            Some(generator) => {
                let response = generator.skip(skips, spacing_mode).await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for ScriptTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn start_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.model_data_generator {
            Some(generator) => {
                let response = generator.start().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("ModelGenerator not configured for ModelTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn step_source_change_generator(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.model_data_generator {
            Some(generator) => {
                let response = generator.step(steps, spacing_mode).await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("ModelGenerator not configured for ModelTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn stop_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.model_data_generator {
            Some(generator) => {
                let response = generator.stop().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("ModelGenerator not configured for ModelTestRunSource: {:?}", &self.id);
            }
        }
    }
}