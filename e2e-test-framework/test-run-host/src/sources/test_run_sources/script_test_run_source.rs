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

use test_data_store::{test_repo_storage::{models::{BootstrapDataGeneratorDefinition, QueryId, ScriptTestSourceDefinition, SourceChangeDispatcherDefinition, SourceChangeGeneratorDefinition, SpacingMode}, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

use crate::sources::{bootstrap_data_generators::{create_bootstrap_data_generator, BootstrapData, BootstrapDataGenerator}, source_change_generators::{create_source_change_generator, SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState}, SourceChangeGeneratorStartMode, TestRunSource, TestRunSourceConfig, TestRunSourceState};

#[derive(Clone, Debug)]
pub struct ScriptTestRunSourceDefinition {
    pub bootstrap_data_generator_def: Option<BootstrapDataGeneratorDefinition>,
    pub id: TestRunSourceId,
    pub source_change_dispatcher_defs: Vec<SourceChangeDispatcherDefinition>,
    pub source_change_generator_def: Option<SourceChangeGeneratorDefinition>,
    pub source_change_generator_start_mode: SourceChangeGeneratorStartMode,    
    pub subscribers: Vec<QueryId>,
}

impl ScriptTestRunSourceDefinition {
    pub fn new( cfg: &TestRunSourceConfig, def: &ScriptTestSourceDefinition) -> anyhow::Result<Self> {
            
        let mut def = Self {
            bootstrap_data_generator_def: def.bootstrap_data_generator_def.clone(),
            id: TestRunSourceId::try_from(cfg)?,
            source_change_dispatcher_defs: def.common.source_change_dispatcher_defs.clone(),
            source_change_generator_def: def.source_change_generator_def.clone(),
            source_change_generator_start_mode: cfg.source_change_generator_start_mode.clone().unwrap_or_default(),
            subscribers: def.common.subscribers.clone(),
        };

        if let Some(overrides) = &cfg.test_run_overrides {
            if let Some(bdg_overrides) = &overrides.bootstrap_data_generator {
                match &mut def.bootstrap_data_generator_def {
                    Some(BootstrapDataGeneratorDefinition::Script { common_config, .. }) => {
                        if let Some(time_mode) = &bdg_overrides.time_mode {
                            common_config.time_mode = time_mode.clone();
                        }
                    },
                    None => {}
                }
            }

            if let Some(scg_overrides) = &overrides.source_change_generator {
                match &mut def.source_change_generator_def {
                    Some(SourceChangeGeneratorDefinition::Script { common_config, .. }) => {
                        if let Some(spacing_mode) = &scg_overrides.spacing_mode {
                            common_config.spacing_mode = spacing_mode.clone();
                        }
                        if let Some(time_mode) = &scg_overrides.time_mode {
                            common_config.time_mode = time_mode.clone();
                        }
                    },
                    None => {}
                }
            }
            
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
pub struct ScriptTestRunSource {
    #[debug(skip)]
    pub bootstrap_data_generator: Option<Box<dyn BootstrapDataGenerator + Send + Sync>>,
    pub id: TestRunSourceId,
    #[debug(skip)]
    pub source_change_generator: Option<Box<dyn SourceChangeGenerator + Send + Sync>>,    
    pub source_change_generator_start_mode: SourceChangeGeneratorStartMode,
    pub subscribers: Vec<QueryId>
}

impl ScriptTestRunSource {
    pub async fn new(
        cfg: &TestRunSourceConfig,
        def: &ScriptTestSourceDefinition,  
        input_storage: TestSourceStorage, 
        output_storage: TestRunSourceStorage      
    ) -> anyhow::Result<Self> {

        let definition = ScriptTestRunSourceDefinition::new(cfg, def)?;

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
    
        let trs = Self { 
            id: definition.id.clone(),
            bootstrap_data_generator, 
            source_change_generator,
            source_change_generator_start_mode: definition.source_change_generator_start_mode,
            subscribers: definition.subscribers,
        };

        if trs.source_change_generator_start_mode == SourceChangeGeneratorStartMode::Auto {
            trs.start_source_change_generator().await?;
        }

        Ok(trs)
    }
}

#[async_trait]
impl TestRunSource for ScriptTestRunSource {
    async fn get_bootstrap_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        log::debug!("Node Labels: {:?}, Rel Labels: {:?}", node_labels, rel_labels);

        let bootstrap_data = if self.bootstrap_data_generator.is_some() {
            self.bootstrap_data_generator.as_ref().unwrap().get_data(node_labels, rel_labels).await
        } else {
            Ok(BootstrapData::new())
        };

        if self.source_change_generator_start_mode == SourceChangeGeneratorStartMode::Bootstrap {
            self.start_source_change_generator().await?;
        };

        bootstrap_data
    }

    async fn get_state(&self) -> anyhow::Result<TestRunSourceState> {

        Ok(TestRunSourceState {
            id: self.id.clone(),
            source_change_generator: self.get_source_change_generator_state().await?,
            source_change_generator_start_mode: self.source_change_generator_start_mode.clone(),
        })
    }

    async fn get_source_change_generator_state(&self) -> anyhow::Result<SourceChangeGeneratorState> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.get_state().await?;
                Ok(response.state)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for ScriptTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn pause_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.pause().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for ScriptTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn reset_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.reset().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for ScriptTestRunSource: {:?}", &self.id);
            }
        }
    }    

    async fn skip_source_change_generator(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
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
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.start().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for ScriptTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn step_source_change_generator(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.step(steps, spacing_mode).await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for ScriptTestRunSource: {:?}", &self.id);
            }
        }
    }

    async fn stop_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.stop().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for ScriptTestRunSource: {:?}", &self.id);
            }
        }
    }
}