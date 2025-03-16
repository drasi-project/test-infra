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

use std::{collections::HashSet, fmt, str::FromStr};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::{self, Deserializer}};

use bootstrap_data_generators::BootstrapData;
use source_change_generators::{ SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState};
use test_data_store::{test_repo_storage::{models::{ QueryId, SourceChangeDispatcherDefinition, SpacingMode, TestSourceDefinition, TimeMode}, TestSourceStorage}, test_run_storage::{ParseTestRunIdError, ParseTestRunSourceIdError, TestRunId, TestRunSourceId, TestRunSourceStorage}};
use test_run_sources::{building_environment_test_run_source::BuildingEnvironmentModelTestRunSource, script_test_run_source::ScriptTestRunSource};

pub mod bootstrap_data_generators;
pub mod source_change_generators;
pub mod source_change_dispatchers;
pub mod test_run_sources;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SourceChangeGeneratorStartMode {
    Auto,
    Bootstrap,
    Manual,
}

impl Default for SourceChangeGeneratorStartMode {
    fn default() -> Self {
        Self::Bootstrap
    }
}

impl FromStr for SourceChangeGeneratorStartMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "bootstrap" => Ok(Self::Bootstrap),
            "manual" => Ok(Self::Manual),
            _ => {
                anyhow::bail!("Invalid SourceChangeGeneratorStartMode value:{}", s);
            }
        }
    }
}

impl std::fmt::Display for SourceChangeGeneratorStartMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Bootstrap => write!(f, "bootstrap"),
            Self::Manual => write!(f, "manual"),
        }
    }
}

impl<'de> Deserialize<'de> for SourceChangeGeneratorStartMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: String = Deserialize::deserialize(deserializer)?;
        value.parse::<SourceChangeGeneratorStartMode>().map_err(de::Error::custom)
    }
}

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
    pub source_change_generator_start_mode: Option<SourceChangeGeneratorStartMode>,    
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: Option<String>,
    pub test_run_overrides: Option<TestRunSourceOverrides>,
    pub test_source_id: String,
}

impl TryFrom<&TestRunSourceConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        Ok(match value.test_run_id.as_deref() {
            Some(test_run_id) => TestRunId::new(&value.test_repo_id, &value.test_id, test_run_id),
            None => TestRunId::new(&value.test_repo_id, &value.test_id, &chrono::Utc::now().format("%Y%m%d%H%M%S").to_string()),
        })
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
#[derive(Debug, Serialize)]
pub struct TestRunSourceState {
    pub id: TestRunSourceId,
    pub source_change_generator: SourceChangeGeneratorState,
    pub source_change_generator_start_mode: SourceChangeGeneratorStartMode,
}

#[async_trait]
pub trait TestRunSource : Send + Sync + std::fmt::Debug {
    async fn get_bootstrap_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData>;
    async fn get_state(&self) -> anyhow::Result<TestRunSourceState>;
    async fn get_source_change_generator_state(&self) -> anyhow::Result<SourceChangeGeneratorState>;
    async fn pause_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn reset_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn skip_source_change_generator(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn start_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn step_source_change_generator(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
    async fn stop_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>;
}

#[async_trait]
impl TestRunSource for Box<dyn TestRunSource + Send + Sync> {
    async fn get_bootstrap_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        (**self).get_bootstrap_data(node_labels, rel_labels).await
    }

    async fn get_state(&self) -> anyhow::Result<TestRunSourceState> {
        (**self).get_state().await
    }

    async fn get_source_change_generator_state(&self) -> anyhow::Result<SourceChangeGeneratorState> {
        (**self).get_source_change_generator_state().await
    }

    async fn pause_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).pause_source_change_generator().await
    }

    async fn reset_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).reset_source_change_generator().await
    }

    async fn skip_source_change_generator(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).skip_source_change_generator(skips, spacing_mode).await
    }

    async fn start_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).start_source_change_generator().await
    }

    async fn step_source_change_generator(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).step_source_change_generator(steps, spacing_mode).await
    }

    async fn stop_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        (**self).stop_source_change_generator().await
    }
}

pub async fn create_test_run_source(cfg: &TestRunSourceConfig,  def: &TestSourceDefinition, input_storage: TestSourceStorage, output_storage: TestRunSourceStorage) -> anyhow::Result<Box<dyn TestRunSource + Send + Sync>> {

    match def {
        TestSourceDefinition::BuildingEnvironmentModel(def) => {
            Ok(Box::new(BuildingEnvironmentModelTestRunSource::new(
                cfg,
                def,
                input_storage,
                output_storage
            ).await?) as Box<dyn TestRunSource + Send + Sync>)
        }, 
        TestSourceDefinition::Script(def) => {
            Ok(Box::new(ScriptTestRunSource::new(
                cfg,
                def,
                input_storage,
                output_storage
            ).await?) as Box<dyn TestRunSource + Send + Sync>)
        },
    }
}