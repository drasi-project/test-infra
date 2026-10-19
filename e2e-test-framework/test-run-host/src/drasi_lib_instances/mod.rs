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

use std::{collections::HashMap, fmt, sync::Arc};

use anyhow::Context;
use derive_more::Debug;
use drasi_lib::{computation::v1::InputEnvelope, DrasiLib, QueryConfig};
use drasi_source_application::ApplicationSourceHandle;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex, RwLock};
use utoipa::ToSchema;

use test_data_store::{
    test_repo_storage::models::{
        DrasiLibInstanceConfig as TestDrasiLibInstanceConfig, TestDrasiLibInstanceDefinition,
    },
    test_run_storage::{
        ParseTestRunDrasiLibInstanceIdError, ParseTestRunIdError, TestRunDrasiLibInstanceId,
        TestRunDrasiLibInstanceStorage, TestRunId,
    },
};

pub mod api_models;
pub mod programmatic_api;

#[cfg(test)]
pub(crate) mod tests;

/// Runtime configuration for a test run drasi-lib instance.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct TestRunDrasiLibInstanceConfig {
    #[serde(default = "default_start_immediately")]
    pub start_immediately: bool,
    pub test_drasi_lib_instance_id: String,
    pub test_run_overrides: Option<TestRunDrasiLibInstanceOverrides>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub test_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub test_repo_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub test_run_id: Option<String>,
}

fn default_start_immediately() -> bool {
    true
}

/// Overrides for drasi-lib instance configuration at runtime.
#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TestRunDrasiLibInstanceOverrides {
    /// Override log level (trace, debug, info, warn, error).
    pub log_level: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct DrasiLibRuntimeInfo {
    pub instance_id: String,
    /// Informational identifier of the sole runtime, not a configuration selector.
    #[schema(example = "computationGraph")]
    pub execution_mode: String,
    pub running: bool,
}

impl TryFrom<&TestRunDrasiLibInstanceConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunDrasiLibInstanceConfig) -> Result<Self, Self::Error> {
        let test_repo_id = value.test_repo_id.as_ref().ok_or_else(|| {
            ParseTestRunIdError::InvalidValues("test_repo_id is required".to_string())
        })?;
        let test_id = value
            .test_id
            .as_ref()
            .ok_or_else(|| ParseTestRunIdError::InvalidValues("test_id is required".to_string()))?;
        let default_run_id = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
        let test_run_id = value
            .test_run_id
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or(default_run_id);

        Ok(TestRunId::new(test_repo_id, test_id, &test_run_id))
    }
}

impl TryFrom<&TestRunDrasiLibInstanceConfig> for TestRunDrasiLibInstanceId {
    type Error = ParseTestRunDrasiLibInstanceIdError;

    fn try_from(value: &TestRunDrasiLibInstanceConfig) -> Result<Self, Self::Error> {
        TestRunId::try_from(value)
            .map(|test_run_id| {
                TestRunDrasiLibInstanceId::new(&test_run_id, &value.test_drasi_lib_instance_id)
            })
            .map_err(|e| ParseTestRunDrasiLibInstanceIdError::InvalidValues(e.to_string()))
    }
}

impl fmt::Display for TestRunDrasiLibInstanceConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TestRunDrasiLibInstanceConfig: Repo: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_drasi_lib_instance_id: {:?}",
            self.test_repo_id, self.test_id, self.test_run_id, self.test_drasi_lib_instance_id
        )
    }
}

/// Combined test and runtime configuration for a drasi-lib instance.
#[derive(Clone, Debug)]
pub struct TestRunDrasiLibInstanceDefinition {
    pub id: TestRunDrasiLibInstanceId,
    pub start_immediately: bool,
    pub test_drasi_lib_instance_definition: TestDrasiLibInstanceDefinition,
    pub test_run_overrides: Option<TestRunDrasiLibInstanceOverrides>,
}

impl TestRunDrasiLibInstanceDefinition {
    /// Create a test run drasi-lib instance definition.
    pub fn new(
        config: TestRunDrasiLibInstanceConfig,
        test_drasi_lib_instance_definition: TestDrasiLibInstanceDefinition,
    ) -> anyhow::Result<Self> {
        let id = TestRunDrasiLibInstanceId::try_from(&config)?;
        Ok(Self {
            id,
            start_immediately: config.start_immediately,
            test_drasi_lib_instance_definition,
            test_run_overrides: config.test_run_overrides,
        })
    }

    /// Get the effective configuration with overrides applied.
    pub fn effective_config(&self) -> TestDrasiLibInstanceConfig {
        let mut config = self.test_drasi_lib_instance_definition.config.clone();
        if let Some(overrides) = &self.test_run_overrides {
            if let Some(log_level) = &overrides.log_level {
                config.log_level = Some(log_level.clone());
            }
        }
        config
    }
}

/// State of a test run drasi-lib instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum TestRunDrasiLibInstanceState {
    Uninitialized,
    Running,
    Stopping,
    Stopped,
    Error(String),
}

impl fmt::Display for TestRunDrasiLibInstanceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uninitialized => write!(f, "Uninitialized"),
            Self::Running => write!(f, "Running"),
            Self::Stopping => write!(f, "Stopping"),
            Self::Stopped => write!(f, "Stopped"),
            Self::Error(message) => write!(f, "Error: {message}"),
        }
    }
}

/// Test run drasi-lib instance component.
#[derive(Debug)]
pub struct TestRunDrasiLibInstance {
    pub definition: TestRunDrasiLibInstanceDefinition,
    #[debug(skip)]
    pub core: Arc<RwLock<Option<Arc<DrasiLib>>>>,
    #[debug(skip)]
    pub source_handles: Arc<RwLock<HashMap<String, ApplicationSourceHandle>>>,
    #[debug(skip)]
    pub reaction_receivers: Arc<RwLock<HashMap<String, mpsc::Receiver<InputEnvelope>>>>,
    pub state: Arc<RwLock<TestRunDrasiLibInstanceState>>,
    pub storage: TestRunDrasiLibInstanceStorage,
    pub id: TestRunDrasiLibInstanceId,
    #[debug(skip)]
    lifecycle_tx: crate::test_run_completion::LifecycleTx,
    #[debug(skip)]
    lifecycle: Mutex<()>,
}

impl TestRunDrasiLibInstance {
    /// Construct an unstarted owner. Register it before awaiting `start`.
    pub fn new(
        definition: TestRunDrasiLibInstanceDefinition,
        storage: TestRunDrasiLibInstanceStorage,
        lifecycle_tx: crate::test_run_completion::LifecycleTx,
    ) -> Self {
        Self {
            id: definition.id.clone(),
            definition,
            core: Arc::new(RwLock::new(None)),
            source_handles: Arc::new(RwLock::new(HashMap::new())),
            reaction_receivers: Arc::new(RwLock::new(HashMap::new())),
            state: Arc::new(RwLock::new(TestRunDrasiLibInstanceState::Uninitialized)),
            storage,
            lifecycle_tx,
            lifecycle: Mutex::new(()),
        }
    }

    /// Start this drasi-lib instance.
    pub async fn start(&self) -> anyhow::Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let current = self.state.read().await.clone();
        if matches!(current, TestRunDrasiLibInstanceState::Running) {
            return Ok(());
        }
        anyhow::ensure!(
            self.core.read().await.is_none(),
            "the previous drasi-lib instance must be stopped before restarting"
        );

        let config = self.definition.effective_config();
        let configured = async {
        let mut owner = self.core.write().await;
        let core = Arc::new(DrasiLib::builder().with_id(self.id.to_string()).build().await?);
        *owner = Some(core.clone());
        drop(owner);
        for source_config in &config.sources {
            if source_config.kind != "application" {
                anyhow::bail!(
                    "Unsupported drasi-lib instance source kind '{}' for source '{}' (only 'application' is supported)",
                    source_config.kind,
                    source_config.id
                );
            }
            let options = if source_config.config.is_null() {
                serde_json::json!({})
            } else {
                source_config.config.clone()
            };
            let handle = Self::add_native_application_source(
                &core,
                &self.id.to_string(),
                &source_config.id,
                &options,
                source_config.auto_start,
            ).await?;
            self.source_handles.write().await.insert(source_config.id.clone(), handle);
        }

        for query_config in &config.queries {
            let defaults = programmatic_api::query_config_from_request(
                &query_config.id,
                query_config.query.clone(),
                query_config.sources.clone(),
                query_config.auto_start,
            );
            let mut options = serde_json::to_value(&defaults)?;
            if let Some(overrides) = query_config.config.as_object() {
                options
                    .as_object_mut()
                    .context("QueryConfig must serialize as an object")?
                    .extend(overrides.clone());
            } else if !query_config.config.is_null() {
                anyhow::bail!("Query options must be an object: {}", query_config.id);
            }
            let mut query: QueryConfig = serde_json::from_value(options)
                .with_context(|| format!("Invalid query config: {}", query_config.id))?;
            query.id = defaults.id;
            query.query = defaults.query;
            query.auto_start = defaults.auto_start;
            query.sources = defaults.sources;
            Self::add_native_query(&core, query).await?;
        }

        for reaction_config in &config.reactions {
            if reaction_config.kind != "application" {
                anyhow::bail!(
                    "Unsupported drasi-lib instance reaction kind '{}' for reaction '{}' (only 'application' is supported)",
                    reaction_config.kind,
                    reaction_config.id
                );
            }
            let properties = if reaction_config.config.is_null() {
                serde_json::json!({})
            } else {
                reaction_config.config.clone()
            };
            let receiver = Self::add_native_application_reaction(
                &core,
                &reaction_config.id,
                &reaction_config.queries,
                reaction_config.auto_start,
                &properties,
            ).await?;
            self.reaction_receivers.write().await.insert(reaction_config.id.clone(), receiver);
        }

        self.storage
            .write_instance_config(&serde_json::to_value(&config)?)
            .await?;
        core.start().await?;
        Ok::<_, anyhow::Error>(())
        }.await;
        if let Err(error) = configured {
            *self.state.write().await = TestRunDrasiLibInstanceState::Error(error.to_string());
            return Err(error);
        }
        *self.state.write().await = TestRunDrasiLibInstanceState::Running;
        self.lifecycle_tx
            .drasi_lib_instance_started(self.id.clone());
        Ok(())
    }

    /// Stop this drasi-lib instance.
    pub async fn stop(&self) -> anyhow::Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let mut instance = self.core.write().await;
        if let Some(core) = instance.as_ref() {
            *self.state.write().await = TestRunDrasiLibInstanceState::Stopping;
            if let Err(error) = core.shutdown().await {
                *self.state.write().await =
                    TestRunDrasiLibInstanceState::Error(format!("{error:#}"));
                return Err(error.into());
            }
        }
        *instance = None;
        self.source_handles.write().await.clear();
        self.reaction_receivers.write().await.clear();
        *self.state.write().await = TestRunDrasiLibInstanceState::Stopped;
        self.lifecycle_tx
            .drasi_lib_instance_stopped(self.id.clone());
        Ok(())
    }

    /// Get current drasi-lib instance state.
    pub async fn get_state(&self) -> TestRunDrasiLibInstanceState {
        self.state.read().await.clone()
    }

    /// Get the underlying DrasiLib instance.
    pub async fn get_drasi_lib(&self) -> Option<Arc<DrasiLib>> {
        self.core.read().await.clone()
    }

    pub async fn get_runtime_info(&self) -> anyhow::Result<DrasiLibRuntimeInfo> {
        let core = self
            .get_drasi_lib()
            .await
            .context("drasi-lib instance is not running")?;
        Ok(DrasiLibRuntimeInfo {
            instance_id: self.id.to_string(),
            execution_mode: "computationGraph".to_string(),
            running: core.is_running().await,
        })
    }

    /// Get an application source handle by source name.
    pub async fn get_source_handle(&self, name: &str) -> anyhow::Result<ApplicationSourceHandle> {
        self.source_handles
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("drasi-lib instance source handle not found: {name}"))
    }

    /// Transfer the application's native reaction receiver to its one consumer.
    pub async fn take_reaction_receiver(
        &self,
        name: &str,
    ) -> anyhow::Result<mpsc::Receiver<InputEnvelope>> {
        let _lifecycle = self.lifecycle.lock().await;
        self.reaction_receivers
            .write()
            .await
            .remove(name)
            .ok_or_else(|| {
                anyhow::anyhow!("reaction receiver '{name}' is unavailable or already taken")
            })
    }

    async fn with_core<F, Fut, T>(&self, f: F) -> anyhow::Result<T>
    where
        F: FnOnce(Arc<DrasiLib>) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let core = self
            .get_drasi_lib()
            .await
            .ok_or_else(|| anyhow::anyhow!("drasi-lib instance is not running"))?;
        f(core).await
    }
}
