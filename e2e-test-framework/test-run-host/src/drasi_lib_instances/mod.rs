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

use derive_more::Debug;
use drasi_bootstrap_noop::NoOpBootstrapProvider;
use drasi_lib::{config::SourceSubscriptionConfig, DrasiLib, QueryConfig, QueryLanguage, Source};
use drasi_reaction_application::{ApplicationReaction, ApplicationReactionHandle};
use drasi_source_application::{
    ApplicationSource, ApplicationSourceConfig, ApplicationSourceHandle,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
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
mod tests;

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
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct TestRunDrasiLibInstanceOverrides {
    /// Override log level (trace, debug, info, warn, error).
    pub log_level: Option<String>,
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
    Stopped,
    Error(String),
}

impl fmt::Display for TestRunDrasiLibInstanceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uninitialized => write!(f, "Uninitialized"),
            Self::Running => write!(f, "Running"),
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
    pub reaction_handles: Arc<RwLock<HashMap<String, ApplicationReactionHandle>>>,
    pub state: Arc<RwLock<TestRunDrasiLibInstanceState>>,
    pub storage: TestRunDrasiLibInstanceStorage,
    pub id: TestRunDrasiLibInstanceId,
    #[debug(skip)]
    lifecycle_tx: crate::test_run_completion::LifecycleTx,
}

impl TestRunDrasiLibInstance {
    /// Create a new drasi-lib instance and optionally start it immediately.
    pub async fn new(
        definition: TestRunDrasiLibInstanceDefinition,
        storage: TestRunDrasiLibInstanceStorage,
        lifecycle_tx: crate::test_run_completion::LifecycleTx,
    ) -> anyhow::Result<Self> {
        let instance = Self {
            id: definition.id.clone(),
            definition,
            core: Arc::new(RwLock::new(None)),
            source_handles: Arc::new(RwLock::new(HashMap::new())),
            reaction_handles: Arc::new(RwLock::new(HashMap::new())),
            state: Arc::new(RwLock::new(TestRunDrasiLibInstanceState::Uninitialized)),
            storage,
            lifecycle_tx,
        };
        if instance.definition.start_immediately {
            instance.start().await?;
        }
        Ok(instance)
    }

    /// Start this drasi-lib instance.
    pub async fn start(&self) -> anyhow::Result<()> {
        let current = self.state.read().await.clone();
        if matches!(current, TestRunDrasiLibInstanceState::Running) {
            return Ok(());
        }

        let config = self.definition.effective_config();
        let mut builder = DrasiLib::builder().with_id(self.id.to_string());
        let mut source_handles = HashMap::new();
        let mut reaction_handles = HashMap::new();

        for source_config in &config.sources {
            if source_config.kind != "application" {
                anyhow::bail!(
                    "Unsupported drasi-lib instance source kind '{}' for source '{}' (only 'application' is supported)",
                    source_config.kind,
                    source_config.id
                );
            }
            let source_properties = serde_json::from_value(source_config.config.clone())
                .unwrap_or_else(|_| HashMap::new());
            let (source, handle) = ApplicationSource::new(
                source_config.id.clone(),
                ApplicationSourceConfig {
                    properties: source_properties,
                },
            )?;
            source
                .set_bootstrap_provider(Box::new(NoOpBootstrapProvider::new()))
                .await;
            source_handles.insert(source_config.id.clone(), handle);
            builder = builder.with_source(source);
        }

        for query_config in &config.queries {
            let mut query: QueryConfig = serde_json::from_value(query_config.config.clone())
                .unwrap_or_else(|_| QueryConfig {
                    id: query_config.id.clone(),
                    query: query_config.query.clone(),
                    query_language: QueryLanguage::Cypher,
                    middleware: Vec::new(),
                    sources: Vec::new(),
                    auto_start: query_config.auto_start,
                    joins: None,
                    enable_bootstrap: true,
                    bootstrap_buffer_size: 10_000,
                    priority_queue_capacity: None,
                    dispatch_buffer_capacity: None,
                    dispatch_mode: None,
                    storage_backend: None,
                    recovery_policy: None,
                });
            query.id = query_config.id.clone();
            query.query = query_config.query.clone();
            query.auto_start = query_config.auto_start;
            query.sources = query_config
                .sources
                .iter()
                .map(|source_id| SourceSubscriptionConfig {
                    source_id: source_id.clone(),
                    nodes: Vec::new(),
                    relations: Vec::new(),
                    pipeline: Vec::new(),
                })
                .collect();
            builder = builder.with_query(query);
        }

        for reaction_config in &config.reactions {
            if reaction_config.kind != "application" {
                anyhow::bail!(
                    "Unsupported drasi-lib instance reaction kind '{}' for reaction '{}' (only 'application' is supported)",
                    reaction_config.kind,
                    reaction_config.id
                );
            }
            let (reaction, handle) = ApplicationReaction::builder(reaction_config.id.clone())
                .with_queries(reaction_config.queries.clone())
                .with_auto_start(reaction_config.auto_start)
                .build();
            reaction_handles.insert(reaction_config.id.clone(), handle);
            builder = builder.with_reaction(reaction);
        }

        let core = Arc::new(builder.build().await?);
        core.start().await?;
        self.storage
            .write_instance_config(&serde_json::to_value(&config)?)
            .await?;
        *self.core.write().await = Some(core);
        *self.source_handles.write().await = source_handles;
        *self.reaction_handles.write().await = reaction_handles;
        *self.state.write().await = TestRunDrasiLibInstanceState::Running;
        self.lifecycle_tx
            .drasi_lib_instance_started(self.id.clone());
        Ok(())
    }

    /// Stop this drasi-lib instance.
    pub async fn stop(&self) -> anyhow::Result<()> {
        if let Some(core) = self.core.write().await.take() {
            core.shutdown().await?;
        }
        self.source_handles.write().await.clear();
        self.reaction_handles.write().await.clear();
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

    /// Get an application source handle by source name.
    pub async fn get_source_handle(&self, name: &str) -> anyhow::Result<ApplicationSourceHandle> {
        self.source_handles
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("drasi-lib instance source handle not found: {name}"))
    }

    /// Get an application reaction handle by reaction name.
    pub async fn get_reaction_handle(
        &self,
        name: &str,
    ) -> anyhow::Result<ApplicationReactionHandle> {
        self.reaction_handles
            .read()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("drasi-lib instance reaction handle not found: {name}"))
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
