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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use derive_more::Debug;
use drasi_server_core::{
    DrasiServerCore,
    Source, Query, Reaction,
    QueryConfig, ReactionConfig, SourceConfig,
    application::ApplicationHandle
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use utoipa::ToSchema;

use test_data_store::{
    test_repo_storage::models::{
        DrasiServerConfig as TestDrasiServerConfig, TestDrasiServerDefinition,
    },
    test_run_storage::{
        ParseTestRunIdError, TestRunDrasiServerId, TestRunDrasiServerStorage, TestRunId,
    },
};

pub mod api_models;
pub mod programmatic_api;

#[cfg(test)]
mod tests;

/// Runtime configuration for a test run Drasi Server
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct TestRunDrasiServerConfig {
    #[serde(default = "default_start_immediately")]
    pub start_immediately: bool,
    pub test_drasi_server_id: String,
    pub test_run_overrides: Option<TestRunDrasiServerOverrides>,
    // Legacy fields for backward compatibility - will be set by TestRun
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

/// Overrides for Drasi Server configuration at runtime
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct TestRunDrasiServerOverrides {
    /// Override authentication settings
    pub auth: Option<test_data_store::test_repo_storage::models::DrasiServerAuthConfig>,

    /// Override storage settings
    pub storage: Option<test_data_store::test_repo_storage::models::DrasiServerStorageConfig>,

    /// Override log level (trace, debug, info, warn, error)
    pub log_level: Option<String>,
}

impl TryFrom<&TestRunDrasiServerConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunDrasiServerConfig) -> Result<Self, Self::Error> {
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

impl TryFrom<&TestRunDrasiServerConfig> for TestRunDrasiServerId {
    type Error = test_data_store::test_run_storage::ParseTestRunDrasiServerIdError;

    fn try_from(value: &TestRunDrasiServerConfig) -> Result<Self, Self::Error> {
        match TestRunId::try_from(value) {
            Ok(test_run_id) => Ok(TestRunDrasiServerId::new(
                &test_run_id,
                &value.test_drasi_server_id,
            )),
            Err(e) => Err(
                test_data_store::test_run_storage::ParseTestRunDrasiServerIdError::InvalidValues(
                    e.to_string(),
                ),
            ),
        }
    }
}

impl fmt::Display for TestRunDrasiServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestRunDrasiServerConfig: Repo: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_drasi_server_id: {:?}", 
            self.test_repo_id, self.test_id, self.test_run_id, self.test_drasi_server_id)
    }
}

/// Combined test and runtime configuration for a Drasi Server
#[derive(Clone, Debug)]
pub struct TestRunDrasiServerDefinition {
    pub id: TestRunDrasiServerId,
    pub start_immediately: bool,
    pub test_drasi_server_definition: TestDrasiServerDefinition,
    pub test_run_overrides: Option<TestRunDrasiServerOverrides>,
}

impl TestRunDrasiServerDefinition {
    pub fn new(
        config: TestRunDrasiServerConfig,
        test_drasi_server_definition: TestDrasiServerDefinition,
    ) -> anyhow::Result<Self> {
        let id = TestRunDrasiServerId::try_from(&config)?;

        Ok(Self {
            id,
            start_immediately: config.start_immediately,
            test_drasi_server_definition,
            test_run_overrides: config.test_run_overrides,
        })
    }

    /// Get the effective configuration with overrides applied
    pub fn effective_config(&self) -> TestDrasiServerConfig {
        let mut config = self.test_drasi_server_definition.config.clone();

        if let Some(overrides) = &self.test_run_overrides {
            if let Some(auth) = &overrides.auth {
                config.auth = Some(auth.clone());
            }
            if let Some(storage) = &overrides.storage {
                config.storage = Some(storage.clone());
            }
            if let Some(log_level) = &overrides.log_level {
                config.log_level = Some(log_level.clone());
            }
        }

        config
    }
}

/// State of a test run Drasi Server
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum TestRunDrasiServerState {
    Uninitialized,
    Running {
        start_time: chrono::DateTime<chrono::Utc>,
    },
    Stopped {
        stop_time: chrono::DateTime<chrono::Utc>,
        reason: Option<String>,
    },
    Error {
        error_time: chrono::DateTime<chrono::Utc>,
        message: String,
    },
}

impl fmt::Display for TestRunDrasiServerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TestRunDrasiServerState::Uninitialized => write!(f, "Uninitialized"),
            TestRunDrasiServerState::Running { start_time } => {
                write!(f, "Running since {}", start_time)
            }
            TestRunDrasiServerState::Stopped { stop_time, reason } => {
                if let Some(reason) = reason {
                    write!(f, "Stopped at {} ({})", stop_time, reason)
                } else {
                    write!(f, "Stopped at {}", stop_time)
                }
            }
            TestRunDrasiServerState::Error {
                error_time,
                message,
            } => {
                write!(f, "Error at {}: {}", error_time, message)
            }
        }
    }
}

/// Test run Drasi Server component
#[derive(Debug)]
pub struct TestRunDrasiServer {
    pub definition: TestRunDrasiServerDefinition,
    pub state: Arc<RwLock<TestRunDrasiServerState>>,
    pub storage: TestRunDrasiServerStorage,
    #[debug(skip)]
    drasi_core: Arc<RwLock<Option<Arc<DrasiServerCore>>>>,
    #[debug(skip)]
    application_handles: Arc<RwLock<HashMap<String, ApplicationHandle>>>,
}

impl TestRunDrasiServer {
    pub async fn new(
        definition: TestRunDrasiServerDefinition,
        storage: TestRunDrasiServerStorage,
    ) -> anyhow::Result<Self> {
        let server = Self {
            definition,
            state: Arc::new(RwLock::new(TestRunDrasiServerState::Uninitialized)),
            storage,
            drasi_core: Arc::new(RwLock::new(None)),
            application_handles: Arc::new(RwLock::new(HashMap::new())),
        };

        // Start immediately if configured
        if server.definition.start_immediately {
            log::info!(
                "Auto-starting Drasi Server {} with start_immediately=true",
                server.definition.id
            );
            match server.start().await {
                Ok(()) => {
                    let endpoint = server.get_api_endpoint().await;
                    log::info!(
                        "Drasi Server {} auto-started successfully at {}",
                        server.definition.id,
                        endpoint.unwrap_or_else(|| "unknown endpoint".to_string())
                    );
                }
                Err(e) => {
                    log::error!(
                        "Failed to auto-start Drasi Server {}: {}",
                        server.definition.id,
                        e
                    );
                    return Err(e);
                }
            }
        } else {
            log::info!(
                "Drasi Server {} created with start_immediately=false, manual start required",
                server.definition.id
            );
        }

        Ok(server)
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let mut state = self.state.write().await;

        match &*state {
            TestRunDrasiServerState::Uninitialized => {
                // Get effective configuration
                let config = self.definition.effective_config();

                // Build sources using the builder API
                let drasi_sources: Vec<SourceConfig> = config
                    .sources
                    .iter()
                    .map(|s| {
                        Source::custom(&s.id, &s.source_type)
                            .auto_start(s.auto_start)
                            .with_properties(s.properties.clone().into())
                            .build()
                    })
                    .collect();

                // Build queries using the builder API
                let drasi_queries: Vec<QueryConfig> = config
                    .queries
                    .iter()
                    .map(|q| {
                        Query::cypher(&q.id)
                            .query(&q.query)
                            .from_sources(q.sources.clone())
                            .auto_start(q.auto_start)
                            .with_properties(q.properties.clone().into())
                            .build()
                    })
                    .collect();

                // Build reactions using the builder API
                let drasi_reactions: Vec<ReactionConfig> = config
                    .reactions
                    .iter()
                    .map(|r| {
                        Reaction::custom(&r.id, &r.reaction_type)
                            .subscribe_to_queries(r.queries.clone())
                            .auto_start(r.auto_start)
                            .with_properties(r.properties.clone().into())
                            .build()
                    })
                    .collect();

                // Log configuration summary
                log::info!(
                    "Building DrasiServerCore with {} sources, {} queries, {} reactions",
                    drasi_sources.len(),
                    drasi_queries.len(),
                    drasi_reactions.len()
                );

                // Use the builder API to create and initialize DrasiServerCore
                log::info!("Creating DrasiServerCore with builder API...");
                let core = DrasiServerCore::builder()
                    .with_id(&self.definition.id.test_drasi_server_id)
                    .add_sources(drasi_sources)
                    .add_queries(drasi_queries)
                    .add_reactions(drasi_reactions)
                    .build()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to build DrasiServerCore: {}", e))?;

                let core = Arc::new(core);

                // Start the core to start all auto-start components
                log::info!("Starting DrasiServerCore to start auto-start components...");
                core.start()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to start DrasiServerCore: {}", e))?;

                // Store configured component names for validation
                let configured_source_names: std::collections::HashSet<String> =
                    config.sources.iter().map(|s| s.id.clone()).collect();
                let configured_query_names: std::collections::HashSet<String> =
                    config.queries.iter().map(|q| q.id.clone()).collect();
                let configured_reaction_names: std::collections::HashSet<String> =
                    config.reactions.iter().map(|r| r.id.clone()).collect();

                // Store the core reference
                {
                    let mut core_guard = self.drasi_core.write().await;
                    *core_guard = Some(core.clone());
                }

                log::info!("DrasiServerCore initialized with {} sources, {} queries, {} reactions configured",
                    config.sources.len(), config.queries.len(), config.reactions.len());

                // Log the status of components
                log::info!("DrasiServerCore ready, verifying component status...");

                // Note: Query status verification is now handled internally by DrasiServerCore
                // The new API does not expose query_manager() directly
                log::info!("All queries configured and started according to auto_start settings");

                // Get and store application handles using the new direct handle access
                {
                    let mut stored_handles = self.application_handles.write().await;
                    stored_handles.clear();

                    // Get handles for configured sources using direct handle access
                    for source_config in &config.sources {
                        match core.source_handle(&source_config.id) {
                            Ok(handle) => {
                                stored_handles.insert(
                                    source_config.id.clone(),
                                    ApplicationHandle::source_only(handle),
                                );
                                log::info!(
                                    "Stored ApplicationHandle for source '{}' on Drasi Server {}",
                                    source_config.id,
                                    self.definition.id
                                );
                            }
                            Err(e) => {
                                log::warn!(
                                    "Could not get ApplicationHandle for source '{}' on Drasi Server {}: {}",
                                    source_config.id,
                                    self.definition.id,
                                    e
                                );
                            }
                        }
                    }

                    // Get handles for configured reactions using direct handle access
                    for reaction_config in &config.reactions {
                        match core.reaction_handle(&reaction_config.id) {
                            Ok(handle) => {
                                stored_handles.insert(
                                    reaction_config.id.clone(),
                                    ApplicationHandle::reaction_only(handle),
                                );
                                log::info!(
                                    "Stored ApplicationHandle for reaction '{}' on Drasi Server {}",
                                    reaction_config.id,
                                    self.definition.id
                                );
                            }
                            Err(e) => {
                                log::warn!(
                                    "Could not get ApplicationHandle for reaction '{}' on Drasi Server {}: {}",
                                    reaction_config.id,
                                    self.definition.id,
                                    e
                                );
                            }
                        }
                    }

                    // Note: Query manager doesn't provide application handles

                    log::info!(
                        "Stored {} application handles for Drasi Server {} after starting",
                        stored_handles.len(),
                        self.definition.id
                    );
                }

                // Log validation information
                if configured_source_names.is_empty()
                    && configured_query_names.is_empty()
                    && configured_reaction_names.is_empty()
                {
                    log::warn!(
                        "Drasi Server {} configured without any sources, queries, or reactions",
                        self.definition.id
                    );
                } else {
                    log::info!(
                        "Drasi Server {} configured with {} sources, {} queries, {} reactions",
                        self.definition.id,
                        configured_source_names.len(),
                        configured_query_names.len(),
                        configured_reaction_names.len()
                    );
                }

                // Update state
                *state = TestRunDrasiServerState::Running {
                    start_time: chrono::Utc::now(),
                };

                // Write server config to storage
                let config_json = serde_json::to_value(&config)?;
                self.storage.write_server_config(&config_json).await?;

                log::info!(
                    "DrasiServerCore {} started successfully",
                    self.definition.id
                );

                // Add a small delay to ensure all async initialization completes
                log::info!("Waiting 100ms for DrasiServerCore components to fully initialize...");
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                Ok(())
            }
            TestRunDrasiServerState::Running { .. } => {
                anyhow::bail!("Server is already running");
            }
            TestRunDrasiServerState::Stopped { .. } => {
                anyhow::bail!("Server has been stopped and cannot be restarted");
            }
            TestRunDrasiServerState::Error { .. } => {
                anyhow::bail!("Server is in error state");
            }
        }
    }

    pub async fn stop(&self, reason: Option<String>) -> anyhow::Result<()> {
        let mut state = self.state.write().await;

        match &*state {
            TestRunDrasiServerState::Running { .. } => {
                // Clear the core reference
                // Note: DrasiServerCore doesn't need explicit shutdown
                {
                    let mut core_guard = self.drasi_core.write().await;
                    *core_guard = None;
                }

                // Clear application handles
                {
                    let mut handles = self.application_handles.write().await;
                    handles.clear();
                }

                // Update state
                *state = TestRunDrasiServerState::Stopped {
                    stop_time: chrono::Utc::now(),
                    reason,
                };

                log::info!("Drasi Server {} stopped", self.definition.id);
                Ok(())
            }
            _ => {
                anyhow::bail!("Server is not running");
            }
        }
    }

    pub async fn get_state(&self) -> TestRunDrasiServerState {
        self.state.read().await.clone()
    }

    pub async fn get_server_core(&self) -> Option<Arc<DrasiServerCore>> {
        let core_guard = self.drasi_core.read().await;
        core_guard.clone()
    }

    pub async fn get_server_port(&self) -> Option<u16> {
        // DrasiServerCore doesn't use ports - it's an embedded library, not a network server
        None
    }

    /// Returns the API endpoint for this Drasi Server.
    ///
    /// **Note**: This always returns `None` because DrasiServerCore is an embedded library
    /// that provides programmatic access to Drasi functionality, not a standalone server
    /// with HTTP endpoints. The test infrastructure wraps DrasiServerCore with its own
    /// REST API (test-service) for external access.
    pub async fn get_api_endpoint(&self) -> Option<String> {
        None
    }

    pub async fn get_application_handle(&self, name: &str) -> Option<ApplicationHandle> {
        self.application_handles.read().await.get(name).cloned()
    }

    pub async fn write_summary(&self) -> anyhow::Result<()> {
        let summary = serde_json::json!({
            "id": self.definition.id.to_string(),
            "name": self.definition.test_drasi_server_definition.name,
            "state": self.get_state().await,
            "config": self.definition.effective_config(),
        });

        self.storage.write_test_run_summary(&summary).await?;
        Ok(())
    }
}

impl Drop for TestRunDrasiServer {
    fn drop(&mut self) {
        // Schedule cleanup of the server if it's still running
        let state = self.state.clone();
        let drasi_core = self.drasi_core.clone();
        let id = self.definition.id.clone();

        tokio::spawn(async move {
            let current_state = state.read().await;
            if matches!(*current_state, TestRunDrasiServerState::Running { .. }) {
                log::warn!(
                    "Drasi Server {} is being dropped while still running, clearing core reference",
                    id
                );

                // Clear the core reference
                let mut core_guard = drasi_core.write().await;
                *core_guard = None;
            }
        });
    }
}
