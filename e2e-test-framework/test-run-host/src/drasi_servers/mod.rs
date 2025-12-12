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

use std::fmt;
use std::sync::Arc;

use derive_more::Debug;
use drasi_lib::{DrasiLib, Query, QueryConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use utoipa::ToSchema;

use crate::test_run_completion::LifecycleTx;
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
                write!(f, "Running since {start_time}")
            }
            TestRunDrasiServerState::Stopped { stop_time, reason } => {
                if let Some(reason) = reason {
                    write!(f, "Stopped at {stop_time} ({reason})")
                } else {
                    write!(f, "Stopped at {stop_time}")
                }
            }
            TestRunDrasiServerState::Error {
                error_time,
                message,
            } => {
                write!(f, "Error at {error_time}: {message}")
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
    drasi_core: Arc<RwLock<Option<Arc<DrasiLib>>>>,
    #[debug(skip)]
    lifecycle_tx: LifecycleTx,
}

impl TestRunDrasiServer {
    pub async fn new(
        definition: TestRunDrasiServerDefinition,
        storage: TestRunDrasiServerStorage,
        lifecycle_tx: LifecycleTx,
    ) -> anyhow::Result<Self> {
        let server = Self {
            definition,
            state: Arc::new(RwLock::new(TestRunDrasiServerState::Uninitialized)),
            storage,
            drasi_core: Arc::new(RwLock::new(None)),
            lifecycle_tx,
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
                    server.set_error(e.to_string()).await;
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

                // NOTE: The new drasi_lib API requires sources and reactions to be passed as
                // pre-built instances implementing Source and Reaction traits.
                // Dynamic configuration from JSON is no longer supported for sources/reactions.
                // Only queries can be configured via the builder.

                if !config.sources.is_empty() {
                    log::warn!(
                        "DrasiLib no longer supports dynamic source configuration. {} sources configured but will be ignored. \
                         Sources must be added as instances implementing the Source trait.",
                        config.sources.len()
                    );
                }

                if !config.reactions.is_empty() {
                    log::warn!(
                        "DrasiLib no longer supports dynamic reaction configuration. {} reactions configured but will be ignored. \
                         Reactions must be added as instances implementing the Reaction trait.",
                        config.reactions.len()
                    );
                }

                // Build queries using the builder API
                let drasi_queries: Vec<QueryConfig> = config
                    .queries
                    .iter()
                    .map(|q| {
                        let mut builder = Query::cypher(&q.id)
                            .query(&q.query)
                            .auto_start(q.auto_start);

                        // Add source subscriptions
                        for source_id in &q.sources {
                            builder = builder.from_source(source_id);
                        }

                        builder.build()
                    })
                    .collect();

                // Log configuration summary
                log::info!(
                    "Building DrasiLib with {} queries (sources and reactions must be added as instances)",
                    drasi_queries.len()
                );

                // Use the builder API to create and initialize DrasiLib
                log::info!("Creating DrasiLib with builder API...");
                let mut builder =
                    DrasiLib::builder().with_id(&self.definition.id.test_drasi_server_id);

                for query_config in drasi_queries {
                    builder = builder.with_query(query_config);
                }

                let core = builder
                    .build()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to build DrasiLib: {e}"))?;

                let core = Arc::new(core);

                // Start the core to start all auto-start components
                log::info!("Starting DrasiLib to start auto-start components...");
                core.start()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to start DrasiLib: {e}"))?;

                // Store the core reference
                {
                    let mut core_guard = self.drasi_core.write().await;
                    *core_guard = Some(core.clone());
                }

                log::info!(
                    "DrasiLib initialized with {} queries configured",
                    config.queries.len()
                );

                // Log the status of components
                log::info!("DrasiLib ready, verifying component status...");
                log::info!("All queries configured and started according to auto_start settings");

                // Log validation information
                if config.queries.is_empty() {
                    log::warn!(
                        "Drasi Server {} configured without any queries",
                        self.definition.id
                    );
                } else {
                    log::info!(
                        "Drasi Server {} configured with {} queries",
                        self.definition.id,
                        config.queries.len()
                    );
                }

                // Update state
                *state = TestRunDrasiServerState::Running {
                    start_time: chrono::Utc::now(),
                };

                // Emit lifecycle event
                self.lifecycle_tx
                    .drasi_server_started(self.definition.id.clone());

                // Write server config to storage
                let config_json = serde_json::to_value(&config)?;
                self.storage.write_server_config(&config_json).await?;

                log::info!("DrasiLib {} started successfully", self.definition.id);

                // Add a small delay to ensure all async initialization completes
                log::info!("Waiting 100ms for DrasiLib components to fully initialize...");
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
                // Note: DrasiLib doesn't need explicit shutdown - dropping the Arc handles cleanup
                {
                    let mut core_guard = self.drasi_core.write().await;
                    *core_guard = None;
                }

                // Update state
                *state = TestRunDrasiServerState::Stopped {
                    stop_time: chrono::Utc::now(),
                    reason: reason.clone(),
                };

                // Emit lifecycle event
                self.lifecycle_tx
                    .drasi_server_stopped(self.definition.id.clone());

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

    /// Transition to error state and emit lifecycle event
    pub async fn set_error(&self, error_message: String) {
        let mut state = self.state.write().await;
        *state = TestRunDrasiServerState::Error {
            error_time: chrono::Utc::now(),
            message: error_message.clone(),
        };

        // Emit lifecycle event
        self.lifecycle_tx
            .drasi_server_error(self.definition.id.clone(), error_message);
    }

    pub async fn get_server_core(&self) -> Option<Arc<DrasiLib>> {
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

    // NOTE: ApplicationHandle is no longer available in drasi_lib.
    // The new plugin architecture requires sources and reactions to be passed as instances.

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
                    "Drasi Server {id} is being dropped while still running, clearing core reference"
                );

                // Clear the core reference - DrasiLib cleanup happens when the Arc is dropped
                let mut core_guard = drasi_core.write().await;
                *core_guard = None;
            }
        });
    }
}
