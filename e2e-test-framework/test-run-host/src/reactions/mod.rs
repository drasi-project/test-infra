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

//! Reaction handling module for the E2E Test Framework
//!
//! This module provides functionality for observing and handling reactions,
//! which are external invocations triggered by Drasi queries. Unlike query
//! results that are streamed, reactions are HTTP callbacks or event notifications.

use std::fmt;

use derive_more::Debug;
use serde::{Deserialize, Serialize};

use output_loggers::OutputLoggerConfig;
use test_data_store::{
    test_repo_storage::models::{ReactionHandlerDefinition, StopTriggerDefinition},
    test_run_storage::{ParseTestRunIdError, TestRunId, TestRunReactionId, TestRunReactionStorage},
};

pub mod output_loggers;
pub mod reaction_handlers;
pub mod reaction_observer;
pub mod reaction_output_handler;
pub mod stop_triggers;

// Re-export commonly used types from reaction_output_handler
pub use reaction_output_handler::{
    create_reaction_handler, ReactionControlSignal, ReactionHandlerError, ReactionHandlerMessage,
    ReactionHandlerPayload, ReactionHandlerStatus, ReactionHandlerType, ReactionInvocation,
    ReactionOutputHandler,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunReactionOverrides {
    pub stop_triggers: Option<Vec<StopTriggerDefinition>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunReactionConfig {
    #[serde(default = "default_start_immediately")]
    pub start_immediately: bool,
    pub test_reaction_id: String,
    pub test_run_overrides: Option<TestRunReactionOverrides>,
    #[serde(default)]
    pub output_loggers: Vec<OutputLoggerConfig>,
    // Legacy fields for backward compatibility - will be set by TestRun
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub test_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub test_repo_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub test_run_id: Option<String>,
}

fn default_start_immediately() -> bool {
    false
}

impl TryFrom<&TestRunReactionConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunReactionConfig) -> Result<Self, Self::Error> {
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

impl fmt::Display for TestRunReactionConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TestRunReactionConfig: Repo: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_query_id: {:?}",
            self.test_repo_id, self.test_id, self.test_run_id, self.test_reaction_id
        )
    }
}

#[derive(Clone, Debug)]
pub struct TestRunReactionDefinition {
    pub id: TestRunReactionId,
    pub start_immediately: bool,
    pub reaction_handler_definition: ReactionHandlerDefinition,
    pub test_reaction_definition:
        test_data_store::test_repo_storage::models::TestReactionDefinition,
    pub test_run_overrides: Option<TestRunReactionOverrides>,
    pub output_loggers: Vec<OutputLoggerConfig>,
}

impl TestRunReactionDefinition {
    pub fn new(
        test_run_reaction_config: TestRunReactionConfig,
        test_reaction_definition: test_data_store::test_repo_storage::models::TestReactionDefinition,
        reaction_handler_definition: ReactionHandlerDefinition,
        output_loggers: Vec<OutputLoggerConfig>,
    ) -> anyhow::Result<Self> {
        let test_run_id = TestRunId::try_from(&test_run_reaction_config)?;
        let id = TestRunReactionId::new(&test_run_id, &test_run_reaction_config.test_reaction_id);

        Ok(Self {
            id,
            start_immediately: test_run_reaction_config.start_immediately,
            reaction_handler_definition,
            test_reaction_definition,
            test_run_overrides: test_run_reaction_config.test_run_overrides,
            output_loggers,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct TestRunReactionState {
    pub id: TestRunReactionId,
    pub reaction_observer: reaction_observer::ReactionObserverExternalState,
    pub start_immediately: bool,
}

#[derive(Debug)]
pub struct TestRunReaction {
    pub id: TestRunReactionId,
    #[debug(skip)]
    pub reaction_observer: reaction_observer::ReactionObserver,
    pub start_immediately: bool,
}

impl TestRunReaction {
    pub async fn new(
        definition: TestRunReactionDefinition,
        output_storage: TestRunReactionStorage,
        lifecycle_tx: crate::test_run_completion::LifecycleTx,
    ) -> anyhow::Result<Self> {
        // Output loggers are already in the correct format
        let output_loggers = definition.output_loggers.clone();

        log::info!(
            "TestRunReaction::new() for {} with {} output loggers: {:?}",
            definition.id,
            output_loggers.len(),
            output_loggers
        );

        // Get stop triggers from test definition, allow overrides
        let mut stop_triggers = definition
            .test_reaction_definition
            .stop_triggers
            .clone()
            .unwrap_or_default();
        if let Some(overrides) = &definition.test_run_overrides {
            if let Some(override_stop_triggers) = &overrides.stop_triggers {
                stop_triggers = override_stop_triggers.clone();
            }
        }

        let reaction_observer = reaction_observer::ReactionObserver::new(
            definition.id.clone(),
            definition.reaction_handler_definition.clone(),
            output_storage,
            output_loggers,
            stop_triggers,
            definition.test_run_overrides,
            lifecycle_tx,
        )
        .await?;

        let reaction = Self {
            id: definition.id.clone(),
            reaction_observer,
            start_immediately: definition.start_immediately,
        };

        // Don't auto-start here - TestRunHost will handle it after setting references
        // if reaction.start_immediately {
        //     reaction.start_reaction_observer().await?;
        // }

        Ok(reaction)
    }

    pub async fn get_state(&self) -> anyhow::Result<TestRunReactionState> {
        Ok(TestRunReactionState {
            id: self.id.clone(),
            reaction_observer: self.get_reaction_observer_state().await?,
            start_immediately: self.start_immediately,
        })
    }

    pub async fn get_reaction_observer_state(
        &self,
    ) -> anyhow::Result<reaction_observer::ReactionObserverExternalState> {
        Ok(self.reaction_observer.get_state().await?.state)
    }

    pub async fn pause_reaction_observer(
        &self,
    ) -> anyhow::Result<reaction_observer::ReactionObserverCommandResponse> {
        self.reaction_observer.pause().await
    }

    pub async fn reset_reaction_observer(
        &self,
    ) -> anyhow::Result<reaction_observer::ReactionObserverCommandResponse> {
        self.reaction_observer.reset().await
    }

    pub async fn start_reaction_observer(
        &self,
    ) -> anyhow::Result<reaction_observer::ReactionObserverCommandResponse> {
        self.reaction_observer.start().await
    }

    pub async fn stop_reaction_observer(
        &self,
    ) -> anyhow::Result<reaction_observer::ReactionObserverCommandResponse> {
        self.reaction_observer.stop().await
    }

    /// Sets the TestRunHost for handlers that need it (e.g., DrasiServerChannelHandler)
    pub fn set_test_run_host(&self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        self.reaction_observer.set_test_run_host(test_run_host);
    }
}

#[cfg(test)]
mod tests;
