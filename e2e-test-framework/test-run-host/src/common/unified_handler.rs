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

//! Unified output handler trait and status management
//!
//! This module provides a unified handler interface that merges the previously
//! separate ResultStreamHandler and ReactionHandler traits. This unification:
//! - Reduces code duplication in the QueryResultObserver
//! - Provides a consistent interface for all output handlers
//! - Simplifies the addition of new handler types
//! - Enables unified stop trigger evaluation

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

use super::output_handler_message::OutputHandlerMessage;

/// Unified status for all output handlers
///
/// This enum combines the states from both ResultStreamHandlerStatus and
/// ReactionHandlerStatus, with bootstrap states as optional transitions
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnifiedHandlerStatus {
    /// Handler has not been initialized
    Uninitialized,
    /// Handler is in bootstrap phase (ResultStream specific)
    BootstrapStarted,
    /// Handler has completed bootstrap (ResultStream specific)
    BootstrapComplete,
    /// Handler is actively processing
    Running,
    /// Handler is temporarily paused
    Paused,
    /// Handler has been stopped
    Stopped,
    /// Handler was deleted/removed
    Deleted,
    /// Handler encountered an error
    Error,
}

impl UnifiedHandlerStatus {
    /// Check if this is a bootstrap-related status
    pub fn is_bootstrap_state(&self) -> bool {
        matches!(self, Self::BootstrapStarted | Self::BootstrapComplete)
    }

    /// Check if the handler is in an active state
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Running | Self::BootstrapStarted | Self::BootstrapComplete
        )
    }

    /// Check if the handler is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Deleted | Self::Error)
    }
}

impl Default for UnifiedHandlerStatus {
    fn default() -> Self {
        Self::Uninitialized
    }
}

impl Serialize for UnifiedHandlerStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let status_str = match self {
            Self::Uninitialized => "Uninitialized",
            Self::BootstrapStarted => "BootstrapStarted",
            Self::BootstrapComplete => "BootstrapComplete",
            Self::Running => "Running",
            Self::Paused => "Paused",
            Self::Stopped => "Stopped",
            Self::Deleted => "Deleted",
            Self::Error => "Error",
        };
        serializer.serialize_str(status_str)
    }
}

/// Unified output handler trait
///
/// This trait provides a common interface for all handlers that process
/// query output, whether from result streams or reactions.
#[async_trait]
pub trait OutputHandler: Send + Sync {
    /// Initialize the handler and return a channel for receiving messages
    async fn init(&self) -> anyhow::Result<Receiver<OutputHandlerMessage>>;

    /// Start or resume handler processing
    async fn start(&self) -> anyhow::Result<()>;

    /// Pause handler processing (can be resumed)
    async fn pause(&self) -> anyhow::Result<()>;

    /// Stop handler processing (terminal state)
    async fn stop(&self) -> anyhow::Result<()>;

    /// Get the current handler status
    async fn status(&self) -> UnifiedHandlerStatus {
        UnifiedHandlerStatus::Uninitialized
    }

    /// Get handler-specific metrics (optional)
    async fn metrics(&self) -> Option<serde_json::Value> {
        None
    }
}

/// Implement OutputHandler for boxed trait objects
#[async_trait]
impl OutputHandler for Box<dyn OutputHandler + Send + Sync> {
    async fn init(&self) -> anyhow::Result<Receiver<OutputHandlerMessage>> {
        (**self).init().await
    }

    async fn start(&self) -> anyhow::Result<()> {
        (**self).start().await
    }

    async fn pause(&self) -> anyhow::Result<()> {
        (**self).pause().await
    }

    async fn stop(&self) -> anyhow::Result<()> {
        (**self).stop().await
    }

    async fn status(&self) -> UnifiedHandlerStatus {
        (**self).status().await
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        (**self).metrics().await
    }
}

/// Configuration for creating output handlers
#[derive(Clone, Debug)]
pub enum OutputHandlerConfig {
    /// Redis result stream handler configuration
    RedisResultStream {
        redis_url: String,
        stream_key: String,
        consumer_group: String,
        consumer_name: String,
    },
    /// HTTP reaction handler configuration
    HttpReaction {
        listen_address: String,
        listen_port: u16,
        path_prefix: String,
    },
    /// Future: EventGrid reaction handler
    EventGridReaction {
        endpoint: String,
        access_key: String,
    },
    /// Future: Dapr pubsub handler
    DaprPubSub { pubsub_name: String, topic: String },
}

/// Conversion utilities for migrating from legacy status types
impl From<crate::queries::result_stream_handlers::ResultStreamHandlerStatus>
    for UnifiedHandlerStatus
{
    fn from(status: crate::queries::result_stream_handlers::ResultStreamHandlerStatus) -> Self {
        match status {
            crate::queries::result_stream_handlers::ResultStreamHandlerStatus::Unknown => {
                Self::Uninitialized
            }
            crate::queries::result_stream_handlers::ResultStreamHandlerStatus::BootstrapStarted => {
                Self::BootstrapStarted
            }
            crate::queries::result_stream_handlers::ResultStreamHandlerStatus::BootstrapComplete => {
                Self::BootstrapComplete
            }
            crate::queries::result_stream_handlers::ResultStreamHandlerStatus::Running => Self::Running,
            crate::queries::result_stream_handlers::ResultStreamHandlerStatus::Stopped => Self::Stopped,
            crate::queries::result_stream_handlers::ResultStreamHandlerStatus::Deleted => Self::Deleted,
        }
    }
}

impl From<crate::reactions::reaction_handlers::ReactionHandlerStatus> for UnifiedHandlerStatus {
    fn from(status: crate::reactions::reaction_handlers::ReactionHandlerStatus) -> Self {
        match status {
            crate::reactions::reaction_handlers::ReactionHandlerStatus::Uninitialized => {
                Self::Uninitialized
            }
            crate::reactions::reaction_handlers::ReactionHandlerStatus::Running => Self::Running,
            crate::reactions::reaction_handlers::ReactionHandlerStatus::Paused => Self::Paused,
            crate::reactions::reaction_handlers::ReactionHandlerStatus::Stopped => Self::Stopped,
            crate::reactions::reaction_handlers::ReactionHandlerStatus::Error => Self::Error,
        }
    }
}

use test_data_store::{
    test_repo_storage::models::{ReactionHandlerDefinition, ResultStreamHandlerDefinition},
    test_run_storage::TestRunQueryId,
};

/// Unified handler definition that encompasses all handler types
#[derive(Clone, Debug)]
pub enum UnifiedHandlerDefinition {
    ResultStream(ResultStreamHandlerDefinition),
    Reaction(ReactionHandlerDefinition),
}

/// Create an output handler from a unified definition
///
/// NOTE: This function is deprecated and will be removed.
/// Use create_query_handler or create_reaction_handler directly instead.
pub async fn create_output_handler(
    _id: TestRunQueryId,
    _definition: UnifiedHandlerDefinition,
) -> anyhow::Result<Box<dyn OutputHandler + Send + Sync>> {
    anyhow::bail!("create_output_handler is deprecated. Use create_query_handler or create_reaction_handler directly instead.")
}
