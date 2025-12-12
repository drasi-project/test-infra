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

//! Reaction-specific output handler trait and status management
//!
//! This module provides handler infrastructure specifically for reaction
//! processing, handling HTTP callbacks and event notifications.

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

/// Reaction handler status enum
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReactionHandlerStatus {
    /// Handler has not been initialized
    Uninitialized,
    /// Handler is actively processing reactions
    Running,
    /// Handler is temporarily paused
    Paused,
    /// Handler has been stopped
    Stopped,
    /// Handler encountered an error
    Error,
}

impl ReactionHandlerStatus {
    /// Check if the handler is in an active state
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Running)
    }

    /// Check if the handler is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Error)
    }
}

impl Default for ReactionHandlerStatus {
    fn default() -> Self {
        Self::Uninitialized
    }
}

impl Serialize for ReactionHandlerStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let status_str = match self {
            Self::Uninitialized => "Uninitialized",
            Self::Running => "Running",
            Self::Paused => "Paused",
            Self::Stopped => "Stopped",
            Self::Error => "Error",
        };
        serializer.serialize_str(status_str)
    }
}

/// Reaction-specific handler message type
#[derive(Clone, Debug)]
pub enum ReactionHandlerMessage {
    /// Invocation containing reaction data
    Invocation(ReactionInvocation),
    /// Control signal for handler lifecycle
    Control(ReactionControlSignal),
    /// Error occurred in handler
    Error(ReactionHandlerError),
}

/// Reaction invocation data
#[derive(Clone, Debug)]
pub struct ReactionInvocation {
    /// Type of handler that received this invocation
    pub handler_type: ReactionHandlerType,
    /// Reaction payload
    pub payload: ReactionHandlerPayload,
}

/// Supported reaction handler types
#[derive(Clone, Debug, PartialEq)]
pub enum ReactionHandlerType {
    Http,
    EventGrid,
    Grpc,
}

/// Reaction payload
#[derive(Clone, Debug)]
pub struct ReactionHandlerPayload {
    /// Raw reaction data as JSON value
    pub value: serde_json::Value,
    /// Timestamp when reaction was triggered
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Optional invocation ID for tracking
    pub invocation_id: Option<String>,
    /// HTTP-specific metadata (headers, method, etc.)
    pub metadata: Option<serde_json::Value>,
}

/// Control signals for reaction handler lifecycle
#[derive(Clone, Debug, PartialEq)]
pub enum ReactionControlSignal {
    /// Handler is starting
    Start,
    /// Handler is pausing
    Pause,
    /// Handler is stopping
    Stop,
    /// Handler received shutdown signal
    Shutdown,
}

/// Reaction handler errors
#[derive(Clone, Debug)]
pub struct ReactionHandlerError {
    /// Error message
    pub message: String,
    /// Whether this error is recoverable
    pub recoverable: bool,
    /// Optional HTTP status code (for HTTP handlers)
    pub status_code: Option<u16>,
}

impl ReactionHandlerError {
    pub fn new(message: impl Into<String>, recoverable: bool) -> Self {
        Self {
            message: message.into(),
            recoverable,
            status_code: None,
        }
    }

    pub fn with_status(mut self, status_code: u16) -> Self {
        self.status_code = Some(status_code);
        self
    }
}

impl std::fmt::Display for ReactionHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

/// Reaction output handler trait
///
/// This trait provides the interface for handlers that process
/// reaction invocations from external systems.
#[async_trait]
pub trait ReactionOutputHandler: Send + Sync {
    /// Initialize the handler and return a channel for receiving messages
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>>;

    /// Start or resume handler processing
    async fn start(&self) -> anyhow::Result<()>;

    /// Pause handler processing (can be resumed)
    async fn pause(&self) -> anyhow::Result<()>;

    /// Stop handler processing (terminal state)
    async fn stop(&self) -> anyhow::Result<()>;

    /// Get the current handler status
    async fn status(&self) -> ReactionHandlerStatus {
        ReactionHandlerStatus::Uninitialized
    }

    /// Get handler-specific metrics
    async fn metrics(&self) -> Option<serde_json::Value> {
        None
    }

    /// Set the TestRunHost for handlers that need it (default implementation does nothing)
    async fn set_test_run_host(&self, _test_run_host: std::sync::Arc<crate::TestRunHost>) {
        // Default implementation does nothing - only some handlers need this
    }
}

/// Implement ReactionOutputHandler for boxed trait objects
#[async_trait]
impl ReactionOutputHandler for Box<dyn ReactionOutputHandler + Send + Sync> {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
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

    async fn status(&self) -> ReactionHandlerStatus {
        (**self).status().await
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        (**self).metrics().await
    }

    async fn set_test_run_host(&self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        (**self).set_test_run_host(test_run_host).await
    }
}

use test_data_store::{
    test_repo_storage::models::ReactionHandlerDefinition, test_run_storage::TestRunQueryId,
};

/// Create a reaction output handler from a reaction handler definition
pub async fn create_reaction_handler(
    id: TestRunQueryId,
    definition: ReactionHandlerDefinition,
) -> anyhow::Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
    // For now, delegate to the existing implementation
    // This will be replaced when we migrate the handlers
    match definition {
        ReactionHandlerDefinition::Http(def) => {
            use super::reaction_handlers::http_reaction_handler::HttpReactionHandler;
            HttpReactionHandler::new(id, def)
                .await
                .map(|h| h as Box<dyn ReactionOutputHandler + Send + Sync>)
        }
        ReactionHandlerDefinition::EventGrid(_) => {
            unimplemented!("EventGridReactionHandler is not implemented yet")
        }
        ReactionHandlerDefinition::Grpc(def) => {
            use super::reaction_handlers::grpc_reaction_handler::GrpcReactionHandler;
            Ok(Box::new(GrpcReactionHandler::new(id, def).await?))
        }
        ReactionHandlerDefinition::DrasiServerCallback(def) => {
            use super::reaction_handlers::drasi_server_callback_handler::DrasiServerCallbackHandler;
            DrasiServerCallbackHandler::new(id, def)
                .await
                .map(|h| h as Box<dyn ReactionOutputHandler + Send + Sync>)
        }
        ReactionHandlerDefinition::DrasiServerChannel(def) => {
            use super::reaction_handlers::drasi_server_channel_handler::DrasiServerChannelHandler;
            DrasiServerChannelHandler::new(id, def)
                .await
                .map(|h| h as Box<dyn ReactionOutputHandler + Send + Sync>)
        }
    }
}
