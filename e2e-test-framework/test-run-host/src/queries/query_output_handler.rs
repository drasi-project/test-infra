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

//! Query-specific output handler trait and status management
//!
//! This module provides handler infrastructure specifically for query result
//! processing, including support for bootstrap phases and result streaming.

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

/// Query handler status enum with bootstrap support
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryHandlerStatus {
    /// Handler has not been initialized
    Uninitialized,
    /// Handler is in bootstrap phase
    BootstrapStarted,
    /// Handler has completed bootstrap
    BootstrapComplete,
    /// Handler is actively processing query results
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

impl QueryHandlerStatus {
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

impl Default for QueryHandlerStatus {
    fn default() -> Self {
        Self::Uninitialized
    }
}

impl Serialize for QueryHandlerStatus {
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

/// Query-specific handler message type
#[derive(Clone, Debug)]
pub enum QueryHandlerMessage {
    /// Record containing query result data
    Record(QueryHandlerRecord),
    /// Control signal for handler lifecycle
    Control(QueryControlSignal),
    /// Error occurred in handler
    Error(QueryHandlerError),
}

/// Query result record
#[derive(Clone, Debug)]
pub struct QueryHandlerRecord {
    /// Type of handler that produced this record
    pub handler_type: QueryHandlerType,
    /// Query result payload
    pub payload: QueryHandlerPayload,
}

/// Supported query handler types
#[derive(Clone, Debug, PartialEq)]
pub enum QueryHandlerType {
    RedisStream,
    DaprPubSub,
}

/// Query result payload
#[derive(Clone, Debug)]
pub struct QueryHandlerPayload {
    /// Raw query result as JSON value
    pub value: serde_json::Value,
    /// Optional timestamp when result was produced
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional sequence number for ordering
    pub sequence: Option<u64>,
}

/// Control signals for query handler lifecycle
#[derive(Clone, Debug, PartialEq)]
pub enum QueryControlSignal {
    /// Bootstrap phase has started
    BootstrapStarted,
    /// Bootstrap phase completed
    BootstrapComplete,
    /// Handler is starting normal operation
    Start,
    /// Handler is pausing
    Pause,
    /// Handler is stopping
    Stop,
    /// Handler encountered end of stream
    EndOfStream,
}

/// Query handler errors
#[derive(Clone, Debug)]
pub struct QueryHandlerError {
    /// Error message
    pub message: String,
    /// Whether this error is recoverable
    pub recoverable: bool,
}

impl QueryHandlerError {
    pub fn new(message: impl Into<String>, recoverable: bool) -> Self {
        Self {
            message: message.into(),
            recoverable,
        }
    }
}

/// Query output handler trait
///
/// This trait provides the interface for handlers that process
/// query result streams with support for bootstrap phases.
#[async_trait]
pub trait QueryOutputHandler: Send + Sync {
    /// Initialize the handler and return a channel for receiving messages
    async fn init(&self) -> anyhow::Result<Receiver<QueryHandlerMessage>>;

    /// Start or resume handler processing
    async fn start(&self) -> anyhow::Result<()>;

    /// Pause handler processing (can be resumed)
    async fn pause(&self) -> anyhow::Result<()>;

    /// Stop handler processing (terminal state)
    async fn stop(&self) -> anyhow::Result<()>;

    /// Get the current handler status
    async fn status(&self) -> QueryHandlerStatus {
        QueryHandlerStatus::Uninitialized
    }

    /// Get handler-specific metrics
    async fn metrics(&self) -> Option<serde_json::Value> {
        None
    }
}

/// Implement QueryOutputHandler for boxed trait objects
#[async_trait]
impl QueryOutputHandler for Box<dyn QueryOutputHandler + Send + Sync> {
    async fn init(&self) -> anyhow::Result<Receiver<QueryHandlerMessage>> {
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

    async fn status(&self) -> QueryHandlerStatus {
        (**self).status().await
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        (**self).metrics().await
    }
}

use test_data_store::{
    test_repo_storage::models::ResultStreamHandlerDefinition, test_run_storage::TestRunQueryId,
};

/// Create a query output handler from a result stream definition
pub async fn create_query_handler(
    id: TestRunQueryId,
    definition: ResultStreamHandlerDefinition,
) -> anyhow::Result<Box<dyn QueryOutputHandler + Send + Sync>> {
    // For now, delegate to the existing implementation
    // This will be replaced when we migrate the handlers
    match definition {
        ResultStreamHandlerDefinition::RedisStream(def) => {
            use super::result_stream_handlers::redis_result_stream_handler::RedisResultStreamHandler;
            RedisResultStreamHandler::new(id, def)
                .await
                .map(|h| h as Box<dyn QueryOutputHandler + Send + Sync>)
        }
        ResultStreamHandlerDefinition::DaprPubSub(_) => {
            unimplemented!("DaprResultStreamHandler is not implemented yet")
        }
    }
}
