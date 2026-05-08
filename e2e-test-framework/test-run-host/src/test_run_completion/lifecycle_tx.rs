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

// Test infrastructure module - allow unwraps for lifecycle handling
#![allow(clippy::unwrap_used)]

//! Lifecycle event sender wrapper that handles timestamp generation and error handling.

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::UnboundedSender;

use super::events::ComponentLifecycleEvent;
use test_data_store::test_run_storage::{
    TestRunDrasiLibInstanceId, TestRunQueryId, TestRunReactionId, TestRunSourceId,
};

/// Wrapper around the lifecycle event channel sender that provides a clean API
/// for emitting lifecycle events without requiring components to handle timestamps,
/// error checking, or optionality.
///
/// This struct encapsulates the `Option<UnboundedSender>` internally, so components
/// can always have a `LifecycleTx` field and call methods on it without checking
/// if the channel exists. If no channel is configured, the methods simply do nothing.
#[derive(Debug, Clone)]
pub struct LifecycleTx {
    tx: Option<UnboundedSender<ComponentLifecycleEvent>>,
}

impl LifecycleTx {
    /// Create a new LifecycleTx from an UnboundedSender
    pub fn new(tx: UnboundedSender<ComponentLifecycleEvent>) -> Self {
        Self { tx: Some(tx) }
    }

    /// Create a disabled LifecycleTx that doesn't emit events
    pub fn disabled() -> Self {
        Self { tx: None }
    }

    /// Get the current timestamp in nanoseconds since UNIX_EPOCH
    fn now_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// Emit a DrasiLibInstance started event
    pub fn drasi_lib_instance_started(&self, id: TestRunDrasiLibInstanceId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::DrasiLibInstanceStarted {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a DrasiLibInstance stopped event
    pub fn drasi_lib_instance_stopped(&self, id: TestRunDrasiLibInstanceId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::DrasiLibInstanceStopped {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a DrasiLibInstance error event
    pub fn drasi_lib_instance_error(&self, id: TestRunDrasiLibInstanceId, error: String) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::DrasiLibInstanceError {
                id,
                timestamp_ns: Self::now_ns(),
                error,
            });
        }
    }

    /// Emit a Source started event
    pub fn source_started(&self, id: TestRunSourceId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::SourceStarted {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Source paused event
    pub fn source_paused(&self, id: TestRunSourceId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::SourcePaused {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Source resumed event
    pub fn source_resumed(&self, id: TestRunSourceId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::SourceResumed {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Source stopped event
    pub fn source_stopped(&self, id: TestRunSourceId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::SourceStopped {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Source finished event
    pub fn source_finished(&self, id: TestRunSourceId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::SourceFinished {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Source error event
    pub fn source_error(&self, id: TestRunSourceId, error: String) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::SourceError {
                id,
                timestamp_ns: Self::now_ns(),
                error,
            });
        }
    }

    /// Emit a Query started event
    pub fn query_started(&self, id: TestRunQueryId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::QueryStarted {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Query stopped event
    pub fn query_stopped(&self, id: TestRunQueryId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::QueryStopped {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Query error event
    pub fn query_error(&self, id: TestRunQueryId, error: String) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::QueryError {
                id,
                timestamp_ns: Self::now_ns(),
                error,
            });
        }
    }

    /// Emit a Reaction started event
    pub fn reaction_started(&self, id: TestRunReactionId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::ReactionStarted {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Reaction stopped event
    pub fn reaction_stopped(&self, id: TestRunReactionId) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::ReactionStopped {
                id,
                timestamp_ns: Self::now_ns(),
            });
        }
    }

    /// Emit a Reaction error event
    pub fn reaction_error(&self, id: TestRunReactionId, error: String) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(ComponentLifecycleEvent::ReactionError {
                id,
                timestamp_ns: Self::now_ns(),
                error,
            });
        }
    }
}
