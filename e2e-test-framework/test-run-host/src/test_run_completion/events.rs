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

use test_data_store::test_run_storage::{
    TestRunDrasiServerId, TestRunQueryId, TestRunReactionId, TestRunSourceId,
};

/// Lifecycle events emitted by test components.
///
/// These events are sent through an unbounded channel from components to the
/// TestRun monitoring task. Events are lightweight and cloneable for efficient
/// channel transmission.
#[derive(Debug, Clone)]
pub enum ComponentLifecycleEvent {
    // DrasiServer events
    DrasiServerStarted {
        id: TestRunDrasiServerId,
        timestamp_ns: u64,
    },
    DrasiServerStopped {
        id: TestRunDrasiServerId,
        timestamp_ns: u64,
    },
    DrasiServerError {
        id: TestRunDrasiServerId,
        timestamp_ns: u64,
        error: String,
    },

    // Source events
    SourceStarted {
        id: TestRunSourceId,
        timestamp_ns: u64,
    },
    SourcePaused {
        id: TestRunSourceId,
        timestamp_ns: u64,
    },
    SourceResumed {
        id: TestRunSourceId,
        timestamp_ns: u64,
    },
    SourceStopped {
        id: TestRunSourceId,
        timestamp_ns: u64,
    },
    SourceFinished {
        id: TestRunSourceId,
        timestamp_ns: u64,
    },
    SourceError {
        id: TestRunSourceId,
        timestamp_ns: u64,
        error: String,
    },

    // Query events
    QueryStarted {
        id: TestRunQueryId,
        timestamp_ns: u64,
    },
    QueryStopped {
        id: TestRunQueryId,
        timestamp_ns: u64,
    },
    QueryError {
        id: TestRunQueryId,
        timestamp_ns: u64,
        error: String,
    },

    // Reaction events
    ReactionStarted {
        id: TestRunReactionId,
        timestamp_ns: u64,
    },
    ReactionStopped {
        id: TestRunReactionId,
        timestamp_ns: u64,
    },
    ReactionError {
        id: TestRunReactionId,
        timestamp_ns: u64,
        error: String,
    },
}
