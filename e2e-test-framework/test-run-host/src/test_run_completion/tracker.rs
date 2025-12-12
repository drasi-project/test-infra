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

use test_data_store::test_run_storage::{
    TestRunDrasiServerId, TestRunQueryId, TestRunReactionId, TestRunSourceId,
};

use super::events::ComponentLifecycleEvent;
use super::types::{
    ComponentCompletionSummary, DrasiServerState, QueryState, ReactionState, SourceState,
};

/// Tracks the state of all components in a TestRun.
///
/// Updated by the monitoring task as lifecycle events are received.
/// Provides the `all_components_finished()` method to check if all
/// components have reached terminal states.
#[derive(Debug)]
pub struct ComponentStateTracker {
    drasi_servers: HashMap<TestRunDrasiServerId, DrasiServerState>,
    sources: HashMap<TestRunSourceId, SourceState>,
    queries: HashMap<TestRunQueryId, QueryState>,
    reactions: HashMap<TestRunReactionId, ReactionState>,
    total_drasi_servers: usize,
    total_sources: usize,
    total_queries: usize,
    total_reactions: usize,
    finish_times: HashMap<String, u64>,
}

impl ComponentStateTracker {
    /// Create a new tracker with expected component counts.
    pub fn new(
        drasi_server_count: usize,
        source_count: usize,
        query_count: usize,
        reaction_count: usize,
    ) -> Self {
        Self {
            drasi_servers: HashMap::new(),
            sources: HashMap::new(),
            queries: HashMap::new(),
            reactions: HashMap::new(),
            total_drasi_servers: drasi_server_count,
            total_sources: source_count,
            total_queries: query_count,
            total_reactions: reaction_count,
            finish_times: HashMap::new(),
        }
    }

    /// Update tracker state based on a lifecycle event.
    pub fn update(&mut self, event: &ComponentLifecycleEvent) {
        match event {
            ComponentLifecycleEvent::DrasiServerStarted { id, .. } => {
                self.drasi_servers
                    .insert(id.clone(), DrasiServerState::Running);
            }
            ComponentLifecycleEvent::DrasiServerStopped { id, timestamp_ns } => {
                self.drasi_servers
                    .insert(id.clone(), DrasiServerState::Stopped);
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }
            ComponentLifecycleEvent::DrasiServerError {
                id,
                timestamp_ns,
                error,
            } => {
                self.drasi_servers
                    .insert(id.clone(), DrasiServerState::Error(error.clone()));
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }

            ComponentLifecycleEvent::SourceStarted { id, .. } => {
                self.sources.insert(id.clone(), SourceState::Running);
            }
            ComponentLifecycleEvent::SourcePaused { id, .. } => {
                self.sources.insert(id.clone(), SourceState::Paused);
            }
            ComponentLifecycleEvent::SourceResumed { id, .. } => {
                self.sources.insert(id.clone(), SourceState::Running);
            }
            ComponentLifecycleEvent::SourceStopped { id, timestamp_ns } => {
                self.sources.insert(id.clone(), SourceState::Stopped);
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }
            ComponentLifecycleEvent::SourceFinished { id, timestamp_ns } => {
                self.sources.insert(id.clone(), SourceState::Finished);
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }
            ComponentLifecycleEvent::SourceError {
                id,
                timestamp_ns,
                error,
            } => {
                self.sources
                    .insert(id.clone(), SourceState::Error(error.clone()));
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }

            ComponentLifecycleEvent::QueryStarted { id, .. } => {
                self.queries.insert(id.clone(), QueryState::Running);
            }
            ComponentLifecycleEvent::QueryStopped { id, timestamp_ns } => {
                self.queries.insert(id.clone(), QueryState::Stopped);
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }
            ComponentLifecycleEvent::QueryError {
                id,
                timestamp_ns,
                error,
            } => {
                self.queries
                    .insert(id.clone(), QueryState::Error(error.clone()));
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }

            ComponentLifecycleEvent::ReactionStarted { id, .. } => {
                self.reactions.insert(id.clone(), ReactionState::Running);
            }
            ComponentLifecycleEvent::ReactionStopped { id, timestamp_ns } => {
                self.reactions.insert(id.clone(), ReactionState::Stopped);
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }
            ComponentLifecycleEvent::ReactionError {
                id,
                timestamp_ns,
                error,
            } => {
                self.reactions
                    .insert(id.clone(), ReactionState::Error(error.clone()));
                self.finish_times.insert(id.to_string(), *timestamp_ns);
            }
        }
    }

    /// Check if all components have finished.
    ///
    /// Returns true when:
    /// - All DrasiServers are in terminal state (Stopped | Error)
    /// - All sources are in terminal state (Finished | Stopped | Error)
    /// - All queries are in terminal state (Stopped | Error)
    /// - All reactions are in terminal state (Stopped | Error)
    /// - All expected components have been registered
    pub fn all_components_finished(&self) -> bool {
        // Check we've seen all components
        let all_registered = self.drasi_servers.len() == self.total_drasi_servers
            && self.sources.len() == self.total_sources
            && self.queries.len() == self.total_queries
            && self.reactions.len() == self.total_reactions;

        if !all_registered {
            return false;
        }

        // All DrasiServers must be in terminal state
        let drasi_servers_done = self
            .drasi_servers
            .values()
            .all(|ds| matches!(ds, DrasiServerState::Stopped | DrasiServerState::Error(_)));

        // All sources must be in terminal state
        let sources_done = self.sources.values().all(|s| {
            matches!(
                s,
                SourceState::Finished | SourceState::Stopped | SourceState::Error(_)
            )
        });

        // All queries must be in terminal state
        let queries_done = self
            .queries
            .values()
            .all(|q| matches!(q, QueryState::Stopped | QueryState::Error(_)));

        // All reactions must be in terminal state
        let reactions_done = self
            .reactions
            .values()
            .all(|r| matches!(r, ReactionState::Stopped | ReactionState::Error(_)));

        drasi_servers_done && sources_done && queries_done && reactions_done
    }

    /// Get a summary of component completion states.
    pub fn get_completion_summary(&self) -> ComponentCompletionSummary {
        let mut summary = ComponentCompletionSummary {
            drasi_servers_stopped: 0,
            drasi_servers_error: 0,
            sources_finished: 0,
            sources_stopped: 0,
            sources_error: 0,
            queries_stopped: 0,
            queries_error: 0,
            reactions_stopped: 0,
            reactions_error: 0,
            component_finish_times: self.finish_times.clone(),
        };

        for state in self.drasi_servers.values() {
            match state {
                DrasiServerState::Stopped => summary.drasi_servers_stopped += 1,
                DrasiServerState::Error(_) => summary.drasi_servers_error += 1,
                _ => {}
            }
        }

        for state in self.sources.values() {
            match state {
                SourceState::Finished => summary.sources_finished += 1,
                SourceState::Stopped => summary.sources_stopped += 1,
                SourceState::Error(_) => summary.sources_error += 1,
                _ => {}
            }
        }

        for state in self.queries.values() {
            match state {
                QueryState::Stopped => summary.queries_stopped += 1,
                QueryState::Error(_) => summary.queries_error += 1,
                _ => {}
            }
        }

        for state in self.reactions.values() {
            match state {
                ReactionState::Stopped => summary.reactions_stopped += 1,
                ReactionState::Error(_) => summary.reactions_error += 1,
                _ => {}
            }
        }

        summary
    }

    /// Get the current state of a DrasiServer.
    pub fn get_drasi_server_state(&self, id: &TestRunDrasiServerId) -> Option<&DrasiServerState> {
        self.drasi_servers.get(id)
    }

    /// Get the current state of a source.
    pub fn get_source_state(&self, id: &TestRunSourceId) -> Option<&SourceState> {
        self.sources.get(id)
    }

    /// Get the current state of a query.
    pub fn get_query_state(&self, id: &TestRunQueryId) -> Option<&QueryState> {
        self.queries.get(id)
    }

    /// Get the current state of a reaction.
    pub fn get_reaction_state(&self, id: &TestRunReactionId) -> Option<&ReactionState> {
        self.reactions.get(id)
    }
}
