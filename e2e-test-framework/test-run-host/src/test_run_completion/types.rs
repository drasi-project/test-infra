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

/// State of a DrasiServer component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrasiServerState {
    Running,
    Stopped,
    Error(String),
}

/// State of a Source component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceState {
    Running,
    Paused,
    Stopped,
    Finished,
    Error(String),
}

/// State of a Query component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryState {
    Running,
    Stopped,
    Error(String),
}

/// State of a Reaction component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReactionState {
    Running,
    Stopped,
    Error(String),
}

/// Summary of component completion states.
///
/// This struct is passed to CompletionHandlers to provide context about
/// which components finished, stopped, or encountered errors.
#[derive(Debug, Clone)]
pub struct ComponentCompletionSummary {
    pub drasi_servers_stopped: usize,
    pub drasi_servers_error: usize,
    pub sources_finished: usize,
    pub sources_stopped: usize,
    pub sources_error: usize,
    pub queries_stopped: usize,
    pub queries_error: usize,
    pub reactions_stopped: usize,
    pub reactions_error: usize,
    /// Component ID -> timestamp when it finished/stopped/errored
    pub component_finish_times: HashMap<String, u64>,
}

impl ComponentCompletionSummary {
    pub fn total_drasi_servers(&self) -> usize {
        self.drasi_servers_stopped + self.drasi_servers_error
    }

    pub fn total_sources(&self) -> usize {
        self.sources_finished + self.sources_stopped + self.sources_error
    }

    pub fn total_queries(&self) -> usize {
        self.queries_stopped + self.queries_error
    }

    pub fn total_reactions(&self) -> usize {
        self.reactions_stopped + self.reactions_error
    }

    pub fn has_errors(&self) -> bool {
        self.drasi_servers_error > 0
            || self.sources_error > 0
            || self.queries_error > 0
            || self.reactions_error > 0
    }
}
