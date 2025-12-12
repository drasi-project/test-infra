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

use async_trait::async_trait;

use record_count::RecordCountStopTrigger;
use test_data_store::test_repo_storage::models::StopTriggerDefinition;

use crate::reactions::reaction_output_handler::ReactionHandlerStatus;

use super::reaction_observer::ReactionObserverMetrics;

pub mod record_count;

#[derive(Debug, thiserror::Error)]
pub enum StopTriggerError {
    Io(#[from] std::io::Error),
    Serde(#[from] serde_json::Error),
}

impl std::fmt::Display for StopTriggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Serde(e) => write!(f, "Serde error: {e}"),
        }
    }
}

#[async_trait]
pub trait StopTrigger: Send + Sync {
    async fn is_true(
        &self,
        handler_status: &ReactionHandlerStatus,
        stats: &ReactionObserverMetrics,
    ) -> anyhow::Result<bool>;
}

#[async_trait]
impl StopTrigger for Box<dyn StopTrigger + Send + Sync> {
    async fn is_true(
        &self,
        handler_status: &ReactionHandlerStatus,
        stats: &ReactionObserverMetrics,
    ) -> anyhow::Result<bool> {
        (**self).is_true(handler_status, stats).await
    }
}

pub async fn create_stop_trigger(
    def: &StopTriggerDefinition,
) -> anyhow::Result<Box<dyn StopTrigger + Send + Sync>> {
    match def {
        StopTriggerDefinition::RecordCount(def) => RecordCountStopTrigger::new(def),
        StopTriggerDefinition::RecordSequenceNumber(_) => {
            // RecordSequenceNumber is not applicable for reactions
            // Return a trigger that never fires
            Ok(Box::new(NeverStopTrigger))
        }
    }
}

// Helper trigger that never fires, used for unsupported trigger types
struct NeverStopTrigger;

#[async_trait]
impl StopTrigger for NeverStopTrigger {
    async fn is_true(
        &self,
        _handler_status: &ReactionHandlerStatus,
        _stats: &ReactionObserverMetrics,
    ) -> anyhow::Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests;
