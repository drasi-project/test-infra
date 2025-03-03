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

use record_sequence_number::RecordSequenceNumberStopTrigger;
use test_data_store::test_repo_storage::models::StopTriggerDefinition;

use super::{query_result_observer::QueryResultObserverMetrics, result_stream_handlers::ResultStreamStatus};

pub mod record_sequence_number;

#[derive(Debug, thiserror::Error)]
pub enum StopTriggerError {
    Io(#[from] std::io::Error),
    Serde(#[from] serde_json::Error),
}

impl std::fmt::Display for StopTriggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
        }
    }
}

#[async_trait]
pub trait StopTrigger : Send + Sync {
    async fn is_true(&self, stream_status: &ResultStreamStatus, stats: &QueryResultObserverMetrics) -> anyhow::Result<bool>;
}

#[async_trait]
impl StopTrigger for Box<dyn StopTrigger + Send + Sync> {
    async fn is_true(&self, stream_status: &ResultStreamStatus, stats: &QueryResultObserverMetrics) -> anyhow::Result<bool> {
        (**self).is_true(stream_status, stats).await
    }
}

pub async fn create_stop_trigger(def: &StopTriggerDefinition) -> anyhow::Result<Box<dyn StopTrigger + Send + Sync>> {
    match def {
        StopTriggerDefinition::RecordSequenceNumber(def) => RecordSequenceNumberStopTrigger::new(def),
    }
}