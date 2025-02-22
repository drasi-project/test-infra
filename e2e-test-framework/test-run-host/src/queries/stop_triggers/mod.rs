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