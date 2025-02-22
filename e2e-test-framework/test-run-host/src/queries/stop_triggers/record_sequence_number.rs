use anyhow::Ok;
use async_trait::async_trait;

use test_data_store::test_repo_storage::models::RecordSequenceNumberStopTriggerDefinition;

use crate::queries::{query_result_observer::QueryResultObserverMetrics, result_stream_handlers::ResultStreamStatus};

use super::StopTrigger;

#[derive(Debug)]
pub struct RecordSequenceNumberStopTriggerSettings {
    pub record_sequence_number: i64,
}

impl RecordSequenceNumberStopTriggerSettings {
    pub fn new(cfg: &RecordSequenceNumberStopTriggerDefinition) -> anyhow::Result<Self> {
        return Ok(Self {
            record_sequence_number: cfg.record_sequence_number,
        });
    }
}

pub struct RecordSequenceNumberStopTrigger {
    settings: RecordSequenceNumberStopTriggerSettings,
}

impl RecordSequenceNumberStopTrigger {
    pub fn new(def: &RecordSequenceNumberStopTriggerDefinition) -> anyhow::Result<Box<dyn StopTrigger + Send + Sync>> {
        log::debug!("Creating RecordSequenceNumberStopTrigger from {:?}, ", def);

        let settings = RecordSequenceNumberStopTriggerSettings::new(&def)?;
        log::trace!("Creating RecordSequenceNumberStopTrigger with settings {:?}, ", settings);

        Ok(Box::new(Self { settings }))
    }
}  

#[async_trait]
impl StopTrigger for RecordSequenceNumberStopTrigger {
    async fn is_true(&self, _stream_status: &ResultStreamStatus, stats: &QueryResultObserverMetrics) -> anyhow::Result<bool> {

        Ok(stats.result_stream_record_seq >= self.settings.record_sequence_number)
    }
}