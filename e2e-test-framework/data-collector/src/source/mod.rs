use bootstrap_data_recorder::BootstrapDataRecorder;
use source_change_recorder::{SourceChangeRecorder, SourceChangeRecorderMessageResponse};
use test_data_store::data_collection_storage::DataCollectionSourceStorage;

use crate::config::DataCollectionSourceConfig;

pub mod bootstrap_data_recorder;
pub mod change_event_loggers;
pub mod change_queue_readers;
pub mod source_change_recorder;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct DataCollectionSource {
    bootstrap_data_recorder: Option<BootstrapDataRecorder>,
    source_id: String,
    source_change_recorder: Option<SourceChangeRecorder>,
    start_immediately: bool,
    storage : DataCollectionSourceStorage,
}

impl DataCollectionSource {
    pub async fn new(config: DataCollectionSourceConfig, storage: DataCollectionSourceStorage) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionSource from config {:#?}", config);
        
        let source_id = config.source_id.clone();

        let bootstrap_data_recorder = match &config.bootstrap_data_recorder {
            Some(config) => Some(BootstrapDataRecorder::new(config, source_id.clone(), storage.clone()).await?),
            None => None,
        };

        let source_change_recorder = match &config.source_change_recorder {
            Some(config) => Some(SourceChangeRecorder::new(config, source_id.clone(), storage.clone()).await?),
            None => None,
        };

        Ok(Self {
            bootstrap_data_recorder,
            source_id: source_id.clone(),
            source_change_recorder,
            start_immediately: config.start_immediately,
            storage,
        })
    }

    pub async fn start_source_change_recorder(&mut self) -> anyhow::Result<SourceChangeRecorderMessageResponse> {
        log::debug!("Starting source change recorder for source {}", self.source_id);

        if let Some(source_change_recorder) = &mut self.source_change_recorder {
            source_change_recorder.start().await
        } else {
            anyhow::bail!("No source change recorder configured for source {}", self.source_id);
        }
    }
    
    pub async fn pause_source_change_recorder(&mut self) -> anyhow::Result<SourceChangeRecorderMessageResponse> {
        log::debug!("Pausing source change recorder for source {}", self.source_id);

        if let Some(source_change_recorder) = &mut self.source_change_recorder {
            source_change_recorder.pause().await
        } else {
            anyhow::bail!("No source change recorder configured for source {}", self.source_id);
        }
    }

    pub async fn stop_source_change_recorder(&mut self) -> anyhow::Result<SourceChangeRecorderMessageResponse> {
        log::debug!("Stopping source change recorder for source {}", self.source_id);

        if let Some(source_change_recorder) = &mut self.source_change_recorder {
            source_change_recorder.stop().await
        } else {
            anyhow::bail!("No source change recorder configured for source {}", self.source_id);
        }
    }
}