use std::path::PathBuf;

use bootstrap_data_recorder::BootstrapDataRecorder;
use source_change_recorder::{SourceChangeRecorder, SourceChangeRecorderMessageResponse};

use crate::config::SourceConfig;

// pub mod bootstrap_data_loggers;
pub mod bootstrap_data_recorder;
pub mod change_event_loggers;
pub mod change_queue_readers;
pub mod source_change_recorder;

#[derive(Clone, Debug)]
pub struct DatasetSource {
    pub bootstrap_data_recorder: Option<BootstrapDataRecorder>,
    pub source_id: String,
    pub source_change_recorder: Option<SourceChangeRecorder>,
    pub start_immediately: bool,
}

impl DatasetSource {
    pub async fn new(config: &SourceConfig, data_store_path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating DatasetSource from config {:#?}", config);
        
        let source_id = config.source_id.clone();

        let data_store_path = data_store_path.join(source_id.clone());

        let bootstrap_data_recorder = match &config.bootstrap_data_recorder {
            Some(config) => Some(BootstrapDataRecorder::new(config, source_id.clone(), data_store_path.clone()).await?),
            None => None,
        };

        let source_change_recorder = match &config.source_change_recorder {
            Some(config) => Some(SourceChangeRecorder::new(config, source_id.clone(), data_store_path.clone()).await?),
            None => None,
        };

        Ok(Self {
            bootstrap_data_recorder,
            source_id: config.source_id.clone(),
            source_change_recorder,
            start_immediately: config.start_immediately,
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