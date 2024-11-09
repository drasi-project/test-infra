use std::path::PathBuf;

use bootstrap_data_recorder::BootstrapDataRecorder;
use source_change_recorder::SourceChangeRecorder;

use crate::config::SourceConfig;

// pub mod bootstrap_data_loggers;
pub mod bootstrap_data_recorder;
pub mod change_event_loggers;
pub mod change_queue_readers;
pub mod source_change_recorder;

#[derive(Debug)]
pub struct DatasetSource {
    pub bootstrap_data_recorder: Option<BootstrapDataRecorder>,
    pub source_id: String,
    pub source_change_recorder: Option<SourceChangeRecorder>,
    pub start_immediately: bool,
}

impl DatasetSource {
    pub async fn try_from_config(config: &SourceConfig, data_store_path: PathBuf) -> anyhow::Result<Self> {
        
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
        
        // // If neither the SourceConfig nor the SourceConfig defaults contain a proxy, set it to None.
        // let proxy = match config.proxy {
        //     Some(ref config) => {
        //         match defaults.proxy {
        //             Some(ref defaults) => TestRunProxy::try_from_config(id.clone(), config, defaults).ok(),
        //             None => TestRunProxy::try_from_config(id.clone(), config, &ProxyConfig::default()).ok(),
        //         }
        //     },
        //     None => {
        //         match defaults.proxy {
        //             Some(ref defaults) => TestRunProxy::try_from_config(id.clone(), defaults, &ProxyConfig::default()).ok(),
        //             None => None,
        //         }
        //     }
        // };

        // // If neither the SourceConfig nor the SourceConfig defaults contain a reactivator, set it to None.
        // let reactivator = match config.reactivator {
        //     Some(ref config) => {
        //         match defaults.reactivator {
        //             Some(ref defaults) => TestRunReactivator::try_from_config(id.clone(), config, defaults).ok(),
        //             None => TestRunReactivator::try_from_config(id.clone(), config, &ReactivatorConfig::default()).ok(),
        //         }
        //     },
        //     None => {
        //         match defaults.reactivator {
        //             Some(ref defaults) => TestRunReactivator::try_from_config(id.clone(), defaults, &ReactivatorConfig::default()).ok(),
        //             None => None,
        //         }
        //     }
        // };

        Ok(Self {
            bootstrap_data_recorder,
            source_id: config.source_id.clone(),
            source_change_recorder,
            start_immediately: config.start_immediately,
        })
    }
}