use std::path::PathBuf;

use crate::config::SourceBootstrapDataRecorderConfig;

#[derive(Clone, Debug)]
pub struct BootstrapDataRecorder {
    pub node_labels: Vec<String>,
    pub relation_labels: Vec<String>,
}

impl BootstrapDataRecorder {
    pub async fn try_from_config(config: &SourceBootstrapDataRecorderConfig, _data_store_path: PathBuf) -> anyhow::Result<Self> {
        Ok(Self {
            node_labels: config.node_labels.clone().unwrap_or_default(),
            relation_labels: config.relation_labels.clone().unwrap_or_default(),
        })
    }
}