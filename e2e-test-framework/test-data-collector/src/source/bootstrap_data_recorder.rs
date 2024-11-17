use test_data_store::data_collection_storage::DataCollectionSourceStorage;

use crate::config::SourceBootstrapDataRecorderConfig;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct BootstrapDataRecorder {
    node_labels: Vec<String>,
    relation_labels: Vec<String>,
    storage: DataCollectionSourceStorage,
}

impl BootstrapDataRecorder {
    pub async fn new(config: &SourceBootstrapDataRecorderConfig, _source_id: String, storage: DataCollectionSourceStorage) -> anyhow::Result<Self> {
        Ok(Self {
            node_labels: config.node_labels.clone().unwrap_or_default(),
            relation_labels: config.relation_labels.clone().unwrap_or_default(),
            storage,
        })
    }
}