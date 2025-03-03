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