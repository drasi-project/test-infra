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

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;

use bootstrap_script_player::ScriptBootstrapDataGenerator;
use serde::{Deserialize, Serialize};
use test_data_store::{
    scripts::{NodeRecord, RelationRecord},
    test_repo_storage::{models::BootstrapDataGeneratorDefinition, TestSourceStorage}, 
    test_run_storage::{TestRunSourceId, TestRunSourceStorage}
};

mod bootstrap_script_player;

#[derive(Debug, thiserror::Error)]
pub enum BootstrapDataGeneratorError {
    // NotConfigured
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapData {
    pub nodes: HashMap<String, Vec<NodeRecord>>,
    pub rels: HashMap<String, Vec<RelationRecord>>,
}

impl BootstrapData {
    pub fn new() -> Self {
        BootstrapData {
            nodes: HashMap::new(),
            rels: HashMap::new(),
        }
    }

    pub fn merge(&mut self, other: BootstrapData) {
        for (label, ids) in other.nodes {
            self.nodes.entry(label).or_insert_with(Vec::new).extend(ids);
        }
        for (label, ids) in other.rels {
            self.rels.entry(label).or_insert_with(Vec::new).extend(ids);
        }
    }
}

#[async_trait]
pub trait BootstrapDataGenerator : Send + Sync + std::fmt::Debug {
    async fn get_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData>;
}

#[async_trait]
impl BootstrapDataGenerator for Box<dyn BootstrapDataGenerator + Send + Sync> {
    async fn get_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        (**self).get_data(node_labels, rel_labels).await
    }
}

pub async fn create_bootstrap_data_generator(
    id: TestRunSourceId, 
    definition: Option<BootstrapDataGeneratorDefinition>,
    input_storage: TestSourceStorage, 
    output_storage: TestRunSourceStorage
) -> anyhow::Result<Option<Box<dyn BootstrapDataGenerator + Send + Sync>>> {
    match definition {
        None => Ok(None),
        Some(BootstrapDataGeneratorDefinition::Script(definition)) => {
            Ok(Some(Box::new(ScriptBootstrapDataGenerator::new(
                id, 
                definition, 
                input_storage, 
                output_storage).await?) as Box<dyn BootstrapDataGenerator + Send + Sync>))
        }
    }
}