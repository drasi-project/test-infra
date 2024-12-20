use std::collections::{HashMap, HashSet};

use async_trait::async_trait;

use bootstrap_script_player::ScriptBootstrapDataGenerator;
use serde::{Deserialize, Serialize};
use test_data_store::{test_repo_storage::{models::BootstrapDataGeneratorDefinition, scripts::{NodeRecord, RelationRecord}, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

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
pub trait BootstrapDataGenerator : Send + Sync {
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
        Some(BootstrapDataGeneratorDefinition::Script{common_config, unique_config}) => {
            Ok(Some(Box::new(ScriptBootstrapDataGenerator::new(
                id, 
                common_config, 
                unique_config, 
                input_storage, 
                output_storage).await?)))
        }
    }
}