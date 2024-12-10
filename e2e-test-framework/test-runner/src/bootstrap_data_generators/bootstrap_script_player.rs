use std::collections::HashSet;

use async_trait::async_trait;
use serde::Serialize;
use test_data_store::{test_repo_storage::{models::{CommonBootstrapDataGeneratorDefinition, ScriptBootstrapDataGeneratorDefinition, TimeMode}, scripts::{bootstrap_script_file_reader::BootstrapScriptReader, BootstrapScriptRecord, NodeRecord, RelationRecord}, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

use super::{BootstrapData, BootstrapDataGenerator};

#[derive(Clone, Debug, Serialize)]
pub struct ScriptBootstrapDataGenerator {
    pub input_storage: TestSourceStorage,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
}

impl ScriptBootstrapDataGenerator {
    pub async fn new(
        test_run_source_id: TestRunSourceId, 
        common_config: CommonBootstrapDataGeneratorDefinition, 
        _unique_config: ScriptBootstrapDataGeneratorDefinition, 
        input_storage: TestSourceStorage, 
        _output_storage: TestRunSourceStorage
    ) -> anyhow::Result<Box<dyn BootstrapDataGenerator + Send + Sync>> {
        Ok(Box::new(Self {
            input_storage,
            test_run_source_id,
            time_mode: common_config.time_mode.clone(),
        }))
    }
}

#[async_trait]
impl BootstrapDataGenerator for ScriptBootstrapDataGenerator {
    async fn get_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        log::debug!("Node labels: [{:?}], Rel labels: [{:?}]", node_labels, rel_labels);

        let mut bootstrap_data = BootstrapData::new();

        let data = self.input_storage.get_dataset().await?;

        for (label, files) in data.bootstrap_data_script_files {
            if node_labels.contains(&label) {
                let mut nodes: Vec<NodeRecord> = Vec::new();

                for record in BootstrapScriptReader::new(files)? {
                    match record?.record {
                        BootstrapScriptRecord::Node(node) => nodes.push(node),
                        BootstrapScriptRecord::Finish(_) => break,
                        _ => {}
                    }
                }
                bootstrap_data.nodes.insert(label.clone(), nodes);
            } else if rel_labels.contains(&label) {
                let mut rels: Vec<RelationRecord> = Vec::new();

                for record in BootstrapScriptReader::new(files)? {
                    match record?.record {
                        BootstrapScriptRecord::Relation(rel) => rels.push(rel),
                        BootstrapScriptRecord::Finish(_) => break,
                        _ => {}
                    }
                }
                bootstrap_data.rels.insert(label.clone(), rels);
            }
        }
        Ok(bootstrap_data)
    }
}
