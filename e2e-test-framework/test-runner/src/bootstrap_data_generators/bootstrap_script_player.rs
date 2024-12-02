use async_trait::async_trait;
use serde::Serialize;
use test_data_store::{test_repo_storage::{models::TimeMode, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

use super::{BootstrapDataGenerator, CommonBootstrapDataGeneratorConfig, ScriptBootstrapDataGeneratorConfig};

#[derive(Clone, Debug, Serialize)]
pub struct ScriptBootstrapDataGenerator {
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
}

impl ScriptBootstrapDataGenerator {
    pub async fn new(
        test_run_source_id: TestRunSourceId, 
        common_config: CommonBootstrapDataGeneratorConfig, 
        _unique_config: ScriptBootstrapDataGeneratorConfig, 
        _input_storage: TestSourceStorage, 
        _output_storage: TestRunSourceStorage
    ) -> anyhow::Result<Box<dyn BootstrapDataGenerator + Send + Sync>> {
        Ok(Box::new(Self {
            test_run_source_id,
            time_mode: common_config.time_mode.clone(),
        }))
    }
}

#[async_trait]
impl BootstrapDataGenerator for ScriptBootstrapDataGenerator {
    async fn get_data(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
