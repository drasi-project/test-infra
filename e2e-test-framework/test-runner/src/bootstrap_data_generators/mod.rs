use async_trait::async_trait;

use bootstrap_script_player::ScriptBootstrapDataGenerator;
use serde::{Deserialize, Serialize};
use test_data_store::{test_repo_storage::{models::TimeMode, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

mod bootstrap_script_player;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum BootstrapDataGeneratorConfig {
    Script {
        #[serde(flatten)]
        common_config: CommonBootstrapDataGeneratorConfig,
        #[serde(flatten)]
        unique_config: ScriptBootstrapDataGeneratorConfig,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonBootstrapDataGeneratorConfig {
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptBootstrapDataGeneratorConfig {
}

#[derive(Debug, thiserror::Error)]
pub enum BootstrapDataGeneratorError {
    // NotConfigured
}

#[async_trait]
pub trait BootstrapDataGenerator : Send + Sync {
    async fn get_data(&self) -> anyhow::Result<()>;
}

#[async_trait]
impl BootstrapDataGenerator for Box<dyn BootstrapDataGenerator + Send + Sync> {
    async fn get_data(&self) -> anyhow::Result<()> {
        (**self).get_data().await
    }
}

pub async fn create_bootstrap_data_generator(
    id: TestRunSourceId, 
    config: Option<BootstrapDataGeneratorConfig>,
    input_storage: TestSourceStorage, 
    output_storage: TestRunSourceStorage
) -> anyhow::Result<Option<Box<dyn BootstrapDataGenerator + Send + Sync>>> {
    match config {
        None => Ok(None),
        Some(BootstrapDataGeneratorConfig::Script{common_config, unique_config}) => {
            Ok(Some(Box::new(ScriptBootstrapDataGenerator::new(
                id, 
                common_config, 
                unique_config, 
                input_storage, 
                output_storage).await?)))
        }
    }
}