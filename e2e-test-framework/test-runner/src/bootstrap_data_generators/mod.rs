use serde::{Deserialize, Serialize};
use test_data_store::{test_repo_storage::models::TimeMode, test_run_storage::TestRunSourceId};


#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BootstrapDataGeneratorConfig {
    #[serde(default)]
    pub time_mode: Option<TimeMode>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BootstrapDataGenerator {
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
}

impl BootstrapDataGenerator {
    pub fn new(test_run_source_id: TestRunSourceId, config: &BootstrapDataGeneratorConfig) -> anyhow::Result<Self> {
        // Before this is called, we have ensured all the missing TestRunSourceConfig fields are filled.
        Ok(Self {
            test_run_source_id,
            time_mode: config.time_mode.clone().unwrap(),
        })
    }
}