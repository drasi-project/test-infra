use std::{collections::{HashMap, HashSet}, sync::Arc};

use derive_more::Debug;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use test_data_store::{test_repo_storage::models::SpacingMode, test_run_storage::TestRunSourceId, TestDataStore};
use sources::{
    bootstrap_data_generators::BootstrapData, 
    source_change_generators::SourceChangeGeneratorCommandResponse,
    TestRunSource, TestRunSourceConfig, TestRunSourceDefinition, TestRunSourceState,
};

pub mod sources;

#[derive(Debug, Deserialize, Serialize)]
pub struct TestRunHostConfig {
    #[serde(default)]
    pub sources: Vec<TestRunSourceConfig>,
    #[serde(default = "is_true")]
    pub start_immediately: bool,    
}
fn is_true() -> bool { true }

impl Default for TestRunHostConfig {
    fn default() -> Self {
        TestRunHostConfig {
            sources: Vec::new(),
            start_immediately: false,
        }
    }
}

// An enum that represents the current state of the TestRunHost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TestRunHostStatus {
    // The TestRunHost is Initialized and is ready to start.
    Initialized,
    // The TestRunHost has been started.
    Running,
    // The TestRunHost is in an Error state. and will not be able to process requests.
    Error(String),
}

#[derive(Debug)]
pub struct TestRunHost {
    data_store: Arc<TestDataStore>,
    sources: Arc<RwLock<HashMap<TestRunSourceId, TestRunSource>>>,
    status: Arc<RwLock<TestRunHostStatus>>,
}

impl TestRunHost {
    pub async fn new(config: TestRunHostConfig, data_store: Arc<TestDataStore>) -> anyhow::Result<Self> {   
        log::debug!("Creating TestRunHost from {:?}", config);

        let test_run_host = TestRunHost {
            data_store,
            sources: Arc::new(RwLock::new(HashMap::new())),
            status: Arc::new(RwLock::new(TestRunHostStatus::Initialized)),
        };

        // Add the initial set of Test Run Sources.
        for source_config in config.sources {
            test_run_host.add_test_source(source_config).await?;
        };

        log::debug!("TestRunHost created -  {:?}", &test_run_host);

        if config.start_immediately {
            test_run_host.start().await?;
        }

        Ok(test_run_host)
    }
    
    pub async fn add_test_source(&self, test_run_config: TestRunSourceConfig) -> anyhow::Result<TestRunSourceId> {
        log::trace!("Adding TestRunSource from {:?}", test_run_config);

        // If the TestRunHost is in an Error state, return an error.
        if let TestRunHostStatus::Error(msg) = &self.get_status().await? {
            anyhow::bail!("TestRunHost is in an Error state: {}", msg);
        };
        
        let id = TestRunSourceId::try_from(&test_run_config)?;

        let mut sources_lock = self.sources.write().await;

        // Fail if the TestRunHost already contains a TestRunSource with the specified Id.
        if sources_lock.contains_key(&id) {
            anyhow::bail!("TestRunHost already contains TestRunSource with ID: {:?}", &id);
        }

        // Get the TestRepoStorage that is associated with the Repo for the TestRunSource
        let repo = self.data_store.get_test_repo_storage(&test_run_config.test_repo_id).await?;
        repo.add_remote_test(&test_run_config.test_id, false).await?;
        let test_source_definition = self.data_store.get_test_source_definition_for_test_run_source(&id).await?;

        let definition = TestRunSourceDefinition::new(test_run_config, test_source_definition)?;
        log::trace!("TestRunSourceDefinition: {:?}", &definition);

        // Get the INPUT Test Data storage for the TestRunSource.
        // This is where the TestRunSource will read the Test Data from.
        let input_storage = self.data_store.get_test_source_storage_for_test_run_source(&id).await?;

        // Get the OUTPUT storage for the new TestRunSource.
        // This is where the TestRunSource will write the output to.
        let output_storage = self.data_store.get_test_run_source_storage(&id).await?;

        // Create the TestRunSource and add it to the TestRunHost.
        let test_run_source = TestRunSource::new(definition, input_storage, output_storage).await?;        

        let start_immediately = 
            self.get_status().await? == TestRunHostStatus::Running && test_run_source.start_immediately;

        sources_lock.insert(id.clone(), test_run_source);
        
        if start_immediately {
            sources_lock.get(&id).unwrap().start_source_change_generator().await?;
        }

        Ok(id)
    }

    pub async fn contains_test_source(&self, test_run_source_id: &str) -> anyhow::Result<bool> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        Ok(self.sources.read().await.contains_key(&test_run_source_id))
    }
    
    pub async fn get_bootstrap_data_for_query(&self, query_id: &str, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        log::debug!("Query ID: {}, Node Labels: {:?}, Rel Labels: {:?}", query_id, node_labels, rel_labels);

        let sources_lock = self.sources.read().await;

        let mut bootstrap_data = BootstrapData::new();

        for (_, source) in &*sources_lock {
            let source_data = source.get_bootstrap_data_for_query(query_id.try_into()?, node_labels, rel_labels).await?;
            bootstrap_data.merge(source_data);
        }

        Ok(bootstrap_data)
    }

    pub async fn get_status(&self) -> anyhow::Result<TestRunHostStatus> {
        Ok(self.status.read().await.clone())
    }

    pub async fn get_test_source_state(&self, test_run_source_id: &str) -> anyhow::Result<TestRunSourceState> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.get_state().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn get_test_source_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.sources.read().await.keys().map(|id| id.to_string()).collect())
    }

    async fn set_status(&self, status: TestRunHostStatus) {
        let mut write_lock = self.status.write().await;
        *write_lock = status.clone();
    }

    pub async fn test_source_pause(&self, test_run_source_id: &str) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.pause_source_change_generator().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_reset(&self, test_run_source_id: &str) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.reset_source_change_generator().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_skip(&self, test_run_source_id: &str, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.skip_source_change_generator(skips, spacing_mode).await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_start(&self, test_run_source_id: &str) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.start_source_change_generator().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_step(&self, test_run_source_id: &str, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.step_source_change_generator(steps, spacing_mode).await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_stop(&self, test_run_source_id: &str) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.stop_source_change_generator().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn start(&self) -> anyhow::Result<TestRunHostStatus> {

        match &self.get_status().await? {
            TestRunHostStatus::Initialized => {
                log::info!("Starting TestRunHost...");
            },
            TestRunHostStatus::Running => {
                let msg = format!("TestRunHost is already been Running, cannot start.");
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
            TestRunHostStatus::Error(_) => {
                let msg = format!("TestRunHost is in an Error state, cannot Start.");
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
        };

        // Set the TestRunHostStatus to Running.
        log::info!("TestRunHost started successfully !!!\n\n");            
        self.set_status(TestRunHostStatus::Running).await;

        // Iterate over the TestRunSources and start each one if it is configured to start immediately.
        // If any of the TestSources fail to start, set the TestRunHostStatus to Error and return an error.
        log::info!("TestRunHost starting auto-start TestRunSources...");            
        let mut auto_start_sources_count: u32 = 0;

        let sources_lock = self.sources.read().await;
        for (_, source) in &*sources_lock {
            if source.start_immediately {
                log::info!("Starting TestRunSource: {}", source.id);

                match source.start_source_change_generator().await {
                    Ok(response) => {
                        match response.result {
                            Ok(_) => { auto_start_sources_count += 1; },
                            Err(e) => {
                                let error = TestRunHostStatus::Error(format!("Error starting TestRunSources: {}", e));
                                self.set_status(error.clone()).await;
                                anyhow::bail!("{:?}", error);
                            }
                        }
                    },
                    Err(e) => {
                        let error = TestRunHostStatus::Error(format!("Error starting TestRunSources: {}", e));
                        self.set_status(error.clone()).await;
                        anyhow::bail!("{:?}", error);
                    }
                }
            }
        }
        
        log::info!("TestRunHost auto started {} of {} TestRunSources.", auto_start_sources_count, sources_lock.len());            
        Ok(TestRunHostStatus::Running)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_data_store::TestDataStore;

    use crate::{TestRunHost, TestRunHostConfig, TestRunHostStatus};

    #[tokio::test]
    async fn test_new_test_run_host() -> anyhow::Result<()> {

        let data_store = Arc::new(TestDataStore::new_temp(None).await?);    
        
        let test_run_host_config = TestRunHostConfig::default();
        let test_run_host = TestRunHost::new(test_run_host_config, data_store.clone()).await.unwrap();

        assert_eq!(test_run_host.get_status().await?, TestRunHostStatus::Initialized);

        Ok(())
    }
}