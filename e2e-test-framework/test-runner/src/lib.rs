use std::{collections::{HashMap, HashSet}, sync::Arc};

use derive_more::Debug;
use serde::{Deserialize, Serialize};
use test_run_sources::{TestRunSource, TestRunSourceConfig, TestRunSourceState};
use tokio::sync::RwLock;

use bootstrap_data_generators::BootstrapData;
use source_change_generators::SourceChangeGeneratorCommandResponse;
use test_data_store::{test_repo_storage::models::TimeMode, test_run_storage::TestRunSourceId, TestDataStore};

pub mod bootstrap_data_generators;
pub mod source_change_generators;
pub mod source_change_dispatchers;
pub mod test_run_sources;

#[derive(Debug, Deserialize, Serialize)]
pub struct TestRunnerConfig {
    #[serde(default)]
    pub sources: Vec<TestRunSourceConfig>,
    #[serde(default = "is_true")]
    pub start_immediately: bool,    
}
fn is_true() -> bool { true }

impl Default for TestRunnerConfig {
    fn default() -> Self {
        TestRunnerConfig {
            sources: Vec::new(),
            start_immediately: false,
        }
    }
}

// An enum that represents the current state of the TestRunner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TestRunnerStatus {
    // The Test Runner is Initialized and is ready to start.
    Initialized,
    // The Test Runner has been started.
    Running,
    // The Test Runner is in an Error state. and will not be able to process requests.
    Error(String),
}

#[derive(Debug)]
pub struct TestRunner {
    data_store: Arc<TestDataStore>,
    sources: Arc<RwLock<HashMap<TestRunSourceId, TestRunSource>>>,
    status: Arc<RwLock<TestRunnerStatus>>,
}

impl TestRunner {
    pub async fn new(config: TestRunnerConfig, data_store: Arc<TestDataStore>) -> anyhow::Result<Self> {   
        log::debug!("Creating TestRunner from {:#?}", config);

        let test_runner = TestRunner {
            data_store,
            sources: Arc::new(RwLock::new(HashMap::new())),
            status: Arc::new(RwLock::new(TestRunnerStatus::Initialized)),
        };

        // Add the initial set of Test Run Sources.
        for source_config in config.sources {
            test_runner.add_test_source(source_config).await?;
        };

        log::debug!("TestRunner created -  {:?}", &test_runner);

        if config.start_immediately {
            test_runner.start().await?;
        }

        Ok(test_runner)
    }
    
    pub async fn add_test_source(&self, test_run_config: TestRunSourceConfig) -> anyhow::Result<TestRunSourceId> {
        log::trace!("Adding TestRunSource from {:#?}", test_run_config);

        // If the TestRunner is in an Error state, return an error.
        if let TestRunnerStatus::Error(msg) = &self.get_status().await? {
            anyhow::bail!("TestRunner is in an Error state: {}", msg);
        };
        
        let id = TestRunSourceId::try_from(&test_run_config)?;

        let mut sources_lock = self.sources.write().await;

        // Fail if the TestRunner already contains a TestRunSource with the specified Id.
        if sources_lock.contains_key(&id) {
            anyhow::bail!("TestRunner already contains TestRunSource with ID: {:?}", &id);
        }

        // Get the SourceDefinition that the TestRunSource is associated with.
        let test_definition = self.data_store.get_test_definition_for_test_run_source(&id).await?;
        let source_definition = match test_definition.sources.iter().find(|source| source.id == test_run_config.test_source_id)
        {
            Some(source_definition) => source_definition.clone(),
            None => anyhow::bail!("SourceDefinition not found for TestRunSource: {:?}", &id)
        };

        // Get the INPUT Test Data storage for the TestRunSource.
        // This is where the TestRunSource will read the Test Data from.
        let input_storage = self.data_store.get_test_source_storage_for_test_run_source(&id).await?;

        // Get the OUTPUT storage for the new TestRunSource.
        // This is where the TestRunSource will write the output to.
        let output_storage = self.data_store.get_test_run_source_storage(&id).await?;

        // Create the TestRunSource and add it to the TestRunner.
        let test_run_source = TestRunSource::new(source_definition, test_run_config, input_storage, output_storage).await?;        

        let start_immediately = 
            self.get_status().await? == TestRunnerStatus::Running && test_run_source.start_immediately;

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

    pub async fn get_status(&self) -> anyhow::Result<TestRunnerStatus> {
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

    async fn set_status(&self, status: TestRunnerStatus) {
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

    pub async fn test_source_skip(&self, test_run_source_id: &str, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.skip_source_change_generator(skips).await
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

    pub async fn test_source_step(&self, test_run_source_id: &str, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.step_source_change_generator(steps).await
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

    pub async fn start(&self) -> anyhow::Result<TestRunnerStatus> {

        match &self.get_status().await? {
            TestRunnerStatus::Initialized => {
                log::info!("Starting TestRunner...");
            },
            TestRunnerStatus::Running => {
                let msg = format!("Test Runner is already been Running, cannot start.");
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
            TestRunnerStatus::Error(_) => {
                let msg = format!("Test Runner is in an Error state, cannot Start.");
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
        };

        // Iterate over the TestRunSources and start each one if it is configured to start immediately.
        // If any of the TestSources fail to start, set the TestRunnerStatus to Error and return an error.
        let sources_lock = self.sources.read().await;
        for (_, source) in &*sources_lock {
            if source.start_immediately {
                match source.start_source_change_generator().await {
                    Ok(response) => {
                        match response.result {
                            Ok(_) => {},
                            Err(e) => {
                                let error = TestRunnerStatus::Error(format!("Error starting TestRunSources: {}", e));
                                self.set_status(error.clone()).await;
                                anyhow::bail!("{:?}", error);
                            }
                        }
                    },
                    Err(e) => {
                        let error = TestRunnerStatus::Error(format!("Error starting TestRunSources: {}", e));
                        self.set_status(error.clone()).await;
                        anyhow::bail!("{:?}", error);
                    }
                }
            }
        }
        
        // Set the TestRunnerStatus to Running.
        log::info!("Test Runner started successfully");            
        self.set_status(TestRunnerStatus::Running).await;

        Ok(TestRunnerStatus::Running)
    }
}