use std::{collections::HashMap, fmt, sync::Arc};

use derive_more::Debug;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use bootstrap_data_generators::{create_bootstrap_data_generator, BootstrapDataGenerator, BootstrapDataGeneratorConfig};
use source_change_generators::{create_source_change_generator, SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorConfig, SourceChangeGeneratorState};
use test_data_store::{test_repo_storage::{models::TimeMode, TestSourceStorage}, test_run_storage::{ParseTestRunIdError, ParseTestRunSourceIdError, TestRunId, TestRunSourceId, TestRunSourceStorage}, TestDataStore};

pub mod bootstrap_data_generators;
pub mod source_change_generators;
pub mod source_change_dispatchers;

#[derive(Debug, Deserialize, Serialize)]
pub struct TestRunnerConfig {
    #[serde(default)]
    pub sources: Vec<TestRunSourceConfig>,
}

impl Default for TestRunnerConfig {
    fn default() -> Self {
        TestRunnerConfig {
            sources: Vec::new(),
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

        Ok(test_runner)
    }
    
    pub async fn add_test_source(&self, config: TestRunSourceConfig) -> anyhow::Result<TestRunSourceId> {
        log::trace!("Adding TestRunSource from {:#?}", config);

        // If the TestRunner is in an Error state, return an error.
        if let TestRunnerStatus::Error(msg) = &self.get_status().await? {
            anyhow::bail!("TestRunner is in an Error state: {}", msg);
        };
        
        let id = TestRunSourceId::try_from(&config)?;

        let mut sources_lock = self.sources.write().await;

        // Fail if the TestRunner already contains a TestRunSource with the specified Id.
        if sources_lock.contains_key(&id) {
            anyhow::bail!("TestRunner already contains TestRunSource with ID: {:?}", &id);
        }

        // Get the INPUT Test Data storage for the TestRunSource.
        // This is where the TestRunSource will read the Test Data from.
        let input_storage = self.data_store.get_test_source_storage_for_test_run_source(&id).await?;

        // Get the OUTPUT storage for the new TestRunSource.
        // This is where the TestRunSource will write the output to.
        let output_storage = self.data_store.get_test_run_source_storage(&id).await?;

        // Create the TestRunSource and add it to the TestRunner.
        let test_run_source = TestRunSource::new(config, input_storage, output_storage).await?;        

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

    // pub async fn match_bootstrap_dataset(&self, requested_labels: &HashSet<String>) -> anyhow::Result<Option<TestSourceDataset>> {
    //     self.data_store.match_bootstrap_dataset(requested_labels)
    // }

    pub async fn start(&self) -> anyhow::Result<TestRunnerStatus> {

        match &self.get_status().await? {
            TestRunnerStatus::Initialized => {
                log::info!("Starting TestRunner...");
            },
            TestRunnerStatus::Running => {
                let msg = format!("Test Runner has already been Running, cannot start.");
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
            TestRunnerStatus::Error(_) => {
                let msg = format!("Test Runner is in an Error state, cannot start.");
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestRunSourceConfig {
    pub test_repo_id: String,
    pub test_id: String,
    #[serde(default = "random_test_run_id")]
    pub test_run_id: String,
    pub test_source_id: String,
    #[serde(default = "is_true")]
    pub start_immediately: bool,    
    pub bootstrap_data_generator: Option<BootstrapDataGeneratorConfig>,
    pub source_change_generator: Option<SourceChangeGeneratorConfig>,
}
fn is_true() -> bool { false }
fn random_test_run_id() -> String { chrono::Utc::now().format("%Y%m%d%H%M%S").to_string() }

impl TryFrom<&TestRunSourceConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        Ok(TestRunId {
            test_repo_id: value.test_repo_id.clone(),
            test_id: value.test_id.clone(),
            test_run_id: value.test_run_id.clone(),
        })
    }
}

impl TryFrom<&TestRunSourceConfig> for TestRunSourceId {
    type Error = ParseTestRunSourceIdError;

    fn try_from(value: &TestRunSourceConfig) -> Result<Self, Self::Error> {
        match TestRunId::try_from(value) {
            Ok(test_run_id) => {
                Ok(TestRunSourceId::new(&test_run_id, &value.test_source_id.clone()))
            }
            Err(e) => return Err(ParseTestRunSourceIdError::InvalidValues(e.to_string())),
        }
    }
}

impl fmt::Display for TestRunSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestRunSourceConfig: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_source_id: {:?}, bootstrap_data_generator: {:?}, source_change_generator: {:?}", 
            self.test_repo_id, self.test_id, self.test_run_id, self.test_source_id, self.bootstrap_data_generator, self.source_change_generator)
    }
}

#[derive(Debug, Serialize)]
pub struct TestRunSourceState {
    pub id: TestRunSourceId,
    pub source_change_generator: SourceChangeGeneratorState,
}

#[derive(Debug)]
pub struct TestRunSource {
    pub id: TestRunSourceId,
    pub start_immediately: bool,
    #[debug(skip)]
    pub bootstrap_data_generator: Option<Box<dyn BootstrapDataGenerator + Send + Sync>>,
    #[debug(skip)]
    pub source_change_generator: Option<Box<dyn SourceChangeGenerator + Send + Sync>>,    
}

impl TestRunSource {
    pub async fn new(config: TestRunSourceConfig, test_data_store: TestSourceStorage, result_store: TestRunSourceStorage) -> anyhow::Result<Self> {
        let id = TestRunSourceId::try_from(&config)?;

        let TestRunSourceConfig { 
            bootstrap_data_generator: bootstrap_data_generator_config, 
            source_change_generator: source_change_generator_config, 
            .. 
        } = config;

        let bootstrap_data_generator = create_bootstrap_data_generator(
            id.clone(),
            bootstrap_data_generator_config,
            test_data_store.clone(),
            result_store.clone()
        ).await?;

        let source_change_generator = create_source_change_generator(
            id.clone(),
            source_change_generator_config,
            test_data_store,
            result_store
        ).await?;
    
        Ok(Self { 
            id, 
            start_immediately: config.start_immediately,
            bootstrap_data_generator, 
            source_change_generator 
        })
    }

    pub async fn get_state(&self) -> anyhow::Result<TestRunSourceState> {

        Ok(TestRunSourceState {
            id: self.id.clone(),
            source_change_generator: self.get_source_change_generator_state().await?,
        })
    }

    pub async fn get_source_change_generator_state(&self) -> anyhow::Result<SourceChangeGeneratorState> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.get_state().await?;
                Ok(response.state)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn pause_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.pause().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn skip_source_change_generator(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.skip(skips).await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn start_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.start().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn step_source_change_generator(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.step(steps).await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }

    pub async fn stop_source_change_generator(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        match &self.source_change_generator {
            Some(generator) => {
                let response = generator.stop().await?;
                Ok(response)
            },
            None => {
                anyhow::bail!("SourceChangeGenerator not configured for TestRunSource: {:?}", &self.id);
            }
        }
    }
}