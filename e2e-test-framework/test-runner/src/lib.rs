use std::{collections::HashMap, sync::Arc};

use derive_more::Debug;
use serde::Serialize;
use tokio::sync::RwLock;

use config::{BootstrapDataGeneratorConfig, TestRunnerConfig, TestRunSourceConfig};
use source_change_generators::{create_source_change_generator, SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState};
use test_data_store::{test_repo_storage::{models::TimeMode, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}, SharedTestDataStore};

pub mod config;
pub mod source_change_generators;
pub mod source_change_dispatchers;

#[derive(Debug, Serialize)]
pub struct TestRunSourceState {
    pub id: TestRunSourceId,
    pub source_change_generator: SourceChangeGeneratorState,
}

#[derive(Debug)]
pub struct TestRunSource {
    pub id: TestRunSourceId,
    pub start_immediately: bool,
    pub bootstrap_data_generator: Option<BootstrapDataGenerator>,
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

        let bootstrap_data_generator = match &bootstrap_data_generator_config {
            Some(bootstrap_data_generator_config) => {
                Some(BootstrapDataGenerator::new(id.clone(), bootstrap_data_generator_config)?)
            },
            None => None,
        };

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

pub type SharedTestRunner = Arc<RwLock<TestRunner>>;

#[derive(Debug)]
pub struct TestRunner {
    data_store: SharedTestDataStore,
    sources: HashMap<TestRunSourceId, TestRunSource>,
    status: TestRunnerStatus,
}

impl TestRunner {
    pub async fn new(config: TestRunnerConfig, data_store: SharedTestDataStore) -> anyhow::Result<Self> {   
        log::debug!("Creating TestRunner from {:#?}", config);

        let mut test_runner = TestRunner {
            data_store,
            sources: HashMap::new(),
            status: TestRunnerStatus::Initialized,
        };

        // Add the initial set of Test Run Sources.
        for source_config in config.sources {
            test_runner.add_test_source(source_config).await?;
        };

        log::debug!("TestRunner created -  {:?}", &test_runner);

        Ok(test_runner)
    }
    
    pub async fn add_test_source(&mut self, config: TestRunSourceConfig) -> anyhow::Result<TestRunSourceId> {
        log::trace!("Adding TestRunSource from {:#?}", config);

        // If the TestRunner is in an Error state, return an error.
        if let TestRunnerStatus::Error(msg) = &self.status {
            anyhow::bail!("TestRunner is in an Error state: {}", msg);
        };
        
        let id = TestRunSourceId::try_from(&config)?;

        // Fail if the TestRunner already contains a TestRunSource with the specified Id.
        if self.sources.contains_key(&id) {
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

        let start_immediately = self.status == TestRunnerStatus::Running && test_run_source.start_immediately;

        self.sources.insert(id.clone(), test_run_source);
        
        if start_immediately {
            self.sources.get(&id).unwrap().start_source_change_generator().await?;
        }

        Ok(id)
    }

    pub async fn contains_test_source(&self, test_run_source_id: &str) -> anyhow::Result<bool> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        Ok(self.sources.contains_key(&test_run_source_id))
    }

    // pub async fn control_change_script_player(&self, player_id: &str, command: ChangeScriptPlayerCommand) -> anyhow::Result<ChangeScriptPlayerMessageResponse> {
    //     log::trace!("Control TestRunSource Player - player_id:{}, command:{:?}", player_id, command);

    //     // If the TestRunner is in an Error state, return an error.
    //     if let TestRunnerStatus::Error(msg) = &self.status {
    //         anyhow::bail!("TestRunner is in an Error state: {}", msg);
    //     };

    //     let key = TestRunSourceId::try_from(player_id)?;

    //     // Get the ChangeScriptPlayer from the TestRunner or fail if it doesn't exist.
    //     let test_run_source = self.sources.get(&key).ok_or_else(|| anyhow::anyhow!("TestRunSource not found: {}", player_id))?;

        // match &test_run_source.change_script_player {
        //     Some(player) => {
        //         match command {
        //             ChangeScriptPlayerCommand::GetState => {
        //                 player.get_state().await
        //             },
        //             ChangeScriptPlayerCommand::Start => {
        //                 player.start().await
        //             },
        //             ChangeScriptPlayerCommand::Step(steps) => {
        //                 player.step(steps).await
        //             },
        //             ChangeScriptPlayerCommand::Skip(skips) => {
        //                 player.skip(skips).await
        //             },
        //             ChangeScriptPlayerCommand::Pause => {
        //                 player.pause().await
        //             },
        //             ChangeScriptPlayerCommand::Stop => {
        //                 player.stop().await
        //             },
        //             ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
        //                 anyhow::bail!("ProcessDelayedRecord not supported");
        //             },
        //         }
        //     },
        //     None => {
        //         anyhow::bail!("ChangeScriptPlayer not found: {}", player_id);
        //     }
        // }
    // }

    pub async fn get_data_store(&self) -> anyhow::Result<SharedTestDataStore> {
        Ok(self.data_store.clone())
    }
    
    pub async fn get_status(&self) -> anyhow::Result<TestRunnerStatus> {
        Ok(self.status.clone())
    }

    pub async fn get_test_source_state(&self, test_run_source_id: &str) -> anyhow::Result<TestRunSourceState> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.get(&test_run_source_id) {
            Some(source) => {
                source.get_state().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    // pub async fn get_test_sources(&self) -> anyhow::Result<Vec<TestRunSource>> {
    //     Ok(self.sources.values().cloned().collect())
    // }

    pub async fn get_test_source_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.sources.keys().map(|id| id.to_string()).collect())
    }

    // pub async fn match_bootstrap_dataset(&self, requested_labels: &HashSet<String>) -> anyhow::Result<Option<TestSourceDataset>> {
    //     self.data_store.match_bootstrap_dataset(requested_labels)
    // }

    pub async fn start(&mut self) -> anyhow::Result<()> {

        match &self.status {
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
        for (_, source) in &self.sources {
            if source.start_immediately {
                source.start_source_change_generator().await?;
            }
        }

        // If any of the SourceChangeGenerators fail to start, set the TestRunnerStatus to Error and return an error.
        // for (_, source) in &self.sources {
        //     if source.change_script_player.as_ref().unwrap().get_settings().source_change_generator.start_immediately {
        //         match &source.change_script_player.as_ref().unwrap().start().await {
        //             Ok(_) => {},
        //             Err(e) => {
        //                 let msg = format!("Error starting ChangeScriptPlayer: {}", e);
        //                 self.status = TestRunnerStatus::Error(msg);
        //                 anyhow::bail!("{:?}", self.status);
        //             }
        //         }
        //     }
        // }
        
        // Set the TestRunnerStatus to Running.
        log::info!("Test Runner started successfully");            
        self.status = TestRunnerStatus::Running;

        Ok(())
    }
}
