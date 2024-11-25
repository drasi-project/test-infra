use std::{collections::HashMap, sync::Arc};

use serde::Serialize;
use test_data_store::{test_repo_storage::{test_metadata::{SpacingMode, TimeMode}, TestSourceDataset}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}, SharedTestDataStore};

use change_script_player::{ChangeScriptPlayer, ChangeScriptPlayerCommand, ChangeScriptPlayerMessageResponse};
use config::{BootstrapDataGeneratorConfig, SourceChangeGeneratorConfig, TestRunnerConfig, SourceChangeDispatcherConfig, TestRunSourceConfig};
use tokio::sync::RwLock;

pub mod change_script_player;
pub mod config;
pub mod source_change_dispatchers;

#[derive(Clone, Debug, Serialize)]
pub struct TestRunSource {
    pub id: TestRunSourceId,
    pub bootstrap_data_generator: Option<BootstrapDataGenerator>,
    pub source_change_generator: Option<SourceChangeGenerator>,    
    pub change_script_player: Option<ChangeScriptPlayer>,
}

impl TestRunSource {
    pub async fn new(id: TestRunSourceId, config: TestRunSourceConfig, dataset: TestSourceDataset, storage: TestRunSourceStorage) -> anyhow::Result<Self> {
        // Before this is called, we have ensured all the missing TestRunSourceConfig fields are filled.
        let bootstrap_data_generator = match &config.bootstrap_data_generator {
            Some(bootstrap_data_generator_config) => {
                Some(BootstrapDataGenerator::new(id.clone(), bootstrap_data_generator_config)?)
            },
            None => None,
        };

        let source_change_generator = match &config.source_change_generator {
            Some(source_change_generator_config) => {
                Some(SourceChangeGenerator::new(id.clone(), source_change_generator_config)?)
            },
            None => None,
        };

        let change_script_player = match &source_change_generator {
            Some(source_change_generator) => {
                if dataset.source_change_script_files.len() > 0 {
                    let player = 
                        ChangeScriptPlayer::new(
                            id.clone(), source_change_generator.clone(), dataset.source_change_script_files, storage.path.clone()).await?;

                    Some(player)
                } else {
                    anyhow::bail!("No change script files available for player: {:?}", &id);
                }
            },
            None => None,
        };

        Ok(Self {
            id,
            bootstrap_data_generator,
            source_change_generator,
            change_script_player,
        })
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

#[derive(Clone, Debug, Serialize)]
pub struct SourceChangeGenerator {
    pub dispatchers: Vec<SourceChangeDispatcherConfig>,
    pub ignore_scripted_pause_commands: bool,    
    pub spacing_mode: SpacingMode,
    pub start_immediately: bool,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
}

impl SourceChangeGenerator {
    pub fn new(test_run_source_id: TestRunSourceId, config: &SourceChangeGeneratorConfig) -> anyhow::Result<Self> {
        // Before this is called, we have ensured all the missing SourceChangeGeneratorConfig fields are filled.

        Ok(Self {
            dispatchers: config.dispatchers.clone().unwrap(),
            ignore_scripted_pause_commands: config.ignore_scripted_pause_commands.unwrap(),
            spacing_mode: config.spacing_mode.clone().unwrap(),
            start_immediately: config.start_immediately.unwrap(),
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
    Started,
    // The Test Runner is in an Error state. and will not be able to process requests.
    Error(String),
}

pub type SharedTestRunner = Arc<RwLock<TestRunner>>;

#[derive(Debug)]
pub struct TestRunner {
    data_store: SharedTestDataStore,
    source_defaults: TestRunSourceConfig,
    sources: HashMap<TestRunSourceId, TestRunSource>,
    status: TestRunnerStatus,
}

impl TestRunner {
    pub async fn new(config: TestRunnerConfig, data_store: SharedTestDataStore) -> anyhow::Result<Self> {   
        log::debug!("Creating TestRunner from {:#?}", config);

        let mut test_runner = TestRunner {
            data_store,
            source_defaults: config.source_defaults,
            sources: HashMap::new(),
            status: TestRunnerStatus::Initialized,
        };

        // Add the initial set of Test Run Sources.
        for ref source_config in config.sources {
            test_runner.add_test_source(source_config).await?;
        };

        log::debug!("TestRunner created -  {:?}", &test_runner);

        Ok(test_runner)
    }
    
    pub async fn add_test_source(&mut self, source_config: &TestRunSourceConfig) -> anyhow::Result<TestRunSource> {
        log::trace!("Adding TestRunSource from {:#?}", source_config);

        // If the TestRunner is in an Error state, return an error.
        if let TestRunnerStatus::Error(msg) = &self.status {
            anyhow::bail!("TestRunner is in an Error state: {}", msg);
        };
        
        // Create a merged TestRunSourceConfig from the source_config and the source_defaults.
        let source_config = TestRunSourceConfig::new(source_config.clone(), self.source_defaults.clone()).await?;

        // We can now safely unwrap the TestRunSourceId from the TestRunSourceConfig.
        let test_run_source_id = TestRunSourceId::try_from(&source_config)?;

        // Fail if the TestRunner already contains the TestRunSource.
        if self.sources.contains_key(&test_run_source_id) {
            anyhow::bail!("TestRunSource already exists: {:?}", &test_run_source_id);
        }

        // Get the storage for the new TestRunSource.
        let test_run_source_storage = {
            let data_store = self.data_store.test_run_store.lock().await;
            let test_run_id = &test_run_source_id.test_run_id;
            let test_run_storage = data_store.get_test_run_storage(&test_run_id, false).await?;
            test_run_storage.get_source_storage(&test_run_source_id, true).await?
        };

        // Get the DataSet for the TestRunSource.
        let dataset = self.data_store.get_test_run_source_content(&test_run_source_id).await?;

        // Create the TestRunSource and add it to the TestRunner.
        let test_run_source = TestRunSource::new(test_run_source_id.clone(), source_config, dataset, test_run_source_storage).await?;
        self.sources.insert(test_run_source_id, test_run_source.clone());
        
        Ok(test_run_source)
    }

    pub async fn contains_test_source(&self, test_run_source_id: &str) -> anyhow::Result<bool> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        Ok(self.sources.contains_key(&test_run_source_id))
    }

    pub async fn control_change_script_player(&self, player_id: &str, command: ChangeScriptPlayerCommand) -> anyhow::Result<ChangeScriptPlayerMessageResponse> {
        log::trace!("Control TestRunSource Player - player_id:{}, command:{:?}", player_id, command);

        // If the TestRunner is in an Error state, return an error.
        if let TestRunnerStatus::Error(msg) = &self.status {
            anyhow::bail!("TestRunner is in an Error state: {}", msg);
        };

        let key = TestRunSourceId::try_from(player_id)?;

        // Get the ChangeScriptPlayer from the TestRunner or fail if it doesn't exist.
        let test_run_source = self.sources.get(&key).ok_or_else(|| anyhow::anyhow!("TestRunSource not found: {}", player_id))?;

        match &test_run_source.change_script_player {
            Some(player) => {
                match command {
                    ChangeScriptPlayerCommand::GetState => {
                        player.get_state().await
                    },
                    ChangeScriptPlayerCommand::Start => {
                        player.start().await
                    },
                    ChangeScriptPlayerCommand::Step(steps) => {
                        player.step(steps).await
                    },
                    ChangeScriptPlayerCommand::Skip(skips) => {
                        player.skip(skips).await
                    },
                    ChangeScriptPlayerCommand::Pause => {
                        player.pause().await
                    },
                    ChangeScriptPlayerCommand::Stop => {
                        player.stop().await
                    },
                    ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
                        anyhow::bail!("ProcessDelayedRecord not supported");
                    },
                }
            },
            None => {
                anyhow::bail!("ChangeScriptPlayer not found: {}", player_id);
            }
        }
    }

    pub async fn get_data_store(&self) -> anyhow::Result<SharedTestDataStore> {
        Ok(self.data_store.clone())
    }
    
    pub async fn get_status(&self) -> anyhow::Result<TestRunnerStatus> {
        Ok(self.status.clone())
    }

    pub async fn get_test_source(&self, test_run_source_id: &str) -> anyhow::Result<Option<TestRunSource>> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        Ok(self.sources.get(&test_run_source_id).cloned())
    }

    pub async fn get_test_sources(&self) -> anyhow::Result<Vec<TestRunSource>> {
        Ok(self.sources.values().cloned().collect())
    }

    pub async fn get_test_source_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.sources.keys().map(|id| id.to_string()).collect())
    }

    // pub async fn match_bootstrap_dataset(&self, requested_labels: &HashSet<String>) -> anyhow::Result<Option<TestSourceDataset>> {
    //     self.data_store.match_bootstrap_dataset(requested_labels)
    // }

    pub async fn start(&mut self) -> anyhow::Result<()> {

        match &self.status {
            TestRunnerStatus::Initialized => {
                log::debug!("Starting TestRunner...");
            },
            TestRunnerStatus::Started => {
                let msg = format!("Test Runner has already been started, cannot start.");
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
            TestRunnerStatus::Error(_) => {
                let msg = format!("Test Runner is in an error state, cannot start. TestRunnerStatus: {:?}", &self.status);
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
        };

        // Iterate over the SourceChangeGenerators and start each one if it is configured to start immediately.
        // If any of the SourceChangeGenerators fail to start, set the TestRunnerStatus to Error and return an error.
        for (_, source) in &self.sources {
            if source.change_script_player.as_ref().unwrap().get_settings().source_change_generator.start_immediately {
                match &source.change_script_player.as_ref().unwrap().start().await {
                    Ok(_) => {},
                    Err(e) => {
                        let msg = format!("Error starting ChangeScriptPlayer: {}", e);
                        self.status = TestRunnerStatus::Error(msg);
                        anyhow::bail!("{:?}", self.status);
                    }
                }
            }
        }

        // Set the TestRunnerStatus to Started .
        log::info!("Test Runner started successfully");            
        self.status = TestRunnerStatus::Started;

        Ok(())
    }
}
