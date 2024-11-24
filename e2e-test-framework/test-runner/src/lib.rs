use std::{collections::HashMap, sync::Arc};

use serde::Serialize;
use test_data_store::{test_repo_storage::{test_metadata::{SpacingMode, TimeMode}, TestSourceDataset}, test_run_storage::{TestRunSourceId, TestRunStorage}, SharedTestDataStore};

use change_script_player::{ChangeScriptPlayer, ChangeScriptPlayerCommand, ChangeScriptPlayerMessageResponse};
use config::{TestRunSourceProxyConfig, TestRunSourceChangeGeneratorConfig, TestRunnerConfig, SourceChangeDispatcherConfig, TestRunSourceConfig};
use tokio::sync::RwLock;

pub mod change_script_player;
pub mod config;
pub mod source_change_dispatchers;

#[derive(Clone, Debug, Serialize)]
pub struct TestRunSource {
    pub id: TestRunSourceId,
    pub proxy: Option<TestRunProxy>,
    pub source_change_generator: Option<TestRunSourceChangeGenerator>,    
    pub change_script_player: Option<ChangeScriptPlayer>,
}

impl TestRunSource {
    pub async fn new(config: &TestRunSourceConfig, defaults: &TestRunSourceConfig, test_source_dataset: TestSourceDataset, test_run_storage: TestRunStorage) -> anyhow::Result<Self> {
        // If neither the TestRunSourceConfig nor the TestRunSourceConfig defaults contain a source_id, return an error.
        let source_id = config.test_source_id.as_ref()
            .or_else( || defaults.test_source_id.as_ref())
            .map(|source_id| source_id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No source_id provided and no default value found."))?;
        
        // If neither the TestRunSourceConfig nor the TestRunSourceConfig defaults contain a test_id, return an error.
        let test_id = config.test_id.as_ref()
            .or_else( || defaults.test_id.as_ref())
            .map(|id| id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No test_id provided and no default value found."))?;

        // If neither the TestRunSourceConfig nor the TestRunSourceConfig defaults contain a test_repo_id, return an error.
        let test_repo_id = config.test_repo_id.as_ref()
            .or_else( || defaults.test_repo_id.as_ref())
            .map(|id| id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No test_repo_id provided and no default value found."))?;

        // If neither the TestRunSourceConfig nor the TestRunSourceConfig defaults contain a test_run_id, generate one.
        let test_run_id = config.test_run_id.as_ref()
            .or_else( || defaults.test_run_id.as_ref())
            .map(|id| id.to_string())
            .unwrap_or_else(|| chrono::Utc::now().format("%Y%m%d%H%M%S").to_string());

        let id = TestRunSourceId::new(&test_run_id, &test_repo_id, &test_id, &source_id);

        // If neither the TestRunSourceConfig nor the TestRunSourceConfig defaults contain a proxy, set it to None.
        let proxy = match config.proxy {
            Some(ref config) => {
                match defaults.proxy {
                    Some(ref defaults) => TestRunProxy::new(id.clone(), config, defaults).ok(),
                    None => TestRunProxy::new(id.clone(), config, &TestRunSourceProxyConfig::default()).ok(),
                }
            },
            None => {
                match defaults.proxy {
                    Some(ref defaults) => TestRunProxy::new(id.clone(), defaults, &TestRunSourceProxyConfig::default()).ok(),
                    None => None,
                }
            }
        };

        // If neither the TestRunSourceConfig nor the TestRunSourceConfig defaults contain a source_change_generator, set it to None.
        let source_change_generator = match config.source_change_generator {
            Some(ref config) => {
                match defaults.source_change_generator {
                    Some(ref defaults) => TestRunSourceChangeGenerator::new(id.clone(), config, defaults).ok(),
                    None => TestRunSourceChangeGenerator::new(id.clone(), config, &TestRunSourceChangeGeneratorConfig::default()).ok(),
                }
            },
            None => {
                match defaults.source_change_generator {
                    Some(ref defaults) => TestRunSourceChangeGenerator::new(id.clone(), defaults, &TestRunSourceChangeGeneratorConfig::default()).ok(),
                    None => None,
                }
            }
        };

        let change_script_player = match &source_change_generator {
            Some(source_change_generator) => {
                let st = test_run_storage.get_source_storage(&test_repo_id, &source_id, false).await?;
                let data_store_path = st.path.clone();

                if test_source_dataset.change_log_script_files.len() > 0 {

                    let player = 
                        ChangeScriptPlayer::new(
                            id.clone(), source_change_generator.clone(), test_source_dataset.change_log_script_files, data_store_path).await?;

                    Some(player)
                } else {
                    anyhow::bail!("No change script files available for player: {:?}", &id);
                }
            },
            None => None,
        };

        Ok(Self {
            id,
            proxy,
            source_change_generator,
            change_script_player,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunProxy {
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
}

impl TestRunProxy {
    pub fn new(test_run_source_id: TestRunSourceId, config: &TestRunSourceProxyConfig, defaults: &TestRunSourceProxyConfig) -> anyhow::Result<Self> {
        // Use the config.time_mode if it exists, otherwise use the defaults.time_mode if it exists, 
        // otherwise use TimeMode::default.
        let time_mode = match &config.time_mode {
            Some(time_mode) => time_mode.clone(),
            None => {
                match &defaults.time_mode {
                    Some(time_mode) => time_mode.clone(),
                    None => TimeMode::default()
                }
            }
        };

        Ok(Self {
            test_run_source_id,
            time_mode,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunSourceChangeGenerator {
    pub dispatchers: Vec<SourceChangeDispatcherConfig>,
    pub ignore_scripted_pause_commands: bool,    
    pub spacing_mode: SpacingMode,
    pub start_immediately: bool,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
}

impl TestRunSourceChangeGenerator {
    pub fn new(test_run_source_id: TestRunSourceId, config: &TestRunSourceChangeGeneratorConfig, defaults: &TestRunSourceChangeGeneratorConfig) -> anyhow::Result<Self> {
        // If neither the SourceChangeGeneratorConfig nor the SourceChangeGeneratorConfig defaults contain a ignore_scripted_pause_commands, 
        // set to false.
        let ignore_scripted_pause_commands = config.ignore_scripted_pause_commands
            .or(defaults.ignore_scripted_pause_commands)
            .unwrap_or(false);

        // Use the config.spacing_mode if it exists, otherwise use the defaults.spacing_mode if it exists, 
        // otherwise use SpacingMode::default.
        let spacing_mode = match &config.spacing_mode {
            Some(spacing_mode) => spacing_mode.clone(),
            None => {
                match &defaults.spacing_mode {
                    Some(spacing_mode) => spacing_mode.clone(),
                    None => SpacingMode::default()
                }
            }
        };

        // If neither the SourceChangeGeneratorConfig nor the SourceChangeGeneratorConfig defaults contain a start_immediately, 
        // set to true.
        let start_immediately = config.start_immediately
            .or(defaults.start_immediately)
            .unwrap_or(true);

        // Use the config.time_mode if it exists, otherwise use the defaults.time_mode if it exists, 
        // otherwise use TimeMode::default.
        let time_mode = match &config.time_mode {
            Some(time_mode) => time_mode.clone(),
            None => {
                match &defaults.time_mode {
                    Some(time_mode) => time_mode.clone(),
                    None => TimeMode::default()
                }
            }
        };

        // If neither the SourceChangeGeneratorConfig nor the SourceChangeGeneratorConfig defaults contain a list of dispatchers, 
        // set to an empty Vec.
        let dispatchers = match &config.dispatchers {
            Some(dispatchers) => dispatchers.clone(),
            None => {
                match &defaults.dispatchers {
                    Some(dispatchers) => dispatchers.clone(),
                    None => Vec::new()
                }
            }
        };

        Ok(Self {
            dispatchers,
            ignore_scripted_pause_commands,
            spacing_mode,
            start_immediately,
            test_run_source_id,
            time_mode,
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
        
        let test_run_source_id = TestRunSourceId::try_from(source_config)?;

        // Fail if the TestRunner already contains the TestRunSource.
        if self.sources.contains_key(&test_run_source_id) {
            anyhow::bail!("TestRunSource already exists: {:?}", &test_run_source_id);
        }

        // Get the DataSet for the TestRunSource.
        let dataset = self.data_store.get_test_run_source_content(&test_run_source_id).await?;

        // Get the path for data
        // let test_source_storage = self.data_store.test_repo_store.lock().await.get_test_repo(id)
        let test_run_storage = self.data_store.get_test_run_storage(&test_run_source_id.test_id, &test_run_source_id.test_run_id).await?;
        // let data_store_path = data_store.path.clone();

        let test_run_source = TestRunSource::new(source_config, &self.source_defaults, dataset, test_run_storage).await?;
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
