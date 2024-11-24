use std::{collections::HashMap, str::FromStr, sync::Arc};

use serde::Serialize;
use test_data_store::{test_repo_storage::TestSourceDataset, test_run_storage::{TestRunSourceId, TestRunStorage}, SharedTestDataStore};

use change_script_player::{ChangeScriptPlayer, ChangeScriptPlayerCommand, ChangeScriptPlayerMessageResponse};
use config::{TestRunSourceProxyConfig, TestRunSourceReactivatorConfig, TestRunnerConfig, SourceChangeDispatcherConfig, TestRunSourceConfig};
use tokio::sync::RwLock;

pub mod change_script_player;
pub mod config;
pub mod source_change_dispatchers;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum TimeMode {
    Live,
    Recorded,
    Rebased(u64),
}

impl FromStr for TimeMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "live" => Ok(Self::Live),
            "recorded" => Ok(Self::Recorded),
            _ => {
                match chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(t) => Ok(Self::Rebased(t.timestamp_nanos_opt().unwrap() as u64)),
                    Err(e) => {
                        anyhow::bail!("Error parsing TimeMode - value:{}, error:{}", s, e);
                    }
                }
            }
        }
    }
}

impl std::fmt::Display for TimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Recorded => write!(f, "recorded"),
            Self::Rebased(time) => write!(f, "{}", time),
        }
    }
}

impl Default for TimeMode {
    fn default() -> Self {
        Self::Recorded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SpacingMode {
    None,
    Recorded,
    Fixed(u64),
}

// Implementation of FromStr for ReplayEventSpacingMode.
// For the Fixed variant, the spacing is specified as a string duration such as '5s' or '100n'.
// Supported units are seconds ('s'), milliseconds ('m'), microseconds ('u'), and nanoseconds ('n').
// If the string can't be parsed as a TimeDelta, an error is returned.
impl FromStr for SpacingMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "none" => Ok(Self::None),
            "recorded" => Ok(Self::Recorded),
            _ => {
                // Parse the string as a number, followed by a time unit character.
                let (num_str, unit_str) = s.split_at(s.len() - 1);
                let num = match num_str.parse::<u64>() {
                    Ok(num) => num,
                    Err(e) => {
                        anyhow::bail!("Error parsing SpacingMode: {}", e);
                    }
                };
                match unit_str {
                    "s" => Ok(Self::Fixed(num * 1000000000)),
                    "m" => Ok(Self::Fixed(num * 1000000)),
                    "u" => Ok(Self::Fixed(num * 1000)),
                    "n" => Ok(Self::Fixed(num)),
                    _ => {
                        anyhow::bail!("Invalid SpacingMode: {}", s);
                    }
                }
            }
        }
    }
}

impl std::fmt::Display for SpacingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Recorded => write!(f, "recorded"),
            Self::Fixed(d) => write!(f, "{}", d),
        }
    }
}

impl Default for SpacingMode {
    fn default() -> Self {
        Self::Recorded
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunSource {
    pub id: TestRunSourceId,
    pub proxy: Option<TestRunProxy>,
    #[serde(skip_serializing)]
    pub reactivator: Option<TestRunReactivator>,    
    #[serde(skip_serializing)]
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

        // If neither the TestRunSourceConfig nor the TestRunSourceConfig defaults contain a reactivator, set it to None.
        let reactivator = match config.reactivator {
            Some(ref config) => {
                match defaults.reactivator {
                    Some(ref defaults) => TestRunReactivator::new(id.clone(), config, defaults).ok(),
                    None => TestRunReactivator::new(id.clone(), config, &TestRunSourceReactivatorConfig::default()).ok(),
                }
            },
            None => {
                match defaults.reactivator {
                    Some(ref defaults) => TestRunReactivator::new(id.clone(), defaults, &TestRunSourceReactivatorConfig::default()).ok(),
                    None => None,
                }
            }
        };

        let change_script_player = match &reactivator {
            Some(reactivator) => {
                let st = test_run_storage.get_source_storage(&test_repo_id, &source_id, false).await?;
                let data_store_path = st.path.clone();

                if test_source_dataset.change_log_script_files.len() > 0 {

                    let player = 
                        ChangeScriptPlayer::new(
                            id.clone(), reactivator.clone(), test_source_dataset.change_log_script_files, data_store_path).await?;

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
            reactivator,
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
        let time_mode = match &config.time_mode {
            Some(time_mode) => TimeMode::from_str(time_mode)?,
            None => {
                match &defaults.time_mode {
                    Some(time_mode) => TimeMode::from_str(time_mode)?,
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
pub struct TestRunReactivator {
    pub dispatchers: Vec<SourceChangeDispatcherConfig>,
    pub ignore_scripted_pause_commands: bool,    
    pub spacing_mode: SpacingMode,
    pub start_immediately: bool,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
}

impl TestRunReactivator {
    pub fn new(test_run_source_id: TestRunSourceId, config: &TestRunSourceReactivatorConfig, defaults: &TestRunSourceReactivatorConfig) -> anyhow::Result<Self> {
        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a ignore_scripted_pause_commands, 
        // set to false.
        let ignore_scripted_pause_commands = config.ignore_scripted_pause_commands
            .or(defaults.ignore_scripted_pause_commands)
            .unwrap_or(false);

        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a spacing_mode, 
        // set to SpacingMode::Recorded.
        let spacing_mode = match &config.spacing_mode {
            Some(spacing_mode) => SpacingMode::from_str(spacing_mode)?,
            None => {
                match &defaults.spacing_mode {
                    Some(spacing_mode) => SpacingMode::from_str(spacing_mode)?,
                    None => SpacingMode::Recorded
                }
            }
        };

        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a start_immediately, 
        // set to true.
        let start_immediately = config.start_immediately
            .or(defaults.start_immediately)
            .unwrap_or(true);

        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a time_mode, 
        // set to TimeMode::Recorded.
        let time_mode = match &config.time_mode {
            Some(time_mode) => TimeMode::from_str(time_mode)?,
            None => {
                match &defaults.time_mode {
                    Some(time_mode) => TimeMode::from_str(time_mode)?,
                    None => TimeMode::Recorded
                }
            }
        };

        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a list of dispatchers, 
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

        // Iterate over the reactivators and start each one if it is configured to start immediately.
        // If any of the reactivators fail to start, set the TestRunnerStatus to Error and return an error.
        for (_, source) in &self.sources {
            if source.change_script_player.as_ref().unwrap().get_settings().reactivator.start_immediately {
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
