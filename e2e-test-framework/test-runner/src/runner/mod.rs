use std::{collections::HashMap, str::FromStr, sync::Arc};

use serde::Serialize;
use tokio::sync::RwLock;

use change_script_player::{ChangeScriptPlayer, ChangeScriptPlayerSettings};
use config::{ProxyConfig, ReactivatorConfig, ServiceConfig, SourceChangeDispatcherConfig, SourceConfig, TestRepoConfig};
use crate::{test_repo::test_repo_cache::TestRepoCache, ServiceParams};

pub mod change_script_player;
pub mod config;

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
    pub id: String,
    pub service_params: ServiceParams,
    pub source_id: String,
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: String,
    pub proxy: Option<TestRunProxy>,
    pub reactivator: Option<TestRunReactivator>,
}

impl TestRunSource {
    pub fn try_from_config(config: &SourceConfig, defaults: &SourceConfig, service_params: ServiceParams) -> anyhow::Result<Self> {
        // If neither the SourceConfig nor the SourceConfig defaults contain a source_id, return an error.
        let source_id = config.source_id.as_ref()
            .or_else( || defaults.source_id.as_ref())
            .map(|source_id| source_id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No source_id provided and no default value found."))?;
        
        // If neither the SourceConfig nor the SourceConfig defaults contain a test_id, return an error.
        let test_id = config.test_id.as_ref()
            .or_else( || defaults.test_id.as_ref())
            .map(|id| id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No test_id provided and no default value found."))?;

        // If neither the SourceConfig nor the SourceConfig defaults contain a test_repo_id, return an error.
        let test_repo_id = config.test_repo_id.as_ref()
            .or_else( || defaults.test_repo_id.as_ref())
            .map(|id| id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No test_repo_id provided and no default value found."))?;

        // If neither the SourceConfig nor the SourceConfig defaults contain a test_run_id, generate one.
        let test_run_id = config.test_run_id.as_ref()
            .or_else( || defaults.test_run_id.as_ref())
            .map(|id| id.to_string())
            .unwrap_or_else(|| chrono::Utc::now().format("%Y%m%d%H%M%S").to_string());

        let id = format!("{}::{}::{}::{}", &test_repo_id, &test_id, &source_id, &test_run_id);

        // If neither the SourceConfig nor the SourceConfig defaults contain a proxy, set it to None.
        let proxy = match config.proxy {
            Some(ref config) => {
                match defaults.proxy {
                    Some(ref defaults) => TestRunProxy::try_from_config(id.clone(), config, defaults).ok(),
                    None => TestRunProxy::try_from_config(id.clone(), config, &ProxyConfig::default()).ok(),
                }
            },
            None => {
                match defaults.proxy {
                    Some(ref defaults) => TestRunProxy::try_from_config(id.clone(), defaults, &ProxyConfig::default()).ok(),
                    None => None,
                }
            }
        };

        // If neither the SourceConfig nor the SourceConfig defaults contain a reactivator, set it to None.
        let reactivator = match config.reactivator {
            Some(ref config) => {
                match defaults.reactivator {
                    Some(ref defaults) => TestRunReactivator::try_from_config(id.clone(), config, defaults).ok(),
                    None => TestRunReactivator::try_from_config(id.clone(), config, &ReactivatorConfig::default()).ok(),
                }
            },
            None => {
                match defaults.reactivator {
                    Some(ref defaults) => TestRunReactivator::try_from_config(id.clone(), defaults, &ReactivatorConfig::default()).ok(),
                    None => None,
                }
            }
        };

        Ok(Self {
            id,
            service_params,
            source_id,
            test_id,
            test_repo_id,
            test_run_id,
            proxy,
            reactivator,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunProxy {
    pub test_run_source_id: String,
    pub time_mode: TimeMode,
}

impl TestRunProxy {
    pub fn try_from_config(test_run_source_id: String, config: &ProxyConfig, defaults: &ProxyConfig) -> anyhow::Result<Self> {
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
    pub test_run_source_id: String,
    pub time_mode: TimeMode,
}

impl TestRunReactivator {
    pub fn try_from_config(test_run_source_id: String, config: &ReactivatorConfig, defaults: &ReactivatorConfig) -> anyhow::Result<Self> {
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


// An enum that represents the current state of the Service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum ServiceStatus {
    // The Test Runner is Initializing, which includes downloading the test data from the Test Repo
    // and creating the initial set of Change Script Players.
    Initializing,
    // The Test Runner has finished initializing and has an active Web API.
    Ready,
    // The Test Runner is in an Error state. and will not be able to process requests.
    Error(String),
}

// Type alias for the SharedState struct.
pub type SharedTestRunner = Arc<RwLock<TestRunner>>;

// The ServiceState struct holds the configuration and current state of the service.
#[derive(Debug)]
pub struct TestRunner {
    pub reactivators: HashMap<String, ChangeScriptPlayer>,
    pub service_params: ServiceParams,
    pub service_status: ServiceStatus,
    pub source_defaults: SourceConfig,
    pub test_repo_cache: Option<TestRepoCache>,
}

impl TestRunner {
    pub async fn new(service_params: ServiceParams) -> Self {   

        log::debug!("Creating ServiceState from {:#?}", service_params);

        // Load the Test Runner Config file if a path is specified in the ServiceParams.
        // If the specified file does not exist, configure the service in an error state.
        // If no file is specified, use defaults.
        let ServiceConfig { source_defaults, sources, test_repos } = match service_params.config_file_path.as_ref() {
            Some(config_file_path) => {
                log::debug!("Loading Test Runner config from {:#?}", config_file_path);

                match ServiceConfig::from_file_path(&config_file_path) {
                    Ok(service_config) => {
                        log::debug!("Loaded Test Runner config {:?}", service_config);
                        service_config
                    },
                    Err(e) => {
                        let msg = format!("Error loading Test Runner config: {}", e);
                        log::error!("{}", msg);
                        return TestRunner {
                            reactivators: HashMap::new(),
                            service_params,
                            service_status: ServiceStatus::Error(msg),
                            source_defaults: SourceConfig::default(),
                            test_repo_cache: None,
                        };
                    }
                }
            },
            None => {
                log::debug!("No config file specified. Using defaults and waiting to be configured via Web API.");
                ServiceConfig::default()
            }
        };

        // Attempt to create a local test repo cache with the data cache path from the ServiceParams.
        // If this fails, configure the service in an error state.
        let mut service_state = match TestRepoCache::new(service_params.data_cache_path.clone()).await {
            Ok(test_repo_cache) => {
                TestRunner {
                    reactivators: HashMap::new(),
                    service_params,
                    service_status: ServiceStatus::Initializing,
                    source_defaults,
                    test_repo_cache: Some(test_repo_cache),
                }
            },
            Err(e) => {
                return TestRunner {
                    reactivators: HashMap::new(),
                    service_params,
                    service_status: ServiceStatus::Error(e.to_string()),
                    source_defaults: source_defaults,
                    test_repo_cache: None,
                };
            }
        };
        
        // Create the set of TestRepos that are defined in the ServiceConfig.
        // If there is an error creating one of the pre-configured TestRepos, set the ServiceStatus to Error,
        // log the error, and return the ServiceState.
        for ref test_repo_config in test_repos {
            match service_state.add_test_repo(test_repo_config).await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("Error creating pre-configured TestRepo - TestRepo: {:?}, Error: {}", test_repo_config, e);
                    log::error!("{}", msg);
                    service_state.service_status = ServiceStatus::Error(msg);
                    return service_state;
                }
            }
        };

        // Create the set of TestRunSources that are defined in the ServiceConfig.
        // If there is an error creating one of the pre-configured Sources, set the ServiceStatus to Error,
        // log the error, and return the ServiceState.
        for ref source_config in sources {
            match service_state.add_test_run_source(source_config).await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("Error creating pre-configured TestRunSource - TestRunSource: {:?}, Error: {}", source_config, e);
                    log::error!("{}", msg);
                    service_state.service_status = ServiceStatus::Error(msg);
                    return service_state;
                }
            }
        };

        service_state.service_status = ServiceStatus::Ready;
        service_state
    }
    
    pub async fn add_test_repo(&mut self, test_repo_config: &TestRepoConfig ) -> anyhow::Result<()> {
        log::trace!("Adding Test Repo from: {:#?}", test_repo_config);
        
        // If the ServiceState is already in an Error state, return an error.
        if let ServiceStatus::Error(msg) = &self.service_status {
            anyhow::bail!("Service is in an Error state: {}", msg);
        };

        // If adding the TestRepo fails, return an error. If this happens during initialization, the service
        // will be disabled in an error state, but if it happens due to a call from the Web API then TestRunner
        // will return an error response.
        self.test_repo_cache.as_mut().unwrap().add_test_repo(test_repo_config.clone()).await?;

        Ok(())
    }

    pub async fn add_test_run_source(&mut self, source_config: &SourceConfig) -> anyhow::Result<Option<ChangeScriptPlayer>> {
        log::trace!("Adding TestRunSource from {:#?}", source_config);

        // If the ServiceState is in an Error state, return an error.
        if let ServiceStatus::Error(msg) = &self.service_status {
            anyhow::bail!("Service is in an Error state: {}", msg);
        };
        
        let test_run_source = TestRunSource::try_from_config(source_config, &self.source_defaults, self.service_params.clone())?;

        // If adding the TestRunSource fails, return an error. If this happens during initialization, the service
        // will be disabled in an error state, but if it happens due to a call from the Web API then TestRunner
        // will return an error response.
        let dataset = self.test_repo_cache.as_mut().unwrap().get_data_set(test_run_source.clone()).await?;

        // Determine if the TestRunSource has a ChangeScriptPlayer that should be created and 
        // possibly started.
        if test_run_source.reactivator.is_some() {
            match dataset.content.change_log_script_files {
                Some(change_log_script_files) => {
                    if change_log_script_files.len() > 0 {
                        let player_settings = ChangeScriptPlayerSettings::try_from_test_run_source(test_run_source, self.service_params.clone(), change_log_script_files)?;

                        log::debug!("Creating ChangeScriptPlayer from {:#?}", &player_settings);

                        let player = ChangeScriptPlayer::new(player_settings).await;

                        self.reactivators.insert(player.get_id().clone(), player.clone());

                        return Ok(Some(player));
                    } else {
                        anyhow::bail!("No change script files available for player: {:?}", &test_run_source);
                    }
                },
                None => {
                    anyhow::bail!("No change script files available for player: {:?}", &test_run_source);
                }
            }
        }

        Ok(None)
    }

    // pub fn contains_source(&self, source_id: &str) -> bool {
    //     self.reactivators.contains_key(source_id)
    // }

    pub fn contains_test_repo(&self, test_repo_id: &str) -> bool {
        self.test_repo_cache.as_ref().unwrap().contains_test_repo(test_repo_id)
    }
}
