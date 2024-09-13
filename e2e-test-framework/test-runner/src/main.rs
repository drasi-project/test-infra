use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use serde::Serialize;
use runner::TestRunSource;
use tokio::sync::RwLock;

use config::{ ServiceConfig, ServiceParams, SourceConfig, TestRepoConfig};
use runner::change_script_player::{ ChangeScriptPlayerSettings, ChangeScriptPlayer};
use test_repo::test_repo_cache::TestRepoCache;

mod config;
mod runner;
mod script_source;
mod source_change_dispatchers;
mod test_repo;
mod web_api;

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
pub type SharedState = Arc<RwLock<ServiceState>>;

// The ServiceState struct holds the configuration and current state of the service.
#[derive(Debug)]
pub struct ServiceState {
    pub reactivators: HashMap<String, ChangeScriptPlayer>,
    pub service_params: ServiceParams,
    pub service_status: ServiceStatus,
    pub source_defaults: SourceConfig,
    pub test_repo_cache: Option<TestRepoCache>,
}

impl ServiceState {
    async fn new(service_params: ServiceParams) -> Self {   

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
                        return ServiceState {
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
                ServiceState {
                    reactivators: HashMap::new(),
                    service_params,
                    service_status: ServiceStatus::Initializing,
                    source_defaults,
                    test_repo_cache: Some(test_repo_cache),
                }
            },
            Err(e) => {
                return ServiceState {
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

    pub fn contains_source(&self, source_id: &str) -> bool {
        self.reactivators.contains_key(source_id)
    }

    pub fn contains_test_repo(&self, test_repo_id: &str) -> bool {
        self.test_repo_cache.as_ref().unwrap().contains_test_repo(test_repo_id)
    }
}

// The main function that starts the starts the Service.
// If the Test Runner is started with a config file, it will initialize the Test Runner with the settings in the file.
// If the Test Runner is started with no config file, it will wait to be managed through the Web API.
#[tokio::main]
async fn main() {
     
     env_logger::init();

    // Parse the command line and env var args into an ServiceParams struct. If the args are invalid, return an error.
    let service_params = ServiceParams::parse();
    log::trace!("{:#?}", service_params);

    // Create the initial ServiceState
    let mut service_state = ServiceState::new(service_params).await;
    log::debug!("Initial ServiceState {:?}", &service_state);

    // Iterate over the initial Change Script Players and start each one if it is configured to start immediately.
    for (_, player) in service_state.reactivators.iter() {
        if player.get_settings().reactivator.start_immediately {
            match player.start().await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("Error starting ChangeScriptPlayer: {}", e);
                    log::error!("{}", msg);
                    service_state.service_status = ServiceStatus::Error(msg);
                    break;
                }
            }
        }
    }

    // Set the ServiceStatus to Ready if it is not already in an Error state.
    match &service_state.service_status {
        ServiceStatus::Error(_) => {
            log::error!("Test Runner failed to initialize correctly, ServiceState: {:?}", &service_state.service_status);            
        },
        _ => {
            service_state.service_status = ServiceStatus::Ready;
            log::info!("Test Runner initialized successfully, ServiceState: {:?}", &service_state.service_status);            
        }
    }

    // Start the Web API.
    web_api::start_web_api(service_state).await;

}