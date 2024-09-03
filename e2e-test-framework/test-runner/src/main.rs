use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use serde::{Serialize, Serializer};
use tokio::sync::RwLock;

use config::{
    SourceConfig, SourceConfigDefaults, ServiceSettings, ServiceConfigFile,
};
use test_repo::{
    dataset::{DataSet, DataSetSettings},
    local_test_repo::LocalTestRepo,
};
use test_script::change_script_player::{
    ChangeScriptPlayerSettings, ChangeScriptPlayer, ChangeScriptPlayerConfig, 
};

mod config;
mod source_change_dispatchers;
mod test_repo;
mod test_script;
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

// The ServiceState struct holds the configuration and current state of the service.
pub struct ServiceState {
    pub service_settings: ServiceSettings,
    pub service_status: ServiceStatus,
    pub source_defaults: SourceConfigDefaults,
    pub reactivators: HashMap<String, ChangeScriptPlayer>,
    pub test_repo: Option<LocalTestRepo>,
}

// Type alias for the SharedState struct.
pub type SharedState = Arc<RwLock<ServiceState>>;

impl ServiceState {
    fn new(service_settings: ServiceSettings, source_defaults: Option<SourceConfigDefaults>) -> Self {

        // Attempt to create a local test repo with the data cache path from the ServiceSettings.
        // If this fails, set the ServiceStatus to Error.
        match LocalTestRepo::new(service_settings.data_cache_path.clone()) {
            Ok(test_repo) => {
                ServiceState {
                    service_settings,
                    service_status: ServiceStatus::Initializing,
                    source_defaults: source_defaults.unwrap_or_default(),
                    reactivators: HashMap::new(),
                    test_repo: Some(test_repo),
                }
            },
            Err(e) => {
                ServiceState {
                    service_settings,
                    service_status: ServiceStatus::Error(e.to_string()),
                    source_defaults: source_defaults.unwrap_or_default(),
                    reactivators: HashMap::new(),
                    test_repo: None,
                }
            }
        }
    }
    
    pub fn set_error(&mut self, error: String) {
        self.service_status = ServiceStatus::Error(error);
    }
}

// The main function that starts the starts the Service.
// If the Test Runner is started with a config file, it will initialize the Test Runner with the settings in the file.
// If the Test Runner is started with no config file, it will wait to be managed through the Web API.
#[tokio::main]
async fn main() {
     
     env_logger::init();

    // Parse the command line and env var args into an ServiceSettings struct. If the args are invalid, return an error.
    let service_settings = ServiceSettings::parse();
    log::trace!("{:#?}", service_settings);

    // Load the Test Runner Config file if a path is specified in the ServiceSettings.
    // If the specified file does not exist, return an error.
    // If no file is specified, use defaults.
    let mut service_state = match &service_settings.config_file_path {
        Some(config_file_path) => {
            match ServiceConfigFile::from_file_path(&config_file_path) {
                Ok(service_config) => {
                    log::trace!("Configuring Test Runner from {:#?}", service_config);
                    let mut service_state = ServiceState::new(service_settings, Some(service_config.defaults));

                    for source_config in service_config.sources {

                        match add_or_get_source(&source_config, &mut service_state).await {
                            Ok(dataset) => {
                                match create_change_script_player(source_config, &mut service_state, &dataset).await {
                                    Ok(player) => {
                                        service_state.reactivators.insert(player.get_id(), player);
                                    },
                                    Err(e) => {
                                        let msg = format!("Error creating ChangeScriptPlayer: {}", e);
                                        log::error!("{}", msg);
                                        service_state.service_status = ServiceStatus::Error(msg);
                                        break;
                                    }
                                }                                
                            },
                            Err(e) => {
                                let msg = format!("Error creating Source: {}", e);
                                log::error!("{}", msg);
                                service_state.service_status = ServiceStatus::Error(msg);
                                break;
                            }
                        }
                    };
                    service_state
                },
                Err(e) => {
                    ServiceState {
                        service_settings,
                        service_status: ServiceStatus::Error(e.to_string()),
                        source_defaults: SourceConfigDefaults::default(),
                        reactivators: HashMap::new(),
                        test_repo: None,
                    }
                }
            }
        },
        None => {
            log::trace!("No config file specified. Using defaults.");
            ServiceState::new(service_settings, None)
        }
    };

    // Iterate over the initial Change Script Players and start each one if it is configured to start immediately.
    for (_, active_player) in service_state.reactivators.iter() {
        if active_player.get_config().player_settings.start_immediately {
            match active_player.start().await {
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
        ServiceStatus::Error(msg) => {
            log::error!("Test Runner failed to initialize correctly due to error: {}", msg);            
        },
        _ => {
            log::info!("Test Runner initialized successfully.");
            service_state.service_status = ServiceStatus::Ready;
        }
    }

    // Start the Web API.
    web_api::start_web_api(service_state).await;

}

async fn add_or_get_source(source_config: &SourceConfig, service_state: &mut ServiceState) -> anyhow::Result<DataSet> {
    log::trace!("Initializing Source from {:#?}", source_config);

    let data_set_settings = DataSetSettings::try_from_source_config(source_config, &service_state.source_defaults )?;
    let test_repo = service_state.test_repo.as_mut().ok_or_else(|| anyhow::anyhow!("Test Repo not initialized."))?;

    Ok(test_repo.add_or_get_data_set(data_set_settings).await?)
}

async fn create_change_script_player(source_config: SourceConfig, service_state: &mut ServiceState, dataset: &DataSet) -> anyhow::Result<ChangeScriptPlayer> {
    log::trace!("Creating ChangeScriptPlayer from {:#?}", &source_config.reactivator);

    let player_settings = ChangeScriptPlayerSettings::try_from_source_config(&source_config, &service_state.source_defaults, &service_state.service_settings)?;

    let script_files = match dataset.get_content() {
        Some(content) => {
            match content.change_log_script_files {
                Some(files) => files,
                None => {
                    anyhow::bail!("No change script files available for player: {}", &player_settings.get_id());
                }
            }
        },
        None => {
            anyhow::bail!("No change script files available for player: {}", &player_settings.get_id());
        }
    };

    let cfg = ChangeScriptPlayerConfig {
        player_settings,
        script_files,
    };
    log::trace!("{:#?}", cfg);

    Ok(ChangeScriptPlayer::new(cfg).await)
}

pub fn mask_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("******")
}