use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use serde::{Serialize, Serializer};
use tokio::sync::RwLock;

use config::{
    SourceConfig, SourceConfigDefaults, ServiceSettings, ServiceConfigFile,
};
use test_repo::{
    dataset::DataSetSettings,
    local_test_repo::LocalTestRepo,
};
use test_script::test_script_player::{
    TestScriptPlayerSettings, TestScriptPlayer, TestScriptPlayerConfig, 
};

mod config;
mod source_change_dispatchers;
mod test_repo;
mod test_script;
mod web_api;

// An enum that represents the current state of the Test Script Service.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum ServiceStatus {
    // The Service is Initializing, which includes downloading the test data from the Test Repo
    // and creating the initial set of Test Script Players.
    Initializing,
    // The Service has finished initializing and has an active Web API.
    Ready,
    // The Service is in an Error state. and will not be able to process requests.
    Error(String),
}

// The ServiceState struct holds the configuration and current state of the service.
pub struct ServiceState {
    pub service_settings: ServiceSettings,
    pub service_status: ServiceStatus,
    pub source_defaults: SourceConfigDefaults,
    pub reactivators: HashMap<String, TestScriptPlayer>,
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

    fn error(service_settings: ServiceSettings, msg: String) -> Self {
        ServiceState {
            service_settings,
            service_status: ServiceStatus::Error(msg),
            source_defaults: SourceConfigDefaults::default(),
            reactivators: HashMap::new(),
            test_repo: None,
        }
    }
}

// The main function that starts the starts the Test Script Service.
// If the Service is started with a config file, it will initialize the Service with the settings in the file.
// If the Service is started with no config file, it will wait to be managed through the Web API.
#[tokio::main]
async fn main() {
     
     env_logger::init();

    // Parse the command line and env var args into an ServiceSettings struct. If the args are invalid, return an error.
    let service_settings = ServiceSettings::parse();
    log::trace!("{:#?}", service_settings);

    // Load the Service Config file if a path is specified in the ServiceSettings.
    // If the specified file does not exist, return an error.
    // If no file is specified, use defaults.
    let mut service_state = match &service_settings.config_file_path {
        Some(config_file_path) => {
            match ServiceConfigFile::from_file_path(&config_file_path) {
                Ok(service_config) => {
                    log::trace!("Configuring Test Script Service from {:#?}", service_config);
                    let mut service_state = ServiceState::new(service_settings, Some(service_config.defaults));

                    // Iterate over the SourceConfigs in the ServiceConfigFile and create a TestScriptPlayer for each one.
                    for source_config in service_config.sources {
                        log::trace!("Initializing Source from {:#?}", source_config);

                        if source_config.reactivator.is_some() {
                            log::trace!("Creating TestScriptPlayer from {:#?}", &source_config.reactivator);

                            match create_test_script_player(source_config, &mut service_state).await {
                                Ok(player) => {
                                    service_state.reactivators.insert(player.get_id(), player);
                                },
                                Err(e) => {
                                    let msg = format!("Error creating TestScriptPlayer: {}", e);
                                    log::error!("{}", msg);
                                    service_state.service_status = ServiceStatus::Error(msg);
                                    break;
                                }
                            }
                        }   
                    }
                    service_state
                },
                Err(e) => {
                    let msg = format!("Error loading service config file {:?}. Error {}", service_settings.config_file_path, e);
                    log::error!("{}", msg);
                    ServiceState::error(service_settings, e)
                }
            }
        },
        None => {
            log::trace!("No config file specified. Using defaults.");
            ServiceState::new(service_settings, None)
        }
    };

    // Iterate over the initial Test Script Players and start each one if it is configured to start immediately.
    for (_, active_player) in service_state.reactivators.iter() {
        if active_player.get_config().player_settings.start_immediately {
            match active_player.start().await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("Error starting TestScriptPlayer: {}", e);
                    log::error!("{}", msg);
                    service_state.service_status = ServiceStatus::Error(msg);
                    break;
                }
            }
        }
    }

    // Start the Web API.
    web_api::start_web_api(service_state).await;

}

async fn create_test_script_player(source_config: SourceConfig, service_state: &mut ServiceState) -> Result<TestScriptPlayer, String> {

    let player_settings = match TestScriptPlayerSettings::try_from_source_config(&source_config, &service_state.source_defaults, &service_state.service_settings) {
        Ok(player_settings) => player_settings,
        Err(e) => {
            let msg = format!("Error creating PlayerSettings: {}", e);
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    let data_set_settings = DataSetSettings::from_test_script_player_settings(&player_settings);
    
    let test_script_files = match service_state.test_repo.as_mut().unwrap().add_data_set(&data_set_settings).await {
        Ok(data_set_content) => match data_set_content.change_log_script_files {
            Some(script_files) => script_files,
            None => {
                let msg = format!("No test script files found for data set: {}", &data_set_settings.get_id());
                log::error!("{}", msg);
                return Err(msg);
            }
        },
        Err(e) => {
            let msg = format!("Error getting test script files: {}", e);
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    let cfg = TestScriptPlayerConfig {
        player_settings,
        script_files: test_script_files,
    };
    log::trace!("{:#?}", cfg);

    Ok(TestScriptPlayer::new(cfg).await)
}

pub fn mask_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("******")
}