use std::{collections::HashMap, net::SocketAddr, sync::Arc, vec};

use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize, Serializer};
use tokio::sync::RwLock;

use config::{
    SourceConfig, SourceConfigDefaults, ServiceSettings, ServiceConfigFile,
};
use test_repo::{
    dataset::{DataSetContent, DataSetSettings},
    local_test_repo::LocalTestRepo,
};
use test_script::test_script_player::{
    TestScriptPlayerSettings, TestScriptPlayer, 
    TestScriptPlayerConfig, TestScriptPlayerState, 
};

mod config;
mod source_change_dispatchers;
mod test_repo;
mod test_script;

// mod u64_as_string {
//     use serde::{self, Serializer};

//     pub fn serialize<S>(number: &u64, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         serializer.serialize_str(&number.to_string())
//     }

    // pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    // where
    //     D: Deserializer<'de>,
    // {
    //     let s = String::deserialize(deserializer)?;
    //     s.parse::<u64>().map_err(serde::de::Error::custom)
    // }
// }

// mod u64_as_string {
//     use serde::{self, Deserialize, Deserializer, Serializer};
//     use serde::de::{self, Visitor};
//     use std::fmt;

//     pub fn serialize<S>(number: &u64, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer,
//     {
//         // Try to serialize as a number first
//         serializer.serialize_u64(*number)
//     }

//     pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
//     where
//         D: Deserializer<'de>,
//     {
//         struct U64OrStringVisitor;

//         impl<'de> Visitor<'de> for U64OrStringVisitor {
//             type Value = u64;

//             fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//                 formatter.write_str("a u64 represented as either a number or a string")
//             }

//             fn visit_u64<E>(self, value: u64) -> Result<u64, E> {
//                 Ok(value)
//             }

//             fn visit_str<E>(self, value: &str) -> Result<u64, E>
//             where
//                 E: de::Error,
//             {
//                 value.parse::<u64>().map_err(E::custom)
//             }
//         }

//         deserializer.deserialize_any(U64OrStringVisitor)
//     }
// }

#[derive(Debug, Serialize, Deserialize)]
struct TestStepConfig {
    #[serde(default)]
    pub num_steps: u64,
}

impl Default for TestStepConfig {
    fn default() -> Self {
        TestStepConfig {
            num_steps: 1,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TestSkipConfig {
    #[serde(default)]
    pub num_skips: u64,
}

impl Default for TestSkipConfig {
    fn default() -> Self {
        TestSkipConfig {
            num_skips: 1,
        }
    }
}

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

#[derive(Debug, Serialize)]
struct ServiceStateResponse {
    service_status: ServiceStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    service_error_msg: Option<String>,
    local_test_repo: LocalTestRepoResponse,
    reactivators: Vec<String>,
}

#[derive(Debug, Serialize)]
struct LocalTestRepoResponse {
    data_cache_path: String,
    data_sets: Vec<DataSetResponse>,
}

#[derive(Debug, Serialize)]
struct DataSetResponse {
    id: String,
    settings: DataSetSettings,
    content: Option<DataSetContent>,
}

#[derive(Debug, Serialize)]
struct PlayerInfoResponse {
    pub config: TestScriptPlayerConfig,
    pub state: TestScriptPlayerState,
}

// Create PlayerResponse from a TestScriptPlayer.
impl PlayerInfoResponse {
    fn new(config: TestScriptPlayerConfig, state: TestScriptPlayerState) -> Self {
        PlayerInfoResponse {
            config,
            state,
        }
    }
}

#[derive(Debug, Serialize)]
struct PlayerCommandResponse {
    pub test_id: String,
    pub test_run_id: String,
    pub source_id: String,
    pub state: TestScriptPlayerState,
}

// Create PlayerResponse from a TestScriptPlayer.
impl PlayerCommandResponse {
    fn new(config: TestScriptPlayerConfig, state: TestScriptPlayerState) -> Self {
        PlayerCommandResponse {
            test_id: config.player_settings.test_id.clone(),
            test_run_id: config.player_settings.test_run_id.clone(),
            source_id: config.player_settings.source_id.clone(),
            state: state,
        }
    }
}

#[derive(Debug, Serialize)]
struct PlayerCommandError {
    pub test_id: String,
    pub test_run_id: String,
    pub source_id: String,
    pub error_msg: String,
}

impl PlayerCommandError {
    fn new(config: TestScriptPlayerConfig, error_msg: String) -> Self {
        PlayerCommandError {
            test_id: config.player_settings.test_id.clone(),
            test_run_id: config.player_settings.test_run_id.clone(),
            source_id: config.player_settings.source_id.clone(),
            error_msg,
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
    // Get the port number the service will listen on from AppState.
    let addr = SocketAddr::from(([0, 0, 0, 0], service_state.service_settings.port));

    // Set the ServiceStatus to Ready if it is not already in an Error state.
    match &service_state.service_status {
        ServiceStatus::Error(msg) => {
            log::error!("Test Script Service failed to initialize correctly due to error: {}", msg);            
        },
        _ => {
            log::info!("Test Script Service initialized successfully.");
            service_state.service_status = ServiceStatus::Ready;
        }
    }

    // Now the Service is initialized, create the shared state and start the Web API.
    let shared_state = Arc::new(RwLock::new(service_state));

    let reactivator_routes = Router::new()
        .route("/", get(get_player))
        .route("/pause", post(pause_player))
        .route("/skip", post(skip_player))
        .route("/start", post(start_player))
        .route("/step", post(step_player))
        .route("/stop", post(stop_player));

    let app = Router::new()
        .route("/", get(service_info))
        .route("/reactivators", get(get_player_list))
        .nest("/reactivators/:id", reactivator_routes)
        .layer(axum::extract::Extension(shared_state));

    log::info!("Listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
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

async fn service_info(
    state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - service_info");

    let state = state.read().await;

    let local_test_repo = LocalTestRepoResponse {
        data_cache_path: state.test_repo.as_ref().unwrap().data_cache_path.to_str().unwrap().to_string(),
        data_sets: state.test_repo.as_ref().unwrap().data_sets.iter().map(|(k, v)| DataSetResponse {
            id: k.clone(),
            settings: v.settings.clone(),
            content: v.content.clone(),
        }).collect(),
    };

    Json(ServiceStateResponse {
        service_status: state.service_status.clone(),
        service_error_msg: match &state.service_status {
            ServiceStatus::Error(msg) => Some(msg.clone()),
            _ => None,
        },
        local_test_repo,
        reactivators: state.reactivators.keys().cloned().collect(),
    }).into_response()

}

async fn get_player_list(
    state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - get_player_list");

    let state = state.read().await;

    // If the service is an Error state, return an error and the description of the error.
    if let ServiceStatus::Error(msg) = &state.service_status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    // Otherwise, return the configuration and state of all the TestScriptPlayers.
    let mut player_list = vec![];
    for (_, player) in state.reactivators.iter() {
        player_list.push(PlayerInfoResponse::new(player.get_config(), player.get_state().await.unwrap().state));
    }

    Json(player_list).into_response()
}

async fn get_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
) -> impl IntoResponse {

    log::info!("Processing call - get_player: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the TestScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Get the state of the TestScriptPlayer and return it.
    match player.get_state().await {
        Ok(response) => {
            Json(PlayerInfoResponse::new(player.get_config(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_config(), e.to_string())).into_response()
        }
    }
}

// async fn start_handler(
//     Path(id): Path<String>,
//     state: Extension<SharedState>,
// ) -> impl IntoResponse {

//     log::info!("Processing call: start");

//     let state = state.read().await;

//     let active_player = match state.players.get(&id) {
//         Some(player) => player,
//         None => {
//             return (StatusCode::NOT_FOUND, Json(ServiceResponse{ service_error_msg: Some("Player not found".to_string()), player_state: None} ));   
//         }
//     };

//     let result = active_player.player.start().await;

//     let response = match result {
//         Ok(response) => {
//             let service_state = PlayerStateResponse::from(response.state);
//             (StatusCode::OK, Json(ServiceResponse{ service_error_msg: None, player_state: Some(service_state) }))
//         },
//         Err(e) => {           
//             let service_error_msg = format!("{}", e); 
//             log::error!("{}", service_error_msg);
//             (StatusCode::INTERNAL_SERVER_ERROR, Json(ServiceResponse{ service_error_msg: Some(service_error_msg), player_state: None} ))
//         }
//     };

//     return response;
// }

async fn pause_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
) -> impl IntoResponse {

    log::info!("Processing call - pause_player: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the TestScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Pause the TestScriptPlayer and return the result.
    match player.pause().await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_config(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_config(), e.to_string())).into_response()
        }
    }
}

async fn skip_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
    body: Json<Option<TestSkipConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - skip_player: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the TestScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    let test_skip_config = body.0;
    log::debug!("{:?}", test_skip_config);

    let num_skips = test_skip_config.unwrap_or_default().num_skips;

    // Skip the TestScriptPlayer and return the result.
    match player.skip(num_skips).await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_config(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_config(), e.to_string())).into_response()
        }
    }
}

async fn start_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - start_player: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the TestScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Start the TestScriptPlayer and return the result.
    match player.start().await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_config(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_config(), e.to_string())).into_response()
        }
    }
}

async fn step_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
    body: Json<Option<TestStepConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - step_player: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the TestScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    let test_step_config = body.0;
    log::debug!("{:?}", test_step_config);

    let num_steps = test_step_config.unwrap_or_default().num_steps;

    // Step the TestScriptPlayer and return the result.
    match player.step(num_steps).await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_config(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_config(), e.to_string())).into_response()
        }
    }

}

async fn stop_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - stop_player: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the TestScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Stop the TestScriptPlayer and return the result.
    match player.stop().await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_config(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_config(), e.to_string())).into_response()
        }
    }
}

pub fn mask_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("******")
}