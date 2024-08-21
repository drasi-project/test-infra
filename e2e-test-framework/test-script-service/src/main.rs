use core::panic;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use config::{
    PlayerConfig, PlayerConfigDefaults, ServiceSettings, ServiceConfigFile,
};
use test_repo::initialize_test_data_cache;
use test_script::test_script_player::{
    self, PlayerSettings, ScheduledTestScriptRecord, TestScriptPlayer, 
    TestScriptPlayerConfig, TestScriptPlayerSpacingMode, TestScriptPlayerState, 
    TestScriptPlayerStatus, TestScriptPlayerTimeMode
};
use test_script::test_script_reader::SequencedTestScriptRecord;

mod config;
mod source_change_dispatchers;
mod test_repo;
mod test_script;

mod u64_as_string {
    use serde::{self, Serializer};

    pub fn serialize<S>(number: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&number.to_string())
    }

    // pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    // where
    //     D: Deserializer<'de>,
    // {
    //     let s = String::deserialize(deserializer)?;
    //     s.parse::<u64>().map_err(serde::de::Error::custom)
    // }
}

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

// An enum that represents the current state of the Test Script Service.
// Valid transitions are:
//   () --run_with_config_file--> Uninitialized --> Initializing --> Working
//   () --run_with_no_config_file--> Uninitialized
//   Uninitialized --init--> Initializing --> Working
//   Initialized --start_immediately=true--> Working
//   Initialized --start_immediately=false--> Working
//   * --error--> Error
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum ServiceStatus {
    Uninitialized,
    // The Service is Initializing, which includes downloading the test data from the Test Repo.
    Initializing,
    // The Service has a working player.
    Working,
    // The Service is in an Error state.
    Error,
}

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

#[derive(Debug, Serialize)]
struct ServiceResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_error_msg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub player_state: Option<PlayerStateResponse>
}

#[derive(Debug, Serialize)]
struct PlayerStateResponse {
    pub status: TestScriptPlayerStatus,
    pub error_message: Option<String>,
    pub time_mode: TestScriptPlayerTimeMode,
    pub spacing_mode: TestScriptPlayerSpacingMode, 
    #[serde(with = "u64_as_string")]   
    pub start_replay_time: u64,
    #[serde(with = "u64_as_string")]
    pub current_replay_time: u64,
    pub skips_remaining: u64,
    pub steps_remaining: u64,
    pub delayed_record: Option<ScheduledTestScriptRecord>,
    pub next_record: Option<SequencedTestScriptRecord>,
}

// Convert TestScriptPlayerState to ServiceState
impl From<test_script_player::TestScriptPlayerState> for PlayerStateResponse {
    fn from(player_state: test_script_player::TestScriptPlayerState) -> Self {
        PlayerStateResponse {
            status: player_state.status,
            error_message: player_state.error_message,
            time_mode: player_state.time_mode,
            spacing_mode: player_state.spacing_mode,
            start_replay_time: player_state.start_replay_time,
            current_replay_time: player_state.current_replay_time,
            skips_remaining: player_state.skips_remaining,
            steps_remaining: player_state.steps_remaining,
            delayed_record: player_state.delayed_record,
            next_record: player_state.next_record,
        }
    }
}


// The ServiceState struct holds the configuration and current state of the service.
pub struct ServiceState {
    pub service_settings: ServiceSettings,
    pub service_status: ServiceStatus,
    pub player_defaults: PlayerConfigDefaults,
    pub players: HashMap<String, TestScriptPlayer>,
}

impl ServiceState {
    fn new(service_settings: ServiceSettings, player_defaults: Option<PlayerConfigDefaults>) -> Self {
        ServiceState {
            service_settings,
            service_status: ServiceStatus::Uninitialized,
            player_defaults: player_defaults.unwrap_or_default(),
            players: HashMap::new(),
        }
    }
}

// Type alias for the SharedState struct.
pub type SharedState = Arc<RwLock<ServiceState>>;

#[derive(Debug, Clone, Serialize)]
struct PlayerInfo {
    pub player_settings: PlayerSettings,
    pub script_files: Vec<PathBuf>,
    pub state: TestScriptPlayerState,
}

#[derive(Debug, Clone, Serialize)]
struct ServiceStateInfo {
    players: HashMap<String, TestScriptPlayerConfig>,
    service_status: ServiceStatus,
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

    let mut initial_player_configs : Vec<PlayerConfig> = Vec::new();

    // Load the Service Config file if a path is specified in the ServiceSettings.
    // If the file does not exist, return an error.
    // If no file is specified, use defaults.
    let shared_state = match &service_settings.config_file_path {
        Some(config_file_path) => {
            match ServiceConfigFile::from_file_path(&config_file_path) {
                Ok(service_config) => {
                    log::trace!("{:#?}", service_config);

                    let service_state = ServiceState::new(service_settings, Some(service_config.player_defaults));
                    initial_player_configs = service_config.players;

                    Arc::new(RwLock::new(service_state))
                },
                Err(e) => {
                    log::error!("Error loading service config file: {}", e);
                    panic!("Error loading service config file: {}", e);
                }
            }
        },
        None => {
            let service_state = ServiceState::new(service_settings, None);

            Arc::new(RwLock::new(service_state))
        }
    };

    // If the local data cache path specified in ServiceSettings does not exist, create it.
    // All data used and generated by the Test Script Service will be stored in this folder.
    let data_cache_path = PathBuf::from(&shared_state.read().await.service_settings.data_cache_path);
    if !data_cache_path.exists() {
        match std::fs::create_dir_all(data_cache_path.clone()) {
            Ok(_) => {},
            Err(e) => {
                log::error!("Error creating data cache folder {}: {}", data_cache_path.display(), e);
                panic!("Error creating data cache folder {}: {}", data_cache_path.display(), e);
            }
        }
    }

    // Loop through the PlayerConfigs and create a TestScriptPlayer for each one.
    // This will download the test data from the Test Repo, BUT it will not start the player
    for player_config in initial_player_configs {     

        match create_player(player_config, shared_state.clone()).await {
            Ok(player) => {
                shared_state.write().await.players.insert(player.get_id(), player);
            },
            Err(e) => {
                log::error!("Error creating Player: {}", e);
            }
        }
    }

    // Iterate over the active players and start each one if it is configured to start immediately.
    for (_, active_player) in shared_state.read().await.players.iter() {
        if active_player.get_config().player_settings.start_immediately {
            match active_player.start().await {
                Ok(_) => {},
                Err(e) => {
                    log::error!("Error starting TestScriptPlayer: {}", e);
                    panic!("Error starting TestScriptPlayer: {}", e);
                }
            }
        }
    }

    // Start the Web API.
    // Get the port number the service will listen on from AppState.
    let port = shared_state.read().await.service_settings.port.parse::<u16>().unwrap();

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    
    let player_routes = Router::new()
        .route("/", get(get_player))
        .route("/pause", post(pause_player))
        .route("/skip", post(skip_player))
        .route("/start", post(start_player))
        .route("/step", post(step_player))
        .route("/stop", post(stop_player));

    let app = Router::new()
        .route("/", get(service_info))
        .route("/players", get(get_player_list))
        .nest("/players/:id", player_routes)
        .layer(axum::extract::Extension(shared_state));

    log::info!("Listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn create_player(player_config: PlayerConfig, service_state: SharedState) -> Result<TestScriptPlayer, String> {

    let player_settings = match PlayerSettings::try_from_player_config(player_config, service_state).await {
        Ok(player_settings) => player_settings,
        Err(e) => {
            let msg = format!("Error creating PlayerSettings: {}", e);
            log::error!("{}", msg);
            return Err(msg);
        }
    };
    log::trace!("{:#?}", player_settings);

    log::info!("Creating Player - test_id: {}, test_run_id: {}", player_settings.test_id, player_settings.test_run_id);

    let test_script_files = match initialize_test_data_cache(&player_settings).await {
        Ok(files) => files,
        Err(e) => {
            let msg = format!("Error initializing test data cache for test_id: {}, test_run_id: {} - {}", player_settings.test_id, player_settings.test_run_id, e);
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    let cfg = TestScriptPlayerConfig {
        player_settings: player_settings.clone(),
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

    let service_info = ServiceStateInfo {
        players: state.players.iter().map(|(k, v)| (k.clone(), v.get_config())).collect(),
        service_status: state.service_status.clone(),
    };

    Json(service_info)
}

// async fn create_player(
//     body: Json<CreatePlayer>,
// ) -> impl IntoResponse {
//     let mut active_players = active_players.write().await;
//     let id = Uuid::new_v4().to_string();
//     let player = TestScriptPlayerInfo {
//         id: id.clone(),
//         name: body.0.name,
//         description: body.0.description,
//     };
//     active_players.insert(id, player.clone());
//     Json(player)
// }

async fn get_player_list(
    state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - get_player_list");

    let state = state.read().await;

    let player_list: Vec<TestScriptPlayerConfig> = state.players.iter().map(|(_, v)| v.get_config()).collect();

    Json(player_list)
}

async fn get_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - get_player: {}", id);

    let state = state.read().await;

    if let Some(player) = state.players.get(&id) {

        let TestScriptPlayerConfig { player_settings, script_files } = player.get_config();

        let player_info = PlayerInfo {
            player_settings,
            script_files,
            state: player.get_state().await.unwrap().state,
        };
        Json(player_info).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
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

    let state = state.read().await;

    if let Some(active_player) = state.players.get(&id) {
        match active_player.pause().await {
            Ok(response) => {
                let service_state = PlayerStateResponse::from(response.state);
                (StatusCode::OK, Json(ServiceResponse {
                    service_error_msg: None,
                    player_state: Some(service_state),
                }))
            }
            Err(e) => {
                log::error!("{}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ServiceResponse {
                    service_error_msg: Some(e.to_string()),
                    player_state: None,
                }))
            }
        }
    } else {
        let service_error_msg = format!("Player {} not found.", id); 
        log::debug!("{}", service_error_msg);
        (StatusCode::NOT_FOUND, Json(ServiceResponse {
            service_error_msg: Some(service_error_msg),
            player_state: None,
        }))
    }
}

async fn skip_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
    body: Json<Option<TestSkipConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - skip_player: {}", id);

    let state = state.read().await;

    if let Some(active_player) = state.players.get(&id) {

        let test_skip_config = body.0;
        log::debug!("{:?}", test_skip_config);

        let num_skips = test_skip_config.unwrap_or_default().num_skips;
    
        match active_player.skip(num_skips).await {
            Ok(response) => {
                let service_state = PlayerStateResponse::from(response.state);
                (StatusCode::OK, Json(ServiceResponse {
                    service_error_msg: None,
                    player_state: Some(service_state),
                }))
            }
            Err(e) => {
                log::error!("{}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ServiceResponse {
                    service_error_msg: Some(e.to_string()),
                    player_state: None,
                }))
            }
        }
    } else {
        let service_error_msg = format!("Player {} not found.", id); 
        log::debug!("{}", service_error_msg);
        (StatusCode::NOT_FOUND, Json(ServiceResponse {
            service_error_msg: Some(service_error_msg),
            player_state: None,
        }))
    }
}

async fn start_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - start_player: {}", id);

    let state = state.read().await;

    if let Some(active_player) = state.players.get(&id) {
        match active_player.start().await {
            Ok(response) => {
                let service_state = PlayerStateResponse::from(response.state);
                (StatusCode::OK, Json(ServiceResponse {
                    service_error_msg: None,
                    player_state: Some(service_state),
                }))
            }
            Err(e) => {
                log::error!("{}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ServiceResponse {
                    service_error_msg: Some(e.to_string()),
                    player_state: None,
                }))
            }
        }
    } else {
        let service_error_msg = format!("Player {} not found.", id); 
        log::debug!("{}", service_error_msg);
        (StatusCode::NOT_FOUND, Json(ServiceResponse {
            service_error_msg: Some(service_error_msg),
            player_state: None,
        }))
    }
}

async fn step_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
    body: Json<Option<TestStepConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - step_player: {}", id);

    let state = state.read().await;

    if let Some(active_player) = state.players.get(&id) {

        let test_step_config = body.0;
        log::debug!("{:?}", test_step_config);

        let num_steps = test_step_config.unwrap_or_default().num_steps;
    
        match active_player.step(num_steps).await {
            Ok(response) => {
                let service_state = PlayerStateResponse::from(response.state);
                (StatusCode::OK, Json(ServiceResponse {
                    service_error_msg: None,
                    player_state: Some(service_state),
                }))
            }
            Err(e) => {
                log::error!("{}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ServiceResponse {
                    service_error_msg: Some(e.to_string()),
                    player_state: None,
                }))
            }
        }
    } else {
        let service_error_msg = format!("Player {} not found.", id); 
        log::debug!("{}", service_error_msg);
        (StatusCode::NOT_FOUND, Json(ServiceResponse {
            service_error_msg: Some(service_error_msg),
            player_state: None,
        }))
    }
}

async fn stop_player(
    Path(id): Path<String>,
    state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - stop_player: {}", id);

    let state = state.read().await;

    if let Some(active_player) = state.players.get(&id) {
        match active_player.stop().await {
            Ok(response) => {
                let service_state = PlayerStateResponse::from(response.state);
                (StatusCode::OK, Json(ServiceResponse {
                    service_error_msg: None,
                    player_state: Some(service_state),
                }))
            }
            Err(e) => {
                log::error!("{}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, Json(ServiceResponse {
                    service_error_msg: Some(e.to_string()),
                    player_state: None,
                }))
            }
        }
    } else {
        let service_error_msg = format!("Player {} not found.", id); 
        log::debug!("{}", service_error_msg);
        (StatusCode::NOT_FOUND, Json(ServiceResponse {
            service_error_msg: Some(service_error_msg),
            player_state: None,
        }))
    }
}



// async fn state_handler(
//     Path(id): Path<String>,
//     state: Extension<SharedState>,
// ) -> impl IntoResponse {

//     log::info!("Processing call: state");

//     let result = state.read().await.script_player.as_ref().unwrap().get_state().await;

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

//     response
// }