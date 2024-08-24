use std::{net::SocketAddr, sync::Arc, vec};

use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{test_repo::dataset::{DataSetContent, DataSetSettings}, test_script::test_script_player::{TestScriptPlayerConfig, TestScriptPlayerState}, ServiceState, ServiceStatus, SharedState};

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

pub(crate) async fn start_web_api(service_state: ServiceState) {
    // Get the port number the service will listen on from AppState.
    let addr = SocketAddr::from(([0, 0, 0, 0], service_state.service_settings.port));

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