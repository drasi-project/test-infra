use std::vec;

use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::{
    runner::config::SourceConfig, 
    runner::{change_script_player::{ChangeScriptPlayerSettings, ChangeScriptPlayerState}, ServiceStatus, SharedTestRunner },
};

#[derive(Debug, Serialize)]
struct PlayerInfoResponse {
    pub settings: ChangeScriptPlayerSettings,
    pub state: ChangeScriptPlayerState,
}

// Create PlayerResponse from a ChangeScriptPlayer.
impl PlayerInfoResponse {
    fn new(settings: ChangeScriptPlayerSettings, state: ChangeScriptPlayerState) -> Self {
        PlayerInfoResponse {
            settings,
            state,
        }
    }
}

#[derive(Debug, Serialize)]
struct PlayerCommandResponse {
    pub test_id: String,
    pub test_run_id: String,
    pub source_id: String,
    pub state: ChangeScriptPlayerState,
}

// Create PlayerResponse from a ChangeScriptPlayer.
impl PlayerCommandResponse {
    fn new(settings: ChangeScriptPlayerSettings, state: ChangeScriptPlayerState) -> Self {
        PlayerCommandResponse {
            test_id: settings.test_run_source.test_id.clone(),
            test_run_id: settings.test_run_source.test_run_id.clone(),
            source_id: settings.test_run_source.source_id.clone(),
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
    fn new(settings: ChangeScriptPlayerSettings, error_msg: String) -> Self {
        PlayerCommandError {
            test_id: settings.test_run_source.test_id.clone(),
            test_run_id: settings.test_run_source.test_run_id.clone(),
            source_id: settings.test_run_source.source_id.clone(),
            error_msg,
        }
    }
}

pub(super) async fn add_source_handler (
    state: Extension<SharedTestRunner>,
    body: Json<SourceConfig>,
) -> impl IntoResponse {
    log::info!("Processing call - add_reactivator");

    let mut service_state = state.write().await;

    // If the service is an Error state, return an error and the description of the error.
    if let ServiceStatus::Error(msg) = &service_state.service_status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    let source_config = body.0;

    match service_state.add_test_run_source(&source_config).await {
        Ok(result) => {
            match result {
                Some(player) => {
                    // Get the state of the ChangeScriptPlayer and return it.
                    match player.get_state().await {
                        Ok(response) => {
                            Json(PlayerInfoResponse::new(player.get_settings(), response.state)).into_response()
                        },
                        Err(e) => {
                            Json(PlayerCommandError::new(player.get_settings(), e.to_string())).into_response()
                        }
                    }
                },
                None => {
                    let msg = format!("Error creating ChangeScriptPlayer: {:?}", source_config);
                    log::error!("{}", &msg);
                    service_state.service_status = ServiceStatus::Error(msg.clone());
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
                }
            }
        },
        Err(e) => {
            let msg = format!("Error creating Source: {}", e);
            log::error!("{}", &msg);
            service_state.service_status = ServiceStatus::Error(msg.clone());
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }   
    }
}

pub(super) async fn get_source_list_handler(
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - get_source_list");

    let state = state.read().await;

    // If the service is an Error state, return an error and the description of the error.
    if let ServiceStatus::Error(msg) = &state.service_status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    // Otherwise, return the configuration and state of all the ChangeScriptPlayers.
    let mut player_list = vec![];
    for (_, player) in state.reactivators.iter() {
        player_list.push(PlayerInfoResponse::new(player.get_settings(), player.get_state().await.unwrap().state));
    }

    Json(player_list).into_response()
}

pub(super) async fn get_source_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {

    log::info!("Processing call - get_source: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the ChangeScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Get the state of the ChangeScriptPlayer and return it.
    match player.get_state().await {
        Ok(response) => {
            Json(PlayerInfoResponse::new(player.get_settings(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_settings(), e.to_string())).into_response()
        }
    }
}

pub(super) async fn pause_reactivator_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {

    log::info!("Processing call - pause_reactivator: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the ChangeScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Pause the ChangeScriptPlayer and return the result.
    match player.pause().await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_settings(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_settings(), e.to_string())).into_response()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TestSkipConfig {
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

pub(super) async fn skip_reactivator_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
    body: Json<Option<TestSkipConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - skip_reactivator: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the ChangeScriptPlayer by id.
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

    // Skip the ChangeScriptPlayer and return the result.
    match player.skip(num_skips).await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_settings(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_settings(), e.to_string())).into_response()
        }
    }
}

pub(super) async fn start_reactivator_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - start_reactivator: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the ChangeScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Start the ChangeScriptPlayer and return the result.
    match player.start().await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_settings(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_settings(), e.to_string())).into_response()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct TestStepConfig {
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

pub(super) async fn step_reactivator_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
    body: Json<Option<TestStepConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - step_reactivator: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the ChangeScriptPlayer by id.
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

    // Step the ChangeScriptPlayer and return the result.
    match player.step(num_steps).await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_settings(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_settings(), e.to_string())).into_response()
        }
    }

}

pub(super) async fn stop_reactivator_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - stop_reactivator: {}", id);

    // Limit the scope of the Read Lock to the error check and player lookup.
    let player = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Look up the ChangeScriptPlayer by id.
        match state.reactivators.get(&id) {
            Some(player) => player.clone(),
            None => {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    };

    // Stop the ChangeScriptPlayer and return the result.
    match player.stop().await {
        Ok(response) => {
            Json(PlayerCommandResponse::new(player.get_settings(), response.state)).into_response()
        },
        Err(e) => {
            Json(PlayerCommandError::new(player.get_settings(), e.to_string())).into_response()
        }
    }
}