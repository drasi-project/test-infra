use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::runner::{change_script_player::{ChangeScriptPlayerCommand, ChangeScriptPlayerSettings, ChangeScriptPlayerState}, config::SourceConfig, SharedTestRunner, TestRunnerStatus};

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

// #[derive(Debug, Serialize)]
// struct PlayerCommandResponse {
//     pub test_id: String,
//     pub test_run_id: String,
//     pub source_id: String,
//     pub state: ChangeScriptPlayerState,
// }

// // Create PlayerResponse from a ChangeScriptPlayer.
// impl PlayerCommandResponse {
//     fn new(settings: ChangeScriptPlayerSettings, state: ChangeScriptPlayerState) -> Self {
//         PlayerCommandResponse {
//             test_id: settings.test_run_source.test_id.clone(),
//             test_run_id: settings.test_run_source.test_run_id.clone(),
//             source_id: settings.test_run_source.source_id.clone(),
//             state: state,
//         }
//     }
// }

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
    log::info!("Processing call - add_source");

    let mut test_runner = state.write().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    let source_config = body.0;

    match test_runner.add_test_run_source(&source_config).await {
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
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
                }
            }
        },
        Err(e) => {
            let msg = format!("Error creating Source: {}", e);
            log::error!("{}", &msg);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }   
    }
}

pub(super) async fn get_source_list_handler(
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - get_source_list");

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.get_test_run_sources() {
        Ok(sources) => {
            Json(sources).into_response()
        },
        Err(e) => {
            let msg = format!("Error getting source list, error: {}", e);
            log::error!("{}", &msg);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response()
        }
    }
}

pub(super) async fn get_source_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {

    log::info!("Processing call - get_source: {}", id);

    let test_runner = state.read().await;

    // Check if the service is an Error state.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.get_test_run_source(&id) {
        Ok(source) => {
            match source {
                Some(source) => {
                    Json(source).into_response()
                },
                None => {
                    StatusCode::NOT_FOUND.into_response()
                }
            }
        },
        Err(e) => {
            let msg = format!("Error getting source - source: {}, error: {}", id, e);
            log::error!("{}", &msg);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response()
        }
    }
}

pub(super) async fn get_player_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - get_player: {}", id);

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::GetState).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
        }
    }  
}

pub(super) async fn pause_player_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - pause_player: {}", id);

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::Pause).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
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

pub(super) async fn skip_player_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
    body: Json<Option<TestSkipConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - skip_player: {}", id);

    let test_skip_config = body.0;
    log::debug!("{:?}", test_skip_config);

    let num_skips = test_skip_config.unwrap_or_default().num_skips;

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::Skip(num_skips)).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
        }
    }  
}

pub(super) async fn start_player_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - start_player: {}", id);

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::Start).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
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

pub(super) async fn step_player_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
    body: Json<Option<TestStepConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - step_player: {}", id);

    let test_step_config = body.0;
    log::debug!("{:?}", test_step_config);

    let num_steps = test_step_config.unwrap_or_default().num_steps;

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::Step(num_steps)).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
        }
    }  
}

pub(super) async fn stop_player_handler(
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - stop_player: {}", id);

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::Stop).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
        }
    }  
}