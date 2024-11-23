use axum::{
    extract::{Extension, Path}, response::IntoResponse, routing::get, Json, Router
};
// use serde::Deserialize, Serialize;
use test_runner::SharedTestRunner;

use super::TestServiceWebApiError;

pub fn get_test_runner_routes() -> Router {
    Router::new()
        // .route("/", get(get_test_runner_handler))
        .route("/sources", get(get_source_list_handler))
        // .route("/sources", get(get_source_list_handler).post(post_source_handler))
        .route("/sources/:id", get(get_source_handler))
        // .route("/sources/:id/player", get(get_source_player_handler))
        // .route("/sources/:id/player/pause", post(player_pause_handler))
        // .route("/sources/:id/player/skip", post(player_skip_handler))
        // .route("/sources/:id/player/start", post(player_start_handler))
        // .route("/sources/:id/player/step", post(player_step_handler))
        // .route("/sources/:id/player/stop", post(player_stop_handler))
}

pub async fn get_source_list_handler(
    test_runner: Extension<SharedTestRunner>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_source_list");

    let test_runner = test_runner.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let sources = test_runner.get_test_source_ids().await?;
    Ok(Json(sources).into_response())
}

pub async fn get_source_handler(
    Path(id): Path<String>,
    test_runner: Extension<SharedTestRunner>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_source: {}", id);

    let test_runner = test_runner.read().await;

    // Check if the service is an Error state.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let source = test_runner.get_test_source(&id).await?;
    match source {
        Some(source) => {
            Ok(Json(source).into_response())
        },
        None => {
            Err(TestServiceWebApiError::NotFound("TestRunSource".to_string(), id))
        }
    }   
}

// pub async fn player_start_handler(
//     Path(id): Path<String>,
//     test_runner: Extension<SharedTestRunner>,
// ) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
//     log::info!("Processing call - player_start: {}", id);

//     let test_runner = test_runner.read().await;

//     // If the TestRunner is an Error state, return an error and a description of the error.
//     // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
//     //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
//     // }

//     test_runner.
//     let source = test_runner.get_test_source(&id).await?;
//     match source {
//         Some(source) => {
//             Ok(Json(source).into_response())
//         },
//         None => {
//             Err(TestServiceWebApiError::NotFound("TestRunSource".to_string(), id))
//         }
//     }   


//     match test_runner.control_player(&id, ChangeScriptPlayerCommand::Start).await {
//         Ok(response) => {
//             Json(response.state).into_response()
//         },
//         Err(e) => {
//             return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
//         }
//     }   
// }






/* 









pub async fn add_dataset_handler (
    test_runner: Extension<SharedTestRunner>,
    body: Json<DatasetConfig>,
) -> impl IntoResponse {
    log::info!("Processing call - add_dataset");

    let mut test_runner = test_runner.write().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

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

#[derive(Debug, Serialize, Deserialize)]
pub struct TestSkipConfig {
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

pub async fn skip_player_handler(
    Path(id): Path<String>,
    test_runner: Extension<SharedTestRunner>,
    body: Json<Option<TestSkipConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - skip_player: {}", id);

    let test_skip_config = body.0;
    log::debug!("{:?}", test_skip_config);

    let num_skips = test_skip_config.unwrap_or_default().num_skips;

    let test_runner = test_runner.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::Skip(num_skips)).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
        }
    }  
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestStepConfig {
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

pub async fn step_player_handler(
    Path(id): Path<String>,
    test_runner: Extension<SharedTestRunner>,
    body: Json<Option<TestStepConfig>>,
) -> impl IntoResponse {
    log::info!("Processing call - step_player: {}", id);

    let test_step_config = body.0;
    log::debug!("{:?}", test_step_config);

    let num_steps = test_step_config.unwrap_or_default().num_steps;

    let test_runner = test_runner.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::Step(num_steps)).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
        }
    }  
}

pub async fn stop_player_handler(
    Path(id): Path<String>,
    test_runner: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - stop_player: {}", id);

    let test_runner = test_runner.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    match test_runner.control_player(&id, ChangeScriptPlayerCommand::Stop).await {
        Ok(response) => {
            Json(response.state).into_response()
        },
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response();
        }
    }  
}
    */