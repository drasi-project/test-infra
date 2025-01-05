use std::sync::Arc;

use axum::{
    extract::{Extension, Path}, response::IntoResponse, routing::{get, post}, Json, Router
};
use serde::{Deserialize, Serialize};

use test_data_store::test_repo_storage::models::SpacingMode;
use test_runner::{test_run_sources::TestRunSourceConfig, TestRunner, TestRunnerStatus};

use super::TestServiceWebApiError;

pub fn get_test_runner_routes() -> Router {
    Router::new()
        .route("/sources", get(get_source_list_handler).post(post_source_handler))
        .route("/sources/:id", get(get_source_handler))
        .route("/sources/:id/pause", post(source_change_generator_pause_handler))
        .route("/sources/:id/reset", post(source_change_generator_reset_handler))
        .route("/sources/:id/skip", post(source_change_generator_skip_handler))
        .route("/sources/:id/start", post(source_change_generator_start_handler))
        .route("/sources/:id/step", post(source_change_generator_step_handler))
        .route("/sources/:id/stop", post(source_change_generator_stop_handler))
}

pub async fn get_source_list_handler(
    test_runner: Extension<Arc<TestRunner>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_source_list");

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let sources = test_runner.get_test_source_ids().await?;
    Ok(Json(sources).into_response())
}

pub async fn get_source_handler(
    Path(id): Path<String>,
    test_runner: Extension<Arc<TestRunner>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_source: {}", id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let source_state = test_runner.get_test_source_state(&id).await?;
    Ok(Json(source_state).into_response())

    // let response = test_runner.get_test_source_state(&id).await;
    // match response {
    //     Ok(source) => {
    //         Ok(Json(source).into_response())
    //     },
    //     Err(_) => {
    //         Err(TestServiceWebApiError::NotFound("TestRunSource".to_string(), id))
    //     }
    // }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestSkipConfig {
    #[serde(default)]
    pub num_skips: u64,
    pub spacing_mode: Option<SpacingMode>,
}

impl Default for TestSkipConfig {
    fn default() -> Self {
        TestSkipConfig {
            num_skips: 1,
            spacing_mode: None,
        }
    }
}

pub async fn source_change_generator_pause_handler (
    Path(id): Path<String>,
    test_runner: Extension<Arc<TestRunner>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_pause: {}", id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let response = test_runner.test_source_pause(&id).await;
    match response {
        Ok(source) => {
            Ok(Json(source.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn source_change_generator_reset_handler (
    Path(id): Path<String>,
    test_runner: Extension<Arc<TestRunner>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_reset: {}", id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let response = test_runner.test_source_reset(&id).await;
    match response {
        Ok(source) => {
            Ok(Json(source.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn source_change_generator_skip_handler (
    Path(id): Path<String>,
    test_runner: Extension<Arc<TestRunner>>,
    body: Json<Option<TestSkipConfig>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_skip: {}", id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let skips_body = body.0.unwrap_or_default();

    let response = 
        test_runner.test_source_skip(&id, skips_body.num_skips, skips_body.spacing_mode).await;
    match response {
        Ok(source) => {
            Ok(Json(source.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn source_change_generator_start_handler (
    Path(id): Path<String>,
    test_runner: Extension<Arc<TestRunner>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_start: {}", id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let response = test_runner.test_source_start(&id).await;
    match response {
        Ok(source) => {
            Ok(Json(source.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestStepConfig {
    #[serde(default)]
    pub num_steps: u64,
    pub spacing_mode: Option<SpacingMode>,
}

impl Default for TestStepConfig {
    fn default() -> Self {
        TestStepConfig {
            num_steps: 1,
            spacing_mode: None,
        }
    }
}

pub async fn source_change_generator_step_handler (
    Path(id): Path<String>,
    test_runner: Extension<Arc<TestRunner>>,
    body: Json<Option<TestStepConfig>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_step: {}", id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let steps_body = body.0.unwrap_or_default();

    let response = 
        test_runner.test_source_step(&id, steps_body.num_steps, steps_body.spacing_mode).await;
    match response {
        Ok(source) => {
            Ok(Json(source.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn source_change_generator_stop_handler (
    Path(id): Path<String>,
    test_runner: Extension<Arc<TestRunner>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_stop: {}", id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let response = test_runner.test_source_stop(&id).await;
    match response {
        Ok(source) => {
            Ok(Json(source.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn post_source_handler (
    test_runner: Extension<Arc<TestRunner>>,
    body: Json<TestRunSourceConfig>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_source");

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let source_config = body.0;

    match test_runner.add_test_source(source_config).await {
        Ok(id) => {
            match test_runner.get_test_source_state(&id.to_string()).await {
                Ok(source) => {
                    Ok(Json(source).into_response())
                },
                Err(_) => {
                    Err(TestServiceWebApiError::NotFound("TestRunSource".to_string(), id.to_string()))
                }
            }
        },
        Err(e) => {
            let msg = format!("Error creating Source: {}", e);
            log::error!("{}", &msg);
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}