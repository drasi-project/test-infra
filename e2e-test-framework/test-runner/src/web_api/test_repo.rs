use axum::{
    extract::{Extension, Path}, http::StatusCode, response::IntoResponse, Json
};
use serde::Serialize;
use serde_json::Value;

use crate::{runner::{config::TestRepoConfig, SharedTestRunner, TestRunnerStatus}, test_repo::{dataset::DataSet, TestSourceContent}};

#[derive(Debug, Serialize)]
pub struct LocalTestRepoResponse {
    pub data_cache_path: String,
    pub datasets: Vec<DataSetResponse>,
}

#[derive(Debug, Serialize)]
pub struct DataSetResponse {
    pub content: TestSourceContent,
    pub id: String,
    pub test_repo_id: String,
    pub test_id: String,
    pub source_id: String,
}

impl From<&DataSet> for DataSetResponse {
    fn from(dataset: &DataSet) -> Self {
        DataSetResponse {
            content: dataset.content.clone(),
            id: dataset.id.clone(),
            test_repo_id: dataset.test_run_source.test_repo_id.clone(),
            test_id: dataset.test_run_source.test_id.clone(),
            source_id: dataset.test_run_source.source_id.clone(),
        }
    }
}

pub(super) async fn add_test_repo_handler (
    state: Extension<SharedTestRunner>,
    body: Json<Value>,
) -> impl IntoResponse {
    log::info!("Processing call - add_test_repo");

    let mut test_runner = state.write().await;

    // If the service is an Error state, return an error and the description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    let test_repo_json = body.0;

    if let Some(id_value) = test_repo_json.get("id") {
        // Convert the "id" field to a string
        let id = match id_value {
            Value::String(s) => s.clone(), // If it's already a string, clone it
            Value::Number(n) => n.to_string(), // Convert numbers to string
            _ => {
                return (StatusCode::BAD_REQUEST, Json("Missing id field in body")).into_response();
            }
        };

        // Check if the TestRepoConfig already exists.
        // If it does, return an error.
        if test_runner.contains_test_repo(&id) {
            return (StatusCode::CONFLICT, Json(format!("TestRepoConfig with id {} already exists", id))).into_response();
        };

        // Deserialize the body into a TestRepoConfig.
        // If the deserialization fails, return an error.
        let test_repo_config: TestRepoConfig = match serde_json::from_value(test_repo_json) {
            Ok(test_repo_config) => test_repo_config,
            Err(e) => return (StatusCode::BAD_REQUEST, Json(format!("Error parsing TestRepoConfig: {}", e))).into_response(),
        };

        // Add the TestRepoConfig to the test_runner.test_repo_configs HashMap.
        match test_runner.add_test_repo(&test_repo_config).await {
            Ok(_) => {
                log::info!("Added TestRepoConfig: {}", id);
                return Json(test_repo_config).into_response();
            },
            Err(e) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(format!("Error adding TestRepoConfig: {}", e))).into_response();
            }
        }
    } else {
        return (StatusCode::BAD_REQUEST, Json("Missing TestRepoConfig id field in body")).into_response();
    }
}

pub(super) async fn get_test_repo_list_handler(
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - get_test_repo_list");

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.get_test_repos() {
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

pub(super) async fn get_test_repo_handler (
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {

    log::info!("Processing call - get_test_repo: {}", id);

    let test_runner = state.read().await;

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    match test_runner.get_test_repo(&id) {
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