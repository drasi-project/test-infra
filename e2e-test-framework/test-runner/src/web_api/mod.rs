use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{Extension, Path}, http::StatusCode, response::IntoResponse, routing::{get, post}, Json, Router
};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::{runner::{config::TestRepoConfig, ServiceStatus, SharedTestRunner, TestRunner}, test_repo::{dataset::DataSet, TestSourceContent}};
use proxy::acquire_handler;
use reactivator::{add_source_handler, get_source_handler, get_source_list_handler, pause_reactivator_handler, skip_reactivator_handler, start_reactivator_handler, step_reactivator_handler, stop_reactivator_handler};

mod proxy;
mod reactivator;

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

#[derive(Debug, Serialize)]
struct ServiceStateResponse {
    service_status: String,
    local_test_repo: LocalTestRepoResponse,
    reactivators: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ServiceStateErrorResponse {
    service_status: String,
    service_status_msg: String,
}

#[derive(Debug, Serialize)]
struct LocalTestRepoResponse {
    data_cache_path: String,
    data_sets: Vec<DataSetResponse>,
}

#[derive(Debug, Serialize)]
struct DataSetResponse {
    content: TestSourceContent,
    id: String,
    test_repo_id: String,
    test_id: String,
    source_id: String,
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

pub(crate) async fn start_web_api(service_state: TestRunner) {
    let addr = SocketAddr::from(([0, 0, 0, 0], service_state.service_params.port));

    // Now the Test Runner is initialized, create the shared state and start the Web API.
    let shared_state = Arc::new(RwLock::new(service_state));

    // let source_routes = Router::new()
    //     .route("/", get(get_source))

    let sources_routes = Router::new()
        .route("/", get(get_source_handler))
        .route("/pause_reactivator", post(pause_reactivator_handler))
        .route("/skip_reactivator", post(skip_reactivator_handler))
        .route("/start_reactivator", post(start_reactivator_handler))
        .route("/step_reactivator", post(step_reactivator_handler))
        .route("/stop_reactivator", post(stop_reactivator_handler));

    let app = Router::new()
        .route("/", get(service_info_handler))
        .route("/acquire", post(acquire_handler))
        .route("/sources", get(get_source_list_handler).post(add_source_handler))
        .nest("/sources/:id", sources_routes)
        .route("/test_repos", get(get_test_repo_list_handler).post(add_test_repo_handler))
        .route("/test_repos/:id", get(get_test_repo_handler))
        .layer(axum::extract::Extension(shared_state));

    log::info!("Listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn service_info_handler(
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {
    log::info!("Processing call - service_info");

    let state = state.read().await;

    match &state.service_status {
        ServiceStatus::Error(msg) => {
            Json(ServiceStateErrorResponse {
                service_status: "Error".to_string(),
                service_status_msg: msg.to_string(),
            }).into_response()
        },
        _ => {
            let local_test_repo = LocalTestRepoResponse {
                data_cache_path: state.test_repo_cache.as_ref().unwrap().data_cache_root_path.to_str().unwrap().to_string(),
                data_sets: state.test_repo_cache.as_ref().unwrap().datasets.iter().map(|(_, v)| v.into()).collect(),
            };
        
            Json(ServiceStateResponse {
                service_status: format!("{:?}", &state.service_status),
                local_test_repo,
                reactivators: state.reactivators.keys().cloned().collect(),
            }).into_response()
        }
    }
}

pub(super) async fn add_test_repo_handler (
    state: Extension<SharedTestRunner>,
    body: Json<Value>,
) -> impl IntoResponse {
    log::info!("Processing call - add_test_repo");

    let mut service_state = state.write().await;

    // If the service is an Error state, return an error and the description of the error.
    if let ServiceStatus::Error(msg) = &service_state.service_status {
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
        if service_state.contains_test_repo(&id) {
            return (StatusCode::CONFLICT, Json(format!("TestRepoConfig with id {} already exists", id))).into_response();
        };

        // Deserialize the body into a TestRepoConfig.
        // If the deserialization fails, return an error.
        let test_repo_config: TestRepoConfig = match serde_json::from_value(test_repo_json) {
            Ok(test_repo_config) => test_repo_config,
            Err(e) => return (StatusCode::BAD_REQUEST, Json(format!("Error parsing TestRepoConfig: {}", e))).into_response(),
        };

        // Add the TestRepoConfig to the service_state.test_repo_configs HashMap.
        match service_state.add_test_repo(&test_repo_config).await {
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

    let state = state.read().await;

    // Check if the service is an Error state.
    if let ServiceStatus::Error(msg) = &state.service_status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    // TODO: Implement this function.
    let test_repo_configs: Vec<TestRepoConfig> = Vec::new();

    Json(test_repo_configs).into_response()
}

pub(super) async fn get_test_repo_handler (
    Path(id): Path<String>,
    state: Extension<SharedTestRunner>,
) -> impl IntoResponse {

    log::info!("Processing call - get_test_repo: {}", id);

    let state = state.read().await;

    // Check if the service is an Error state.
    if let ServiceStatus::Error(msg) = &state.service_status {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    }

    // Look up the TestRepoConfig by id.
    // state.test_repo_configs.get(&id)
    //     .map_or_else(
    //         || StatusCode::NOT_FOUND.into_response(),
    //         |test_repo_config| Json(test_repo_config).into_response()
    //     )

    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
}