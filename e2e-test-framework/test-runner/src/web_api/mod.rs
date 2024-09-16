use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Extension, http::StatusCode, response::IntoResponse, routing::{get, post}, Json, Router
};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::runner::{ SharedTestRunner, TestRunner, TestRunnerStatus};
use proxy::acquire_handler;
use source::{add_source_handler, get_player_handler, get_source_handler, get_source_list_handler, pause_player_handler, skip_player_handler, start_player_handler, step_player_handler, stop_player_handler};
use test_repo::{add_test_repo_handler, get_test_repo_handler, get_test_repo_list_handler, LocalTestRepoResponse};

mod proxy;
mod source;
mod test_repo;

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
    status: String,
    test_repo_ids: Vec<String>,
    test_run_source_ids: Vec<String>,
    data_cache: LocalTestRepoResponse,
}

#[derive(Debug, Serialize)]
struct ServiceStateErrorResponse {
    status: String,
    error_detail: String,
}

pub(crate) async fn start_web_api(port: u16, test_runner: TestRunner) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    // Now the Test Runner is initialized, create the shared state and start the Web API.
    let shared_test_runner = Arc::new(RwLock::new(test_runner));

    let sources_routes = Router::new()
        .route("/", get(get_source_handler))
        .route("/player", get(get_player_handler))
        .route("/player/pause", post(pause_player_handler))
        .route("/player/skip", post(skip_player_handler))
        .route("/player/start", post(start_player_handler))
        .route("/player/step", post(step_player_handler))
        .route("/player/stop", post(stop_player_handler));

    let app = Router::new()
        .route("/", get(service_info_handler))
        .route("/acquire", post(acquire_handler))
        .route("/sources", get(get_source_list_handler).post(add_source_handler))
        .nest("/sources/:id", sources_routes)
        .route("/test_repos", get(get_test_repo_list_handler).post(add_test_repo_handler))
        .route("/test_repos/:id", get(get_test_repo_handler))
        .layer(axum::extract::Extension(shared_test_runner));

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

    let test_runner = state.read().await;

    match &test_runner.get_status() {
        TestRunnerStatus::Error(msg) => {
            Json(ServiceStateErrorResponse {
                status: "Error".to_string(),
                error_detail: msg.to_string(),
            }).into_response()
        },
        _ => {
            let test_repo_ids = match test_runner.get_test_repo_ids() {
                Ok(ids) => ids,
                Err(e) => {
                    let msg = format!("Error getting test repo ids: {:?}", e);
                    log::error!("{}", msg);
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
                }
            };

            let test_run_source_ids = match test_runner.get_test_run_source_ids() {
                Ok(ids) => ids,
                Err(e) => {
                    let msg = format!("Error getting test run source ids: {:?}", e);
                    log::error!("{}", msg);
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
                }
            };

            let datasets = match test_runner.get_datasets() {
                Ok(datasets) => datasets.iter().map(|d| d.into()).collect(),
                Err(e) => {
                    let msg = format!("Error getting datasets: {:?}", e);
                    log::error!("{}", msg);
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
                }
            };

            let data_cache = LocalTestRepoResponse {
                data_cache_path: test_runner.get_data_store_path().to_string(),
                datasets
            };
        
            Json(ServiceStateResponse {
                status: format!("{:?}", &test_runner.get_status()),
                test_repo_ids,
                test_run_source_ids,
                data_cache,
            }).into_response()
        }
    }
}