use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Extension, response::IntoResponse, routing::{get, post}, Json, Router
};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::runner::{ TestRunnerStatus, SharedTestRunner, TestRunner};
use proxy::acquire_handler;
use source::{add_source_handler, get_source_handler, get_source_list_handler, pause_reactivator_handler, skip_reactivator_handler, start_reactivator_handler, step_reactivator_handler, stop_reactivator_handler};
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
    service_status: String,
    local_test_repo: LocalTestRepoResponse,
    sources: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ServiceStateErrorResponse {
    service_status: String,
    service_status_msg: String,
}

pub(crate) async fn start_web_api(port: u16, test_runner: TestRunner) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    // Now the Test Runner is initialized, create the shared state and start the Web API.
    let shared_test_runner = Arc::new(RwLock::new(test_runner));

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

    match &test_runner.status {
        TestRunnerStatus::Error(msg) => {
            Json(ServiceStateErrorResponse {
                service_status: "Error".to_string(),
                service_status_msg: msg.to_string(),
            }).into_response()
        },
        _ => {
            let local_test_repo = LocalTestRepoResponse {
                data_cache_path: test_runner.test_repo_cache.data_cache_root_path.to_str().unwrap().to_string(),
                data_sets: test_runner.test_repo_cache.datasets.iter().map(|(_, v)| v.into()).collect(),
            };
        
            Json(ServiceStateResponse {
                service_status: format!("{:?}", &test_runner.status),
                local_test_repo,
                sources: test_runner.reactivators.keys().cloned().collect(),
            }).into_response()
        }
    }
}