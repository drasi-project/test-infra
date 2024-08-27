use std::{net::SocketAddr, sync::Arc,};

use axum::{
    extract::Extension,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{test_repo::dataset::{DataSetContent, DataSetSettings}, ServiceState, ServiceStatus, SharedState};
use proxy::acquire_handler;
use reactivator::{get_player, get_player_list, pause_player, skip_player, start_player, step_player, stop_player};

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
    id: String,
    settings: DataSetSettings,
    content: Option<DataSetContent>,
}

pub(crate) async fn start_web_api(service_state: ServiceState) {
    let addr = SocketAddr::from(([0, 0, 0, 0], service_state.service_settings.port));

    // Now the Test Runner is initialized, create the shared state and start the Web API.
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
        .route("/acquire", post(acquire_handler))
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

    match &state.service_status {
        ServiceStatus::Error(msg) => {
            Json(ServiceStateErrorResponse {
                service_status: "Error".to_string(),
                service_status_msg: msg.to_string(),
            }).into_response()
        },
        _ => {
            let local_test_repo = LocalTestRepoResponse {
                data_cache_path: state.test_repo.as_ref().unwrap().data_cache_path.to_str().unwrap().to_string(),
                data_sets: state.test_repo.as_ref().unwrap().data_sets.iter().map(|(k, v)| DataSetResponse {
                    id: k.clone(),
                    settings: v.get_settings(),
                    content: v.get_content(),
                }).collect(),
            };
        
            Json(ServiceStateResponse {
                service_status: format!("{:?}", &state.service_status),
                local_test_repo,
                reactivators: state.reactivators.keys().cloned().collect(),
            }).into_response()
        }
    }
}