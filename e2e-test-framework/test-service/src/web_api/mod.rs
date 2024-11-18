use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Extension, http::StatusCode, response::{IntoResponse, Response}, routing::get, Json, Router
};
use serde::Serialize;
use tokio::{io::{self, AsyncBufReadExt}, select, signal, sync::RwLock};

use test_data_collector::{SharedTestDataCollector, TestDataCollector};
use test_runner::{ SharedTestRunner, TestRunner};

// use proxy::acquire_handler;
// use source::{add_source_handler, get_player_handler, get_source_handler, get_source_list_handler, pause_player_handler, skip_player_handler, start_player_handler, step_player_handler, stop_player_handler};
// use test_repo::{add_test_repo_handler, get_test_repo_handler, get_test_repo_list_handler, LocalTestRepoResponse};


// pub mod dataset;
// pub mod proxy;
// pub mod source;
// pub mod test_repo;

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

struct TestServiceError(anyhow::Error);

impl IntoResponse for TestServiceError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for TestServiceError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[derive(Debug, Serialize)]
struct TestServiceStateResponse {
    pub test_runner: TestRunnerStateResponse,
    pub test_data_collector: TestDataCollectorStateResponse,
}

#[derive(Debug, Serialize)]
struct TestRunnerStateResponse {
    pub data_store: DataStoreStateResponse,
    pub status: String,
    pub test_run_source_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TestDataCollectorStateResponse {
    pub data_store: DataStoreStateResponse,
    pub status: String,
    pub data_collection_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DataStoreStateResponse {
    pub data_cache_path: String,
    pub test_repo_ids: Vec<String>,
}

pub(crate) async fn start_web_api(port: u16, test_runner: TestRunner, test_data_collector: TestDataCollector) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let shared_test_runner = Arc::new(RwLock::new(test_runner));
    let shared_test_data_collector = Arc::new(RwLock::new(test_data_collector));

    // let datasets_routes = Router::new()
    //     .route("/", get(get_dataset_handler))
    //     .route("/sources/start", post(start_dataset_sources_handler));

    // let sources_routes = Router::new()
    //     .route("/", get(get_source_handler))
    //     .route("/player", get(get_player_handler))
    //     .route("/player/pause", post(pause_player_handler))
    //     .route("/player/skip", post(skip_player_handler))
    //     .route("/player/start", post(start_player_handler))
    //     .route("/player/step", post(step_player_handler))
    //     .route("/player/stop", post(stop_player_handler));

    let app = Router::new()
        .route("/", get(service_info_handler))
        // .route("/acquire", post(acquire_handler))
        // // .route("/datasets", get(get_dataset_list_handler).post(add_dataset_handler))
        // // .nest("/datasets/:id", datasets_routes)
        // .route("/sources", get(get_source_list_handler).post(add_source_handler))
        // .nest("/sources/:id", sources_routes)
        // .route("/test_repos", get(get_test_repo_list_handler).post(add_test_repo_handler))
        // .route("/test_repos/:id", get(get_test_repo_handler))
        .layer(axum::extract::Extension(shared_test_runner))
        .layer(axum::extract::Extension(shared_test_data_collector));

    log::info!("Listening on {}", addr);

    let server = axum::Server::bind(&addr)
        .serve(app.into_make_service());

    // Graceful shutdown when receiving `Ctrl+C` or ENTER
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    println!("\n\nPress ENTER to stop the server...\n\n");

    if let Err(err) = graceful.await {
        eprintln!("Server error: {}", err);
    }

}

async fn shutdown_signal() {
    // Create two tasks: one for Ctrl+C and another for ENTER
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("\nReceived Ctrl+C, shutting down...");
    };

    let enter = async {
        let mut input = String::new();
        let mut reader = io::BufReader::new(io::stdin());
        reader.read_line(&mut input).await.expect("Failed to read line");
        println!("Received ENTER, shutting down...");
    };

    // Wait for either signal
    select! {
        _ = ctrl_c => {},
        _ = enter => {},
    }

    println!("Cleaning up resources...");
    // TODO: Perform cleanup here...
    println!("Resources cleaned up.");
}

async fn service_info_handler(
    test_runner_state: Extension<SharedTestRunner>,
    test_data_collector_state: Extension<SharedTestDataCollector>,
) -> anyhow::Result<impl IntoResponse, TestServiceError> {
    log::info!("Processing call - service_info");

    let test_runner = test_runner_state.read().await;
    let test_data_collector = test_data_collector_state.read().await;

    Ok(Json(TestServiceStateResponse {
        test_runner: TestRunnerStateResponse {
            data_store: DataStoreStateResponse {
                data_cache_path: test_runner.get_data_store_path().await?.to_string_lossy().to_string(),
                test_repo_ids: test_runner.get_test_repo_ids().await?,
            },
            status: format!("{:?}", test_runner.get_status().await?),
            test_run_source_ids: test_runner.get_test_source_ids().await?
        },
        test_data_collector: TestDataCollectorStateResponse {
            data_store: DataStoreStateResponse {
                data_cache_path: test_data_collector.get_data_store_path().await?.to_string_lossy().to_string(),
                test_repo_ids: test_data_collector.get_test_repo_ids().await?,
            },
            status: format!("{:?}", test_data_collector.get_status().await?),
            data_collection_ids: test_data_collector.get_data_collection_ids().await?,
        },
    }))
        

    // match &test_runner.get_status().await {
    //     TestRunnerStatus::Error(msg) => {
    //         Json(ServiceStateErrorResponse {
    //             status: "Error".to_string(),
    //             error_detail: msg.to_string(),
    //         }).into_response()
    //     },
    //     _ => {
    //         let test_repo_ids = match test_runner.get_test_repo_ids() {
    //             Ok(ids) => ids,
    //             Err(e) => {
    //                 let msg = format!("Error getting test repo ids: {:?}", e);
    //                 log::error!("{}", msg);
    //                 return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    //             }
    //         };

    //         let test_run_source_ids = match test_runner.get_test_source_ids() {
    //             Ok(ids) => ids,
    //             Err(e) => {
    //                 let msg = format!("Error getting test run source ids: {:?}", e);
    //                 log::error!("{}", msg);
    //                 return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    //             }
    //         };

    //         let datasets = match test_runner.get_datasets() {
    //             Ok(datasets) => datasets.iter().map(|d| d.into()).collect(),
    //             Err(e) => {
    //                 let msg = format!("Error getting datasets: {:?}", e);
    //                 log::error!("{}", msg);
    //                 return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    //             }
    //         };

    //         let data_cache = LocalTestRepoResponse {
    //             data_cache_path: test_runner.get_data_store_path().to_string(),
    //             datasets
    //         };
        
    //         Json(ServiceStateResponse {
    //             status: format!("{:?}", &test_runner.get_status()),
    //             test_repo_ids,
    //             test_run_source_ids,
    //             data_cache,
    //         }).into_response()
    //     }
    // }
}