use std::net::SocketAddr;

use axum::{
    extract::Extension, http::StatusCode, response::{IntoResponse, Response}, routing::get, Json, Router
};
use serde::Serialize;
use thiserror::Error;
use tokio::{io::{self, AsyncBufReadExt}, select, signal};

use test_data_collector::SharedTestDataCollector;
use test_data_store::SharedTestDataStore;
use test_repo::get_test_repo_routes;
use test_runner::SharedTestRunner;

pub mod test_repo;

#[derive(Debug, Error)]
pub enum TestServiceWebApiError {
    #[error("Error: {0}")]
    AnyhowError(anyhow::Error),
    #[error("Error: {0}")]
    SerdeJsonError(serde_json::Error),
}

impl From<anyhow::Error> for TestServiceWebApiError {
    fn from(error: anyhow::Error) -> Self {
        TestServiceWebApiError::AnyhowError(error)
    }
}

impl From<serde_json::Error> for TestServiceWebApiError {
    fn from(error: serde_json::Error) -> Self {
        TestServiceWebApiError::SerdeJsonError(error)
    }
}

impl IntoResponse for TestServiceWebApiError {
    fn into_response(self) -> Response {
        match self {
            TestServiceWebApiError::AnyhowError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            },
            TestServiceWebApiError::SerdeJsonError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct TestServiceStateResponse {
    pub data_store: TestDataStoreStateResponse,
    pub test_runner: TestRunnerStateResponse,
    pub test_data_collector: TestDataCollectorStateResponse,
}

#[derive(Debug, Serialize)]
struct TestDataStoreStateResponse {
    pub path: String,
    pub test_repo_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TestRunnerStateResponse {
    pub status: String,
    pub test_run_source_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TestDataCollectorStateResponse {
    pub status: String,
    pub data_collection_ids: Vec<String>,
}

pub(crate) async fn start_web_api(port: u16, test_data_store: SharedTestDataStore, test_runner: SharedTestRunner, test_data_collector: SharedTestDataCollector) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

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
        .nest("/test_repos", get_test_repo_routes())
        .layer(axum::extract::Extension(test_data_collector))
        .layer(axum::extract::Extension(test_data_store))
        .layer(axum::extract::Extension(test_runner));

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
    test_data_collector_state: Extension<SharedTestDataCollector>,
    test_data_store: Extension<SharedTestDataStore>,
    test_runner_state: Extension<SharedTestRunner>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - service_info");

    let test_runner = test_runner_state.read().await;
    let test_data_collector = test_data_collector_state.read().await;

    Ok(Json(TestServiceStateResponse {
        data_store: TestDataStoreStateResponse {
            path: test_data_store.get_data_store_path().await?.to_string_lossy().to_string(),
            test_repo_ids: test_data_store.get_test_repo_ids().await?,
        },
        test_runner: TestRunnerStateResponse {
            status: format!("{:?}", test_runner.get_status().await?),
            test_run_source_ids: test_runner.get_test_source_ids().await?,
        },
        test_data_collector: TestDataCollectorStateResponse {
            status: format!("{:?}", test_data_collector.get_status().await?),
            data_collection_ids: test_data_collector.get_data_collection_ids().await?,
        },
    }))
}