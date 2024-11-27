use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Extension, http::StatusCode, response::{IntoResponse, Response}, routing::get, Json, Router
};
use serde::Serialize;
use thiserror::Error;
use tokio::{io::{self, AsyncBufReadExt}, select, signal};

use data_collector::DataCollector;
use repo::get_test_repo_routes;
use runner::get_test_runner_routes;
use test_data_store::TestDataStore;
use test_runner::TestRunner;

pub mod repo;
pub mod runner;

#[derive(Debug, Error)]
pub enum TestServiceWebApiError {
    #[error("Error: {0}")]
    AnyhowError(anyhow::Error),
    #[error("{0} with ID {1} not found")]
    NotFound(String, String),
    #[error("Error: {0}")]
    SerdeJsonError(serde_json::Error),
    #[error("TestRunner is in an Error state: {0}")]
    TestRunnerError(String),
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
            TestServiceWebApiError::NotFound(kind, id) => {
                (StatusCode::NOT_FOUND, Json(format!("{} with ID {} not found", kind, id))).into_response()
            },
            TestServiceWebApiError::SerdeJsonError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            },
            TestServiceWebApiError::TestRunnerError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response()
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct TestServiceStateResponse {
    pub data_store: TestDataStoreStateResponse,
    pub test_runner: TestRunnerStateResponse,
    pub data_collector: DataCollectorStateResponse,
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
struct DataCollectorStateResponse {
    pub status: String,
    pub data_collection_ids: Vec<String>,
}

pub(crate) async fn start_web_api(port: u16, test_data_store: Arc<TestDataStore>, test_runner: Arc<TestRunner>, data_collector: Arc<DataCollector>) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let app = Router::new()
        .route("/", get(service_info_handler))
        // .route("/acquire", post(acquire_handler))
        .nest("/test_repos", get_test_repo_routes())
        .nest("/test_runner", get_test_runner_routes())
        .layer(axum::extract::Extension(data_collector))
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
    data_collector: Extension<Arc<DataCollector>>,
    test_data_store: Extension<Arc<TestDataStore>>,
    test_runner: Extension<Arc<TestRunner>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - service_info");

    Ok(Json(TestServiceStateResponse {
        data_store: TestDataStoreStateResponse {
            path: test_data_store.get_data_store_path().await?.to_string_lossy().to_string(),
            test_repo_ids: test_data_store.get_test_repo_ids().await?,
        },
        test_runner: TestRunnerStateResponse {
            status: format!("{:?}", test_runner.get_status().await?),
            test_run_source_ids: test_runner.get_test_source_ids().await?,
        },
        data_collector: DataCollectorStateResponse {
            status: format!("{:?}", data_collector.get_status().await?),
            data_collection_ids: data_collector.get_data_collection_ids().await?,
        },
    }))
}