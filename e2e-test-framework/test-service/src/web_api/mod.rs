use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Extension, http::StatusCode, response::{IntoResponse, Response}, routing::get, Json, Router
};
use serde::Serialize;
use thiserror::Error;
use tokio::{select, signal};

use data_collector::DataCollector;
use repo::get_test_repo_routes;
use runner::get_test_run_host_routes;
use test_data_store::TestDataStore;
use test_run_host::TestRunHost;

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
    #[error("TestRunHost is in an Error state: {0}")]
    TestRunHostError(String),
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
            TestServiceWebApiError::TestRunHostError(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response()
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct TestServiceStateResponse {
    pub data_store: TestDataStoreStateResponse,
    pub test_run_host: TestRunHostStateResponse,
    pub data_collector: DataCollectorStateResponse,
}

#[derive(Debug, Serialize)]
struct TestDataStoreStateResponse {
    pub path: String,
    pub test_repo_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TestRunHostStateResponse {
    pub status: String,
    pub test_run_reaction_ids: Vec<String>,
    pub test_run_source_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
struct DataCollectorStateResponse {
    pub status: String,
    pub data_collection_ids: Vec<String>,
}

pub(crate) async fn start_web_api(port: u16, test_data_store: Arc<TestDataStore>, test_run_host: Arc<TestRunHost>, data_collector: Arc<DataCollector>) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let app = Router::new()
        .route("/", get(get_service_info_handler))
        .nest("/test_repos", get_test_repo_routes())
        .nest("/test_run_host", get_test_run_host_routes())
        .layer(axum::extract::Extension(data_collector))
        .layer(axum::extract::Extension(test_data_store))
        .layer(axum::extract::Extension(test_run_host));

    println!("\n\nTest Service Web API listening on http://{}", addr);

    let server = axum::Server::bind(&addr)
        .serve(app.into_make_service());

    // Graceful shutdown when receiving `Ctrl+C` or SIGTERM
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    println!("\n\nPress CTRL-C to stop the server...\n\n");

    if let Err(err) = graceful.await {
        eprintln!("Server error: {}", err);
    }

}

async fn shutdown_signal() {
    // Create two tasks: one for Ctrl+C and another for SIGTERM
    // Either will trigger a shutdown
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("\nReceived Ctrl+C, shutting down...");
    };

    // Listen for SIGTERM (Docker stop signal)
    let sigterm = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to listen for SIGTERM");
            sigterm.recv().await;
            println!("\nReceived SIGTERM, shutting down...");
        }
        #[cfg(not(unix))]
        futures::future::pending::<()>().await; // Fallback for non-Unix systems
    };
    
    // Wait for either signal
    select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }

    println!("Cleaning up resources...");
    // TODO: Perform cleanup here...
    println!("Resources cleaned up.");
}

async fn get_service_info_handler(
    data_collector: Extension<Arc<DataCollector>>,
    test_data_store: Extension<Arc<TestDataStore>>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - service_info");

    Ok(Json(TestServiceStateResponse {
        data_store: TestDataStoreStateResponse {
            path: test_data_store.get_data_store_path().await?.to_string_lossy().to_string(),
            test_repo_ids: test_data_store.get_test_repo_ids().await?,
        },
        test_run_host: TestRunHostStateResponse {
            status: test_run_host.get_status().await?.to_string(),
            test_run_reaction_ids: test_run_host.get_test_reaction_ids().await?,
            test_run_source_ids: test_run_host.get_test_source_ids().await?,
        },
        data_collector: DataCollectorStateResponse {
            status: data_collector.get_status().await?.to_string(),
            data_collection_ids: data_collector.get_data_collection_ids().await?,
        },
    }))
}