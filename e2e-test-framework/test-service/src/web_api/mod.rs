// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use thiserror::Error;
use tokio::{select, signal};
use utoipa::{OpenApi, ToSchema};

use data_collector::DataCollector;
use repo::get_test_repo_routes;
use std::collections::HashMap;
use test_data_store::{test_run_storage::TestRunId, TestDataStore};
use test_run_host::TestRunHost;
use test_runs::get_test_runs_routes;
use utoipa_swagger_ui::SwaggerUi;

use crate::openapi::ApiDoc;

pub mod repo;
pub mod test_runs;

#[derive(Debug, Error)]
pub enum TestServiceWebApiError {
    #[error("Anyhow Error: {0}")]
    AnyhowError(anyhow::Error),
    #[error("{0} with ID {1} not found")]
    NotFound(String, String),
    #[error("Serde Error: {0}")]
    SerdeJsonError(serde_json::Error),
    #[error("NotReady: {0}")]
    NotReady(String),
    #[error("IO Error: {0}")]
    IOError(std::io::Error),
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

impl From<std::io::Error> for TestServiceWebApiError {
    fn from(error: std::io::Error) -> Self {
        TestServiceWebApiError::IOError(error)
    }
}

impl IntoResponse for TestServiceWebApiError {
    fn into_response(self) -> Response {
        match self {
            TestServiceWebApiError::AnyhowError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            }
            TestServiceWebApiError::NotFound(kind, id) => (
                StatusCode::NOT_FOUND,
                Json(format!("{kind} with ID {id} not found")),
            )
                .into_response(),
            TestServiceWebApiError::SerdeJsonError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            }
            TestServiceWebApiError::NotReady(msg) => {
                (StatusCode::SERVICE_UNAVAILABLE, Json(msg)).into_response()
            }
            TestServiceWebApiError::IOError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            }
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "data_collector": {
        "status": "Running",
        "data_collection_ids": ["collection-1", "collection-2"]
    },
    "data_store": {
        "path": "/data/test-data-store",
        "test_repo_ids": ["repo-1", "repo-2"]
    },
    "test_run_host": {
        "status": "Active",
        "test_runs": [
            {
                "id": "test_repo.test_id.run_001",
                "test_id": "test_id",
                "test_repo_id": "test_repo",
                "test_run_id": "run_001",
                "sources": ["facilities-db"],
                "queries": ["query-1"],
                "reactions": ["building-comfort"],
                "drasi_lib_instances": []
            },
            {
                "id": "test_repo.test_id.run_002",
                "test_id": "test_id",
                "test_repo_id": "test_repo",
                "test_run_id": "run_002",
                "sources": ["source-1", "source-2"],
                "queries": [],
                "reactions": ["reaction-1"],
                "drasi_lib_instances": ["instance-1"]
            }
        ]
    }
}))]
pub struct TestServiceStateResponse {
    /// Data collector state information
    pub data_collector: DataCollectorStateResponse,
    /// Test data store state information
    pub data_store: TestDataStoreStateResponse,
    /// Test run host state information
    pub test_run_host: TestRunHostStateResponse,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "path": "/data/test-data-store",
    "test_repo_ids": ["population-test", "performance-test"]
}))]
pub struct TestDataStoreStateResponse {
    /// File system path where test data is stored
    pub path: String,
    /// List of available test repository IDs
    pub test_repo_ids: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "status": "Active",
    "test_runs": [
        {
            "id": "test_repo.test_id.run_001",
            "test_id": "test_id",
            "test_repo_id": "test_repo",
            "test_run_id": "run_001",
            "sources": ["facilities-db"],
            "queries": ["query-1"],
            "reactions": ["building-comfort"],
            "drasi_lib_instances": []
        }
    ]
}))]
pub struct TestRunHostStateResponse {
    /// Current status of the test run host
    pub status: String,
    /// List of test runs with their nested resources
    pub test_runs: Vec<TestRunSummary>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TestRunSummary {
    /// Full test run ID
    pub id: String,
    /// Test ID component
    pub test_id: String,
    /// Test repository ID component
    pub test_repo_id: String,
    /// Test run ID component
    pub test_run_id: String,
    /// Source IDs within this test run
    pub sources: Vec<String>,
    /// Query IDs within this test run
    pub queries: Vec<String>,
    /// Reaction IDs within this test run
    pub reactions: Vec<String>,
    /// drasi-lib instance IDs within this test run
    pub drasi_lib_instances: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "status": "Running",
    "data_collection_ids": ["collection-123", "collection-456"]
}))]
pub struct DataCollectorStateResponse {
    /// Current status of the data collector
    pub status: String,
    /// List of active data collection IDs
    pub data_collection_ids: Vec<String>,
}

pub(crate) async fn start_web_api(
    port: u16,
    test_data_store: Arc<TestDataStore>,
    test_run_host: Arc<TestRunHost>,
    data_collector: Arc<DataCollector>,
) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    // Create the main API router
    let api_router = Router::new()
        .route("/", get(get_service_info_handler))
        .nest("/test_repos", get_test_repo_routes())
        // Hierarchical API routes
        .merge(get_test_runs_routes());

    // Create the complete application with Swagger UI
    let app = api_router
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .layer(axum::extract::Extension(data_collector))
        .layer(axum::extract::Extension(test_data_store.clone()))
        .layer(axum::extract::Extension(test_run_host));

    log::info!("Test Service Web API listening on http://{addr}");
    log::info!("API Documentation available at http://{addr}/docs");
    log::info!("OpenAPI JSON specification available at http://{addr}/api-docs/openapi.json");

    let instance = axum::Server::bind(&addr).serve(app.into_make_service());

    // Graceful shutdown when receiving `Ctrl+C` or SIGTERM
    let graceful = instance.with_graceful_shutdown(shutdown_signal(test_data_store));

    log::info!("Press CTRL-C to stop the instance...");

    if let Err(err) = graceful.await {
        eprintln!("Instance error: {err}");
    }
}

/// Handles graceful shutdown signals (SIGINT/Ctrl+C and SIGTERM) for the test service.
///
/// This function performs the following cleanup operations:
/// - Listens for both SIGINT (Ctrl+C) and SIGTERM signals
/// - When a signal is received, explicitly cleans up the TestDataStore if delete_on_stop is enabled
/// - Ensures cleanup happens before the instance shuts down
///
/// The cleanup is performed explicitly here rather than relying solely on Drop trait
/// to ensure it executes reliably during signal-based shutdown.
async fn shutdown_signal(test_data_store: Arc<TestDataStore>) {
    // Create two tasks: one for Ctrl+C and another for SIGTERM
    // Either will trigger a shutdown
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        log::info!("Received Ctrl+C, shutting down...");
    };

    // Listen for SIGTERM (Docker stop signal)
    let sigterm = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to listen for SIGTERM");
            sigterm.recv().await;
            log::info!("Received SIGTERM, shutting down...");
        }
        #[cfg(not(unix))]
        futures::future::pending::<()>().await; // Fallback for non-Unix systems
    };

    // Wait for either signal
    select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }

    log::info!("Cleaning up resources...");

    // Perform explicit cleanup of TestDataStore
    if test_data_store.should_delete_on_stop() {
        log::info!("Performing TestDataStore cleanup on shutdown signal...");
        if let Err(e) = test_data_store.cleanup_async().await {
            log::error!("Error during TestDataStore cleanup: {e}");
        } else {
            log::info!("TestDataStore cleanup completed successfully.");
        }
    }

    log::info!("Resources cleaned up.");
}

#[utoipa::path(
    get,
    path = "/",
    tag = "service",
    responses(
        (status = 200, description = "Service information retrieved successfully", body = TestServiceStateResponse),
        (status = 500, description = "Internal instance error", body = ErrorResponse)
    )
)]
async fn get_service_info_handler(
    data_collector: Extension<Arc<DataCollector>>,
    test_data_store: Extension<Arc<TestDataStore>>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - service_info");

    // Get all resource IDs
    let test_run_ids = test_run_host.get_test_run_ids().await?;
    let source_ids = test_run_host.get_test_source_ids().await?;
    let query_ids = test_run_host.get_test_query_ids().await?;
    let reaction_ids = test_run_host.get_test_reaction_ids().await?;
    let drasi_lib_instance_ids = test_run_host.get_test_drasi_lib_instance_ids().await?;

    // Build hierarchical structure
    let mut test_runs_map: HashMap<String, TestRunSummary> = HashMap::new();

    // Process each test run
    for run_id_str in test_run_ids {
        if let Ok(run_id) = TestRunId::try_from(run_id_str.as_str()) {
            let test_run = TestRunSummary {
                id: run_id_str.clone(),
                test_id: run_id.test_id.clone(),
                test_repo_id: run_id.test_repo_id.clone(),
                test_run_id: run_id.test_run_id.clone(),
                sources: Vec::new(),
                queries: Vec::new(),
                reactions: Vec::new(),
                drasi_lib_instances: Vec::new(),
            };
            test_runs_map.insert(run_id_str, test_run);
        }
    }

    // Add sources to their test runs
    for source_id in source_ids {
        // Extract test run ID from source ID (format: test_repo.test_id.run_id.source_id)
        if let Some(run_id) = extract_test_run_id(&source_id) {
            if let Some(test_run) = test_runs_map.get_mut(&run_id) {
                // Extract just the source name
                if let Some(source_name) = source_id.split('.').next_back() {
                    test_run.sources.push(source_name.to_string());
                }
            }
        }
    }

    // Add queries to their test runs
    for query_id in query_ids {
        if let Some(run_id) = extract_test_run_id(&query_id) {
            if let Some(test_run) = test_runs_map.get_mut(&run_id) {
                if let Some(query_name) = query_id.split('.').next_back() {
                    test_run.queries.push(query_name.to_string());
                }
            }
        }
    }

    // Add reactions to their test runs
    for reaction_id in reaction_ids {
        if let Some(run_id) = extract_test_run_id(&reaction_id) {
            if let Some(test_run) = test_runs_map.get_mut(&run_id) {
                if let Some(reaction_name) = reaction_id.split('.').next_back() {
                    test_run.reactions.push(reaction_name.to_string());
                }
            }
        }
    }

    // Add drasi instances to their test runs
    for instance_id in drasi_lib_instance_ids {
        if let Some(run_id) = extract_test_run_id(&instance_id) {
            if let Some(test_run) = test_runs_map.get_mut(&run_id) {
                if let Some(instance_name) = instance_id.split('.').next_back() {
                    test_run.drasi_lib_instances.push(instance_name.to_string());
                }
            }
        }
    }

    let test_runs: Vec<TestRunSummary> = test_runs_map.into_values().collect();

    Ok(Json(TestServiceStateResponse {
        data_store: TestDataStoreStateResponse {
            path: test_data_store
                .get_data_store_path()
                .await?
                .to_string_lossy()
                .to_string(),
            test_repo_ids: test_data_store.get_test_repo_ids().await?,
        },
        test_run_host: TestRunHostStateResponse {
            status: test_run_host.get_status().await?.to_string(),
            test_runs,
        },
        data_collector: DataCollectorStateResponse {
            status: data_collector.get_status().await?.to_string(),
            data_collection_ids: data_collector.get_data_collection_ids().await?,
        },
    }))
}

/// Extract test run ID from a full resource ID
/// Format: test_repo_id.test_id.test_run_id.resource_id
/// Returns: test_repo_id.test_id.test_run_id
fn extract_test_run_id(full_id: &str) -> Option<String> {
    let parts: Vec<&str> = full_id.split('.').collect();
    if parts.len() >= 4 {
        Some(format!("{}.{}.{}", parts[0], parts[1], parts[2]))
    } else {
        None
    }
}
