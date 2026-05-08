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

use std::sync::Arc;

use axum::{
    extract::{Extension, Path},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use test_data_store::test_run_storage::TestRunId;
use test_run_host::{TestRunConfig, TestRunStatus};

use super::TestServiceWebApiError;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct TestRunCreatedResponse {
    pub id: String,
}

#[derive(Serialize, ToSchema)]
pub struct TestRunInfo {
    pub id: String,
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: String,
    #[serde(serialize_with = "serialize_status")]
    #[schema(value_type = String)]
    pub status: TestRunStatus,
}

fn serialize_status<S>(status: &TestRunStatus, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let status_str = match status {
        TestRunStatus::Initialized => "Initialized",
        TestRunStatus::Running => "Running",
        TestRunStatus::Stopped => "Stopped",
        TestRunStatus::Error(msg) => return serializer.serialize_str(&format!("Error: {msg}")),
    };
    serializer.serialize_str(status_str)
}

pub fn get_test_runs_routes() -> Router {
    Router::new()
        .route("/api/test_runs", post(create_test_run).get(list_test_runs))
        .route(
            "/api/test_runs/:run_id",
            get(get_test_run).delete(delete_test_run),
        )
        .route("/api/test_runs/:run_id/start", post(start_test_run))
        .route("/api/test_runs/:run_id/stop", post(stop_test_run))
        // Nested routes for components
        .route(
            "/api/test_runs/:run_id/sources",
            get(list_test_run_sources).post(create_test_run_source),
        )
        .route(
            "/api/test_runs/:run_id/sources/:source_id",
            get(get_test_run_source).delete(delete_test_run_source),
        )
        .route(
            "/api/test_runs/:run_id/sources/:source_id/start",
            post(start_test_run_source),
        )
        .route(
            "/api/test_runs/:run_id/sources/:source_id/stop",
            post(stop_test_run_source),
        )
        .route(
            "/api/test_runs/:run_id/sources/:source_id/pause",
            post(pause_test_run_source),
        )
        .route(
            "/api/test_runs/:run_id/sources/:source_id/reset",
            post(reset_test_run_source),
        )
        .route(
            "/api/test_runs/:run_id/queries",
            get(list_test_run_queries).post(create_test_run_query),
        )
        .route(
            "/api/test_runs/:run_id/queries/:query_id",
            get(get_test_run_query).delete(delete_test_run_query),
        )
        .route(
            "/api/test_runs/:run_id/queries/:query_id/start",
            post(start_test_run_query),
        )
        .route(
            "/api/test_runs/:run_id/queries/:query_id/stop",
            post(stop_test_run_query),
        )
        .route(
            "/api/test_runs/:run_id/queries/:query_id/pause",
            post(pause_test_run_query),
        )
        .route(
            "/api/test_runs/:run_id/queries/:query_id/reset",
            post(reset_test_run_query),
        )
        .route(
            "/api/test_runs/:run_id/reactions",
            get(list_test_run_reactions).post(create_test_run_reaction),
        )
        .route(
            "/api/test_runs/:run_id/reactions/:reaction_id",
            get(get_test_run_reaction).delete(delete_test_run_reaction),
        )
        .route(
            "/api/test_runs/:run_id/reactions/:reaction_id/start",
            post(start_test_run_reaction),
        )
        .route(
            "/api/test_runs/:run_id/reactions/:reaction_id/stop",
            post(stop_test_run_reaction),
        )
        .route(
            "/api/test_runs/:run_id/reactions/:reaction_id/pause",
            post(pause_test_run_reaction),
        )
        .route(
            "/api/test_runs/:run_id/reactions/:reaction_id/reset",
            post(reset_test_run_reaction),
        )
        .route(
            "/api/test_runs/:run_id/drasi_lib_instances",
            get(list_test_run_drasi_lib_instances).post(create_test_run_drasi_lib_instance),
        )
        .route(
            "/api/test_runs/:run_id/drasi_lib_instances/:instance_id",
            get(get_test_run_drasi_lib_instance).delete(delete_test_run_drasi_lib_instance),
        )
}

/// Create a new test run
#[utoipa::path(
    post,
    path = "/api/test_runs",
    request_body = TestRunConfig,
    responses(
        (status = 201, description = "Test run created successfully", body = TestRunCreatedResponse),
        (status = 400, description = "Invalid configuration"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
pub async fn create_test_run(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Json(config): Json<TestRunConfig>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    match test_run_host.add_test_run(config).await {
        Ok(id) => Ok((
            StatusCode::CREATED,
            Json(TestRunCreatedResponse { id: id.to_string() }),
        )),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

/// List all test runs
#[utoipa::path(
    get,
    path = "/api/test_runs",
    responses(
        (status = 200, description = "List of test runs", body = Vec<TestRunInfo>),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
pub async fn list_test_runs(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let run_ids = test_run_host.get_test_run_ids().await?;
    let mut runs = Vec::new();

    log::info!("Found {} test run IDs", run_ids.len());

    for id_str in run_ids {
        log::debug!("Processing test run ID: {id_str}");
        if let Ok(run_id) = TestRunId::try_from(id_str.as_str()) {
            if let Ok(status) = test_run_host.get_test_run_status(&run_id).await {
                runs.push(TestRunInfo {
                    id: id_str,
                    test_id: run_id.test_id.clone(),
                    test_repo_id: run_id.test_repo_id.clone(),
                    test_run_id: run_id.test_run_id.clone(),
                    status,
                });
            }
        }
    }

    log::info!("Returning {} test runs", runs.len());
    Ok(Json(runs))
}

/// Get a specific test run
#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    responses(
        (status = 200, description = "Test run details", body = TestRunInfo),
        (status = 404, description = "Test run not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
pub async fn get_test_run(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    match test_run_host.get_test_run_status(&test_run_id).await {
        Ok(status) => Ok(Json(TestRunInfo {
            id: run_id,
            test_id: test_run_id.test_id.clone(),
            test_repo_id: test_run_id.test_repo_id.clone(),
            test_run_id: test_run_id.test_run_id.clone(),
            status,
        })),
        Err(_) => Err(TestServiceWebApiError::NotFound(
            "TestRun".to_string(),
            run_id,
        )),
    }
}

/// Delete a test run
#[utoipa::path(
    delete,
    path = "/api/test_runs/{run_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    responses(
        (status = 204, description = "Test run deleted successfully"),
        (status = 404, description = "Test run not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
pub async fn delete_test_run(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    test_run_host.delete_test_run(&test_run_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Start a test run
#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/start",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    responses(
        (status = 200, description = "Test run started successfully"),
        (status = 404, description = "Test run not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
pub async fn start_test_run(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    test_run_host.start_test_run(&test_run_id).await?;
    Ok(StatusCode::OK)
}

/// Stop a test run
#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/stop",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    responses(
        (status = 200, description = "Test run stopped successfully"),
        (status = 404, description = "Test run not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
pub async fn stop_test_run(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    test_run_host.stop_test_run(&test_run_id).await?;
    Ok(StatusCode::OK)
}

// Source-related endpoints
#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}/sources",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    responses(
        (status = 200, description = "List of source IDs within the test run", body = Vec<String>),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn list_test_run_sources(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let _test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    // Get all source IDs and filter by test run
    let all_ids = test_run_host.get_test_source_ids().await?;
    let filtered: Vec<String> = all_ids
        .into_iter()
        .filter(|id| id.starts_with(&run_id))
        .collect();

    Ok(Json(filtered))
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/sources",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    request_body = test_run_host::sources::TestRunSourceConfig,
    responses(
        (status = 201, description = "Source created successfully"),
        (status = 400, description = "Invalid configuration"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn create_test_run_source(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
    Json(mut config): Json<test_run_host::sources::TestRunSourceConfig>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    // Set the test run IDs
    config.test_id = Some(test_run_id.test_id.clone());
    config.test_repo_id = Some(test_run_id.test_repo_id.clone());
    config.test_run_id = Some(test_run_id.test_run_id.clone());

    match test_run_host.add_test_source(&test_run_id, config).await {
        Ok(id) => Ok((
            StatusCode::CREATED,
            Json(serde_json::json!({ "id": id.to_string() })),
        )),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}/sources/{source_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("source_id" = String, Path, description = "Source ID")
    ),
    responses(
        (status = 200, description = "Source details"),
        (status = 404, description = "Source not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn get_test_run_source(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, source_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{source_id}");

    match test_run_host.get_test_source_state(&full_id).await {
        Ok(state) => Ok(Json(state)),
        Err(_) => Err(TestServiceWebApiError::NotFound(
            "Source".to_string(),
            source_id,
        )),
    }
}

#[utoipa::path(
    delete,
    path = "/api/test_runs/{run_id}/sources/{source_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("source_id" = String, Path, description = "Source ID")
    ),
    responses(
        (status = 204, description = "Source deleted successfully"),
        (status = 404, description = "Source not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn delete_test_run_source(
    Path((_run_id, source_id)): Path<(String, String)>,
) -> Result<StatusCode, TestServiceWebApiError> {
    // TODO: Implement source deletion
    Err(TestServiceWebApiError::NotReady(format!(
        "Source deletion not implemented: {source_id}"
    )))
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/sources/{source_id}/start",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("source_id" = String, Path, description = "Source ID")
    ),
    responses(
        (status = 200, description = "Source started successfully"),
        (status = 404, description = "Source not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn start_test_run_source(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, source_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{source_id}");
    test_run_host.test_source_start(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/sources/{source_id}/stop",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("source_id" = String, Path, description = "Source ID")
    ),
    responses(
        (status = 200, description = "Source stopped successfully"),
        (status = 404, description = "Source not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn stop_test_run_source(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, source_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{source_id}");
    test_run_host.test_source_stop(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/sources/{source_id}/pause",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("source_id" = String, Path, description = "Source ID")
    ),
    responses(
        (status = 200, description = "Source paused successfully"),
        (status = 404, description = "Source not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn pause_test_run_source(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, source_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{source_id}");
    test_run_host.test_source_pause(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/sources/{source_id}/reset",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("source_id" = String, Path, description = "Source ID")
    ),
    responses(
        (status = 200, description = "Source reset successfully"),
        (status = 404, description = "Source not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn reset_test_run_source(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, source_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{source_id}");
    test_run_host.test_source_reset(&full_id).await?;
    Ok(StatusCode::OK)
}

// Query-related endpoints
#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}/queries",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    responses(
        (status = 200, description = "List of query IDs within the test run", body = Vec<String>),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn list_test_run_queries(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let _test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    // Get all query IDs and filter by test run
    let all_ids = test_run_host.get_test_query_ids().await?;
    let filtered: Vec<String> = all_ids
        .into_iter()
        .filter(|id| id.starts_with(&run_id))
        .collect();

    Ok(Json(filtered))
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/queries",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    request_body = test_run_host::queries::TestRunQueryConfig,
    responses(
        (status = 201, description = "Query created successfully"),
        (status = 400, description = "Invalid configuration"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn create_test_run_query(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
    Json(mut config): Json<test_run_host::queries::TestRunQueryConfig>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    // Set the test run IDs
    config.test_id = Some(test_run_id.test_id.clone());
    config.test_repo_id = Some(test_run_id.test_repo_id.clone());
    config.test_run_id = Some(test_run_id.test_run_id.clone());

    match test_run_host.add_test_query(&test_run_id, config).await {
        Ok(id) => Ok((
            StatusCode::CREATED,
            Json(serde_json::json!({ "id": id.to_string() })),
        )),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}/queries/{query_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Query details"),
        (status = 404, description = "Query not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn get_test_run_query(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, query_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{query_id}");

    match test_run_host.get_test_query_state(&full_id).await {
        Ok(state) => Ok(Json(state)),
        Err(_) => Err(TestServiceWebApiError::NotFound(
            "Query".to_string(),
            query_id,
        )),
    }
}

#[utoipa::path(
    delete,
    path = "/api/test_runs/{run_id}/queries/{query_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 204, description = "Query deleted successfully"),
        (status = 404, description = "Query not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn delete_test_run_query(
    Path((_run_id, query_id)): Path<(String, String)>,
) -> Result<StatusCode, TestServiceWebApiError> {
    // TODO: Implement query deletion
    Err(TestServiceWebApiError::NotReady(format!(
        "Query deletion not implemented: {query_id}"
    )))
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/queries/{query_id}/start",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Query started successfully"),
        (status = 404, description = "Query not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn start_test_run_query(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, query_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{query_id}");
    test_run_host.test_query_start(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/queries/{query_id}/stop",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Query stopped successfully"),
        (status = 404, description = "Query not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn stop_test_run_query(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, query_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{query_id}");
    test_run_host.test_query_stop(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/queries/{query_id}/pause",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Query paused successfully"),
        (status = 404, description = "Query not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn pause_test_run_query(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, query_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{query_id}");
    test_run_host.test_query_pause(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/queries/{query_id}/reset",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("query_id" = String, Path, description = "Query ID")
    ),
    responses(
        (status = 200, description = "Query reset successfully"),
        (status = 404, description = "Query not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn reset_test_run_query(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, query_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{query_id}");
    test_run_host.test_query_reset(&full_id).await?;
    Ok(StatusCode::OK)
}

// Reaction-related endpoints
#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}/reactions",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    responses(
        (status = 200, description = "List of reaction IDs within the test run", body = Vec<String>),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn list_test_run_reactions(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let _test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    // Get all reaction IDs and filter by test run
    let all_ids = test_run_host.get_test_reaction_ids().await?;
    let filtered: Vec<String> = all_ids
        .into_iter()
        .filter(|id| id.starts_with(&run_id))
        .collect();

    Ok(Json(filtered))
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/reactions",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    request_body = test_run_host::reactions::TestRunReactionConfig,
    responses(
        (status = 201, description = "Reaction created successfully"),
        (status = 400, description = "Invalid configuration"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn create_test_run_reaction(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
    Json(mut config): Json<test_run_host::reactions::TestRunReactionConfig>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    // Set the test run IDs
    config.test_id = Some(test_run_id.test_id.clone());
    config.test_repo_id = Some(test_run_id.test_repo_id.clone());
    config.test_run_id = Some(test_run_id.test_run_id.clone());

    match test_run_host.add_test_reaction(&test_run_id, config).await {
        Ok(id) => Ok((
            StatusCode::CREATED,
            Json(serde_json::json!({ "id": id.to_string() })),
        )),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}/reactions/{reaction_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("reaction_id" = String, Path, description = "Reaction ID")
    ),
    responses(
        (status = 200, description = "Reaction details"),
        (status = 404, description = "Reaction not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn get_test_run_reaction(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, reaction_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{reaction_id}");

    match test_run_host.get_test_reaction_state(&full_id).await {
        Ok(state) => Ok(Json(state)),
        Err(_) => Err(TestServiceWebApiError::NotFound(
            "Reaction".to_string(),
            reaction_id,
        )),
    }
}

#[utoipa::path(
    delete,
    path = "/api/test_runs/{run_id}/reactions/{reaction_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("reaction_id" = String, Path, description = "Reaction ID")
    ),
    responses(
        (status = 204, description = "Reaction deleted successfully"),
        (status = 404, description = "Reaction not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn delete_test_run_reaction(
    Path((_run_id, reaction_id)): Path<(String, String)>,
) -> Result<StatusCode, TestServiceWebApiError> {
    // TODO: Implement reaction deletion
    Err(TestServiceWebApiError::NotReady(format!(
        "Reaction deletion not implemented: {reaction_id}"
    )))
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/reactions/{reaction_id}/start",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("reaction_id" = String, Path, description = "Reaction ID")
    ),
    responses(
        (status = 200, description = "Reaction started successfully"),
        (status = 404, description = "Reaction not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn start_test_run_reaction(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, reaction_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{reaction_id}");
    test_run_host.test_reaction_start(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/reactions/{reaction_id}/stop",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("reaction_id" = String, Path, description = "Reaction ID")
    ),
    responses(
        (status = 200, description = "Reaction stopped successfully"),
        (status = 404, description = "Reaction not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn stop_test_run_reaction(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, reaction_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{reaction_id}");
    test_run_host.test_reaction_stop(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/reactions/{reaction_id}/pause",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("reaction_id" = String, Path, description = "Reaction ID")
    ),
    responses(
        (status = 200, description = "Reaction paused successfully"),
        (status = 404, description = "Reaction not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn pause_test_run_reaction(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, reaction_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{reaction_id}");
    test_run_host.test_reaction_pause(&full_id).await?;
    Ok(StatusCode::OK)
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/reactions/{reaction_id}/reset",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("reaction_id" = String, Path, description = "Reaction ID")
    ),
    responses(
        (status = 200, description = "Reaction reset successfully"),
        (status = 404, description = "Reaction not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn reset_test_run_reaction(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, reaction_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{reaction_id}");
    test_run_host.test_reaction_reset(&full_id).await?;
    Ok(StatusCode::OK)
}

// drasi-lib instance-related endpoints
#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}/drasi_lib_instances",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    responses(
        (status = 200, description = "List of drasi-lib instance IDs within the test run", body = Vec<String>),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn list_test_run_drasi_lib_instances(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let _test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    // Get all instance IDs and filter by test run
    let all_ids = test_run_host.get_test_drasi_lib_instance_ids().await?;
    let filtered: Vec<String> = all_ids
        .into_iter()
        .filter(|id| id.starts_with(&run_id))
        .collect();

    Ok(Json(filtered))
}

#[utoipa::path(
    post,
    path = "/api/test_runs/{run_id}/drasi_lib_instances",
    params(
        ("run_id" = String, Path, description = "Test run ID")
    ),
    request_body = test_run_host::drasi_lib_instances::TestRunDrasiLibInstanceConfig,
    responses(
        (status = 201, description = "drasi-lib instance created successfully"),
        (status = 400, description = "Invalid configuration"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn create_test_run_drasi_lib_instance(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path(run_id): Path<String>,
    Json(mut config): Json<test_run_host::drasi_lib_instances::TestRunDrasiLibInstanceConfig>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let test_run_id = TestRunId::try_from(run_id.as_str())
        .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    // Set the test run IDs
    config.test_id = Some(test_run_id.test_id.clone());
    config.test_repo_id = Some(test_run_id.test_repo_id.clone());
    config.test_run_id = Some(test_run_id.test_run_id.clone());

    match test_run_host
        .add_test_drasi_lib_instance(&test_run_id, config)
        .await
    {
        Ok(id) => Ok((
            StatusCode::CREATED,
            Json(serde_json::json!({ "id": id.to_string() })),
        )),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/test_runs/{run_id}/drasi_lib_instances/{instance_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("instance_id" = String, Path, description = "drasi-lib instance ID")
    ),
    responses(
        (status = 200, description = "drasi-lib instance details"),
        (status = 404, description = "drasi-lib instance not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn get_test_run_drasi_lib_instance(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, instance_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{instance_id}");
    let instance_id =
        test_data_store::test_run_storage::TestRunDrasiLibInstanceId::try_from(full_id.as_str())
            .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    match test_run_host
        .get_test_drasi_lib_instance(&instance_id)
        .await?
    {
        Some(state) => Ok(Json(state)),
        None => Err(TestServiceWebApiError::NotFound(
            "DrasiLibInstance".to_string(),
            full_id,
        )),
    }
}

#[utoipa::path(
    delete,
    path = "/api/test_runs/{run_id}/drasi_lib_instances/{instance_id}",
    params(
        ("run_id" = String, Path, description = "Test run ID"),
        ("instance_id" = String, Path, description = "drasi-lib instance ID")
    ),
    responses(
        (status = 204, description = "drasi-lib instance deleted successfully"),
        (status = 404, description = "drasi-lib instance not found"),
        (status = 500, description = "Internal instance error")
    ),
    tag = "test-runs"
)]
async fn delete_test_run_drasi_lib_instance(
    Extension(test_run_host): Extension<Arc<test_run_host::TestRunHost>>,
    Path((run_id, instance_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, TestServiceWebApiError> {
    let full_id = format!("{run_id}.{instance_id}");
    let instance_id =
        test_data_store::test_run_storage::TestRunDrasiLibInstanceId::try_from(full_id.as_str())
            .map_err(|e| TestServiceWebApiError::AnyhowError(anyhow::anyhow!(e)))?;

    test_run_host
        .remove_test_drasi_lib_instance(&instance_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
