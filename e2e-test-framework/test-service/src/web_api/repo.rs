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
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use test_data_store::{
    test_repo_storage::{
        models::{LocalTestDefinition, TestDefinition},
        repo_clients::TestRepoConfig,
        TestRepoStorage, TestSourceScriptSet, TestSourceStorage, TestStorage,
    },
    TestDataStore,
};

use super::TestServiceWebApiError;

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "id": "my-test-repo",
    "path": "/data/test-repos/my-test-repo",
    "test_ids": ["test-1", "test-2", "test-3"]
}))]
pub struct TestRepoResponse {
    /// Repository identifier
    pub id: String,
    /// File system path of the repository
    pub path: String,
    /// List of test IDs available in this repository
    pub test_ids: Vec<String>,
}

#[allow(dead_code)]
impl TestRepoResponse {
    async fn new(test_repo: &TestRepoStorage) -> anyhow::Result<Self> {
        Ok(TestRepoResponse {
            id: test_repo.id.clone(),
            path: test_repo.path.to_string_lossy().to_string(),
            test_ids: test_repo.get_test_ids().await?,
        })
    }
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(tag = "type")]
pub enum TestPostBody {
    /// Add a local test definition
    Local {
        /// Whether to replace existing test if it exists
        #[serde(default)]
        replace: bool,
        /// Test definition to add
        test_definition: LocalTestDefinition,
    },
    /// Add a remote test by reference
    Remote {
        /// ID of the remote test to add
        test_id: String,
        /// Whether to replace existing test if it exists
        #[serde(default)]
        replace: bool,
    },
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "id": "test-123",
    "path": "/data/test-repos/my-repo/tests/test-123",
    "definition": {
        "sources": ["source-1", "source-2"],
        "queries": ["query-1", "query-2"]
    }
}))]
pub struct TestResponse {
    /// Test identifier
    pub id: String,
    /// File system path of the test
    pub path: String,
    /// Complete test definition
    pub definition: TestDefinition,
}

#[allow(dead_code)]
impl TestResponse {
    async fn new(test: &TestStorage) -> anyhow::Result<Self> {
        Ok(TestResponse {
            id: test.id.clone(),
            path: test.path.to_string_lossy().to_string(),
            definition: test.test_definition.clone(),
        })
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct TestSourcePostBody {
    /// Source identifier
    pub id: String,
    /// Whether to replace existing source if it exists
    #[serde(default)]
    pub replace: bool,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "id": "source-123",
    "path": "/data/test-repos/my-repo/tests/test-1/sources/source-123",
    "dataset": {
        "bootstrap_scripts": ["bootstrap.json"],
        "change_scripts": ["changes-001.json", "changes-002.json"]
    }
}))]
pub struct TestSourceResponse {
    /// Source identifier
    pub id: String,
    /// File system path of the source
    pub path: String,
    /// Dataset containing the source scripts
    pub dataset: TestSourceScriptSet,
}

#[allow(dead_code)]
impl TestSourceResponse {
    async fn new(test_source: &TestSourceStorage) -> anyhow::Result<Self> {
        Ok(TestSourceResponse {
            id: test_source.id.clone(),
            path: test_source.path.to_string_lossy().to_string(),
            dataset: test_source.get_script_files().await?,
        })
    }
}

pub fn get_test_repo_routes() -> Router {
    Router::new()
        .route(
            "/api/test_repos",
            get(get_test_repo_list_handler).post(post_test_repo_handler),
        )
        .route("/api/test_repos/:repo_id", get(get_test_repo_handler))
        .route(
            "/api/test_repos/:repo_id/tests",
            get(get_test_repo_test_list_handler).post(post_test_repo_test_handler),
        )
        .route(
            "/api/test_repos/:repo_id/tests/:test_id",
            get(get_test_repo_test_handler),
        )
        .route(
            "/api/test_repos/:repo_id/tests/:test_id/sources",
            get(get_test_repo_test_source_list_handler).post(post_test_repo_test_source_handler),
        )
        .route(
            "/api/test_repos/:repo_id/tests/:test_id/sources/:source_id",
            get(get_test_repo_test_source_handler),
        )
}

#[utoipa::path(
    get,
    path = "/api/test_repos",
    tag = "repos",
    responses(
        (status = 200, description = "List of repository IDs", body = Vec<String>),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_test_repo_list_handler(
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo_list");

    let repo_ids = test_data_store.get_test_repo_ids().await?;
    Ok(Json(repo_ids).into_response())
}

#[utoipa::path(
    get,
    path = "/api/test_repos/{repo_id}/tests",
    tag = "repos",
    params(
        ("repo_id" = String, Path, description = "Repository identifier")
    ),
    responses(
        (status = 200, description = "List of test IDs in the repository", body = Vec<String>),
        (status = 404, description = "Repository not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_test_repo_test_list_handler(
    Path(repo_id): Path<String>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo_test_list - repo_id:{repo_id}");

    let test_ids = test_data_store.get_test_repo_test_ids(&repo_id).await?;
    Ok(Json(test_ids).into_response())
}

#[utoipa::path(
    get,
    path = "/api/test_repos/{repo_id}/tests/{test_id}/sources",
    tag = "repos",
    params(
        ("repo_id" = String, Path, description = "Repository identifier"),
        ("test_id" = String, Path, description = "Test identifier")
    ),
    responses(
        (status = 200, description = "List of source IDs for the test", body = Vec<String>),
        (status = 404, description = "Repository or test not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_test_repo_test_source_list_handler(
    Path((repo_id, test_id)): Path<(String, String)>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!(
        "Processing call - get_test_repo_test_source_list - repo_id:{repo_id}, test_id:{test_id}"
    );

    let source_ids = test_data_store
        .get_test_storage(&repo_id, &test_id)
        .await?
        .get_test_source_ids()
        .await?;
    Ok(Json(source_ids).into_response())
}

#[utoipa::path(
    get,
    path = "/api/test_repos/{repo_id}",
    tag = "repos",
    params(
        ("repo_id" = String, Path, description = "Repository identifier")
    ),
    responses(
        (status = 200, description = "Repository information", body = TestRepoResponse),
        (status = 404, description = "Repository not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_test_repo_handler(
    Path(repo_id): Path<String>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo - repo_id:{repo_id}");

    let repo = test_data_store.get_test_repo_storage(&repo_id).await?;
    Ok(Json(TestRepoResponse::new(&repo).await?).into_response())
}

#[utoipa::path(
    get,
    path = "/api/test_repos/{repo_id}/tests/{test_id}",
    tag = "repos",
    params(
        ("repo_id" = String, Path, description = "Repository identifier"),
        ("test_id" = String, Path, description = "Test identifier")
    ),
    responses(
        (status = 200, description = "Test information", body = TestResponse),
        (status = 404, description = "Repository or test not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_test_repo_test_handler(
    Path((repo_id, test_id)): Path<(String, String)>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo_test - repo_id:{repo_id}, test_id:{test_id}");

    let test = test_data_store.get_test_storage(&repo_id, &test_id).await?;
    Ok(Json(TestResponse::new(&test).await?).into_response())
}

#[utoipa::path(
    get,
    path = "/api/test_repos/{repo_id}/tests/{test_id}/sources/{source_id}",
    tag = "repos",
    params(
        ("repo_id" = String, Path, description = "Repository identifier"),
        ("test_id" = String, Path, description = "Test identifier"),
        ("source_id" = String, Path, description = "Source identifier")
    ),
    responses(
        (status = 200, description = "Source information", body = TestSourceResponse),
        (status = 404, description = "Repository, test, or source not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn get_test_repo_test_source_handler(
    Path((repo_id, test_id, source_id)): Path<(String, String, String)>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!(
        "Processing call - get_test_repo_test_source - repo_id:{repo_id}, test_id:{test_id}, source_id:{source_id}"
    );

    let source = test_data_store
        .get_test_source_storage(&repo_id, &test_id, &source_id)
        .await?;
    Ok(Json(TestSourceResponse::new(&source).await?).into_response())
}

#[utoipa::path(
    post,
    path = "/api/test_repos",
    tag = "repos",
    request_body = test_data_store::test_repo_storage::repo_clients::TestRepoConfig,
    responses(
        (status = 200, description = "Repository created successfully", body = TestRepoResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn post_test_repo_handler(
    test_data_store: Extension<Arc<TestDataStore>>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_test_repo");

    let repo_config: TestRepoConfig = serde_json::from_value(body.0)?;

    let repo = test_data_store.add_test_repo(repo_config).await?;
    Ok(Json(TestRepoResponse::new(&repo).await?).into_response())
}

#[utoipa::path(
    post,
    path = "/api/test_repos/{repo_id}/tests",
    tag = "repos",
    params(
        ("repo_id" = String, Path, description = "Repository identifier")
    ),
    request_body = TestPostBody,
    responses(
        (status = 200, description = "Test created successfully", body = TestResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Repository not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn post_test_repo_test_handler(
    Path(repo_id): Path<String>,
    test_data_store: Extension<Arc<TestDataStore>>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_test_repo_test - repo_id:{repo_id}");

    let test_post_body: TestPostBody = serde_json::from_value(body.0)?;

    match test_post_body {
        TestPostBody::Local {
            test_definition,
            replace,
        } => {
            let test = test_data_store
                .add_local_test(&repo_id, test_definition, replace)
                .await?;
            Ok(Json(TestResponse::new(&test).await?).into_response())
        }
        TestPostBody::Remote { test_id, replace } => {
            let test = test_data_store
                .add_remote_test(&repo_id, &test_id, replace)
                .await?;
            Ok(Json(TestResponse::new(&test).await?).into_response())
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/test_repos/{repo_id}/tests/{test_id}/sources",
    tag = "repos",
    params(
        ("repo_id" = String, Path, description = "Repository identifier"),
        ("test_id" = String, Path, description = "Test identifier")
    ),
    request_body = TestSourcePostBody,
    responses(
        (status = 200, description = "Source created successfully", body = TestSourceResponse),
        (status = 400, description = "Invalid request body", body = ErrorResponse),
        (status = 404, description = "Repository or test not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
pub async fn post_test_repo_test_source_handler(
    Path((repo_id, test_id)): Path<(String, String)>,
    test_data_store: Extension<Arc<TestDataStore>>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!(
        "Processing call - post_test_repo_test_source - repo_id:{repo_id}, test_id:{test_id}"
    );

    let test_source_post_body: TestSourcePostBody = serde_json::from_value(body.0)?;

    let test = test_data_store.get_test_storage(&repo_id, &test_id).await?;
    let source = test
        .get_test_source(&test_source_post_body.id, test_source_post_body.replace)
        .await?;
    Ok(Json(TestSourceResponse::new(&source).await?).into_response())
}
