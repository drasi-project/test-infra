use std::sync::Arc;

use axum::{ extract::{Extension, Path}, response::IntoResponse, routing::get, Json, Router };
use serde::{Deserialize, Serialize};
use serde_json::Value;

use test_data_store::{test_repo_storage::{models::TestDefinition, repo_clients::TestRepoConfig, TestRepoStorage, TestSourceScriptSet, TestSourceStorage, TestStorage}, TestDataStore};

use super::TestServiceWebApiError;

#[derive(Debug, Serialize)]
pub struct TestRepoResponse {
    pub id: String,
    pub path: String,
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum TestPostBody {
    Local {
        #[serde(default)]
        replace: bool,
        test_definition: TestDefinition,
    },
    Remote {
        test_id: String,
        #[serde(default)]
        replace: bool,
    },
}

#[derive(Debug, Serialize)]
pub struct TestResponse {
    pub id: String,
    pub path: String,
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

#[derive(Debug, Deserialize)]
pub struct TestSourcePostBody {
    pub id: String,
    #[serde(default)]
    pub replace: bool,
}

#[derive(Debug, Serialize)]
pub struct TestSourceResponse {
    pub id: String,    
    pub path: String,
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
        .route("/", get(get_test_repo_list_handler).post(post_test_repo_handler))
        .route("/:repo_id", get(get_test_repo_handler))
        .route("/:repo_id/tests", get(get_test_repo_test_list_handler).post(post_test_repo_test_handler))
        .route("/:repo_id/tests/:test_id", get(get_test_repo_test_handler))
        .route("/:repo_id/tests/:test_id/sources", get(get_test_repo_test_source_list_handler).post(post_test_repo_test_source_handler))
        .route("/:repo_id/tests/:test_id/sources/:source_id", get(get_test_repo_test_source_handler))
}

pub async fn get_test_repo_list_handler(
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo_list");

    let repo_ids = test_data_store.get_test_repo_ids().await?;
    Ok(Json(repo_ids).into_response())
}

pub async fn get_test_repo_test_list_handler(
    Path(repo_id): Path<String>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo_test_list - repo_id:{}", repo_id);

    let test_ids = test_data_store.get_test_repo_test_ids(&repo_id).await?;
    Ok(Json(test_ids).into_response())
}

pub async fn get_test_repo_test_source_list_handler(
    Path((repo_id, test_id)): Path<(String, String)>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo_test_source_list - repo_id:{}, test_id:{}", repo_id, test_id);

    let source_ids = test_data_store.get_test_storage(&repo_id, &test_id).await?.get_test_source_ids().await?;
    Ok(Json(source_ids).into_response())
}

pub async fn get_test_repo_handler (
    Path(repo_id): Path<String>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo - repo_id:{}", repo_id);

    let repo = test_data_store.get_test_repo_storage(&repo_id).await?;
    Ok(Json(TestRepoResponse::new(&repo).await?).into_response())
}

pub async fn get_test_repo_test_handler (
    Path((repo_id, test_id)): Path<(String, String)>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo_test - repo_id:{}, test_id:{}", repo_id, test_id);

    let test = test_data_store.get_test_storage(&repo_id, &test_id).await?;
    Ok(Json(TestResponse::new(&test).await?).into_response())
}

pub async fn get_test_repo_test_source_handler (
    Path((repo_id, test_id, source_id)): Path<(String, String, String)>,
    test_data_store: Extension<Arc<TestDataStore>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_test_repo_test_source - repo_id:{}, test_id:{}, source_id:{}", repo_id, test_id, source_id);

    let source = test_data_store.get_test_source_storage(&repo_id, &test_id, &source_id).await?;
    Ok(Json(TestSourceResponse::new(&source).await?).into_response())
}

pub async fn post_test_repo_handler (
    test_data_store: Extension<Arc<TestDataStore>>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_test_repo");

    let repo_config: TestRepoConfig = serde_json::from_value(body.0)?;

    let repo = test_data_store.add_test_repo(repo_config).await?;
    Ok(Json(TestRepoResponse::new(&repo).await?).into_response())
}

pub async fn post_test_repo_test_handler (
    Path(repo_id): Path<String>,
    test_data_store: Extension<Arc<TestDataStore>>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_test_repo_test - repo_id:{}", repo_id);

    let test_post_body: TestPostBody = serde_json::from_value(body.0)?;

    match test_post_body {
        TestPostBody::Local { test_definition, replace } => {
            let test = test_data_store.add_local_test(&repo_id, test_definition, replace).await?;
            Ok(Json(TestResponse::new(&test).await?).into_response())
        },
        TestPostBody::Remote { test_id, replace } => {
            let test = test_data_store.add_remote_test(&repo_id, &test_id, replace).await?;
            Ok(Json(TestResponse::new(&test).await?).into_response())
        }
    }
}

pub async fn post_test_repo_test_source_handler (
    Path((repo_id, test_id)): Path<(String, String)>,
    test_data_store: Extension<Arc<TestDataStore>>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_test_repo_test_source - repo_id:{}, test_id:{}", repo_id, test_id);

    let test_source_post_body: TestSourcePostBody = serde_json::from_value(body.0)?;

    let test = test_data_store.get_test_storage(&repo_id, &test_id ).await?;
    let source = test.get_test_source(&test_source_post_body.id, test_source_post_body.replace).await?;
    Ok(Json(TestSourceResponse::new(&source).await?).into_response())
}