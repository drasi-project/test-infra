use axum::{ extract::{Extension, Path}, response::IntoResponse, Json };
use serde::Serialize;
use serde_json::Value;

use test_data_store::{test_repo_storage::{repo_clients::RemoteTestRepoConfig, test_metadata::TestDefinition, TestRepoStorage, TestSourceDataset, TestSourceStorage, TestStorage}, SharedTestDataStore};

use super::TestServiceError;

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

#[derive(Debug, Serialize)]
pub struct TestSourceResponse {
    pub id: String,    
    pub path: String,
    pub dataset: TestSourceDataset,
}

#[allow(dead_code)]
impl TestSourceResponse {
    async fn new(test_source: &TestSourceStorage) -> anyhow::Result<Self> {
        Ok(TestSourceResponse {
            id: test_source.id.clone(),
            path: test_source.path.to_string_lossy().to_string(),
            dataset: test_source.get_dataset().await?,
        })
    }
}

// #[derive(Debug, Serialize)]
// pub struct SourceDatasetResponse {
//     pub id: String,
//     pub content: TestSourceContent,

// }

// impl From<&DataSet> for SourceDatasetResponse {
//     fn from(dataset: &DataSet) -> Self {
//         SourceDatasetResponse {
//             id: dataset.id.clone(),
//             content: dataset.content.clone(),
//         }
//     }
// }

pub async fn get_test_repo_list_handler(
    test_data_store: Extension<SharedTestDataStore>,
) -> anyhow::Result<impl IntoResponse, TestServiceError> {
    log::info!("Processing call - get_test_repo_list");

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let repo_ids = test_data_store.test_repo_store.lock().await.get_test_repo_ids().await?;
    Ok(Json(repo_ids).into_response())
}

pub async fn get_test_repo_test_list_handler(
    Path(repo_id): Path<String>,
    test_data_store: Extension<SharedTestDataStore>,
) -> anyhow::Result<impl IntoResponse, TestServiceError> {
    log::info!("Processing call - get_test_repo_test_list");

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let repo = test_data_store.test_repo_store.lock().await.get_test_repo(&repo_id).await?;
    let test_ids = repo.get_test_ids().await?;
    Ok(Json(test_ids).into_response())
}

pub async fn get_test_repo_test_source_list_handler(
    Path((repo_id, test_id)): Path<(String, String)>,
    test_data_store: Extension<SharedTestDataStore>,
) -> anyhow::Result<impl IntoResponse, TestServiceError> {
    log::info!("Processing call - get_test_repo_test_source_list");

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let repo = test_data_store.test_repo_store.lock().await.get_test_repo(&repo_id).await?;
    let test = repo.get_test(&test_id, false).await?;
    let source_ids = test.get_test_source_ids().await?;
    Ok(Json(source_ids).into_response())
}

pub async fn get_test_repo_handler (
    Path(repo_id): Path<String>,
    test_data_store: Extension<SharedTestDataStore>,
) -> anyhow::Result<impl IntoResponse, TestServiceError> {
    log::info!("Processing call - get_test_repo: {}", repo_id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let repo = test_data_store.test_repo_store.lock().await.get_test_repo(&repo_id).await?;
    Ok(Json(TestRepoResponse::new(&repo).await?).into_response())
}

pub async fn get_test_repo_test_handler (
    Path((repo_id, test_id)): Path<(String, String)>,
    test_data_store: Extension<SharedTestDataStore>,
) -> anyhow::Result<impl IntoResponse, TestServiceError> {
    log::info!("Processing call - get_test_repo_test: {}", repo_id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let repo = test_data_store.test_repo_store.lock().await.get_test_repo(&repo_id).await?;
    let test = repo.get_test(&test_id, false).await?;
    Ok(Json(TestResponse::new(&test).await?).into_response())
}

pub async fn get_test_repo_test_source_handler (
    Path((repo_id, test_id, source_id)): Path<(String, String, String)>,
    test_data_store: Extension<SharedTestDataStore>,
) -> anyhow::Result<impl IntoResponse, TestServiceError> {
    log::info!("Processing call - get_test_repo_test_source: {}", repo_id);

    // If the TestRunner is an Error state, return an error and a description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let repo = test_data_store.test_repo_store.lock().await.get_test_repo(&repo_id).await?;
    let test = repo.get_test(&test_id, false).await?;
    let source = test.get_test_source(&source_id, false).await?;
    Ok(Json(TestSourceResponse::new(&source).await?).into_response())
}

pub async fn post_test_repo_handler (
    test_data_store: Extension<SharedTestDataStore>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestServiceError> {
    log::info!("Processing call - post_test_repo");

    // let mut test_runner = state.write().await;

    // If the service is an Error state, return an error and the description of the error.
    // if let TestRunnerStatus::Error(msg) = &test_runner.get_status() {
    //     return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
    // }

    let repo_config: RemoteTestRepoConfig = serde_json::from_value(body.0)?;

    // Add the TestRepoConfig to the test_runner.test_repo_configs HashMap.
    let repo = test_data_store.test_repo_store.lock().await.add_test_repo(repo_config, false).await?;
    Ok(Json(TestRepoResponse::new(&repo).await?).into_response())
}