use std::{fmt::Debug, collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use serde::Serialize;

pub mod azure_storage_blob_test_repo;
pub mod dataset;
pub mod test_repo_cache;

#[derive(Debug, thiserror::Error)]
pub enum TestRepoError {
    #[error("Invalid TestRepo ID: {0}")]
    _InvalidTestRepoId(String),
    #[error("Invalid TestRepo Type: {0}")]
    _InvalidType(String),
}

#[derive(Clone, Debug, Serialize)]
pub struct TestSourceContent {
    pub bootstrap_script_files: Option<HashMap<String, Vec<PathBuf>>>,
    pub change_log_script_files: Option<Vec<PathBuf>>,
}

#[async_trait]
pub trait TestRepo : Send + Sync + Debug {
    async fn download_test_source_content(&self, test_id: String, source_id: String, dataset_cache_path: PathBuf) -> anyhow::Result<TestSourceContent>;
}

#[async_trait]
impl TestRepo for Box<dyn TestRepo> {
    async fn download_test_source_content(&self, test_id: String, source_id: String, dataset_cache_path: PathBuf) -> anyhow::Result<TestSourceContent> {
        (**self).download_test_source_content(test_id, source_id, dataset_cache_path).await
    }
}