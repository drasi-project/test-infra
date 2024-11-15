use std::path::PathBuf;

use async_trait::async_trait;
use azure_storage_blob_test_repo_client::AzureStorageBlobTestRepoClient;

use crate::{config::TestRepoConfig, TestSourceDataset};

pub mod azure_storage_blob_test_repo_client;

#[async_trait]
pub trait TestRepoClient : Send + Sync {
    async fn download_test_source_dataset(&self, test_id: String, source_id: String, dataset_cache_path: PathBuf) -> anyhow::Result<TestSourceDataset>;
}

#[async_trait]
impl TestRepoClient for Box<dyn TestRepoClient + Send + Sync> {
    async fn download_test_source_dataset(&self, test_id: String, source_id: String, dataset_cache_path: PathBuf) -> anyhow::Result<TestSourceDataset> {
        (**self).download_test_source_dataset(test_id, source_id, dataset_cache_path).await
    }
}

pub async fn get_test_repo_client(config: TestRepoConfig, data_cache_path: PathBuf ) -> anyhow::Result<Box<dyn TestRepoClient + Send + Sync>> {
    match config {
        TestRepoConfig::AzureStorageBlob{common_config, unique_config} 
            => AzureStorageBlobTestRepoClient::new(common_config, unique_config, data_cache_path).await
    }
}