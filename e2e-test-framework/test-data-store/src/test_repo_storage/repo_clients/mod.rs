use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize, Serializer};

use azure_storage_blob_test_repo_client::AzureStorageBlobTestRepoClient;

use super::{test_metadata::TestDefinition, TestSourceDataset};

pub mod azure_storage_blob_test_repo_client;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RemoteTestRepoConfig {
    AzureStorageBlob {
        #[serde(flatten)]
        common_config: CommonTestRepoConfig,
        #[serde(flatten)]
        unique_config: AzureStorageBlobTestRepoConfig,
    },
}

impl RemoteTestRepoConfig {
    pub fn get_id(&self) -> String {
        match self {
            RemoteTestRepoConfig::AzureStorageBlob { common_config, .. } => common_config.id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonTestRepoConfig {
    #[serde(default = "is_false")]
    pub force_cache_refresh: bool,
    pub id: String,
}
fn is_false() -> bool { false }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AzureStorageBlobTestRepoConfig {
    pub account_name: String,
    #[serde(serialize_with = "mask_secret")]
    pub access_key: String,
    pub container: String,
    pub root_path: String,
}
pub fn mask_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("******")
}

#[async_trait]
pub trait RemoteTestRepoClient : Send + Sync {
    async fn get_test_definition(&self, test_id: String, test_store_path: PathBuf) -> anyhow::Result<PathBuf>;
    // async fn get_test_source(&self, test_id: String, source_id: String, dataset_cache_path: PathBuf) -> anyhow::Result<TestSourceDataset>;
    async fn get_test_source_content_from_def(&self, test_def: &TestDefinition, source_id: String, bootstrap_data_store_path: PathBuf, source_change_store_path: PathBuf) -> anyhow::Result<TestSourceDataset>;
}

#[async_trait]
impl RemoteTestRepoClient for Box<dyn RemoteTestRepoClient + Send + Sync> {
    async fn get_test_definition(&self, test_id: String, test_store_path: PathBuf) -> anyhow::Result<PathBuf> {
        (**self).get_test_definition(test_id, test_store_path).await
    }

    // async fn get_test_source(&self, test_id: String, source_id: String, dataset_cache_path: PathBuf) -> anyhow::Result<TestSourceDataset> {
    //     (**self).get_test_source(test_id, source_id, dataset_cache_path).await
    // }

    async fn get_test_source_content_from_def(&self, test_def: &TestDefinition, source_id: String, bootstrap_path: PathBuf, change_path: PathBuf) -> anyhow::Result<TestSourceDataset> {
        (**self).get_test_source_content_from_def(test_def, source_id, bootstrap_path, change_path ).await
    }

}

pub async fn create_test_repo_client(config: RemoteTestRepoConfig) -> anyhow::Result<Box<dyn RemoteTestRepoClient + Send + Sync>> {
    match config {
        RemoteTestRepoConfig::AzureStorageBlob{common_config, unique_config} 
            => AzureStorageBlobTestRepoClient::new(common_config, unique_config).await
    }
}