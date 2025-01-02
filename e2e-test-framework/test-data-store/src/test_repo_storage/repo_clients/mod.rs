use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize, Serializer};

use azure_storage_blob_test_repo_client::AzureStorageBlobTestRepoClient;

use super::models::TestSourceDefinition;

pub mod azure_storage_blob_test_repo_client;
pub mod local_storage_test_repo_client;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum TestRepoConfig {
    AzureStorageBlob {
        #[serde(flatten)]
        common_config: CommonTestRepoConfig,
        #[serde(flatten)]
        unique_config: AzureStorageBlobTestRepoConfig,
    },
    LocalStorage {
        #[serde(flatten)]
        common_config: CommonTestRepoConfig,
        #[serde(flatten)]
        unique_config: LocalStorageTestRepoConfig,
    },
}

impl TestRepoConfig {
    pub fn get_id(&self) -> String {
        match self {
            TestRepoConfig::AzureStorageBlob { common_config, .. } => common_config.id.clone(),
            TestRepoConfig::LocalStorage { common_config, .. } => common_config.id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonTestRepoConfig {
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AzureStorageBlobTestRepoConfig {
    pub account_name: String,
    #[serde(serialize_with = "mask_secret")]
    pub access_key: String,
    pub container: String,
    #[serde(default = "is_false")]
    pub force_cache_refresh: bool,
    pub root_path: String,
}
fn is_false() -> bool { false }
fn mask_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("******")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalStorageTestRepoConfig {
    pub source_path: Option<String>,
}

#[async_trait]
pub trait RemoteTestRepoClient : Send + Sync {
    async fn copy_test_definition(&self, test_id: String, test_def_path: PathBuf) -> anyhow::Result<()>;
    async fn copy_test_source_content(&self, test_data_folder: String, test_source_def: &TestSourceDefinition, test_source_data_path: PathBuf) -> anyhow::Result<()>;
}

#[async_trait]
impl RemoteTestRepoClient for Box<dyn RemoteTestRepoClient + Send + Sync> {
    async fn copy_test_definition(&self, test_id: String, test_def_path: PathBuf) -> anyhow::Result<()> {
        (**self).copy_test_definition(test_id, test_def_path).await
    }

    async fn copy_test_source_content(&self, test_data_folder: String, test_source_def: &TestSourceDefinition, test_source_data_path: PathBuf) -> anyhow::Result<()> {
        (**self).copy_test_source_content(test_data_folder, test_source_def, test_source_data_path ).await
    }
}

pub async fn create_test_repo_client(config: TestRepoConfig) -> anyhow::Result<Box<dyn RemoteTestRepoClient + Send + Sync>> {
    match config {
        TestRepoConfig::AzureStorageBlob{common_config, unique_config} 
            => AzureStorageBlobTestRepoClient::new(common_config, unique_config).await,
        TestRepoConfig::LocalStorage{common_config, unique_config}
            => local_storage_test_repo_client::LocalStorageTestRepoClient::new(common_config, unique_config).await,
    }
}