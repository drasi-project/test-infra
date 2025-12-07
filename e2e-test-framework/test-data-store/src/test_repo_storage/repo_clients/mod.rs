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

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize, Serializer};

use azure_storage_blob_test_repo_client::AzureStorageBlobTestRepoClient;
use github_test_repo_client::GithubTestRepoClient;

use super::models::{LocalTestDefinition, TestSourceDefinition};

pub mod azure_storage_blob_test_repo_client;
pub mod github_test_repo_client;
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
    GitHub {
        #[serde(flatten)]
        common_config: CommonTestRepoConfig,
        #[serde(flatten)]
        unique_config: GithubTestRepoConfig,
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
            TestRepoConfig::GitHub { common_config, .. } => common_config.id.clone(),
            TestRepoConfig::LocalStorage { common_config, .. } => common_config.id.clone(),
        }
    }

    pub fn get_local_tests(&self) -> Vec<LocalTestDefinition> {
        match self {
            TestRepoConfig::AzureStorageBlob { common_config, .. } => {
                common_config.local_tests.clone()
            }
            TestRepoConfig::GitHub { common_config, .. } => common_config.local_tests.clone(),
            TestRepoConfig::LocalStorage { common_config, .. } => common_config.local_tests.clone(),
        }
    }

    pub fn get_force_cache_refresh(&self) -> bool {
        match self {
            TestRepoConfig::AzureStorageBlob { unique_config, .. } => {
                unique_config.force_cache_refresh
            }
            TestRepoConfig::GitHub { unique_config, .. } => unique_config.force_cache_refresh,
            TestRepoConfig::LocalStorage { .. } => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonTestRepoConfig {
    pub id: String,
    #[serde(default)]
    pub local_tests: Vec<LocalTestDefinition>,
}

/// Azure Storage Blob repository configuration
///
/// Supports two authentication methods:
///
/// 1. **Access Key Authentication** (provide `access_key`):
///    ```yaml
///    kind: AzureStorageBlob
///    account_name: mystorageaccount
///    access_key: "your-access-key-here"
///    container: test-data
///    root_path: tests
///    ```
///
/// 2. **Identity-Based Authentication** (omit `access_key`):
///    Uses Azure DefaultAzureCredential chain:
///    - Managed Identity (in Azure VMs, Container Apps, etc.)
///    - Azure CLI (`az login`)
///    - Environment variables (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)
///    - Visual Studio Code
///    - Azure PowerShell
///
///    ```yaml
///    kind: AzureStorageBlob
///    account_name: mystorageaccount
///    # access_key omitted - will use identity
///    container: test-data
///    root_path: tests
///    ```
///
///    Requirements for identity-based auth:
///    - Ensure the identity has "Storage Blob Data Reader" role on the container
///    - For local development: Run `az login` before running tests
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AzureStorageBlobTestRepoConfig {
    pub account_name: String,
    /// Optional access key for storage account authentication.
    /// If not provided, will use Azure identity (managed identity, Azure CLI, environment, etc.)
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "mask_secret_option")]
    pub access_key: Option<String>,
    pub container: String,
    #[serde(default = "is_false")]
    pub force_cache_refresh: bool,
    pub root_path: String,
}
fn is_false() -> bool {
    false
}
fn mask_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("******")
}

fn mask_secret_option<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(_) => serializer.serialize_str("******"),
        None => serializer.serialize_none(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GithubTestRepoConfig {
    #[serde(default = "drasi_project")]
    pub owner: String,
    #[serde(default = "test_repo")]
    pub repo: String,
    #[serde(default = "main_branch")]
    pub branch: String,
    #[serde(default = "is_false")]
    pub force_cache_refresh: bool,
    pub root_path: String,
    pub token: Option<String>,
}

fn drasi_project() -> String {
    "drasi-project".to_string()
}

fn test_repo() -> String {
    "test-repo".to_string()
}

fn main_branch() -> String {
    "main".to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalStorageTestRepoConfig {
    pub source_path: Option<String>,
}

#[async_trait]
pub trait RemoteTestRepoClient: Send + Sync {
    async fn copy_test_definition(
        &self,
        test_id: String,
        test_def_path: PathBuf,
    ) -> anyhow::Result<()>;
    async fn copy_test_source_content(
        &self,
        test_data_folder: String,
        test_source_def: &TestSourceDefinition,
        test_source_data_path: PathBuf,
    ) -> anyhow::Result<()>;
}

#[async_trait]
impl RemoteTestRepoClient for Box<dyn RemoteTestRepoClient + Send + Sync> {
    async fn copy_test_definition(
        &self,
        test_id: String,
        test_def_path: PathBuf,
    ) -> anyhow::Result<()> {
        (**self).copy_test_definition(test_id, test_def_path).await
    }

    async fn copy_test_source_content(
        &self,
        test_data_folder: String,
        test_source_def: &TestSourceDefinition,
        test_source_data_path: PathBuf,
    ) -> anyhow::Result<()> {
        (**self)
            .copy_test_source_content(test_data_folder, test_source_def, test_source_data_path)
            .await
    }
}

pub async fn create_test_repo_client(
    config: TestRepoConfig,
) -> anyhow::Result<Box<dyn RemoteTestRepoClient + Send + Sync>> {
    match config {
        TestRepoConfig::AzureStorageBlob {
            common_config,
            unique_config,
        } => AzureStorageBlobTestRepoClient::new(common_config, unique_config).await,
        TestRepoConfig::GitHub {
            common_config,
            unique_config,
        } => GithubTestRepoClient::new(common_config, unique_config).await,
        TestRepoConfig::LocalStorage {
            common_config,
            unique_config,
        } => {
            local_storage_test_repo_client::LocalStorageTestRepoClient::new(
                common_config,
                unique_config,
            )
            .await
        }
    }
}
