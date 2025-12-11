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

use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::Mutex;

use data_collection_storage::{DataCollectionStorage, DataCollectionStore};
use test_repo_storage::{
    models::{
        LocalTestDefinition, TestDefinition, TestQueryDefinition, TestReactionDefinition,
        TestSourceDefinition,
    },
    repo_clients::TestRepoConfig,
    TestRepoStorage, TestRepoStore, TestSourceScriptSet, TestSourceStorage, TestStorage,
};
use test_run_storage::{
    TestRunDrasiServerId, TestRunDrasiServerStorage, TestRunId, TestRunQueryId,
    TestRunQueryStorage, TestRunReactionId, TestRunReactionStorage, TestRunSourceId,
    TestRunSourceStorage, TestRunStorage, TestRunStore,
};

pub mod data_collection_storage;
pub mod scripts;
pub mod test_repo_storage;
pub mod test_run_storage;

const DEFAULT_ROOT_PATH: &str = "drasi_data_store";
const DEFAULT_DATA_COLLECTION_STORE_FOLDER: &str = "data_collections";
const DEFAULT_TEST_REPO_STORE_FOLDER: &str = "test_repos";
const DEFAULT_TEST_RUN_STORE_FOLDER: &str = "test_runs";

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct TestDataStoreConfig {
    pub data_collection_folder: Option<String>,
    pub data_store_path: Option<String>,
    pub delete_on_start: Option<bool>,
    pub delete_on_stop: Option<bool>,
    pub test_repos: Option<Vec<TestRepoConfig>>,
    pub test_repo_folder: Option<String>,
    pub test_run_folder: Option<String>,
}

#[derive(Debug)]
pub struct TestDataStoreInfo {
    pub data_collection_ids: Vec<String>,
    pub root_path: PathBuf,
    pub test_repo_ids: Vec<String>,
    pub test_run_ids: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct TestDataStore {
    pub data_collection_store: Arc<Mutex<DataCollectionStore>>,
    pub delete_on_stop: bool,
    pub root_path: PathBuf,
    pub test_repo_store: Arc<Mutex<TestRepoStore>>,
    pub test_run_store: Arc<Mutex<TestRunStore>>,
    cleaned_up: Arc<Mutex<bool>>,
}

impl TestDataStore {
    pub async fn new(config: TestDataStoreConfig) -> anyhow::Result<Self> {
        log::debug!("Creating TestDataStore using config: {:?}", &config);

        let root_path = PathBuf::from(
            config
                .data_store_path
                .unwrap_or(DEFAULT_ROOT_PATH.to_string()),
        );

        // Delete the data store folder if it exists and the delete_on_start flag is set.
        if config.delete_on_start.unwrap_or(false) && root_path.exists() {
            log::info!("Deleting TestDataStore at - {:?}", &root_path);
            tokio::fs::remove_dir_all(&root_path).await?;
        }

        // Create the data store folder if it doesn't exist.
        if !root_path.exists() {
            log::info!("Creating TestDataStore in - {:?}", &root_path);
            tokio::fs::create_dir_all(&root_path).await?
        }

        let data_collection_store = Arc::new(Mutex::new(
            DataCollectionStore::new(
                config
                    .data_collection_folder
                    .unwrap_or(DEFAULT_DATA_COLLECTION_STORE_FOLDER.to_string()),
                root_path.clone(),
                false,
            )
            .await?,
        ));

        let test_repo_store = Arc::new(Mutex::new(
            TestRepoStore::new(
                config
                    .test_repo_folder
                    .unwrap_or(DEFAULT_TEST_REPO_STORE_FOLDER.to_string()),
                root_path.clone(),
                false,
                config.test_repos,
            )
            .await?,
        ));

        let test_run_store = Arc::new(Mutex::new(
            TestRunStore::new(
                config
                    .test_run_folder
                    .unwrap_or(DEFAULT_TEST_RUN_STORE_FOLDER.to_string()),
                root_path.clone(),
                false,
            )
            .await?,
        ));

        let test_data_store = TestDataStore {
            data_collection_store,
            delete_on_stop: config.delete_on_stop.unwrap_or(false),
            root_path: root_path.clone(),
            test_repo_store,
            test_run_store,
            cleaned_up: Arc::new(Mutex::new(false)),
        };

        Ok(test_data_store)
    }

    pub async fn new_temp(test_repos: Option<Vec<TestRepoConfig>>) -> anyhow::Result<Self> {
        log::debug!(
            "Creating temporary TestDataStore with repos: {:?}",
            &test_repos
        );

        let config = TestDataStoreConfig {
            #[allow(deprecated)]
            data_store_path: Some(
                TempDir::new()
                    .expect("Failed to create temporary directory for TestDataStore")
                    .into_path()
                    .to_string_lossy()
                    .to_string(),
            ),
            delete_on_stop: Some(true),
            test_repos,
            ..TestDataStoreConfig::default()
        };

        TestDataStore::new(config).await
    }

    pub async fn get_data_store_path(&self) -> anyhow::Result<PathBuf> {
        Ok(self.root_path.clone())
    }

    // Test Repo functions
    pub async fn add_local_test(
        &self,
        repo_id: &str,
        test_def: LocalTestDefinition,
        replace: bool,
    ) -> anyhow::Result<TestStorage> {
        self.test_repo_store
            .lock()
            .await
            .get_test_repo_storage(repo_id)
            .await?
            .add_local_test(test_def, replace)
            .await
    }

    pub async fn add_remote_test(
        &self,
        repo_id: &str,
        test_id: &str,
        replace: bool,
    ) -> anyhow::Result<TestStorage> {
        self.test_repo_store
            .lock()
            .await
            .get_test_repo_storage(repo_id)
            .await?
            .add_remote_test(test_id, replace)
            .await
    }

    pub async fn add_test_repo(&self, config: TestRepoConfig) -> anyhow::Result<TestRepoStorage> {
        self.test_repo_store
            .lock()
            .await
            .add_test_repo(config, false)
            .await
    }

    pub async fn contains_test_repo(&self, id: &str) -> anyhow::Result<bool> {
        self.test_repo_store
            .lock()
            .await
            .contains_test_repo(id)
            .await
    }

    pub async fn get_test_definition(
        &self,
        repo_id: &str,
        test_id: &str,
    ) -> anyhow::Result<TestDefinition> {
        self.test_repo_store
            .lock()
            .await
            .get_test_repo_storage(repo_id)
            .await?
            .get_test_definition(test_id)
            .await
    }

    pub async fn get_test_repo_ids(&self) -> anyhow::Result<Vec<String>> {
        self.test_repo_store.lock().await.get_test_repo_ids().await
    }

    pub async fn get_test_repo_storage(&self, repo_id: &str) -> anyhow::Result<TestRepoStorage> {
        self.test_repo_store
            .lock()
            .await
            .get_test_repo_storage(repo_id)
            .await
    }

    pub async fn get_test_repo_test_ids(&self, repo_id: &str) -> anyhow::Result<Vec<String>> {
        self.test_repo_store
            .lock()
            .await
            .get_test_repo_storage(repo_id)
            .await?
            .get_test_ids()
            .await
    }

    pub async fn get_test_source_scripts(
        &self,
        repo_id: &str,
        test_id: &str,
        source_id: &str,
    ) -> anyhow::Result<TestSourceScriptSet> {
        self.test_repo_store
            .lock()
            .await
            .get_test_repo_storage(repo_id)
            .await?
            .get_test_storage(test_id)
            .await?
            .get_test_source(source_id, false)
            .await?
            .get_script_files()
            .await
    }

    pub async fn get_test_source_storage(
        &self,
        repo_id: &str,
        test_id: &str,
        source_id: &str,
    ) -> anyhow::Result<TestSourceStorage> {
        self.test_repo_store
            .lock()
            .await
            .get_test_repo_storage(repo_id)
            .await?
            .get_test_storage(test_id)
            .await?
            .get_test_source(source_id, false)
            .await
    }

    pub async fn get_test_storage(
        &self,
        repo_id: &str,
        test_id: &str,
    ) -> anyhow::Result<TestStorage> {
        self.test_repo_store
            .lock()
            .await
            .get_test_repo_storage(repo_id)
            .await?
            .get_test_storage(test_id)
            .await
    }

    // Test Run functions
    pub async fn contains_test_run(&self, id: &TestRunId) -> anyhow::Result<bool> {
        self.test_run_store.lock().await.contains_test_run(id).await
    }

    pub async fn get_test_definition_for_test_run_source(
        &self,
        test_run_source_id: &TestRunSourceId,
    ) -> anyhow::Result<TestDefinition> {
        self.get_test_definition(
            &test_run_source_id.test_run_id.test_repo_id,
            &test_run_source_id.test_run_id.test_id,
        )
        .await
    }

    pub async fn get_test_query_definition_for_test_run_query(
        &self,
        test_run_query_id: &TestRunQueryId,
    ) -> anyhow::Result<TestQueryDefinition> {
        self.get_test_definition(
            &test_run_query_id.test_run_id.test_repo_id,
            &test_run_query_id.test_run_id.test_id,
        )
        .await?
        .get_test_query(&test_run_query_id.test_query_id)
    }

    pub async fn get_test_source_definition_for_test_run_source(
        &self,
        test_run_source_id: &TestRunSourceId,
    ) -> anyhow::Result<TestSourceDefinition> {
        self.get_test_definition(
            &test_run_source_id.test_run_id.test_repo_id,
            &test_run_source_id.test_run_id.test_id,
        )
        .await?
        .get_test_source(&test_run_source_id.test_source_id)
    }

    pub async fn get_test_source_scripts_for_test_run_source(
        &self,
        test_run_source_id: &TestRunSourceId,
    ) -> anyhow::Result<TestSourceScriptSet> {
        self.get_test_source_scripts(
            &test_run_source_id.test_run_id.test_repo_id,
            &test_run_source_id.test_run_id.test_id,
            &test_run_source_id.test_source_id,
        )
        .await
    }

    pub async fn get_test_source_storage_for_test_run_source(
        &self,
        test_run_source_id: &TestRunSourceId,
    ) -> anyhow::Result<TestSourceStorage> {
        self.get_test_source_storage(
            &test_run_source_id.test_run_id.test_repo_id,
            &test_run_source_id.test_run_id.test_id,
            &test_run_source_id.test_source_id,
        )
        .await
    }

    pub async fn get_test_run_ids(&self) -> anyhow::Result<Vec<TestRunId>> {
        self.test_run_store.lock().await.get_test_run_ids().await
    }

    pub async fn get_test_run_storage(
        &self,
        test_run_id: &TestRunId,
    ) -> anyhow::Result<TestRunStorage> {
        self.test_run_store
            .lock()
            .await
            .get_test_run_storage(test_run_id, false)
            .await
    }

    pub async fn get_test_run_query_storage(
        &self,
        test_run_query_id: &TestRunQueryId,
    ) -> anyhow::Result<TestRunQueryStorage> {
        self.test_run_store
            .lock()
            .await
            .get_test_run_storage(&test_run_query_id.test_run_id, false)
            .await?
            .get_query_storage(test_run_query_id, false)
            .await
    }

    pub async fn get_test_run_source_storage(
        &self,
        test_run_source_id: &TestRunSourceId,
    ) -> anyhow::Result<TestRunSourceStorage> {
        self.test_run_store
            .lock()
            .await
            .get_test_run_storage(&test_run_source_id.test_run_id, false)
            .await?
            .get_source_storage(test_run_source_id, false)
            .await
    }

    pub async fn get_test_run_reaction_storage(
        &self,
        test_run_reaction_id: &TestRunReactionId,
    ) -> anyhow::Result<TestRunReactionStorage> {
        self.test_run_store
            .lock()
            .await
            .get_test_run_storage(&test_run_reaction_id.test_run_id, false)
            .await?
            .get_reaction_storage(test_run_reaction_id, false)
            .await
    }

    pub async fn get_test_run_drasi_server_storage(
        &self,
        test_run_drasi_server_id: &TestRunDrasiServerId,
    ) -> anyhow::Result<TestRunDrasiServerStorage> {
        self.test_run_store
            .lock()
            .await
            .get_test_run_storage(&test_run_drasi_server_id.test_run_id, false)
            .await?
            .get_drasi_server_storage(test_run_drasi_server_id, false)
            .await
    }

    pub async fn get_test_reaction_definition_for_test_run_reaction(
        &self,
        test_run_reaction_id: &TestRunReactionId,
    ) -> anyhow::Result<TestReactionDefinition> {
        self.get_test_definition(
            &test_run_reaction_id.test_run_id.test_repo_id,
            &test_run_reaction_id.test_run_id.test_id,
        )
        .await?
        .get_test_reaction(&test_run_reaction_id.test_reaction_id)
    }

    // Data Collection functions
    pub async fn contains_data_collection(&self, id: &str) -> anyhow::Result<bool> {
        self.data_collection_store
            .lock()
            .await
            .contains_data_collection(id)
            .await
    }

    pub async fn get_data_collection_ids(&self) -> anyhow::Result<Vec<String>> {
        self.data_collection_store
            .lock()
            .await
            .get_data_collection_ids()
            .await
    }

    pub async fn get_data_collection_storage(
        &self,
        id: &str,
    ) -> anyhow::Result<DataCollectionStorage> {
        self.data_collection_store
            .lock()
            .await
            .get_data_collection_storage(id, false)
            .await
    }

    /// Returns whether this TestDataStore is configured to delete its data on stop.
    pub fn should_delete_on_stop(&self) -> bool {
        self.delete_on_stop
    }

    /// Synchronously cleans up the TestDataStore by removing its root directory.
    /// This method is primarily used as a fallback in the Drop trait.
    /// For async contexts, prefer using `cleanup_async()`.
    pub fn cleanup(&self) -> Result<(), std::io::Error> {
        if self.delete_on_stop && self.root_path.exists() {
            log::info!("Cleaning up TestDataStore at - {:?}", &self.root_path);
            std::fs::remove_dir_all(&self.root_path)?;
            log::info!("TestDataStore cleaned up successfully.");
        }
        Ok(())
    }

    /// Asynchronously cleans up the TestDataStore by removing its root directory.
    ///
    /// This method:
    /// - Checks if cleanup has already been performed to prevent double cleanup
    /// - Uses async I/O operations to avoid blocking the runtime
    /// - Sets a flag to indicate cleanup completion
    ///
    /// This is the preferred cleanup method for async contexts, especially
    /// in signal handlers where blocking operations should be avoided.
    pub async fn cleanup_async(&self) -> Result<(), std::io::Error> {
        // Check if already cleaned up
        let mut cleaned_up = self.cleaned_up.lock().await;
        if *cleaned_up {
            log::debug!("TestDataStore already cleaned up, skipping...");
            return Ok(());
        }

        if self.delete_on_stop && self.root_path.exists() {
            log::info!("Cleaning up TestDataStore at - {:?}", &self.root_path);
            tokio::fs::remove_dir_all(&self.root_path).await?;
            log::info!("TestDataStore cleaned up successfully.");
            *cleaned_up = true;
        }
        Ok(())
    }
}

impl Drop for TestDataStore {
    fn drop(&mut self) {
        // Check if already cleaned up using try_lock to avoid blocking
        if let Ok(cleaned_up) = self.cleaned_up.try_lock() {
            if *cleaned_up {
                log::debug!(
                    "TestDataStore already cleaned up in signal handler, skipping Drop cleanup"
                );
                return;
            }
        }

        if self.delete_on_stop && self.root_path.exists() {
            log::info!(
                "Deleting TestDataStore at - {:?} (in Drop)",
                &self.root_path
            );

            match std::fs::remove_dir_all(&self.root_path) {
                Ok(_) => log::info!("TestDataStore deleted successfully (in Drop)."),
                Err(err) => log::error!("Error deleting TestDataStore (in Drop): {err:?}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{
        test_repo_storage::repo_clients::{
            AzureStorageBlobTestRepoConfig, CommonTestRepoConfig, LocalStorageTestRepoConfig,
            TestRepoConfig,
        },
        TestDataStoreConfig,
    };

    use super::TestDataStore;

    #[tokio::test]
    async fn test_temp_testdatastore() -> anyhow::Result<()> {
        let data_store = TestDataStore::new_temp(None).await?;

        let temp_dir_path = data_store.root_path.clone();
        assert!(temp_dir_path.exists());

        assert!(data_store.get_data_collection_ids().await?.is_empty());
        assert!(data_store.get_test_repo_ids().await?.is_empty());
        assert!(data_store.get_test_run_ids().await?.is_empty());

        drop(data_store);

        assert!(!temp_dir_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_temp_testdatastore_initial_test_repos() -> anyhow::Result<()> {
        let test_repos: Vec<TestRepoConfig> = vec![
            TestRepoConfig::LocalStorage {
                common_config: CommonTestRepoConfig {
                    id: "test_repo_1".to_string(),
                    local_tests: Vec::new(),
                },
                unique_config: LocalStorageTestRepoConfig { source_path: None },
            },
            TestRepoConfig::LocalStorage {
                common_config: CommonTestRepoConfig {
                    id: "test_repo_2".to_string(),
                    local_tests: Vec::new(),
                },
                unique_config: LocalStorageTestRepoConfig {
                    source_path: Some("test_source_path".to_string()),
                },
            },
            TestRepoConfig::AzureStorageBlob {
                common_config: CommonTestRepoConfig {
                    id: "test_repo_3".to_string(),
                    local_tests: Vec::new(),
                },
                unique_config: AzureStorageBlobTestRepoConfig {
                    account_name: "test_account_name".to_string(),
                    access_key: "test_access_key".to_string(),
                    container: "test_container".to_string(),
                    force_cache_refresh: false,
                    root_path: "test_root_path".to_string(),
                },
            },
        ];

        let data_store = TestDataStore::new_temp(Some(test_repos)).await?;

        assert!(data_store.get_data_collection_ids().await?.is_empty());
        assert!(data_store.get_test_run_ids().await?.is_empty());

        let test_repo_ids = data_store.get_test_repo_ids().await?;
        assert_eq!(test_repo_ids.len(), 3);
        assert!(test_repo_ids.contains(&"test_repo_1".to_string()));
        assert!(test_repo_ids.contains(&"test_repo_2".to_string()));
        assert!(test_repo_ids.contains(&"test_repo_3".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_testdatastore_create() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_dir_path = temp_dir.path();

        let data_store_config = TestDataStoreConfig {
            data_store_path: Some(temp_dir_path.to_string_lossy().to_string()),
            delete_on_stop: Some(false),
            ..TestDataStoreConfig::default()
        };

        let data_store = TestDataStore::new(data_store_config).await?;
        assert!(temp_dir_path.exists());

        assert!(data_store.get_data_collection_ids().await?.is_empty());
        assert!(data_store.get_test_repo_ids().await?.is_empty());
        assert!(data_store.get_test_run_ids().await?.is_empty());

        drop(data_store);

        // The TempDir should still exist because the TestDataStoreConfig.delete_on_stop flag is false.
        assert!(temp_dir_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_testdatastore_delete_on_stop() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_dir_path = temp_dir.path();

        let data_store_config = TestDataStoreConfig {
            data_store_path: Some(temp_dir_path.to_string_lossy().to_string()),
            delete_on_stop: Some(true),
            ..TestDataStoreConfig::default()
        };

        let data_store = TestDataStore::new(data_store_config).await?;
        assert!(temp_dir_path.exists());

        assert!(data_store.get_data_collection_ids().await?.is_empty());
        assert!(data_store.get_test_repo_ids().await?.is_empty());
        assert!(data_store.get_test_run_ids().await?.is_empty());

        drop(data_store);

        // The TempDir should not exist because the TestDataStoreConfig.delete_on_stop flag is true.
        assert!(!temp_dir_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_testdatastore_create_initial_test_repos() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let temp_dir_path = temp_dir.path();

        let test_repos: Vec<TestRepoConfig> = vec![
            TestRepoConfig::LocalStorage {
                common_config: CommonTestRepoConfig {
                    id: "test_repo_1".to_string(),
                    local_tests: Vec::new(),
                },
                unique_config: LocalStorageTestRepoConfig { source_path: None },
            },
            TestRepoConfig::LocalStorage {
                common_config: CommonTestRepoConfig {
                    id: "test_repo_2".to_string(),
                    local_tests: Vec::new(),
                },
                unique_config: LocalStorageTestRepoConfig {
                    source_path: Some("test_source_path".to_string()),
                },
            },
            TestRepoConfig::AzureStorageBlob {
                common_config: CommonTestRepoConfig {
                    id: "test_repo_3".to_string(),
                    local_tests: Vec::new(),
                },
                unique_config: AzureStorageBlobTestRepoConfig {
                    account_name: "test_account_name".to_string(),
                    access_key: "test_access_key".to_string(),
                    container: "test_container".to_string(),
                    force_cache_refresh: false,
                    root_path: "test_root_path".to_string(),
                },
            },
        ];

        let data_store_config = TestDataStoreConfig {
            data_store_path: Some(temp_dir_path.to_string_lossy().to_string()),
            delete_on_stop: Some(true),
            test_repos: Some(test_repos),
            ..TestDataStoreConfig::default()
        };

        let data_store = TestDataStore::new(data_store_config).await?;

        assert!(data_store.get_data_collection_ids().await?.is_empty());
        assert!(data_store.get_test_run_ids().await?.is_empty());

        let test_repo_ids = data_store.get_test_repo_ids().await?;
        assert_eq!(test_repo_ids.len(), 3);
        assert!(test_repo_ids.contains(&"test_repo_1".to_string()));
        assert!(test_repo_ids.contains(&"test_repo_2".to_string()));
        assert!(test_repo_ids.contains(&"test_repo_3".to_string()));

        drop(data_store);

        // The TempDir should not exist because the TestDataStoreConfig.delete_on_stop flag is true.
        assert!(!temp_dir_path.exists());

        Ok(())
    }
}
