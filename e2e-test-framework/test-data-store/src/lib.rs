use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use test_run_storage::{TestRunId, TestRunSourceId, TestRunSourceStorage, TestRunStorage, TestRunStore};

use data_collection_storage::{DataCollectionStorage, DataCollectionStore};
use test_repo_storage::{models::TestDefinition, repo_clients::RemoteTestRepoConfig, TestRepoStorage, TestRepoStore, TestSourceDataset, TestSourceStorage};
use tokio::sync::Mutex;

pub mod data_collection_storage;
pub mod test_repo_storage;
pub mod test_run_storage;

const DEFAULT_ROOT_PATH: &str = "drasi_data_store";
const DEFAULT_DATA_COLLECTION_STORE_FOLDER: &str = "data_collections";
const DEFAULT_TEST_REPO_STORE_FOLDER: &str = "test_repos";
const DEFAULT_TEST_RUN_STORE_FOLDER: &str = "test_runs";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TestDataStoreConfig {
    pub data_collection_folder: Option<String>,
    pub data_store_path: Option<String>,
    pub delete_data_store: Option<bool>,
    pub test_repos: Option<Vec<RemoteTestRepoConfig>>,
    pub test_repo_folder: Option<String>,
    pub test_run_folder: Option<String>,
}

impl Default for TestDataStoreConfig {
    fn default() -> Self {
        TestDataStoreConfig {
            data_collection_folder: None,
            data_store_path: None,
            delete_data_store: None,
            test_repos: None,
            test_repo_folder: None,
            test_run_folder: None,
        }
    }
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
    pub root_path: PathBuf,    
    pub test_repo_store: Arc<Mutex<TestRepoStore>>,
    pub test_run_store: Arc<Mutex<TestRunStore>>, 
}

impl TestDataStore {
    pub async fn new(config: TestDataStoreConfig) -> anyhow::Result<Self> {
        log::debug!("Creating TestDataStore using config: {:?}", &config);

        let root_path = PathBuf::from(config.data_store_path.unwrap_or(DEFAULT_ROOT_PATH.to_string()));

        // Delete the data store folder if it exists and the delete_data_store flag is set.
        if config.delete_data_store.unwrap_or(false) && root_path.exists() {
            log::info!("Deleting TestDataStore at - {:?}", &root_path);
            tokio::fs::remove_dir_all(&root_path).await?;
        }

        // Create the data store folder if it doesn't exist.
        if !root_path.exists() {
            log::info!("Creating TestDataStore in - {:?}", &root_path);
            tokio::fs::create_dir_all(&root_path).await?
        }

        let test_data_store = TestDataStore {
            data_collection_store: Arc::new(Mutex::new(DataCollectionStore::new(config.data_collection_folder.unwrap_or(DEFAULT_DATA_COLLECTION_STORE_FOLDER.to_string()), root_path.clone(), false).await?)),
            root_path: root_path.clone(),
            test_repo_store: Arc::new(Mutex::new(TestRepoStore::new(config.test_repo_folder.unwrap_or(DEFAULT_TEST_REPO_STORE_FOLDER.to_string()), root_path.clone(), false, config.test_repos).await?)),
            test_run_store: Arc::new(Mutex::new(TestRunStore::new(config.test_run_folder.unwrap_or(DEFAULT_TEST_RUN_STORE_FOLDER.to_string()), root_path.clone(), false).await?)),
        };

        Ok(test_data_store)
    }

    pub async fn get_data_store_path(&self) -> anyhow::Result<PathBuf> {
        Ok(self.root_path.clone())
    }

    // Test Repo functions
    pub async fn add_remote_test_repo(&self, config: RemoteTestRepoConfig ) -> anyhow::Result<TestRepoStorage> {
        Ok(self.test_repo_store.lock().await.add_test_repo(config, false).await?)
    }

    pub async fn contains_test_repo(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.test_repo_store.lock().await.contains_test_repo(id).await?)
    }

    pub async fn get_test_definition(&self, repo_id: &str, test_id: &str) -> anyhow::Result<TestDefinition> {
        Ok(self.test_repo_store.lock().await
            .get_test_repo(repo_id).await?
            .get_test(test_id, false).await?
            .test_definition.clone())
    }

    pub async fn get_test_repo_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.test_repo_store.lock().await.get_test_repo_ids().await?)
    }

    pub async fn get_test_source_dataset(&self, repo_id: &str, test_id: &str, source_id: &str) -> anyhow::Result<TestSourceDataset> {
        Ok(self.test_repo_store.lock().await
            .get_test_repo(repo_id).await?
            .get_test(test_id, false).await?
            .get_test_source(source_id, false).await?
            .get_dataset().await?)
    }
    
    pub async fn get_test_source_storage(&self, repo_id: &str, test_id: &str, source_id: &str) -> anyhow::Result<TestSourceStorage> {
        Ok(self.test_repo_store.lock().await
            .get_test_repo(repo_id).await?
            .get_test(test_id, false).await?
            .get_test_source(source_id, false).await?)
    }

    pub async fn get_test_source_dataset_for_test_run_source(&self, test_run_source_id: &TestRunSourceId) -> anyhow::Result<TestSourceDataset> {
        self.get_test_source_dataset(
            &test_run_source_id.test_run_id.test_repo_id, 
            &test_run_source_id.test_run_id.test_id, 
            &test_run_source_id.test_source_id
        ).await
    }

    pub async fn get_test_definition_for_test_run_source(&self, test_run_source_id: &TestRunSourceId) -> anyhow::Result<TestDefinition> {
        self.get_test_definition(
            &test_run_source_id.test_run_id.test_repo_id, 
            &test_run_source_id.test_run_id.test_id
        ).await
    }

    pub async fn get_test_source_storage_for_test_run_source(&self, test_run_source_id: &TestRunSourceId) -> anyhow::Result<TestSourceStorage> {
        self.get_test_source_storage(
            &test_run_source_id.test_run_id.test_repo_id, 
            &test_run_source_id.test_run_id.test_id, 
            &test_run_source_id.test_source_id
        ).await
    }

    // Test Run functions
    pub async fn contains_test_run(&self, id: &TestRunId) -> anyhow::Result<bool> {
        Ok(self.test_run_store.lock().await.contains_test_run(id).await?)
    }

    pub async fn get_test_run_ids(&self) -> anyhow::Result<Vec<TestRunId>> {
        Ok(self.test_run_store.lock().await.get_test_run_ids().await?)
    }

    pub async fn get_test_run_storage(&self, test_run_id: &TestRunId) -> anyhow::Result<TestRunStorage> {
        Ok(self.test_run_store.lock().await.get_test_run_storage(test_run_id, false).await?)
    }

    pub async fn get_test_run_source_storage(&self, test_run_source_id: &TestRunSourceId) -> anyhow::Result<TestRunSourceStorage> {
        Ok(self.test_run_store.lock().await
            .get_test_run_storage(&test_run_source_id.test_run_id, false).await?
            .get_source_storage(test_run_source_id, false).await?)
    }

    // Data Collection functions
    pub async fn contains_data_collection(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.data_collection_store.lock().await.contains_data_collection(id).await?)
    }

    pub async fn get_data_collection_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.data_collection_store.lock().await.get_data_collection_ids().await?)
    }

    pub async fn get_data_collection_storage(&self, id: &str) -> anyhow::Result<DataCollectionStorage> {
        Ok(self.data_collection_store.lock().await.get_data_collection_storage(id, false).await?)
    }
}