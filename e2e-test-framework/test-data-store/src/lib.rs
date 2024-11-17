use std::{collections::HashMap, path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::{fs, sync::Mutex};

use data_collection_storage::DataCollectionStorage;
use test_repo_storage::{repo_clients::RemoteTestRepoConfig, TestRepoStorage};

pub mod data_collection_storage;
pub mod scripts;
pub mod test_repo_storage;

#[derive(Debug, Deserialize, Serialize)]
pub struct TestDataStoreConfig {
    pub data_collection_folder: Option<String>,
    pub data_store_path: Option<String>,
    pub delete_data_store: Option<bool>,
    pub test_repos: Option<Vec<RemoteTestRepoConfig>>,
    pub test_repo_folder: Option<String>,
}

impl Default for TestDataStoreConfig {
    fn default() -> Self {
        TestDataStoreConfig {
            data_collection_folder: None,
            data_store_path: None,
            delete_data_store: None,
            test_repos: None,
            test_repo_folder: None,
        }
    }
}

#[derive(Debug)]
pub struct TestDataStore {
    pub data_collection_root_path: PathBuf,
    pub root_path: PathBuf,    
    pub test_repo_root_path: PathBuf,
    pub test_repo_stores: Arc<Mutex<HashMap<String, TestRepoStorage>>>,
}

impl TestDataStore {
    pub async fn new(config: TestDataStoreConfig) -> anyhow::Result<Self> {
        log::debug!("Creating TestDataStore using config: {:?}", &config);

        let root_path = PathBuf::from(config.data_store_path.unwrap_or("./drasi_test_data_store".to_string()));

        // Delete the data store folder if it exists and the delete_data_store flag is set.
        if config.delete_data_store.unwrap_or(false) && root_path.exists() {
            log::info!("Deleting data store data: {:?}", &root_path);
            tokio::fs::remove_dir_all(&root_path).await?;
        }

        // Create the data store folder if it doesn't exist.
        if !root_path.exists() {
            log::info!("Creating data store data: {:?}", &root_path);
            tokio::fs::create_dir_all(&root_path).await?
        }

        let data_collection_root_path = root_path.join(config.data_collection_folder.unwrap_or("data_collections".to_string()));
        let test_repo_root_path = root_path.join(config.test_repo_folder.unwrap_or("test_repos".to_string()));

        let mut test_data_store = TestDataStore {
            data_collection_root_path,
            root_path,
            test_repo_root_path,
            test_repo_stores: Arc::new(Mutex::new(HashMap::new())),
        };

        // Create the initial TestDataRepo instances.
        for test_repo_config in config.test_repos.unwrap_or_default() {
            test_data_store.add_test_repo_storage(test_repo_config).await?;
        };

        Ok(test_data_store)
    }

    pub async fn add_test_repo_storage(&mut self, config: RemoteTestRepoConfig ) -> anyhow::Result<TestRepoStorage> {
        log::debug!("Adding TestRepoStorage from config: {:?}", &config);

        let mut test_repo_stores = self.test_repo_stores.lock().await;

        if test_repo_stores.contains_key(&config.get_id()) {
            anyhow::bail!("TestRepoStore with ID {} already exists", &config.get_id());
        }

        let test_repo_storage = TestRepoStorage::new(config.clone(), self.test_repo_root_path.clone()).await?;
        test_repo_stores.insert(test_repo_storage.id.clone(), test_repo_storage.clone());

        Ok(test_repo_storage)
    }

    pub async fn contains_data_collection(&self, id: &str) -> anyhow::Result<bool> {
        let mut path = self.data_collection_root_path.clone();
        path.push(format!("{}/", id));

        Ok(path.exists())
    }

    pub async fn contains_test_repo(&self, id: &str) -> anyhow::Result<bool> {
        let mut path = self.test_repo_root_path.clone();
        path.push(format!("{}/", id));

        Ok(path.exists())
    }

    pub async fn get_data_collection_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut data_collection_ids = Vec::new();

        let mut entries = fs::read_dir(&self.data_collection_root_path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    data_collection_ids.push(folder_name.to_string());
                }
            }
        }

        Ok(data_collection_ids)      
    }

    pub async fn get_data_collection_storage(&self, id: &str) -> anyhow::Result<DataCollectionStorage> {
        log::debug!("Getting DataCollectionStorage for ID: {:?}", &id);

        let mut path = self.data_collection_root_path.clone();
        path.push(format!("{}/", id));

        Ok(DataCollectionStorage::new(id, path).await?)
    }

    pub async fn get_data_store_path(&self) -> anyhow::Result<PathBuf> {
        Ok(self.root_path.clone())
    }

    pub async fn get_test_repo_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut test_repo_ids = Vec::new();

        let mut entries = fs::read_dir(&self.data_collection_root_path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    test_repo_ids.push(folder_name.to_string());
                }
            }
        }

        Ok(test_repo_ids)      
    }

    pub async fn get_test_repo_storage(&self, id: &str) -> anyhow::Result<TestRepoStorage> {
        log::debug!("Getting TestRepoStorage for ID: {:?}", &id);

        let test_repo_stores = self.test_repo_stores.lock().await;

        if let Some(test_repo_storage) = test_repo_stores.get(id) {
            Ok(test_repo_storage.clone())
        } else {
            anyhow::bail!("TestRepoStorage with ID {} not found", &id);
        }
    }
    
    // This function is used to figure out which dataset to use to service a bootstrap request.
    // It is a workaround for the fact that a Drasi Source issues a simple "acquire" request, assuming
    // the SourceProxy it is connected to will only be dealing with a single DB connection, whereas the 
    // TestRunner can be simulating multiple sources concurrently. Would be better to do something else here, 
    // but it is sufficient for now.
    // pub async fn match_bootstrap_dataset(&self, requested_labels: &HashSet<String>) -> anyhow::Result<Option<TestSourceDataset>> {
    //     let mut best_match = None;
    //     let mut best_match_count = 0;

    //     let test_repo_stores = self.test_repo_stores.lock().await;

    //     for (_, repo) in test_repo_stores.iter() {

    //         let test_source_datasets = repo.test_source_datasets.lock().await;
    //         let test_source_datasets_iter = test_source_datasets.iter();

    //         for (_, ds) in test_source_datasets_iter {
    //             let match_count = ds.count_bootstrap_type_intersection(&requested_labels);

    //             if match_count > best_match_count {
    //                 best_match = Some(ds);
    //                 best_match_count = match_count;
    //             }
    //         }
    //     }

    //     Ok(best_match.map(|ds| ds.clone()))
    // }
}