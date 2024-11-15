use std::{collections::{HashMap, HashSet}, path::PathBuf};

use derive_more::Debug;
use repo_clients::{get_test_repo_client, TestRepoClient};
use serde::Serialize;

use config::TestRepoConfig;

pub mod config;
pub mod repo_clients;
pub mod scripts;

// #[derive(Debug, thiserror::Error)]
// pub enum TestRepoError {
//     #[error("Invalid TestRepo Id: {0}")]
//     _InvalidTestRepoId(String),
//     #[error("Invalid TestRepo Type: {0}")]
//     _InvalidType(String),
// }

#[derive(Debug)]
pub struct TestDataStore {
    pub root_path: PathBuf,
    pub test_repos: HashMap<String, TestRepoCache>,
}

impl TestDataStore {
    pub async fn new(root_path: PathBuf, delete_data_store: bool) -> anyhow::Result<Self> {

        // Delete the data store folder if it exists and the delete_data_store flag is set.
        if delete_data_store && root_path.exists() {
            log::info!("Deleting data store data: {:?}", &root_path);
            tokio::fs::remove_dir_all(&root_path).await?;
        }

        // Create the data store folder if it doesn't exist.
        if !root_path.exists() {
            log::info!("Creating data store data: {:?}", &root_path);
            tokio::fs::create_dir_all(&root_path).await?
        }

        Ok(TestDataStore {
            root_path,
            test_repos: HashMap::new(),
        })
    }

    pub async fn add_test_repo(&mut self, config: TestRepoConfig ) -> anyhow::Result<()> {

        let id = config.get_id();

        // Formulate the local folder path where the files from the TestRepo should be stored.
        let mut data_cache_path = self.root_path.clone();
        data_cache_path.push(format!("test_repos/{}/", &id));
        
        let test_repo_client = get_test_repo_client(config, data_cache_path.clone()).await?;

        let test_repo_cache = TestRepoCache {
            id: id.clone(),
            path: data_cache_path,
            test_repo_client,
            test_source_datasets: HashMap::new(),
        };

        self.test_repos.insert(id, test_repo_cache);

        Ok(())
    }

    pub fn contains_test_repo(&self, test_repo_id: &str) -> bool {
        self.test_repos.contains_key(test_repo_id)
    }

    pub fn get_test_repo_info(&self, test_repo_id: &str) -> anyhow::Result<Option<TestRepoInfo>> {
        
        match self.test_repos.get(test_repo_id) {
            Some(test_repo) => {
                Ok(Some(test_repo.into()))
            },
            None => {
                Ok(None)
            }
        }
    }

    pub fn get_test_repos_info(&self) -> anyhow::Result<Vec<TestRepoInfo>> {
        let mut test_repos_info = Vec::new();

        for (_, test_repo) in &self.test_repos {
            test_repos_info.push(test_repo.into());
        }

        Ok(test_repos_info)
    }

    pub fn get_test_repo_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.test_repos.keys().cloned().collect())
    }

    pub async fn get_test_source_dataset(&mut self, test_repo_id: &str, test_id: &str, source_id: &str) -> anyhow::Result<TestSourceDataset> {

        // Attempt to get the TestRepo associated with the provided test_repo_id.
        let test_repo = match self.test_repos.get_mut(test_repo_id) {
            Some(test_repo) => test_repo,
            None => {
                let msg = format!("Invalid TestRepo Id: {:?}", test_repo_id);
                anyhow::bail!(msg);
            },
        };
        
        // Attempt to get the TestSourceDataset associated with the provided test_id and source_id.
        let test_source_id = format!("{}/{}", test_id, source_id);

        let test_source_dataset = match test_repo.test_source_datasets.get(&test_source_id) {
            Some(test_source_dataset) => test_source_dataset.clone(),
            None => {
                // Attempt to download the TestSourceDataset from the TestRepo.
                let mut test_source_dataset_path = test_repo.path.clone();
                test_source_dataset_path.push(test_source_id.clone());
        
                let test_source_dataset = test_repo.test_repo_client.download_test_source_dataset(test_id.to_string(), source_id.to_string(), test_source_dataset_path).await?;

                // Store the TestSourceDataset in the TestRepo cache.
                test_repo.test_source_datasets.insert(test_source_id, test_source_dataset.clone());

                test_source_dataset
            }
        };

        Ok(test_source_dataset)
    }

    pub fn list_test_source_datasets(&self) -> anyhow::Result<Vec<TestSourceDataset>> {
        let mut test_source_datasets = Vec::new();

        for (_, test_repo) in &self.test_repos {
            for (_, test_source_dataset) in &test_repo.test_source_datasets {
                test_source_datasets.push(test_source_dataset.clone());
            }
        }

        Ok(test_source_datasets)
    }

    // This function is used to figure out which dataset to use to service a bootstrap request.
    // It is a workaround for the fact that a Drasi Source issues a simple "acquire" request, assuming
    // the SourceProxy it is connected to will only be dealing with a single DB connection, whereas the 
    // TestRunner can be simulating multiple sources concurrently. Would be better to do something else here, 
    // but it is sufficient for now.
    pub fn match_bootstrap_dataset(&self, requested_labels: &HashSet<String>) -> anyhow::Result<Option<TestSourceDataset>> {
        let mut best_match = None;
        let mut best_match_count = 0;

        for (_, repo) in &self.test_repos {
            for (_, ds) in &repo.test_source_datasets {
                let match_count = ds.count_bootstrap_type_intersection(&requested_labels);

                if match_count > best_match_count {
                    best_match = Some(ds);
                    best_match_count = match_count;
                }
            }
        }

        Ok(best_match.map(|ds| ds.clone()))
    }
}

#[derive(Debug)]
pub struct TestRepoCache {
    pub id: String,
    pub path: PathBuf,
    #[debug(skip)]
    pub test_repo_client: Box<dyn TestRepoClient>,
    pub test_source_datasets: HashMap<String, TestSourceDataset>,
}

#[derive(Debug)]
pub struct TestRepoInfo {
    pub id: String,
    pub path: PathBuf,
    pub test_source_datasets: HashMap<String, TestSourceDataset>,
}

impl From<&TestRepoCache> for TestRepoInfo {
    fn from(test_repo_cache: &TestRepoCache) -> Self {
        TestRepoInfo {
            id: test_repo_cache.id.clone(),
            path: test_repo_cache.path.clone(),
            test_source_datasets: test_repo_cache.test_source_datasets.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestSourceDataset {
    pub bootstrap_script_files: Option<HashMap<String, Vec<PathBuf>>>,
    pub change_log_script_files: Option<Vec<PathBuf>>,
}

impl TestSourceDataset {
    pub fn count_bootstrap_type_intersection(&self, requested_labels: &HashSet<String>) -> usize {
        let mut match_count = 0;

        if self.bootstrap_script_files.is_some() {
            // Iterate through the requested labels (data types) and count the number of matches.
            self.bootstrap_script_files.as_ref().unwrap().keys().filter(|key| requested_labels.contains(*key)).for_each(|_| {
                match_count += 1;
            });
        }

        match_count
    }
}