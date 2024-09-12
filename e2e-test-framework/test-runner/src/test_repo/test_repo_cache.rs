use std::{collections::{HashMap, HashSet}, path::PathBuf};

use crate::{config::TestRepoConfig, test_run::TestRunSource};

use super::{
    azure_storage_blob_test_repo::{AzureStorageBlobTestRepo, AzureStorageBlobTestRepoSettings}, 
    dataset::DataSet, 
    TestRepo,
};

#[derive(Debug)]
pub struct TestRepoCache {
    pub data_cache_root_path: PathBuf,
    pub datasets: HashMap<String, DataSet>,
    pub test_repos: HashMap<String, Box<dyn TestRepo>>,
}

impl TestRepoCache {
    pub async fn new(data_cache_root_path: String) -> anyhow::Result<Self> {

        let data_cache_root_path = PathBuf::from(&data_cache_root_path);

        // Test if the path exists, and if not create it.
        // If there are errors creating the path, log the error and return it.
        if !data_cache_root_path.exists() {
            match std::fs::create_dir_all(&data_cache_root_path) {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("Error creating data cache folder {:?}: {:?}", &data_cache_root_path, e);
                    log::error!("{}", msg);
                    anyhow::bail!(msg);
                }
            }
        }

        Ok(TestRepoCache {
            data_cache_root_path,
            datasets: HashMap::new(),
            test_repos: HashMap::new(),
        })
    }

    pub async fn add_test_repo(&mut self, test_repo_config: TestRepoConfig ) -> anyhow::Result<()> {
        match test_repo_config {
            TestRepoConfig::AzureStorageBlob{common_config, unique_config} => {
                // Formulate the local folder path where the files from the TestRepo should be stored.
                let mut data_cache_repo_path = self.data_cache_root_path.clone();
                data_cache_repo_path.push(format!("test_repos/{}/", &common_config.id));

                let settings = AzureStorageBlobTestRepoSettings::try_from_config(data_cache_repo_path, common_config, unique_config).await?;
                let test_repo = AzureStorageBlobTestRepo::new(settings).await?;    
                self.test_repos.insert(test_repo.settings.test_repo_id.clone(), Box::new(test_repo));
            }
        }

        Ok(())
    }

    pub fn contains_test_repo(&self, test_repo_id: &str) -> bool {
        self.test_repos.contains_key(test_repo_id)
    }

    // pub fn get_test_repo(&self, test_repo_id: &str) -> anyhow::Result<TestRepoConfig> {
    //     match self.test_repos.get(test_repo_id) {
    //         Some(test_repo) => Ok(test_repo.settings.clone()),
    //         None => {
    //             let msg = format!("TestRepo {:?} not found in cache", test_repo_id);
    //             anyhow::bail!(msg);
    //         }
    //     }
    // }

    pub async fn get_data_set(&mut self, test_run_source: TestRunSource) -> anyhow::Result<DataSet> {

        let TestRunSource { ref id, ref source_id, ref test_id, ref test_repo_id, .. } = test_run_source;

        // If the DataSet is already cached, return it.
        if let Some(dataset) = self.datasets.get(id) {
            return Ok(dataset.clone()); // Return cached DataSet
        };

        // Otherwise, ensure the provide TestRunSource is associated with an existing TestRepo. If not return an error.
        let test_repo = match self.test_repos.get(test_repo_id) {
            Some(test_repo) => test_repo,
            None => {
                let msg = format!("TestRunSource {:?} refers to undefined TestRepo {:?}", id, test_repo_id);
                anyhow::bail!(msg);
            },
        };

        // Formulate the local folder path where the DataSet files will be stored.
        let mut dataset_cache_path = self.data_cache_root_path.clone();
        dataset_cache_path.push(format!("test_repos/{}/{}/sources/{}/", test_repo_id, test_id, source_id));

        let test_source_content = test_repo.download_test_source_content(test_id.clone(), source_id.clone(), dataset_cache_path).await?;

        // Store in cache
        let dataset = DataSet::new(test_run_source, test_source_content);

        self.datasets.insert(dataset.id.clone(), dataset.clone());

        Ok(dataset)
    }

    // This function is used to figure out which dataset to use to service a bootstrap request.
    // It is a workaround for the fact that a Drasi Source issues a simple "acquire" request, assuming
    // the SourceProxy it is connected to will only be dealing with a single DB connection, whereas the 
    // TestRunner can be simulating multiple sources concurrently. Would be better to do something else here, 
    // but it is sufficient for now.
    pub fn get_dataset_for_bootstrap(&self, requested_labels: &HashSet<String>) -> Option<DataSet> {
        let mut best_match = None;
        let mut best_match_count = 0;

        for (_, ds) in &self.datasets {
            let match_count = ds.count_bootstrap_type_intersection(&requested_labels);

            if match_count > best_match_count {
                best_match = Some(ds);
                best_match_count = match_count;
            }
        }

        best_match.map(|ds| ds.clone())
    }
}