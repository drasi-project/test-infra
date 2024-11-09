use std::{collections::HashMap, path::PathBuf, sync::Arc};

use serde::Serialize;
use tokio::{fs::remove_dir_all, sync::RwLock};

use test_runner::{config::TestRepoConfig, test_repo::test_repo_cache::TestRepoCache};

use config::{DatasetConfig, TestDatasetCollectorConfig, SourceConfig};
use source::{DatasetSource, source_change_recorder::SourceChangeRecorder};

pub mod config;
// pub mod query;
pub mod source;

// An enum that represents the current state of the TestDatasetCollector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TestDatasetCollectorStatus {
    // The Test Dataset Collector is Initialized and is ready to start.
    Initialized,
    // The Test Dataset Collector has been started.
    Started,
    // The Test Dataset Collector is in an Error state. and will not be able to process requests.
    Error(String),
}

pub type SharedTestDatasetCollector = Arc<RwLock<TestDatasetCollector>>;

#[derive(Debug)]
pub struct TestDatasetCollector {
    _source_change_recorders: HashMap<String, SourceChangeRecorder>,
    data_store_path: PathBuf,
    datasets: HashMap<String, Dataset>,
    status: TestDatasetCollectorStatus,
    test_repos: HashMap<String, TestRepoConfig>,
    test_repo_cache: TestRepoCache,
}

impl TestDatasetCollector {
    pub async fn new(config: TestDatasetCollectorConfig) -> anyhow::Result<Self> {   

        log::debug!("Creating TestDatasetCollector from {:#?}", config);

        let data_store_path = PathBuf::from(&config.data_store_path);

        let mut test_dataset_collector = TestDatasetCollector {
            _source_change_recorders: HashMap::new(),
            data_store_path,
            datasets: HashMap::new(),
            status: TestDatasetCollectorStatus::Initialized,
            test_repos: HashMap::new(),
            test_repo_cache: TestRepoCache::new(config.data_store_path.clone()).await?,
        };

        // If the prune_data_store flag is set, and the folder exists, remove it.
        if config.prune_data_store_path && std::path::Path::new(&config.data_store_path).exists() {
            log::info!("Pruning data store folder: {:?}", &config.data_store_path);
            remove_dir_all(&config.data_store_path).await.unwrap_or_else(|err| {
                panic!("Error Pruning data store folder - path:{}, error:{}", &config.data_store_path, err);
            });
        }

        // Add the initial set of test repos.
        // Fail construction if any of the TestRepoConfigs fail to create.
        for ref test_repo_config in config.test_repos {
            test_dataset_collector.add_test_repo(test_repo_config).await?;
        };

        // Add the initial set of datasets.
        // Fail construction if any of the DatasetConfigs fail to create.
        for ref dataset_config in config.datasets {
            test_dataset_collector.add_dataset(dataset_config).await?;
        };

        log::debug!("TestDatasetCollector created -  {:?}", &test_dataset_collector);

        Ok(test_dataset_collector)
    }
    
    pub async fn add_dataset(&mut self, dataset_config: &DatasetConfig) -> anyhow::Result<()> {
        log::trace!("Adding Dataset from {:#?}", dataset_config);

        // If the TestDatasetCollector is in an Error state, return an error.
        if let TestDatasetCollectorStatus::Error(msg) = &self.status {
            anyhow::bail!("TestDatasetCollector is in an Error state: {}", msg);
        };
        
        let dataset = Dataset::try_from_config(dataset_config, self.data_store_path.clone()).await?;
        self.datasets.insert(dataset.dataset_id.clone(), dataset);

        Ok(())
    }

    pub async fn add_test_repo(&mut self, test_repo_config: &TestRepoConfig ) -> anyhow::Result<()> {
        log::trace!("Adding TestRepo from {:#?}", test_repo_config);

        // If the TestDatasetCollector is in an Error state, return an error.
        if let TestDatasetCollectorStatus::Error(msg) = &self.status {
            anyhow::bail!("TestDatasetCollector is in an Error state: {}", msg);
        };

        let test_repo_id = self.test_repo_cache.add_test_repo(test_repo_config.clone()).await?;  
        self.test_repos.insert(test_repo_id, test_repo_config.clone());

        Ok(())
    }

    // pub async fn control_player(&self, player_id: &str, command: ChangeScriptPlayerCommand) -> anyhow::Result<ChangeScriptPlayerMessageResponse> {
    //     log::trace!("Control Player - player_id:{}, command:{:?}", player_id, command);

    //     // If the TestDatasetCollector is in an Error state, return an error.
    //     if let TestDatasetCollectorStatus::Error(msg) = &self.status {
    //         anyhow::bail!("TestDatasetCollector is in an Error state: {}", msg);
    //     };

    //     // Get the ChangeScriptPlayer from the TestDatasetCollector or fail if it doesn't exist.
    //     match self.source_change_recorders.get(player_id) {
    //         Some(player) => {
    //             match command {
    //                 SourceChangeRecorderCommand::GetState => {
    //                     player.get_state().await
    //                 },
    //                 SourceChangeRecorderCommand::Start => {
    //                     player.start().await
    //                 },
    //                 SourceChangeRecorderCommand::Pause => {
    //                     player.pause().await
    //                 },
    //                 SourceChangeRecorderCommand::Stop => {
    //                     player.stop().await
    //                 },
    //                 }
    //         },
    //         None => {
    //             anyhow::bail!("ChangeScriptPlayer not found: {}", player_id);
    //         }
    //     }
    // }

    pub fn contains_test_repo(&self, test_repo_id: &str) -> bool {
        self.test_repos.contains_key(test_repo_id)
    }

    pub fn contains_dataset(&self, dataset_id: &str) -> bool {
        self.datasets.contains_key(dataset_id)
    }

    pub fn get_data_store_path(&self) -> PathBuf {
        self.data_store_path.clone()
    }

    // pub fn get_datasets(&self) -> anyhow::Result<Vec<DataSet>> {
    //     self.test_repo_cache.get_datasets()
    // }

    pub fn get_status(&self) -> &TestDatasetCollectorStatus {
        &self.status
    }

    pub fn get_test_repo(&self, test_repo_id: &str) -> anyhow::Result<Option<TestRepoConfig>> {
        Ok(self.test_repos.get(test_repo_id).cloned())
    }

    pub fn get_test_repos(&self) -> anyhow::Result<Vec<TestRepoConfig>> {
        Ok(self.test_repos.values().cloned().collect())
    }

    pub fn get_test_repo_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.test_repos.keys().cloned().collect())
    }

    // pub async fn start(&mut self) -> anyhow::Result<()> {

    //     match &self.status {
    //         TestDatasetCollectorStatus::Initialized => {
    //             log::debug!("Starting TestDatasetCollector...");
    //         },
    //         TestDatasetCollectorStatus::Started => {
    //             let msg = format!("Test Dataset Collector has already been started, cannot start.");
    //             log::error!("{}", msg);
    //             anyhow::bail!("{}", msg);
    //         },
    //         TestDatasetCollectorStatus::Error(_) => {
    //             let msg = format!("Test Dataset Collector is in an error state, cannot start. TestDatasetCollectorStatus: {:?}", &self.status);
    //             log::error!("{}", msg);
    //             anyhow::bail!("{}", msg);
    //         },
    //     };

    //     // Iterate over the reactivators and start each one if it is configured to start immediately.
    //     // If any of the reactivators fail to start, set the TestDatasetCollectorStatus to Error and return an error.
    //     for (_, player) in &self.source_change_recorders {
    //         if player.get_settings().reactivator.start_immediately {
    //             match player.start().await {
    //                 Ok(_) => {},
    //                 Err(e) => {
    //                     let msg = format!("Error starting ChangeScriptPlayer: {}", e);
    //                     self.status = TestDatasetCollectorStatus::Error(msg);
    //                     bail!("{:?}", self.status);
    //                 }
    //             }
    //         }
    //     }

    //     // Set the TestDatasetCollectorStatus to Started.
    //     log::info!("Test Dataset Collector started successfully");            
    //     self.status = TestDatasetCollectorStatus::Started;

    //     Ok(())
    // }
}


#[derive(Debug)]
pub struct Dataset {
    data_store_path: PathBuf,
    dataset_id: String,
    sources: HashMap<String, DatasetSource>,    
}

impl Dataset {
    pub async fn try_from_config(config: &DatasetConfig, data_store_path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating Dataset from {:#?}", config);

        let data_store_path = data_store_path.join(config.dataset_id.clone());

        let mut dataset = Dataset {
            data_store_path,   
            dataset_id: config.dataset_id.clone(),
            sources: HashMap::new(),
        };
    
        for source_config in &config.sources {
            dataset.add_source(source_config).await?;
        };

        log::debug!("Dataset created -  {:?}", &dataset);

        Ok(dataset)

    }

    pub async fn add_source(&mut self, config: &SourceConfig) -> anyhow::Result<()> {
        log::trace!("Adding DatasetSource from {:#?}", config);

        // Fail if the Dataset already contains the Source
        if self.contains_source(&config.source_id) {
            anyhow::bail!("DatasetSource already exists: {:?}", &config);
        }
        
        let source = DatasetSource::try_from_config(config, self.data_store_path.clone()).await?;

        self.sources.insert(source.source_id.clone(), source);

        Ok(())
    }    

    pub fn contains_source(&self, source_id: &str) -> bool {
        self.sources.contains_key(source_id)
    }
}

// Unit tests
// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_add() {
//         // Assert that 2 + 2 equals 4
//         assert_eq!(2+2, 4);
//     }
// }