use std::{collections::HashMap, path::PathBuf, sync::Arc};

use futures::future::join_all;
use serde::Serialize;
use tokio::{fs::remove_dir_all, sync::RwLock};

use test_runner::{config::TestRepoConfig, test_repo::test_repo_cache::TestRepoCache};

use config::{DatasetConfig, TestDatasetCollectorConfig, SourceConfig};
use source::DatasetSource;

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
        
        let dataset = Dataset::new(dataset_config, self.data_store_path.clone()).await?;
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

    pub async fn start_dataset_sources(&mut self, dataset_id: &str, record_bootstrap_data: bool, start_change_recorder:bool) -> anyhow::Result<()> {
        log::trace!("Starting Dataset Sources - dataset_id:{}, record_bootstrap_data:{}, start_change_recorder:{}", dataset_id, record_bootstrap_data, start_change_recorder);

        // If the TestDatasetCollector is in an Error state, return an error.
        if let TestDatasetCollectorStatus::Error(msg) = &self.status {
            anyhow::bail!("TestDatasetCollector is in an Error state: {}", msg);
        };

        // Get the Dataset from the TestDatasetCollector or fail if it doesn't exist.
        match self.datasets.get_mut(dataset_id) {
            Some(dataset) => {
                dataset.start_sources(record_bootstrap_data, start_change_recorder).await
            },
            None => {
                anyhow::bail!("Dataset not found: {}", dataset_id);
            }
        }
    }

    pub fn contains_test_repo(&self, test_repo_id: &str) -> bool {
        self.test_repos.contains_key(test_repo_id)
    }

    pub fn contains_dataset(&self, dataset_id: &str) -> bool {
        self.datasets.contains_key(dataset_id)
    }

    pub fn get_data_store_path(&self) -> PathBuf {
        self.data_store_path.clone()
    }

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
}


#[derive(Debug)]
pub struct Dataset {
    data_store_path: PathBuf,
    dataset_id: String,
    sources: HashMap<String, DatasetSource>,    
}

impl Dataset {
    pub async fn new(config: &DatasetConfig, data_store_path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating Dataset from config {:#?}", config);

        let dataset_id = config.dataset_id.clone();

        let data_store_path = data_store_path.join(&dataset_id);

        let mut dataset = Dataset {
            data_store_path,   
            dataset_id: dataset_id,
            sources: HashMap::new(),
        };
    
        for source_config in &config.sources {
            dataset.add_source(source_config).await?;
        };

        Ok(dataset)
    }

    pub async fn add_source(&mut self, config: &SourceConfig) -> anyhow::Result<()> {
        log::trace!("Adding DatasetSource from {:#?}", config);

        let source_id = config.source_id.clone();

        // Fail if the Dataset already contains the Source
        if self.contains_source(&source_id) {
            anyhow::bail!("DatasetSource already exists: {:?}", &config);
        }
        
        let source = DatasetSource::new(config, self.data_store_path.clone()).await?;

        self.sources.insert(source_id.clone(), source);

        Ok(())
    }    

    pub fn contains_source(&self, source_id: &str) -> bool {
        self.sources.contains_key(source_id)
    }

    pub async fn start_sources(&mut self, record_bootstrap_data: bool, start_change_recorder:bool) -> anyhow::Result<()> {
        log::trace!("Starting DatasetSources - record_bootstrap_data:{}, start_change_recorder:{}", record_bootstrap_data, start_change_recorder);

        // TODO: If record_bootstrap_data is true, start the BootstrapDataRecorder for each Source.

        // If start_change_recorder is true, start the SourceChangeRecorder for each DatasetSource.
        // Start each DatasetSource in parallel, and wait for all to complete before returning.
        if start_change_recorder {
            let tasks: Vec<_> = self.sources.iter_mut().map(|(_, source)| {
                let mut source = source.clone();
                tokio::spawn(async move {
                    source.start_source_change_recorder().await
                })
            }).collect();

            let results = join_all(tasks).await;

            for result in results {
                match result {
                    Ok(Ok(_)) => continue, // Task and source.start() both succeeded
                    Ok(Err(e)) => return Err(e.into()), // source.start() returned an error
                    Err(e) => return Err(anyhow::Error::new(e)), // Task itself panicked
                }
            }
        }

        Ok(())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use config::{RedisSourceChangeQueueReaderConfig, SourceChangeQueueReaderConfig, SourceChangeRecorderConfig};

    use super::*;

    // #[tokio::test]
    // async fn test_dataset_collector_data_cache_create() {
    //     // Create a random String to use as the data store name.
    //     let data_store_path = format!("tests/{}", uuid::Uuid::new_v4().to_string());
    //     let data_store_path_buf = PathBuf::from(&data_store_path);

    //     let config = TestDatasetCollectorConfig {
    //         data_store_path,
    //         prune_data_store_path: false,
    //         test_repos: vec![],
    //         datasets: vec![],
    //     };

    //     let test_dataset_collector = TestDatasetCollector::new(config).await.unwrap();

    //     // Check that the TestDatasetCollector is in the Initialized state.
    //     assert_eq!(test_dataset_collector.get_status(), &TestDatasetCollectorStatus::Initialized);

    //     // Check that the data cache folder was created.
    //     assert_eq!(test_dataset_collector.get_data_store_path(), data_store_path_buf);

    //     // Delete the data cache folder and test that the operation was successful;
    //     assert_eq!(remove_dir_all(data_store_path_buf).await.is_ok(), true);
    // }

    // #[tokio::test]
    // async fn test_dataset_collector_test_beacon() {
    //     // Create a random String to use as the data store name.
    //     let data_store_path = format!("tests/{}", uuid::Uuid::new_v4().to_string());
    //     let data_store_path_buf = PathBuf::from(&data_store_path);

    //     let config = TestDatasetCollectorConfig {
    //         data_store_path,
    //         prune_data_store_path: false,
    //         test_repos: vec![],
    //         datasets: vec![DatasetConfig {
    //             dataset_id: "test_beacon".to_string(),
    //             queries: vec![],
    //             sources: vec![SourceConfig {
    //                 source_id: "beacon".to_string(),
    //                 bootstrap_data_recorder: None,
    //                 source_change_recorder: Some(SourceChangeRecorderConfig {
    //                     drain_queue_on_stop: Some(false),
    //                     change_queue_reader: Some(SourceChangeQueueReaderConfig::TestBeacon(TestBeaconSourceChangeQueueReaderConfig {
    //                         interval_ns: Some(1000),
    //                         record_count: Some(10),
    //                     })),
    //                     change_event_loggers: vec![],
    //                 }),
    //                 start_immediately: false,
    //             }],
    //         }],
    //     };

    //     let mut test_dataset_collector = TestDatasetCollector::new(config).await.unwrap();

    //     // Check that the TestDatasetCollector is in the Initialized state.
    //     assert_eq!(test_dataset_collector.get_status(), &TestDatasetCollectorStatus::Initialized);

    //     // Start the Source, which will start the SourceChangeRecorder
    //     test_dataset_collector.start_dataset_sources("test_beacon", false, true).await.unwrap();        

    //     // Wait for 15 seconds
    //     // tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    //     // Stop the Source, which will stop the SourceChangeRecorder
    //     // test_dataset_collector.stop_dataset_source("test_beacon", "beacon").await.unwrap();        

    //     // Delete the data cache folder and test that the operation was successful;
    //     assert_eq!(remove_dir_all(data_store_path_buf).await.is_ok(), true);
    // }

    #[tokio::test]
    async fn interactive_test() {
        // Create a random String to use as the data store name.
        let data_store_path = format!("tests/{}", uuid::Uuid::new_v4().to_string());
        let data_store_path_buf = PathBuf::from(&data_store_path);

        let config = TestDatasetCollectorConfig {
            data_store_path,
            prune_data_store_path: false,
            test_repos: vec![],
            datasets: vec![DatasetConfig {
                dataset_id: "hello-world".to_string(),
                queries: vec![],
                sources: vec![SourceConfig {
                    source_id: "hello-world".to_string(),
                    bootstrap_data_recorder: None,
                    source_change_recorder: Some(SourceChangeRecorderConfig {
                        drain_queue_on_stop: Some(false),
                        change_queue_reader: Some(SourceChangeQueueReaderConfig::Redis(RedisSourceChangeQueueReaderConfig {
                            host: Some("localhost".to_string()),
                            port: Some(6379),
                            queue_name: Some("hello-world-change".to_string()),
                        })),
                        change_event_loggers: vec![],
                    }),
                    start_immediately: false,
                }],
            }],
        };

        let mut test_dataset_collector = TestDatasetCollector::new(config).await.unwrap();

        // Check that the TestDatasetCollector is in the Initialized state.
        assert_eq!(test_dataset_collector.get_status(), &TestDatasetCollectorStatus::Initialized);

        // Start the Source, which will start the SourceChangeRecorder
        test_dataset_collector.start_dataset_sources("hello-world", false, true).await.unwrap();        

        println!("Press Enter to continue...");
        let _ = std::io::stdin().read_line(&mut String::new()).unwrap();

        // Stop the Source, which will stop the SourceChangeRecorder
        // test_dataset_collector.stop_dataset_source("hello-world", "beacon").await.unwrap();        

        // Delete the data cache folder and test that the operation was successful;
        assert_eq!(remove_dir_all(data_store_path_buf).await.is_ok(), true);

    }
}