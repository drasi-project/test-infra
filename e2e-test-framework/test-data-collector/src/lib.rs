use std::{collections::HashMap, path::PathBuf, sync::Arc};

use futures::future::join_all;
use serde::Serialize;
use test_data_store::{config::TestRepoConfig, TestDataStore, TestRepoInfo};
use tokio::sync::RwLock;

use config::{DataCollectionConfig, TestDataCollectorConfig, DataCollectionSourceConfig};
use source::DataCollectionSource;

pub mod config;
// pub mod query;
pub mod source;

// An enum that represents the current state of the TestDataCollector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TestDataCollectorStatus {
    // The Test Data Collector is Initialized and is ready to start.
    Initialized,
    // The Test Data Collector has been started.
    Started,
    // The Test Data Collector is in an Error state. and will not be able to process requests.
    Error(String),
}

pub type SharedTestDataCollector = Arc<RwLock<TestDataCollector>>;

#[derive(Debug)]
pub struct TestDataCollector {
    data_store_path: PathBuf,
    data_collections: HashMap<String, DataCollection>,
    status: TestDataCollectorStatus,
    test_data_store: TestDataStore,
}

impl TestDataCollector {
    pub async fn new(config: TestDataCollectorConfig) -> anyhow::Result<Self> {   

        log::debug!("Creating TestDataCollector from {:#?}", config);

        let data_store_path = PathBuf::from(&config.data_store_path);

        let mut test_data_collector = TestDataCollector {
            data_store_path: data_store_path.clone(),
            data_collections: HashMap::new(),
            status: TestDataCollectorStatus::Initialized,
            test_data_store: TestDataStore::new(data_store_path, config.delete_data_store).await?,
        };

        // Add the initial set of test repos.
        // Fail construction if any of the TestRepoConfigs fail to create.
        for ref test_repo_config in config.test_repos {
            test_data_collector.add_test_repo(test_repo_config).await?;
        };

        // Add the initial set of DataCollections.
        // Fail construction if any of the DataCollectionConfigs fail to create.
        for ref data_collection_config in config.data_collections {
            test_data_collector.add_data_collection(data_collection_config).await?;
        };

        log::debug!("TestDataCollector created -  {:?}", &test_data_collector);

        Ok(test_data_collector)
    }
    
    pub async fn add_data_collection(&mut self, data_collection_config: &DataCollectionConfig) -> anyhow::Result<()> {
        log::trace!("Adding DataCollection from {:#?}", data_collection_config);

        // If the TestDataCollector is in an Error state, return an error.
        if let TestDataCollectorStatus::Error(msg) = &self.status {
            anyhow::bail!("TestDataCollector is in an Error state: {}", msg);
        };
        
        let data_collection = DataCollection::new(data_collection_config, self.data_store_path.clone()).await?;
        self.data_collections.insert(data_collection.id.clone(), data_collection);

        Ok(())
    }

    pub async fn add_test_repo(&mut self, test_repo_config: &TestRepoConfig ) -> anyhow::Result<()> {
        log::trace!("Adding TestRepo from {:#?}", test_repo_config);

        // If the TestDataCollector is in an Error state, return an error.
        if let TestDataCollectorStatus::Error(msg) = &self.status {
            anyhow::bail!("TestDataCollector is in an Error state: {}", msg);
        };

        self.test_data_store.add_test_repo(test_repo_config.clone()).await?;  

        Ok(())
    }

    pub async fn start_data_collection_sources(&mut self, data_collection_id: &str, record_bootstrap_data: bool, start_change_recorder:bool) -> anyhow::Result<()> {
        log::trace!("Starting DataCollection Sources - data_collection_id:{}, record_bootstrap_data:{}, start_change_recorder:{}", data_collection_id, record_bootstrap_data, start_change_recorder);

        // If the TestDataCollector is in an Error state, return an error.
        if let TestDataCollectorStatus::Error(msg) = &self.status {
            anyhow::bail!("TestDataCollector is in an Error state: {}", msg);
        };

        // Get the DataCollection from the TestDataCollector or fail if it doesn't exist.
        match self.data_collections.get_mut(data_collection_id) {
            Some(data_collection) => {
                data_collection.start_sources(record_bootstrap_data, start_change_recorder).await
            },
            None => {
                anyhow::bail!("DataCollection not found: {}", data_collection_id);
            }
        }
    }

    pub fn contains_test_repo(&self, test_repo_id: &str) -> bool {
        self.test_data_store.contains_test_repo(test_repo_id)
    }

    pub fn contains_data_collection(&self, data_collection_id: &str) -> bool {
        self.data_collections.contains_key(data_collection_id)
    }

    pub fn get_data_store_path(&self) -> PathBuf {
        self.data_store_path.clone()
    }

    pub fn get_status(&self) -> &TestDataCollectorStatus {
        &self.status
    }

    pub fn get_test_repo(&self, test_repo_id: &str) -> anyhow::Result<Option<TestRepoInfo>> {
        self.test_data_store.get_test_repo_info(test_repo_id)
    }

    pub fn get_test_repos(&self) -> anyhow::Result<Vec<TestRepoInfo>> {
        self.test_data_store.get_test_repos_info()
    }

    pub fn get_test_repo_ids(&self) -> anyhow::Result<Vec<String>> {
        self.test_data_store.get_test_repo_ids()
    }
}


#[derive(Debug)]
pub struct DataCollection {
    data_store_path: PathBuf,
    id: String,
    sources: HashMap<String, DataCollectionSource>,    
}

impl DataCollection {
    pub async fn new(config: &DataCollectionConfig, data_store_path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollection from config {:#?}", config);

        let id = config.id.clone();

        let data_store_path = data_store_path.join(&id);

        let mut data_collection = DataCollection {
            data_store_path,   
            id,
            sources: HashMap::new(),
        };
    
        for source_config in &config.sources {
            data_collection.add_source(source_config).await?;
        };

        Ok(data_collection)
    }

    pub async fn add_source(&mut self, config: &DataCollectionSourceConfig) -> anyhow::Result<()> {
        log::trace!("Adding DataCollectionSource from {:#?}", config);

        let source_id = config.source_id.clone();

        // Fail if the DataCollection already contains the Source
        if self.contains_source(&source_id) {
            anyhow::bail!("DataCollectionSource already exists: {:?}", &config);
        }
        
        let source = DataCollectionSource::new(config, self.data_store_path.clone()).await?;

        self.sources.insert(source_id.clone(), source);

        Ok(())
    }    

    pub fn contains_source(&self, source_id: &str) -> bool {
        self.sources.contains_key(source_id)
    }

    pub async fn start_sources(&mut self, record_bootstrap_data: bool, start_change_recorder:bool) -> anyhow::Result<()> {
        log::trace!("Starting DataCollectionSources - record_bootstrap_data:{}, start_change_recorder:{}", record_bootstrap_data, start_change_recorder);

        // TODO: If record_bootstrap_data is true, start the BootstrapDataRecorder for each Source.

        // If start_change_recorder is true, start the SourceChangeRecorder for each DataCollectionSource.
        // Start each DataCollectionSource in parallel, and wait for all to complete before returning.
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
    use tokio::fs::remove_dir_all;

    use super::*;

    #[tokio::test]
    async fn test_data_collector_data_cache_create() {
        // Create a random String to use as the data store name.
        let data_store_path = format!("tests/{}", uuid::Uuid::new_v4().to_string());
        let data_store_path_buf = PathBuf::from(&data_store_path);

        let config = TestDataCollectorConfig {
            data_store_path,
            delete_data_store: false,
            test_repos: vec![],
            data_collections: vec![],
        };

        let test_data_collector = TestDataCollector::new(config).await.unwrap();

        // Check that the TestDataCollector is in the Initialized state.
        assert_eq!(test_data_collector.get_status(), &TestDataCollectorStatus::Initialized);

        // Check that the data cache folder was created.
        assert_eq!(test_data_collector.get_data_store_path(), data_store_path_buf);

        // Delete the data cache folder and test that the operation was successful;
        assert_eq!(remove_dir_all(data_store_path_buf).await.is_ok(), true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn interactive_test() {
        // Create a random String to use as the data store name.
        let data_store_path = format!("tests/{}", uuid::Uuid::new_v4().to_string());
        let data_store_path_buf = PathBuf::from(&data_store_path);

        let config = TestDataCollectorConfig {
            data_store_path,
            delete_data_store: false,
            test_repos: vec![],
            data_collections: vec![DataCollectionConfig {
                id: "hello-world".to_string(),
                queries: vec![],
                sources: vec![DataCollectionSourceConfig {
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

        let mut test_data_collector = TestDataCollector::new(config).await.unwrap();

        // Check that the TestDataCollector is in the Initialized state.
        assert_eq!(test_data_collector.get_status(), &TestDataCollectorStatus::Initialized);

        // Start the Source, which will start the SourceChangeRecorder
        test_data_collector.start_data_collection_sources("hello-world", false, true).await.unwrap();        

        println!("Press Enter to continue...");
        let _ = std::io::stdin().read_line(&mut String::new()).unwrap();

        // Stop the Source, which will stop the SourceChangeRecorder
        // test_data_collector.stop_data_collection_source("hello-world", "beacon").await.unwrap();        

        // Delete the data cache folder and test that the operation was successful;
        assert_eq!(remove_dir_all(data_store_path_buf).await.is_ok(), true);

    }
}