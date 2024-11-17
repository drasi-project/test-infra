use std::{collections::HashMap, path::PathBuf, sync::Arc};

use futures::future::join_all;
use serde::Serialize;
use tokio::sync::RwLock;

use config::{DataCollectionConfig, TestDataCollectorConfig, DataCollectionSourceConfig};
use source::DataCollectionSource;
use test_data_store::{data_collection_storage::DataCollectionStorage, test_repo_storage::{repo_clients::RemoteTestRepoConfig, TestRepoStorage}, TestDataStore};

pub mod config;
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
    data_collections: HashMap<String, DataCollection>,
    data_store: TestDataStore,
    status: TestDataCollectorStatus,
}

impl TestDataCollector {
    pub async fn new(config: TestDataCollectorConfig) -> anyhow::Result<Self> {   
        log::debug!("Creating TestDataCollector from {:#?}", config);

        let mut test_data_collector = TestDataCollector {
            data_collections: HashMap::new(),
            status: TestDataCollectorStatus::Initialized,
            data_store: TestDataStore::new(config.data_store).await?,
        };

        // Add the initial set of DataCollections.
        // Fail construction if any of the DataCollectionConfigs fail to create.
        for data_collection_config in config.data_collections {
            test_data_collector.add_data_collection(data_collection_config).await?;
        };

        log::debug!("TestDataCollector created -  {:?}", &test_data_collector);

        Ok(test_data_collector)
    }
    
    pub async fn add_data_collection(&mut self, data_collection_config: DataCollectionConfig) -> anyhow::Result<DataCollection> {
        log::trace!("Adding DataCollection from {:#?}", data_collection_config);

        // If the TestDataCollector is in an Error state, return an error.
        if let TestDataCollectorStatus::Error(msg) = &self.status {
            anyhow::bail!("TestDataCollector is in an Error state: {}", msg);
        };

        // Fail if the TestDataCollector already contains a DataCollection with the same ID.
        if self.contains_data_collection(&data_collection_config.id).await? {
            anyhow::bail!("DataCollection already exists: {:?}", &data_collection_config);
        }

        // Create storage for the DataCollection.
        let storage = self.data_store.get_data_collection_storage(&data_collection_config.id).await?;
        
        let data_collection = DataCollection::new(data_collection_config, storage).await?;
        self.data_collections.insert(data_collection.id.clone(), data_collection.clone());

        Ok(data_collection)
    }

    pub async fn add_test_repo(&mut self, test_repo_config: RemoteTestRepoConfig ) -> anyhow::Result<TestRepoStorage> {
        log::trace!("Adding TestRepo from {:#?}", test_repo_config);

        // If the TestDataCollector is in an Error state, return an error.
        if let TestDataCollectorStatus::Error(msg) = &self.status {
            anyhow::bail!("TestDataCollector is in an Error state: {}", msg);
        };

        self.data_store.add_test_repo_storage(test_repo_config.clone()).await
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

    pub async fn contains_test_repo(&self, test_repo_id: &str) -> anyhow::Result<bool> {
        self.data_store.contains_test_repo(test_repo_id).await
    }

    pub async fn contains_data_collection(&self, data_collection_id: &str) -> anyhow::Result<bool>  {
        Ok(self.data_collections.contains_key(data_collection_id))
    }

    pub async fn get_data_store_path(&self) -> anyhow::Result<PathBuf> {
        self.data_store.get_data_store_path().await
    }

    pub async fn get_status(&self) -> &TestDataCollectorStatus {
        &self.status
    }
}


#[derive(Clone, Debug)]
pub struct DataCollection {
    id: String,
    sources: HashMap<String, DataCollectionSource>,    
    storage: DataCollectionStorage,
}

impl DataCollection {
    pub async fn new(config: DataCollectionConfig, storage: DataCollectionStorage) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollection from config {:#?}", config);

        let mut data_collection = DataCollection {
            id: config.id.clone(),
            sources: HashMap::new(),
            storage: storage,
        };
    
        for source_config in config.sources {
            data_collection.add_source(source_config).await?;
        };

        Ok(data_collection)
    }

    pub async fn add_source(&mut self, config: DataCollectionSourceConfig) -> anyhow::Result<DataCollectionSource> {
        log::trace!("Adding DataCollectionSource from config {:#?}", config);

        let source_id = config.source_id.clone();

        // Fail if the DataCollection already contains the Source
        if self.contains_source(&source_id).await? {
            anyhow::bail!("DataCollectionSource already exists: {:?}", &config);
        }
        
        let source = DataCollectionSource::new(config, self.storage.get_source_storage(&source_id, false).await?).await?;

        self.sources.insert(source_id.clone(), source.clone());

        Ok(source)
    }    

    pub async fn contains_source(&self, source_id: &str) -> anyhow::Result<bool> {
        Ok(self.sources.contains_key(source_id))
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
    use std::path::PathBuf;

    use config::{RedisSourceChangeQueueReaderConfig, SourceChangeQueueReaderConfig, SourceChangeRecorderConfig};
    use test_data_store::TestDataStoreConfig;
    use tokio::fs::remove_dir_all;

    use super::*;

    #[tokio::test]
    async fn test_data_collector_data_cache_create() {
        // Create a random path to use as the Data Store folder name.
        let data_store_path = format!("./tests/{}", uuid::Uuid::new_v4().to_string());
        let data_store_path_buf = PathBuf::from(&data_store_path);

        let config = TestDataCollectorConfig {
            data_collections: vec![],
            data_store: TestDataStoreConfig {
                data_collection_folder: None,
                data_store_path: Some(data_store_path),
                delete_data_store: Some(true),
                test_repos: None,
                test_repo_folder: None,
                test_run_folder: None,
            }
        };

        let test_data_collector = TestDataCollector::new(config).await.unwrap();

        // Check that the TestDataCollector is in the Initialized state.
        assert_eq!(test_data_collector.get_status().await, &TestDataCollectorStatus::Initialized);

        // Check that the data cache folder was created.
        assert_eq!(test_data_collector.get_data_store_path().await.unwrap(), data_store_path_buf);

        // Delete the data cache folder and test that the operation was successful;
        assert_eq!(remove_dir_all(data_store_path_buf).await.is_ok(), true);
    }

    #[tokio::test]
    async fn interactive_test() {
        // Create a random String to use as the data store name.
        let data_store_path = format!("tests/{}", uuid::Uuid::new_v4().to_string());
        let data_store_path_buf = PathBuf::from(&data_store_path);

        let config = TestDataCollectorConfig {
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
            data_store: TestDataStoreConfig {
                data_collection_folder: None,
                data_store_path: Some(data_store_path),
                delete_data_store: Some(true),
                test_repos: None,
                test_repo_folder: None,
                test_run_folder: None,
            }
        };

        let mut test_data_collector = TestDataCollector::new(config).await.unwrap();

        // Check that the TestDataCollector is in the Initialized state.
        assert_eq!(test_data_collector.get_status().await, &TestDataCollectorStatus::Initialized);

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