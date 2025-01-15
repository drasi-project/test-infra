use std::{collections::HashMap, fmt, sync::Arc};

use futures::future::join_all;
use serde::Serialize;

use config::{DataCollectionConfig, DataCollectorConfig, DataCollectionSourceConfig};
use source::DataCollectionSource;
use test_data_store::{data_collection_storage::DataCollectionStorage, TestDataStore};
use tokio::sync::RwLock;

pub mod config;
pub mod source;

// An enum that represents the current state of the DataCollector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum DataCollectorStatus {
    // The Test Data Collector is Initialized and is ready to start.
    Initialized,
    // The Test Data Collector has been started.
    Started,
    // The Test Data Collector is in an Error state. and will not be able to process requests.
    Error(String),
}

impl fmt::Display for DataCollectorStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataCollectorStatus::Initialized => write!(f, "Initialized"),
            DataCollectorStatus::Started => write!(f, "Started"),
            DataCollectorStatus::Error(msg) => write!(f, "Error: {}", msg),
        }
    }
}

#[derive(Debug)]
pub struct DataCollector {
    data_collections: Arc<RwLock<HashMap<String, DataCollection>>>,
    data_store: Arc<TestDataStore>,
    status: Arc<RwLock<DataCollectorStatus>>,
}

impl DataCollector {
    pub async fn new(config: DataCollectorConfig, data_store: Arc<TestDataStore>) -> anyhow::Result<Self> {   
        log::debug!("Creating DataCollector from {:#?}", config);

        let data_collector = DataCollector {
            data_collections: Arc::new(RwLock::new(HashMap::new())),
            data_store,
            status: Arc::new(RwLock::new(DataCollectorStatus::Initialized)),
        };

        // Add the initial set of DataCollections.
        // Fail construction if any of the DataCollectionConfigs fail to create.
        for data_collection_config in config.data_collections {
            data_collector.add_data_collection(data_collection_config).await?;
        };

        log::debug!("DataCollector created -  {:?}", &data_collector);

        Ok(data_collector)
    }
    
    pub async fn add_data_collection(&self, data_collection_config: DataCollectionConfig) -> anyhow::Result<DataCollection> {
        log::trace!("Adding DataCollection from {:#?}", data_collection_config);

        // If the DataCollector is in an Error state, return an error.
        if let DataCollectorStatus::Error(msg) = &self.get_status().await? {
            anyhow::bail!("DataCollector is in an Error state: {}", msg);
        };

        let mut collections_lock = self.data_collections.write().await;

        // Fail if the DataCollector already contains a DataCollection with the same ID.
        if self.contains_data_collection(&data_collection_config.id).await? {
            anyhow::bail!("DataCollection already exists: {:?}", &data_collection_config);
        }

        // Create storage for the DataCollection.
        let storage = self.data_store.get_data_collection_storage(&data_collection_config.id).await?;
        
        let data_collection = DataCollection::new(data_collection_config, storage).await?;
        collections_lock.insert(data_collection.id.clone(), data_collection.clone());

        Ok(data_collection)
    }

    pub async fn contains_data_collection(&self, data_collection_id: &str) -> anyhow::Result<bool>  {
        Ok(self.data_collections.read().await.contains_key(data_collection_id))
    }

    pub async fn get_data_collection_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.data_collections.read().await.keys().cloned().collect())
    }

    pub async fn get_status(&self) -> anyhow::Result<DataCollectorStatus> {
        Ok(self.status.read().await.clone())
    }

    async fn _set_status(&self, status: DataCollectorStatus) {
        let mut write_lock = self.status.write().await;
        *write_lock = status.clone();
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        log::trace!("Starting DataCollector");

        Ok(())
    }

    pub async fn start_data_collection_sources(&self, data_collection_id: &str, record_bootstrap_data: bool, start_change_recorder:bool) -> anyhow::Result<()> {
        log::trace!("Starting DataCollection Sources - data_collection_id:{}, record_bootstrap_data:{}, start_change_recorder:{}", data_collection_id, record_bootstrap_data, start_change_recorder);

        // If the DataCollector is in an Error state, return an error.
        if let DataCollectorStatus::Error(msg) = &self.get_status().await? {
            anyhow::bail!("DataCollector is in an Error state: {}", msg);
        };

        // Get the DataCollection from the DataCollector or fail if it doesn't exist.
        match self.data_collections.write().await.get_mut(data_collection_id) {
            Some(data_collection) => {
                data_collection.start_sources(record_bootstrap_data, start_change_recorder).await
            },
            None => {
                anyhow::bail!("DataCollection not found: {}", data_collection_id);
            }
        }
    }
}


#[derive(Clone, Debug)]
pub struct DataCollection {
    id: String,
    sources: Arc<RwLock<HashMap<String, DataCollectionSource>>>,  
    storage: DataCollectionStorage
}

impl DataCollection {
    pub async fn new(config: DataCollectionConfig, storage: DataCollectionStorage) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollection from config {:#?}", config);

        let data_collection = DataCollection {
            id: config.id.clone(),
            sources: Arc::new(RwLock::new(HashMap::new())),
            storage,
        };
    
        for source_config in config.sources {
            data_collection.add_source(source_config).await?;
        };

        Ok(data_collection)
    }

    pub async fn add_source(&self, config: DataCollectionSourceConfig) -> anyhow::Result<DataCollectionSource> {
        log::trace!("Adding DataCollectionSource from config {:#?}", config);

        let source_id = config.source_id.clone();

        // Fail if the DataCollection already contains the Source
        if self.contains_source(&source_id).await? {
            anyhow::bail!("DataCollectionSource already exists: {:?}", &config);
        }
        
        let source = DataCollectionSource::new(config, self.storage.get_source_storage(&source_id, false).await?).await?;

        let mut sources_lock = self.sources.write().await;
        sources_lock.insert(source_id.clone(), source.clone());

        Ok(source)
    }    

    pub async fn contains_source(&self, source_id: &str) -> anyhow::Result<bool> {
        Ok(self.sources.read().await.contains_key(source_id))
    }

    pub async fn start_sources(&self, record_bootstrap_data: bool, start_change_recorder:bool) -> anyhow::Result<()> {
        log::trace!("Starting DataCollectionSources - record_bootstrap_data:{}, start_change_recorder:{}", record_bootstrap_data, start_change_recorder);

        // TODO: If record_bootstrap_data is true, start the BootstrapDataRecorder for each Source.

        // If start_change_recorder is true, start the SourceChangeRecorder for each DataCollectionSource.
        // Start each DataCollectionSource in parallel, and wait for all to complete before returning.
        if start_change_recorder {
            let mut sources_lock = self.sources.write().await;
            let tasks: Vec<_> = sources_lock.iter_mut().map(|(_, source)| {
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

    use test_data_store::{TestDataStore, TestDataStoreConfig};

    use super::*;

    #[tokio::test]
    async fn create_datacollector() -> anyhow::Result<()> {
        // Create a random path to use as the Data Store folder name.
        let data_store_path = format!("./tests/{}", uuid::Uuid::new_v4().to_string());
        let data_store_path_buf = PathBuf::from(&data_store_path);

        let data_store_config = TestDataStoreConfig {
            data_store_path: Some(data_store_path),
            delete_on_stop: Some(true),
            ..TestDataStoreConfig::default()
        };
        let test_data_store = Arc::new(TestDataStore::new(data_store_config).await?);

        let config = DataCollectorConfig {
            data_collections: vec![],
        };
        let data_collector = DataCollector::new(config, test_data_store).await.unwrap();

        // Check that the DataCollector is in the Initialized state.
        assert_eq!(data_collector.get_status().await?, DataCollectorStatus::Initialized);

        drop(data_collector);        

        // Ensure the Data Store folder was removed.
        assert_eq!(data_store_path_buf.exists(), false);

        Ok(())
    }

    // #[tokio::test]
    // async fn interactive_test()  -> anyhow::Result<()> {
    //     // Create a random String to use as the data store name.
    //     let data_store_path = format!("tests/{}", uuid::Uuid::new_v4().to_string());
    //     let data_store_path_buf = PathBuf::from(&data_store_path);

    //     let data_store_config = TestDataStoreConfig {
    //         data_collection_folder: None,
    //         data_store_path: Some(data_store_path),
    //         delete_on_start: Some(true),
    //         delete_on_stop: Some(true),
    //         test_repos: None,
    //         test_repo_folder: None,
    //         test_run_folder: None,
    //     };
    //     let test_data_store = Arc::new(TestDataStore::new(data_store_config).await?);

    //     let config = DataCollectorConfig {
    //         data_collections: vec![DataCollectionConfig {
    //             id: "hello-world".to_string(),
    //             queries: vec![],
    //             sources: vec![DataCollectionSourceConfig {
    //                 source_id: "hello-world".to_string(),
    //                 bootstrap_data_recorder: None,
    //                 source_change_recorder: Some(SourceChangeRecorderConfig {
    //                     drain_queue_on_stop: Some(false),
    //                     change_queue_reader: Some(SourceChangeQueueReaderConfig::Redis(RedisSourceChangeQueueReaderConfig {
    //                         host: Some("localhost".to_string()),
    //                         port: Some(6379),
    //                         queue_name: Some("hello-world-change".to_string()),
    //                     })),
    //                     change_event_loggers: vec![],
    //                 }),
    //                 start_immediately: false,
    //             }],
    //         }]
    //     };
    //     let data_collector = DataCollector::new(config, test_data_store).await.unwrap();

    //     // Check that the DataCollector is in the Initialized state.
    //     assert_eq!(data_collector.get_status().await?, DataCollectorStatus::Initialized);

    //     // Start the Source, which will start the SourceChangeRecorder
    //     data_collector.start_data_collection_sources("hello-world", false, true).await.unwrap();        

    //     println!("Press Enter to continue...");
    //     let _ = std::io::stdin().read_line(&mut String::new()).unwrap();

    //     // Stop the Source, which will stop the SourceChangeRecorder
    //     // data_collector.stop_data_collection_source("hello-world", "beacon").await.unwrap();        

    //     // Ensure the Data Store folder was removed.
    //     assert_eq!(data_store_path_buf.exists(), false);

    //     Ok(())
    // }
}