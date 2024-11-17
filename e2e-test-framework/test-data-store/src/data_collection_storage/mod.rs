use std::path::PathBuf;

use derive_more::Debug;
use tokio::fs;

#[derive(Clone, Debug)]
pub struct DataCollectionStorage {
    pub id: String,
    pub path: PathBuf,
}

impl DataCollectionStorage {
    pub async fn new(id: &str, path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionStorage for ID {:?} in folder: {:?}", id, &path);

        // Create the data store folder if it doesn't exist.
        if !path.exists() {
            log::info!("Creating DataCollectionStorage folder: {:?}", &path);
            tokio::fs::create_dir_all(&path).await?
        }

        Ok(Self {
            id: id.to_string(),
            path,
        })
    }

    pub async fn get_source_storage(&self, id: &str) -> anyhow::Result<DataCollectionSourceStorage> {
        log::debug!("Getting DataCollectionSourceStorage for ID: {:?}", &id);

        let mut path = self.path.clone();
        path.push(format!("sources/{}/", id));

        Ok(DataCollectionSourceStorage::new(id, path).await?)
    }

    pub async fn get_source_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut data_collection_sources = Vec::new();

        let mut entries = fs::read_dir(&self.path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    data_collection_sources.push(folder_name.to_string());
                }
            }
        }

        Ok(data_collection_sources)        
    }
}

#[derive(Clone, Debug)]
pub struct DataCollectionSourceStorage {
    pub id: String,
    pub path: PathBuf,
}

impl DataCollectionSourceStorage {
    pub async fn new(id: &str, path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionSourceStorage for ID {:?} in folder: {:?}", id, &path);

        // Create the data storage folders if they don't exist.
        // Need a folder for the bootstrap data.
        let bootstrap_data_path = path.join("bootstrap_data");            
        if !bootstrap_data_path.exists() {
            fs::create_dir_all(&bootstrap_data_path).await?;
        }

        // Need a folder for the change log data.
        let change_log_path = path.join("change_log");
        if !change_log_path.exists() {
            fs::create_dir_all(&change_log_path).await?;
        }

        Ok(Self {
            id: id.to_string(),
            path,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DataCollectionQueryStorage {
    pub id: String,
    pub path: PathBuf,
}

impl DataCollectionQueryStorage {
    pub async fn new(id: &str, path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionQueryStorage for ID {:?} in folder: {:?}", id, &path);

        // Create the data storage folders if they don't exist.
        // Need a folder for the result snapshots.
        let result_snapshot_path = path.join("result_snapshot");            
        if !result_snapshot_path.exists() {
            fs::create_dir_all(&result_snapshot_path).await?;
        }

        Ok(Self {
            id: id.to_string(),
            path,
        })
    }
}