use std::path::PathBuf;

use derive_more::Debug;
use tokio::fs;

#[derive(Clone, Debug)]
pub struct DataCollectionStorage {
    pub id: String,
    pub path: PathBuf,
    pub source_path: PathBuf,
}

impl DataCollectionStorage {
    pub async fn new(id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionStorage for ID {:?} in folder: {:?}", id, &parent_path);

        let path = parent_path.join(&id);
        let source_path = path.join("sources/");

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
            fs::create_dir_all(&source_path).await?;
        }

        Ok(Self {
            id: id.to_string(),
            path,
            source_path,
        })
    }

    pub async fn get_source_storage(&self, id: &str, replace: bool) -> anyhow::Result<DataCollectionSourceStorage> {
        log::debug!("Getting DataCollectionSourceStorage for ID: {:?}", &id);

        Ok(DataCollectionSourceStorage::new(id, self.source_path.clone(), replace).await?)
    }

    pub async fn get_source_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut data_collection_sources = Vec::new();

        let mut entries = fs::read_dir(&self.source_path).await?;     
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
    pub async fn new(id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionSourceStorage for ID {:?} in folder: {:?}", id, &parent_path);

        let path = parent_path.join(&id);
        let bootstrap_data_path = path.join("bootstrap_data");            
        let change_log_path = path.join("change_log");

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
            fs::create_dir_all(&bootstrap_data_path).await?;
            fs::create_dir_all(&change_log_path).await?;
        }

        Ok(Self {
            id: id.to_string(),
            path: path,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DataCollectionQueryStorage {
    pub id: String,
    pub path: PathBuf,
}

impl DataCollectionQueryStorage {
    pub async fn new(id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionQueryStorage for ID {:?} in folder: {:?}", id, &parent_path);

        let path = parent_path.join(&id);
        let snapshot_path = path.join("snapshots/");

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
            fs::create_dir_all(&snapshot_path).await?;
        }

        Ok(Self {
            id: id.to_string(),
            path,
        })
    }
}