// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;

use derive_more::Debug;
use tokio::fs;

const SOURCES_FOLDER_NAME: &str = "sources";
const BOOTSTRAP_DATA_FOLDER_NAME: &str = "bootstrap_data";
const CHANGE_LOG_FOLDER_NAME: &str = "change_log";
const SNAPSHOTS_FOLDER_NAME: &str = "snapshots";

#[derive(Clone, Debug)]
pub struct DataCollectionStore {
    pub path: PathBuf,
}

impl DataCollectionStore {
    pub async fn new(folder_name: String, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {

        let path = parent_path.join(&folder_name);
        log::debug!("Creating DataCollectionStore in folder: {:?}", &parent_path);

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
        }

        Ok(Self {
            path,
        })
    }

    pub async fn contains_data_collection(&self, id: &str) -> anyhow::Result<bool> {
        let path = self.path.join(id);
        Ok(path.exists())
    }

    pub async fn contains_data_collection_source(&self, data_collection_id: &str, source_id: &str) -> anyhow::Result<bool> {
        let path = self.path.join(format!("{}/{}/{}", data_collection_id, SOURCES_FOLDER_NAME, &source_id));
        Ok(path.exists())
    }

    pub async fn get_data_collection_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut data_collection_ids = Vec::new();

        let mut entries = fs::read_dir(&self.path).await?;     
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
    
    pub async fn get_data_collection_storage(&self, id: &str, replace: bool) -> anyhow::Result<DataCollectionStorage> {
        log::debug!("Getting DataCollectionStorage for ID: {:?}", &id);

        DataCollectionStorage::new(id, self.path.clone(), replace).await
    }
}

#[derive(Clone, Debug)]
pub struct DataCollectionStorage {
    pub id: String,
    pub path: PathBuf,
    pub sources_path: PathBuf,
}

impl DataCollectionStorage {
    pub async fn new(id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionStorage for ID {:?} in folder: {:?}", id, &parent_path);

        let path = parent_path.join(id);
        let sources_path = path.join(SOURCES_FOLDER_NAME);

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            // fs::create_dir_all(&path).await?;
            fs::create_dir_all(&sources_path).await?;
        }

        Ok(Self {
            id: id.to_string(),
            path,
            sources_path,
        })
    }

    pub async fn get_source_storage(&self, id: &str, replace: bool) -> anyhow::Result<DataCollectionSourceStorage> {
        log::debug!("Getting DataCollectionSourceStorage for ID: {:?}", &id);

        DataCollectionSourceStorage::new(id, self.sources_path.clone(), replace).await
    }

    pub async fn get_source_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut data_collection_sources = Vec::new();

        let mut entries = fs::read_dir(&self.sources_path).await?;     
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

        let path = parent_path.join(id);
        let bootstrap_data_path = path.join(BOOTSTRAP_DATA_FOLDER_NAME);            
        let change_log_path = path.join(CHANGE_LOG_FOLDER_NAME);

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
    pub async fn new(id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        log::debug!("Creating DataCollectionQueryStorage for ID {:?} in folder: {:?}", id, &parent_path);

        let path = parent_path.join(id);
        let snapshot_path = path.join(SNAPSHOTS_FOLDER_NAME);

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