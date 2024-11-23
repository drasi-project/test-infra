use std::{fmt, path::PathBuf};

use serde::Serialize;
use tokio::fs;

const SOURCES_FOLDER_NAME: &str = "sources";
const BOOTSTRAP_DATA_FOLDER_NAME: &str = "bootstrap_data";
const CHANGE_LOG_FOLDER_NAME: &str = "change_log";

fn get_test_run_id(test_id: &str, test_run_id: &str) -> String {
    format!("{}__{}", test_id, test_run_id)
}

fn get_test_run_source_id(repo_id: &str, source_id: &str) -> String {
    format!("{}__{}", repo_id, source_id)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct TestRunSourceId {
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: String,
    pub test_source_id: String,
}

impl TestRunSourceId {
    pub fn new(test_run_id: &str, test_repo_id: &str, test_id: &str, test_source_id: &str) -> Self {
        Self {
            test_id: test_id.to_string(),
            test_repo_id: test_repo_id.to_string(),
            test_run_id: test_run_id.to_string(),
            test_source_id: test_source_id.to_string(),
        }
    }
}

impl fmt::Display for TestRunSourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}.{}",
            self.test_run_id, self.test_repo_id, self.test_id, self.test_source_id
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseTestRunSourceIdError {
    #[error("Invalid format for TestRunSourceId - {0}")]
    InvalidFormat(String),
    #[error("Invalid values for TestRunSourceId - {0}")]
    InvalidValues(String),
}

impl TryFrom<&str> for TestRunSourceId {
    type Error = ParseTestRunSourceIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() == 4 {
            Ok(TestRunSourceId {
                test_run_id: parts[0].to_string(),
                test_repo_id: parts[1].to_string(),
                test_id: parts[2].to_string(),
                test_source_id: parts[3].to_string(),
            })
        } else {
            Err(ParseTestRunSourceIdError::InvalidFormat(value.to_string()))
        }
    }
}

#[derive(Clone, Debug)]
pub struct TestRunStore {
    pub path: PathBuf,
}

impl TestRunStore {
    pub async fn new(folder_name: String, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {

        let path = parent_path.join(&folder_name);
        log::debug!("Creating TestRunStore in folder: {:?}", &parent_path);

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

    pub async fn contains_test_run(&self, test_id: &str, test_run_id: &str) -> anyhow::Result<bool> {
        let path = self.path.join(get_test_run_id(test_id, test_run_id));
        Ok(path.exists())
    }

    pub async fn get_test_run_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut test_run_ids = Vec::new();

        let mut entries = fs::read_dir(&self.path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    test_run_ids.push(folder_name.to_string());
                }
            }
        }

        Ok(test_run_ids)
    }    

    pub async fn get_test_run_storage(&self, test_id: &str, test_run_id: &str, replace: bool) -> anyhow::Result<TestRunStorage> {
        log::debug!("Getting TestRunStorage for ID: {:?}", get_test_run_id(test_id, test_run_id));

        Ok(TestRunStorage::new(test_id, test_run_id, self.path.clone(), replace).await?)
    }
}

pub struct TestRunStorage {
    pub id: String,
    pub path: PathBuf,
    pub source_path: PathBuf,
    pub test_id: String,
    pub test_run_id: String,
}

impl TestRunStorage {
    pub async fn new(test_id: &str, test_run_id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        
        let id = get_test_run_id(test_id, test_run_id);
        log::debug!("Creating TestRunStorage for ID {:?} in folder: {:?}", &id, &parent_path);

        let path = parent_path.join(&id);
        let source_path = path.join(SOURCES_FOLDER_NAME);

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
            fs::create_dir_all(&source_path).await?;
        }

        Ok(Self {
            id,
            test_id: test_id.to_string(),
            test_run_id: test_run_id.to_string(),
            path,
            source_path,
        })
    }

    pub async fn get_source_storage(&self, repo_id: &str, source_id: &str, replace: bool) -> anyhow::Result<TestRunSourceStorage> {
        log::debug!("Getting TestRunSourceStorage for ID: {:?}", get_test_run_source_id(repo_id, source_id));

        Ok(TestRunSourceStorage::new(&self.test_id, &self.test_run_id, repo_id, source_id, self.source_path.clone(), replace).await?)
    }

    pub async fn get_source_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut test_run_sources = Vec::new();

        let mut entries = fs::read_dir(&self.source_path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    test_run_sources.push(folder_name.to_string());
                }
            }
        }

        Ok(test_run_sources)        
    }
}

#[derive(Clone, Debug)]
pub struct TestRunSourceStorage {
    pub id: String,
    pub path: PathBuf,
    pub repo_id: String,
    pub source_id: String,
    pub test_id: String,
    pub test_run_id: String,
}

impl TestRunSourceStorage {
    pub async fn new(test_id: &str, test_run_id: &str, repo_id: &str, source_id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        
        let id = get_test_run_source_id(repo_id, source_id);
        log::debug!("Creating TestRunSourceStorage for ID {:?} in folder: {:?}", &id, &parent_path);

        let path = parent_path.join(&id);
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
            id,
            path: path,
            repo_id: repo_id.to_string(),
            source_id: source_id.to_string(),
            test_id: test_id.to_string(),
            test_run_id: test_run_id.to_string(),
        })
    }
}