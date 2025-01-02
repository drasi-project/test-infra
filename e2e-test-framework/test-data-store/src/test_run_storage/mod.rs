use std::{fmt, path::PathBuf};

use serde::Serialize;
use serde_json::Value;
use tokio::fs;

const SOURCES_FOLDER_NAME: &str = "sources";
const SOURCE_CHANGE_FOLDER_NAME: &str = "source_change";

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct TestRunId {
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: String,
}

impl TestRunId {
    pub fn new(test_repo_id: &str, test_id: &str, test_run_id: &str) -> Self {
        Self {
            test_id: test_id.to_string(),
            test_repo_id: test_repo_id.to_string(),
            test_run_id: test_run_id.to_string(),
        }
    }
}

impl fmt::Display for TestRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.test_repo_id, self.test_id, self.test_run_id
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseTestRunIdError {
    #[error("Invalid format for TestRunId - {0}")]
    InvalidFormat(String),
    #[error("Invalid values for TestRunId - {0}")]
    InvalidValues(String),
}

impl TryFrom<&str> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() == 3 {
            Ok(TestRunId {
                test_repo_id: parts[0].to_string(), 
                test_id: parts[1].to_string(), 
                test_run_id: parts[2].to_string(),
            })
        } else {
            Err(ParseTestRunIdError::InvalidFormat(value.to_string()))
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct TestRunSourceId {
    pub test_run_id: TestRunId,
    pub test_source_id: String,
}

impl TestRunSourceId {
    pub fn new(test_run_id: &TestRunId, test_source_id: &str) -> Self {
        Self {
            test_run_id: test_run_id.clone(),
            test_source_id: test_source_id.to_string(),
        }
    }
}

impl fmt::Display for TestRunSourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}",
            self.test_run_id, self.test_source_id
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
                test_run_id: TestRunId::new(parts[0], parts[1], parts[2]),
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
        log::info!("Creating (replace = {}) TestRunStore in folder: {:?}", replace, &path);

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

    pub async fn contains_test_run(&self, test_run_id: &TestRunId) -> anyhow::Result<bool> {
        Ok(self.path.join(test_run_id.to_string()).exists())
    }

    pub async fn get_test_run_ids(&self) -> anyhow::Result<Vec<TestRunId>> {
        let mut test_run_ids = Vec::new();

        let mut entries = fs::read_dir(&self.path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    test_run_ids.push(TestRunId::try_from(folder_name)?);
                }
            }
        }

        Ok(test_run_ids)
    }    

    pub async fn get_test_run_storage(&self, test_run_id: &TestRunId, replace: bool) -> anyhow::Result<TestRunStorage> {
        log::info!("Getting (replace = {}) TestRunStorage for ID: {:?}", replace, &test_run_id);

        let test_run_path = self.path.join(test_run_id.to_string());
        let sources_path = test_run_path.join(SOURCES_FOLDER_NAME);

        if replace && test_run_path.exists() {
            fs::remove_dir_all(&test_run_path).await?;
        }

        if !test_run_path.exists() {
            // fs::create_dir_all(&path).await?;
            fs::create_dir_all(&sources_path).await?;
        }

        Ok(TestRunStorage {
            id: test_run_id.clone(),
            path: test_run_path,
            sources_path,
        })
    }
}

pub struct TestRunStorage {
    pub id: TestRunId,
    pub path: PathBuf,
    pub sources_path: PathBuf,
}

impl TestRunStorage {
    pub async fn get_source_storage(&self, source_id: &TestRunSourceId, replace: bool) -> anyhow::Result<TestRunSourceStorage> {
        log::info!("Getting (replace = {}) TestRunSourceStorage for ID: {:?}", replace, source_id);

        let source_path = self.sources_path.join(&source_id.test_source_id);
        let source_change_path = source_path.join(SOURCE_CHANGE_FOLDER_NAME);

        if replace && source_path.exists() {
            fs::remove_dir_all(&source_path).await?;
        }

        if !source_path.exists() {
            // fs::create_dir_all(&source_path).await?;
            fs::create_dir_all(&source_change_path).await?;
        }

        Ok(TestRunSourceStorage {
            id: source_id.clone(),
            path: source_path,
            source_change_path,
        })
    }

    pub async fn get_source_ids(&self) -> anyhow::Result<Vec<TestRunSourceId>> {
        let mut test_run_sources = Vec::new();

        let mut entries = fs::read_dir(&self.path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    test_run_sources.push(TestRunSourceId::new(&self.id, folder_name));
                }
            }
        }
        Ok(test_run_sources)        
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunSourceStorage {
    pub id: TestRunSourceId,
    pub path: PathBuf,
    pub source_change_path: PathBuf,
}

impl TestRunSourceStorage {
    pub async fn write_result_summary(&self, summary: &Value) -> anyhow::Result<()> {
        let summary_path = self.path.join("result_summary.json");
        fs::write(summary_path, serde_json::to_string_pretty(summary)?).await?;
        Ok(())
    }
}