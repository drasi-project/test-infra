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

use std::{fmt, path::PathBuf};

use serde::Serialize;
use serde_json::Value;
use tokio::fs;

const QUERIES_FOLDER_NAME: &str = "queries";
const QUERY_RESULT_LOG_FOLDER_NAME: &str = "result_stream_log";

const SOURCES_FOLDER_NAME: &str = "sources";
const SOURCE_CHANGE_LOG_FOLDER_NAME: &str = "source_change_log";

const REACTIONS_FOLDER_NAME: &str = "reactions";
const REACTION_OUTPUT_LOG_FOLDER_NAME: &str = "output_log";

const DRASI_LIB_INSTANCES_FOLDER_NAME: &str = "drasi_lib_instances";

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
            Ok(Self {
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
        write!(f, "{}.{}", self.test_run_id, self.test_source_id)
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
            Ok(Self {
                test_run_id: TestRunId::new(parts[0], parts[1], parts[2]),
                test_source_id: parts[3].to_string(),
            })
        } else {
            Err(ParseTestRunSourceIdError::InvalidFormat(value.to_string()))
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct TestRunQueryId {
    pub test_run_id: TestRunId,
    pub test_query_id: String,
}

impl TestRunQueryId {
    pub fn new(test_run_id: &TestRunId, test_query_id: &str) -> Self {
        Self {
            test_run_id: test_run_id.clone(),
            test_query_id: test_query_id.to_string(),
        }
    }
}

impl fmt::Display for TestRunQueryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.test_run_id, self.test_query_id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseTestRunQueryIdError {
    #[error("Invalid format for TestRunQueryId - {0}")]
    InvalidFormat(String),
    #[error("Invalid values for TestRunQueryId - {0}")]
    InvalidValues(String),
}

impl TryFrom<&str> for TestRunQueryId {
    type Error = ParseTestRunQueryIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() == 4 {
            Ok(Self {
                test_run_id: TestRunId::new(parts[0], parts[1], parts[2]),
                test_query_id: parts[3].to_string(),
            })
        } else {
            Err(ParseTestRunQueryIdError::InvalidFormat(value.to_string()))
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct TestRunReactionId {
    pub test_run_id: TestRunId,
    pub test_reaction_id: String,
}

impl TestRunReactionId {
    pub fn new(test_run_id: &TestRunId, test_reaction_id: &str) -> Self {
        Self {
            test_run_id: test_run_id.clone(),
            test_reaction_id: test_reaction_id.to_string(),
        }
    }
}

impl fmt::Display for TestRunReactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.test_run_id, self.test_reaction_id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseTestRunReactionIdError {
    #[error("Invalid format for TestRunReactionId - {0}")]
    InvalidFormat(String),
    #[error("Invalid values for TestRunReactionId - {0}")]
    InvalidValues(String),
}

impl TryFrom<&str> for TestRunReactionId {
    type Error = ParseTestRunReactionIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() == 4 {
            Ok(Self {
                test_run_id: TestRunId::new(parts[0], parts[1], parts[2]),
                test_reaction_id: parts[3].to_string(),
            })
        } else {
            Err(ParseTestRunReactionIdError::InvalidFormat(
                value.to_string(),
            ))
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct TestRunDrasiLibInstanceId {
    pub test_run_id: TestRunId,
    pub test_drasi_lib_instance_id: String,
}

impl TestRunDrasiLibInstanceId {
    pub fn new(test_run_id: &TestRunId, test_drasi_lib_instance_id: &str) -> Self {
        Self {
            test_run_id: test_run_id.clone(),
            test_drasi_lib_instance_id: test_drasi_lib_instance_id.to_string(),
        }
    }
}

impl fmt::Display for TestRunDrasiLibInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}",
            self.test_run_id, self.test_drasi_lib_instance_id
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseTestRunDrasiLibInstanceIdError {
    #[error("Invalid format for TestRunDrasiLibInstanceId - {0}")]
    InvalidFormat(String),
    #[error("Invalid values for TestRunDrasiLibInstanceId - {0}")]
    InvalidValues(String),
}

impl TryFrom<&str> for TestRunDrasiLibInstanceId {
    type Error = ParseTestRunDrasiLibInstanceIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() == 4 {
            Ok(Self {
                test_run_id: TestRunId::new(parts[0], parts[1], parts[2]),
                test_drasi_lib_instance_id: parts[3].to_string(),
            })
        } else {
            Err(ParseTestRunDrasiLibInstanceIdError::InvalidFormat(
                value.to_string(),
            ))
        }
    }
}

#[derive(Clone, Debug)]
pub struct TestRunStore {
    pub path: PathBuf,
}

impl TestRunStore {
    pub async fn new(
        folder_name: String,
        parent_path: PathBuf,
        replace: bool,
    ) -> anyhow::Result<Self> {
        let path = parent_path.join(&folder_name);
        log::debug!(
            "Creating (replace = {}) TestRunStore in folder: {:?}",
            replace,
            &path
        );

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
        }

        Ok(Self { path })
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

    pub async fn get_test_run_storage(
        &self,
        test_run_id: &TestRunId,
        replace: bool,
    ) -> anyhow::Result<TestRunStorage> {
        log::debug!(
            "Getting (replace = {}) TestRunStorage for ID: {:?}",
            replace,
            &test_run_id
        );

        let test_run_path = self.path.join(test_run_id.to_string());
        let queries_path = test_run_path.join(QUERIES_FOLDER_NAME);
        let sources_path = test_run_path.join(SOURCES_FOLDER_NAME);
        let reactions_path = test_run_path.join(REACTIONS_FOLDER_NAME);
        let drasi_lib_instances_path = test_run_path.join(DRASI_LIB_INSTANCES_FOLDER_NAME);

        if replace && test_run_path.exists() {
            fs::remove_dir_all(&test_run_path).await?;
        }

        if !test_run_path.exists() {
            fs::create_dir_all(&test_run_path).await?;
        }

        Ok(TestRunStorage {
            id: test_run_id.clone(),
            path: test_run_path,
            queries_path,
            sources_path,
            reactions_path,
            drasi_lib_instances_path,
        })
    }
}

pub struct TestRunStorage {
    pub id: TestRunId,
    pub path: PathBuf,
    pub queries_path: PathBuf,
    pub sources_path: PathBuf,
    pub reactions_path: PathBuf,
    pub drasi_lib_instances_path: PathBuf,
}

impl TestRunStorage {
    pub async fn get_query_storage(
        &self,
        query_id: &TestRunQueryId,
        replace: bool,
    ) -> anyhow::Result<TestRunQueryStorage> {
        log::debug!("Getting (replace = {replace}) TestRunQueryStorage for ID: {query_id:?}");

        let query_path = self.queries_path.join(&query_id.test_query_id);
        let result_change_path = query_path.join(QUERY_RESULT_LOG_FOLDER_NAME);

        if replace && query_path.exists() {
            fs::remove_dir_all(&query_path).await?;
        }

        if !query_path.exists() {
            fs::create_dir_all(&result_change_path).await?;
        }

        Ok(TestRunQueryStorage {
            id: query_id.clone(),
            path: query_path,
            result_change_path,
        })
    }

    pub async fn get_query_ids(&self) -> anyhow::Result<Vec<TestRunQueryId>> {
        let mut test_run_queries = Vec::new();

        if self.queries_path.exists() {
            let mut entries = fs::read_dir(&self.queries_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let metadata = entry.metadata().await?;
                if metadata.is_dir() {
                    if let Some(folder_name) = entry.file_name().to_str() {
                        test_run_queries.push(TestRunQueryId::new(&self.id, folder_name));
                    }
                }
            }
        }
        Ok(test_run_queries)
    }

    pub async fn get_source_storage(
        &self,
        source_id: &TestRunSourceId,
        replace: bool,
    ) -> anyhow::Result<TestRunSourceStorage> {
        log::debug!("Getting (replace = {replace}) TestRunSourceStorage for ID: {source_id:?}");

        let source_path = self.sources_path.join(&source_id.test_source_id);
        let source_change_path = source_path.join(SOURCE_CHANGE_LOG_FOLDER_NAME);

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

        if self.sources_path.exists() {
            let mut entries = fs::read_dir(&self.sources_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let metadata = entry.metadata().await?;
                if metadata.is_dir() {
                    if let Some(folder_name) = entry.file_name().to_str() {
                        test_run_sources.push(TestRunSourceId::new(&self.id, folder_name));
                    }
                }
            }
        }
        Ok(test_run_sources)
    }

    pub async fn get_reaction_storage(
        &self,
        reaction_id: &TestRunReactionId,
        replace: bool,
    ) -> anyhow::Result<TestRunReactionStorage> {
        log::debug!("Getting (replace = {replace}) TestRunReactionStorage for ID: {reaction_id:?}");

        let reaction_path = self.reactions_path.join(&reaction_id.test_reaction_id);
        let reaction_output_path = reaction_path.join(REACTION_OUTPUT_LOG_FOLDER_NAME);

        if replace && reaction_path.exists() {
            fs::remove_dir_all(&reaction_path).await?;
        }

        if !reaction_path.exists() {
            fs::create_dir_all(&reaction_output_path).await?;
        }

        Ok(TestRunReactionStorage {
            id: reaction_id.clone(),
            path: reaction_path,
            reaction_output_path,
        })
    }

    pub async fn get_reaction_ids(&self) -> anyhow::Result<Vec<TestRunReactionId>> {
        let mut test_run_reactions = Vec::new();

        if self.reactions_path.exists() {
            let mut entries = fs::read_dir(&self.reactions_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let metadata = entry.metadata().await?;
                if metadata.is_dir() {
                    if let Some(folder_name) = entry.file_name().to_str() {
                        test_run_reactions.push(TestRunReactionId::new(&self.id, folder_name));
                    }
                }
            }
        }
        Ok(test_run_reactions)
    }

    pub async fn get_drasi_lib_instance_storage(
        &self,
        drasi_lib_instance_id: &TestRunDrasiLibInstanceId,
        replace: bool,
    ) -> anyhow::Result<TestRunDrasiLibInstanceStorage> {
        log::debug!(
            "Getting (replace = {replace}) TestRunDrasiLibInstanceStorage for ID: {drasi_lib_instance_id:?}"
        );

        let drasi_lib_instance_path = self
            .drasi_lib_instances_path
            .join(&drasi_lib_instance_id.test_drasi_lib_instance_id);

        if replace && drasi_lib_instance_path.exists() {
            fs::remove_dir_all(&drasi_lib_instance_path).await?;
        }

        if !drasi_lib_instance_path.exists() {
            fs::create_dir_all(&drasi_lib_instance_path).await?;
        }

        Ok(TestRunDrasiLibInstanceStorage {
            id: drasi_lib_instance_id.clone(),
            path: drasi_lib_instance_path,
        })
    }

    pub async fn get_drasi_lib_instance_ids(
        &self,
    ) -> anyhow::Result<Vec<TestRunDrasiLibInstanceId>> {
        let mut test_run_drasi_lib_instances = Vec::new();

        if self.drasi_lib_instances_path.exists() {
            let mut entries = fs::read_dir(&self.drasi_lib_instances_path).await?;
            while let Some(entry) = entries.next_entry().await? {
                let metadata = entry.metadata().await?;
                if metadata.is_dir() {
                    if let Some(folder_name) = entry.file_name().to_str() {
                        let drasi_lib_instance_id =
                            TestRunDrasiLibInstanceId::new(&self.id, folder_name);
                        test_run_drasi_lib_instances.push(drasi_lib_instance_id);
                    }
                }
            }
        }

        Ok(test_run_drasi_lib_instances)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunQueryStorage {
    pub id: TestRunQueryId,
    pub path: PathBuf,
    pub result_change_path: PathBuf,
}

impl TestRunQueryStorage {
    pub async fn write_test_run_summary(&self, summary: &Value) -> anyhow::Result<()> {
        let summary_path = self.path.join("test_run_summary.json");
        fs::write(summary_path, serde_json::to_string_pretty(summary)?).await?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunSourceStorage {
    pub id: TestRunSourceId,
    pub path: PathBuf,
    pub source_change_path: PathBuf,
}

impl TestRunSourceStorage {
    pub async fn write_test_run_summary(&self, summary: &Value) -> anyhow::Result<()> {
        let summary_path = self.path.join("test_run_summary.json");
        fs::write(summary_path, serde_json::to_string_pretty(summary)?).await?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunReactionStorage {
    pub id: TestRunReactionId,
    pub path: PathBuf,
    pub reaction_output_path: PathBuf,
}

impl TestRunReactionStorage {
    pub async fn write_test_run_summary(&self, summary: &Value) -> anyhow::Result<()> {
        let summary_path = self.path.join("test_run_summary.json");
        fs::write(summary_path, serde_json::to_string_pretty(summary)?).await?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunDrasiLibInstanceStorage {
    pub id: TestRunDrasiLibInstanceId,
    pub path: PathBuf,
}

impl TestRunDrasiLibInstanceStorage {
    pub async fn write_test_run_summary(&self, summary: &Value) -> anyhow::Result<()> {
        let summary_path = self.path.join("test_run_summary.json");
        fs::write(summary_path, serde_json::to_string_pretty(summary)?).await?;
        Ok(())
    }

    pub async fn write_instance_config(&self, config: &Value) -> anyhow::Result<()> {
        let config_path = self.path.join("instance_config.json");
        fs::write(config_path, serde_json::to_string_pretty(config)?).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
