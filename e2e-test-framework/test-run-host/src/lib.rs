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

use core::fmt;
use std::{collections::{HashMap, HashSet}, sync::Arc};

use derive_more::Debug;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use queries::{query_result_observer::QueryResultObserverCommandResponse, result_stream_loggers::ResultStreamLoggerResult, TestRunQuery, TestRunQueryConfig, TestRunQueryDefinition, TestRunQueryState};
use sources::{
    bootstrap_data_generators::BootstrapData, create_test_run_source, source_change_generators::SourceChangeGeneratorCommandResponse, TestRunSource, TestRunSourceConfig, TestRunSourceState
};
use test_data_store::{test_repo_storage::models::SpacingMode, test_run_storage::{TestRunQueryId, TestRunSourceId}, TestDataStore};

pub mod queries;
pub mod sources;

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct TestRunHostConfig {
    #[serde(default)]
    pub queries: Vec<TestRunQueryConfig>,
    #[serde(default)]
    pub sources: Vec<TestRunSourceConfig>,
}

// An enum that represents the current state of the TestRunHost.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TestRunHostStatus {
    // The TestRunHost is Initialized and is ready to start.
    Initialized,
    // The TestRunHost has been started.
    Running,
    // The TestRunHost is in an Error state. and will not be able to process requests.
    Error(String),
}

impl fmt::Display for TestRunHostStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestRunHostStatus::Initialized => write!(f, "Initialized"),
            TestRunHostStatus::Running => write!(f, "Running"),
            TestRunHostStatus::Error(msg) => write!(f, "Error: {}", msg),
        }
    }
}

#[derive(Debug)]
pub struct TestRunHost {
    data_store: Arc<TestDataStore>,
    queries: Arc<RwLock<HashMap<TestRunQueryId, TestRunQuery>>>,
    sources: Arc<RwLock<HashMap<TestRunSourceId, Box<dyn TestRunSource + Send + Sync>>>>,
    status: Arc<RwLock<TestRunHostStatus>>,
}

impl TestRunHost {
    pub async fn new(config: TestRunHostConfig, data_store: Arc<TestDataStore>) -> anyhow::Result<Self> {   
        log::debug!("Creating TestRunHost from {:?}", config);

        let test_run_host = TestRunHost {
            data_store,
            queries: Arc::new(RwLock::new(HashMap::new())),
            sources: Arc::new(RwLock::new(HashMap::new())),
            status: Arc::new(RwLock::new(TestRunHostStatus::Initialized)),
        };

        // Add the initial set of Test Run Queries.
        for query_config in config.queries {
            test_run_host.add_test_query(query_config).await?;
        };
        
        // Add the initial set of Test Run Sources.
        for source_config in config.sources {
            test_run_host.add_test_source(source_config).await?;
        };
        
        log::debug!("TestRunHost created -  {:?}", &test_run_host);

        match &test_run_host.get_status().await? {
            TestRunHostStatus::Initialized => {
                log::info!("Starting TestRunHost...");
                test_run_host.set_status(TestRunHostStatus::Running).await;
            },
            TestRunHostStatus::Running => {
                let msg = "TestRunHost created with unexpected status: Running";
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
            TestRunHostStatus::Error(_) => {
                let msg = "TestRunHost is in an Error state, cannot Start.".to_string();
                log::error!("{}", msg);
                anyhow::bail!("{}", msg);
            },
        };

        Ok(test_run_host)
    }
    
    pub async fn add_test_query(&self, test_run_query: TestRunQueryConfig) -> anyhow::Result<TestRunQueryId> {
        log::trace!("Adding TestRunQuery from {:?}", test_run_query);

        // If the TestRunHost is in an Error state, return an error.
        if let TestRunHostStatus::Error(msg) = &self.get_status().await? {
            anyhow::bail!("TestRunHost is in an Error state: {}", msg);
        };
        
        let id = TestRunQueryId::try_from(&test_run_query)?;

        let mut queries_lock = self.queries.write().await;

        // Fail if the TestRunHost already contains a TestRunQuery with the specified Id.
        if queries_lock.contains_key(&id) {
            anyhow::bail!("TestRunHost already contains TestRunQuery with ID: {:?}", &id);
        }

        // Get the TestRepoStorage that is associated with the Repo for the TestRunQuery
        let repo = self.data_store.get_test_repo_storage(&test_run_query.test_repo_id).await?;
        repo.add_remote_test(&test_run_query.test_id, false).await?;
        let test_query_definition = self.data_store.get_test_query_definition_for_test_run_query(&id).await?;

        let definition = TestRunQueryDefinition::new(test_run_query, test_query_definition)?;
        log::trace!("TestRunQueryDefinition: {:?}", &definition);

        // Get the OUTPUT storage for the new TestRunQuery.
        // This is where the TestRunQuery will write the output to.
        let output_storage = self.data_store.get_test_run_query_storage(&id).await?;

        // Create the TestRunQuery and add it to the TestRunHost.
        let test_run_query = TestRunQuery::new(definition, output_storage).await?;        

        queries_lock.insert(id.clone(), test_run_query);
        
        Ok(id)
    }

    pub async fn add_test_source(&self, test_run_config: TestRunSourceConfig) -> anyhow::Result<TestRunSourceId> {
        log::trace!("Adding TestRunSource from {:?}", test_run_config);

        // If the TestRunHost is in an Error state, return an error.
        if let TestRunHostStatus::Error(msg) = &self.get_status().await? {
            anyhow::bail!("TestRunHost is in an Error state: {}", msg);
        };
        
        let id = TestRunSourceId::try_from(&test_run_config)?;

        let mut sources_lock = self.sources.write().await;

        // Fail if the TestRunHost already contains a TestRunSource with the specified Id.
        if sources_lock.contains_key(&id) {
            anyhow::bail!("TestRunHost already contains TestRunSource with ID: {:?}", &id);
        }

        // Get the TestRepoStorage that is associated with the Repo for the TestRunSource
        let repo = self.data_store.get_test_repo_storage(&test_run_config.test_repo_id).await?;
        repo.add_remote_test(&test_run_config.test_id, false).await?;
        let test_source_definition = self.data_store.get_test_source_definition_for_test_run_source(&id).await?;

        // Get the INPUT Test Data storage for the TestRunSource.
        // This is where the TestRunSource will read the Test Data from.
        let input_storage = self.data_store.get_test_source_storage_for_test_run_source(&id).await?;

        // Get the OUTPUT storage for the new TestRunSource.
        // This is where the TestRunSource will write the output to.
        let output_storage = self.data_store.get_test_run_source_storage(&id).await?;

        // Create the TestRunSource and add it to the TestRunHost.
        let test_run_source = create_test_run_source(&test_run_config, &test_source_definition, input_storage, output_storage).await?;
        sources_lock.insert(id.clone(), test_run_source);

        Ok(id)
    }

    pub async fn contains_test_source(&self, test_run_source_id: &str) -> anyhow::Result<bool> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        Ok(self.sources.read().await.contains_key(&test_run_source_id))
    }
    
    pub async fn get_status(&self) -> anyhow::Result<TestRunHostStatus> {
        Ok(self.status.read().await.clone())
    }

    pub async fn get_source_bootstrap_data(&self, test_run_source_id: &str, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        log::debug!("Source ID: {}, Node Labels: {:?}, Rel Labels: {:?}", test_run_source_id, node_labels, rel_labels);

        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.get_bootstrap_data(node_labels, rel_labels).await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn get_test_query_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.queries.read().await.keys().map(|id| id.to_string()).collect())
    }

    pub async fn get_test_query_state(&self, test_run_query_id: &str) -> anyhow::Result<TestRunQueryState> {
        let test_run_query_id = TestRunQueryId::try_from(test_run_query_id)?;
        match self.queries.read().await.get(&test_run_query_id) {
            Some(query) => {
                query.get_state().await
            },
            None => {
                anyhow::bail!("TestRunQuery not found: {:?}", test_run_query_id);
            }
        }
    }

    pub async fn get_test_query_result_logger_output(&self, test_run_query_id: &str) -> anyhow::Result<Vec<ResultStreamLoggerResult>> {
        let test_run_query_id = TestRunQueryId::try_from(test_run_query_id)?;
        match self.queries.read().await.get(&test_run_query_id) {
            Some(query) => {
                Ok(query.get_query_result_observer_state().await?.logger_results)
            },
            None => {
                anyhow::bail!("TestRunQuery not found: {:?}", test_run_query_id);
            }
        }
    }

    pub async fn get_test_source_ids(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.sources.read().await.keys().map(|id| id.to_string()).collect())
    }

    pub async fn get_test_source_state(&self, test_run_source_id: &str) -> anyhow::Result<TestRunSourceState> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.get_state().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    async fn set_status(&self, status: TestRunHostStatus) {
        let mut write_lock = self.status.write().await;
        *write_lock = status.clone();
    }

    pub async fn test_query_pause(&self, test_run_query_id: &str) -> anyhow::Result<QueryResultObserverCommandResponse> {
        let test_run_query_id = TestRunQueryId::try_from(test_run_query_id)?;
        match self.queries.read().await.get(&test_run_query_id) {
            Some(query) => {
                query.pause_query_result_observer().await
            },
            None => {
                anyhow::bail!("TestRunQuery not found: {:?}", test_run_query_id);
            }
        }
    }

    pub async fn test_query_reset(&self, test_run_query_id: &str) -> anyhow::Result<QueryResultObserverCommandResponse> {
        let test_run_query_id = TestRunQueryId::try_from(test_run_query_id)?;
        match self.queries.read().await.get(&test_run_query_id) {
            Some(query) => {
                query.reset_query_result_observer().await
            },
            None => {
                anyhow::bail!("TestRunQuery not found: {:?}", test_run_query_id);
            }
        }
    }

    pub async fn test_query_start(&self, test_run_query_id: &str) -> anyhow::Result<QueryResultObserverCommandResponse> {
        let test_run_query_id = TestRunQueryId::try_from(test_run_query_id)?;
        match self.queries.read().await.get(&test_run_query_id) {
            Some(query) => {
                query.start_query_result_observer().await
            },
            None => {
                anyhow::bail!("TestRunQuery not found: {:?}", test_run_query_id);
            }
        }
    }

    pub async fn test_query_stop(&self, test_run_query_id: &str) -> anyhow::Result<QueryResultObserverCommandResponse> {
        let test_run_query_id = TestRunQueryId::try_from(test_run_query_id)?;
        match self.queries.read().await.get(&test_run_query_id) {
            Some(query) => {
                query.stop_query_result_observer().await
            },
            None => {
                anyhow::bail!("TestRunQuery not found: {:?}", test_run_query_id);
            }
        }
    }

    pub async fn test_source_pause(&self, test_run_source_id: &str) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.pause_source_change_generator().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_reset(&self, test_run_source_id: &str) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.reset_source_change_generator().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_skip(&self, test_run_source_id: &str, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.skip_source_change_generator(skips, spacing_mode).await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_start(&self, test_run_source_id: &str) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.start_source_change_generator().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_step(&self, test_run_source_id: &str, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.step_source_change_generator(steps, spacing_mode).await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }

    pub async fn test_source_stop(&self, test_run_source_id: &str) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let test_run_source_id = TestRunSourceId::try_from(test_run_source_id)?;
        match self.sources.read().await.get(&test_run_source_id) {
            Some(source) => {
                source.stop_source_change_generator().await
            },
            None => {
                anyhow::bail!("TestRunSource not found: {:?}", test_run_source_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_data_store::TestDataStore;

    use crate::{TestRunHost, TestRunHostConfig, TestRunHostStatus};

    #[tokio::test]
    async fn test_new_test_run_host() -> anyhow::Result<()> {

        let data_store = Arc::new(TestDataStore::new_temp(None).await?);    
        
        let test_run_host_config = TestRunHostConfig::default();
        let test_run_host = TestRunHost::new(test_run_host_config, data_store.clone()).await.unwrap();

        assert_eq!(test_run_host.get_status().await?, TestRunHostStatus::Initialized);

        Ok(())
    }
}