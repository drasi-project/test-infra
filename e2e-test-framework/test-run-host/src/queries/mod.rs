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

use std::fmt;

use derive_more::Debug;
use serde::{Deserialize, Serialize};

use result_stream_loggers::ResultStreamLoggerConfig;
use query_result_observer::{QueryResultObserver, QueryResultObserverCommandResponse, QueryResultObserverExternalState};
use test_data_store::{test_repo_storage::models::{StopTriggerDefinition, TestQueryDefinition}, test_run_storage::{ParseTestRunIdError, ParseTestRunQueryIdError, TestRunId, TestRunQueryId, TestRunQueryStorage}};

pub mod query_result_observer;
mod result_stream_handlers;
pub mod result_stream_loggers;
mod result_stream_record;
mod stop_triggers;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunQueryOverrides {
    pub stop_trigger: Option<StopTriggerDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestRunQueryConfig {
    #[serde(default="default_start_immediately")]
    pub start_immediately: bool,    
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: Option<String>,
    pub test_query_id: String,
    pub test_run_overrides: Option<TestRunQueryOverrides>,
    #[serde(default)]
    pub loggers: Vec<ResultStreamLoggerConfig>,
}
fn default_start_immediately() -> bool { false }

impl TryFrom<&TestRunQueryConfig> for TestRunId {
    type Error = ParseTestRunIdError;

    fn try_from(value: &TestRunQueryConfig) -> Result<Self, Self::Error> {
        Ok(TestRunId::new(
            &value.test_repo_id, 
            &value.test_id, 
            value.test_run_id
                .as_deref()
                .unwrap_or(&chrono::Utc::now().format("%Y%m%d%H%M%S").to_string())))
    }
}

impl TryFrom<&TestRunQueryConfig> for TestRunQueryId {
    type Error = ParseTestRunQueryIdError;

    fn try_from(value: &TestRunQueryConfig) -> Result<Self, Self::Error> {
        match TestRunId::try_from(value) {
            Ok(test_run_id) => {
                Ok(TestRunQueryId::new(&test_run_id, &value.test_query_id))
            }
            Err(e) => Err(ParseTestRunQueryIdError::InvalidValues(e.to_string())),
        }
    }
}

impl fmt::Display for TestRunQueryConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestRunQueryDefinition: Repo: test_repo_id: {:?}, test_id: {:?}, test_run_id: {:?}, test_query_id: {:?}", 
            self.test_repo_id, self.test_id, self.test_run_id, self.test_query_id)
    }
}

#[derive(Clone, Debug)]
pub struct TestRunQueryDefinition {
    pub id: TestRunQueryId,
    pub loggers: Vec<ResultStreamLoggerConfig>,
    pub start_immediately: bool,    
    pub test_query_definition: TestQueryDefinition,
    pub test_run_overrides: Option<TestRunQueryOverrides>,
}

impl TestRunQueryDefinition {
    pub fn new( test_run_query_config: TestRunQueryConfig, test_query_definition: TestQueryDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            id: TestRunQueryId::try_from(&test_run_query_config)?,
            loggers: test_run_query_config.loggers,
            start_immediately: test_run_query_config.start_immediately,
            test_query_definition,
            test_run_overrides: test_run_query_config.test_run_overrides,
        })
    }
}

#[derive(Debug, Serialize)]
pub struct TestRunQueryState {
    pub id: TestRunQueryId,
    pub query_observer: QueryResultObserverExternalState,
    pub start_immediately: bool,
}

#[derive(Debug)]
pub struct TestRunQuery {
    pub id: TestRunQueryId,
    #[debug(skip)]
    pub query_result_observer: QueryResultObserver,
    pub start_immediately: bool,
}

impl TestRunQuery {
    pub async fn new(definition: TestRunQueryDefinition, output_storage: TestRunQueryStorage) -> anyhow::Result<Self> {

        let query_result_observer = QueryResultObserver::new(
            definition.id.clone(),
            definition.test_query_definition.clone(), 
            output_storage,
            definition.loggers,
            definition.test_run_overrides
        ).await?;

        let trr = Self { 
            id: definition.id.clone(),
            query_result_observer,
            start_immediately: definition.start_immediately,
        };

        if trr.start_immediately {
            trr.start_query_result_observer().await?;
        }

        Ok(trr)
    }

    pub async fn get_state(&self) -> anyhow::Result<TestRunQueryState> {
        Ok(TestRunQueryState {
            id: self.id.clone(),
            query_observer: self.get_query_result_observer_state().await?,
            start_immediately: self.start_immediately,
        })
    }

    pub async fn get_query_result_observer_state(&self) -> anyhow::Result<QueryResultObserverExternalState> {
        Ok(self.query_result_observer.get_state().await?.state)
    }

    pub async fn pause_query_result_observer(&self) -> anyhow::Result<QueryResultObserverCommandResponse> {
        self.query_result_observer.pause().await
    }

    pub async fn reset_query_result_observer(&self) -> anyhow::Result<QueryResultObserverCommandResponse> {
        self.query_result_observer.reset().await
    }    

    pub async fn start_query_result_observer(&self) -> anyhow::Result<QueryResultObserverCommandResponse> {
        self.query_result_observer.start().await
    }

    pub async fn stop_query_result_observer(&self) -> anyhow::Result<QueryResultObserverCommandResponse> {
        self.query_result_observer.stop().await
    }
}