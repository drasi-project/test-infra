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

use async_trait::async_trait;

use test_data_store::{scripts::SourceChangeEvent, test_repo_storage::models::SourceChangeDispatcherDefinition, test_run_storage::TestRunSourceStorage};

pub mod console_dispatcher;
pub mod dapr_dispatcher;
pub mod jsonl_file_dispatcher;
pub mod redis_stream_disspatcher;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeDispatcherError {
    Io(#[from] std::io::Error),
    Serde(#[from]serde_json::Error),
}

impl std::fmt::Display for SourceChangeDispatcherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
        }
    }
}

#[async_trait]
pub trait SourceChangeDispatcher : Send + Sync {
    async fn close(&mut self) -> anyhow::Result<()>;
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()>;
}

#[async_trait]
impl SourceChangeDispatcher for Box<dyn SourceChangeDispatcher + Send + Sync> {
    async fn close(&mut self) -> anyhow::Result<()> {
        (**self).close().await
    }
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {
        (**self).dispatch_source_change_events(events).await
    }
}

pub async fn create_source_change_dispatcher(def: &SourceChangeDispatcherDefinition, output_storage: &TestRunSourceStorage) -> anyhow::Result<Box<dyn SourceChangeDispatcher + Send + Sync>> {
    match def {
        SourceChangeDispatcherDefinition::Console(def) => {
            Ok(Box::new(console_dispatcher::ConsoleSourceChangeDispatcher::new(def, output_storage)?) as Box<dyn SourceChangeDispatcher + Send + Sync>)
        },
        SourceChangeDispatcherDefinition::Dapr(def) => {
            Ok(Box::new(dapr_dispatcher::DaprSourceChangeDispatcher::new(def, output_storage)?) as Box<dyn SourceChangeDispatcher + Send + Sync>)
        },
        SourceChangeDispatcherDefinition::JsonlFile(def) => {
            Ok(Box::new(jsonl_file_dispatcher::JsonlFileSourceChangeDispatcher::new(def, output_storage).await?) as Box<dyn SourceChangeDispatcher + Send + Sync>)
        },
        SourceChangeDispatcherDefinition::RedisStream(def) => {
            Ok(Box::new(redis_stream_disspatcher::RedisStreamSourceChangeDispatcher::new(def, output_storage).await?) as Box<dyn SourceChangeDispatcher + Send + Sync>)
        },
    }
}