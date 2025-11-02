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

use test_data_store::{
    scripts::SourceChangeEvent, test_repo_storage::models::SourceChangeDispatcherDefinition,
    test_run_storage::TestRunSourceStorage,
};

pub mod adaptive_grpc_dispatcher;
pub mod adaptive_http_dispatcher;
pub mod console_dispatcher;
pub mod dapr_dispatcher;
pub mod drasi_server_api_dispatcher;
pub mod drasi_server_channel_dispatcher;
pub mod grpc_dispatcher;
pub mod http_dispatcher;
pub mod jsonl_file_dispatcher;
pub mod redis_stream_disspatcher;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeDispatcherError {
    Io(#[from] std::io::Error),
    Serde(#[from] serde_json::Error),
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
pub trait SourceChangeDispatcher: Send + Sync {
    async fn close(&mut self) -> anyhow::Result<()>;
    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()>;

    /// Sets the TestRunHost for dispatchers that need it (optional)
    fn set_test_run_host(&mut self, _test_run_host: std::sync::Arc<crate::TestRunHost>) {
        // Default implementation does nothing - only some dispatchers need this
    }
}

#[async_trait]
impl SourceChangeDispatcher for Box<dyn SourceChangeDispatcher + Send + Sync> {
    async fn close(&mut self) -> anyhow::Result<()> {
        (**self).close().await
    }
    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        (**self).dispatch_source_change_events(events).await
    }
    fn set_test_run_host(&mut self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        (**self).set_test_run_host(test_run_host)
    }
}

pub async fn create_source_change_dispatcher(
    def: &SourceChangeDispatcherDefinition,
    output_storage: &TestRunSourceStorage,
) -> anyhow::Result<Box<dyn SourceChangeDispatcher + Send + Sync>> {
    match def {
        SourceChangeDispatcherDefinition::Console(def) => Ok(Box::new(
            console_dispatcher::ConsoleSourceChangeDispatcher::new(def, output_storage)?,
        )
            as Box<dyn SourceChangeDispatcher + Send + Sync>),
        SourceChangeDispatcherDefinition::Dapr(def) => Ok(Box::new(
            dapr_dispatcher::DaprSourceChangeDispatcher::new(def, output_storage)?,
        )
            as Box<dyn SourceChangeDispatcher + Send + Sync>),
        SourceChangeDispatcherDefinition::Http(def) => {
            // Use adaptive dispatcher if enabled
            if def.adaptive_enabled.unwrap_or(false) {
                Ok(Box::new(
                    adaptive_http_dispatcher::AdaptiveHttpSourceChangeDispatcher::new(
                        def,
                        output_storage.clone(),
                    )?,
                )
                    as Box<dyn SourceChangeDispatcher + Send + Sync>)
            } else {
                Ok(Box::new(http_dispatcher::HttpSourceChangeDispatcher::new(
                    def,
                    output_storage.clone(),
                )?)
                    as Box<dyn SourceChangeDispatcher + Send + Sync>)
            }
        }
        SourceChangeDispatcherDefinition::Grpc(def) => {
            // Use adaptive dispatcher if enabled
            if def.adaptive_enabled.unwrap_or(false) {
                Ok(Box::new(
                    adaptive_grpc_dispatcher::AdaptiveGrpcSourceChangeDispatcher::new(
                        def,
                        output_storage.clone(),
                    )
                    .await?,
                )
                    as Box<dyn SourceChangeDispatcher + Send + Sync>)
            } else {
                Ok(Box::new(
                    grpc_dispatcher::GrpcSourceChangeDispatcher::new(def, output_storage.clone())
                        .await?,
                )
                    as Box<dyn SourceChangeDispatcher + Send + Sync>)
            }
        }
        SourceChangeDispatcherDefinition::JsonlFile(def) => Ok(Box::new(
            jsonl_file_dispatcher::JsonlFileSourceChangeDispatcher::new(def, output_storage)
                .await?,
        )
            as Box<dyn SourceChangeDispatcher + Send + Sync>),
        SourceChangeDispatcherDefinition::RedisStream(def) => Ok(Box::new(
            redis_stream_disspatcher::RedisStreamSourceChangeDispatcher::new(def, output_storage)
                .await?,
        )
            as Box<dyn SourceChangeDispatcher + Send + Sync>),
        SourceChangeDispatcherDefinition::DrasiServerApi(def) => Ok(Box::new(
            drasi_server_api_dispatcher::DrasiServerApiSourceChangeDispatcher::new(
                def,
                output_storage,
            )?,
        )
            as Box<dyn SourceChangeDispatcher + Send + Sync>),
        SourceChangeDispatcherDefinition::DrasiServerChannel(def) => Ok(Box::new(
            drasi_server_channel_dispatcher::DrasiServerChannelSourceChangeDispatcher::new(
                def,
                output_storage,
            )?,
        )
            as Box<dyn SourceChangeDispatcher + Send + Sync>),
    }
}
