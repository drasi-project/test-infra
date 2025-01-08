use async_trait::async_trait;

use test_data_store::{scripts::SourceChangeEvent, test_repo_storage::models::SourceChangeDispatcherDefinition, test_run_storage::TestRunSourceStorage};

pub mod console_dispatcher;
pub mod dapr_dispatcher;
pub mod jsonl_file_dispatcher;

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
        SourceChangeDispatcherDefinition::Console(def) => console_dispatcher::ConsoleSourceChangeDispatcher::new(def, output_storage),
        SourceChangeDispatcherDefinition::Dapr(def) => dapr_dispatcher::DaprSourceChangeDispatcher::new(def, output_storage),
        SourceChangeDispatcherDefinition::JsonlFile(def) => jsonl_file_dispatcher::JsonlFileSourceChangeDispatcher::new(def, output_storage).await,
    }
}