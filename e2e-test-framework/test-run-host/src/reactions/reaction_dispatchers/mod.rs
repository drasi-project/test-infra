use async_trait::async_trait;

use test_data_store::{test_repo_storage::models::TestReactionDispatcherDefinition, test_run_storage::{ReactionDataEvent, TestRunReactionStorage}};

pub mod console_dispatcher;
pub mod jsonl_file_dispatcher;

#[derive(Debug, thiserror::Error)]
pub enum ReactionDataDispatcherError {
    Io(#[from] std::io::Error),
    Serde(#[from]serde_json::Error),
}

impl std::fmt::Display for ReactionDataDispatcherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
        }
    }
}

#[async_trait]
pub trait ReactionDataDispatcher : Send + Sync {
    async fn close(&mut self) -> anyhow::Result<()>;
    async fn dispatch_reaction_data(&mut self, events: Vec<&ReactionDataEvent>) -> anyhow::Result<()>;
}

#[async_trait]
impl ReactionDataDispatcher for Box<dyn ReactionDataDispatcher + Send + Sync> {
    async fn close(&mut self) -> anyhow::Result<()> {
        (**self).close().await
    }
    async fn dispatch_reaction_data(&mut self, events: Vec<&ReactionDataEvent>) -> anyhow::Result<()> {
        (**self).dispatch_reaction_data(events).await
    }
}

pub async fn create_reaction_data_dispatcher(def: &TestReactionDispatcherDefinition, output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionDataDispatcher + Send + Sync>> {
    match def {
        TestReactionDispatcherDefinition::Console(def) => console_dispatcher::ConsoleReactionDataDispatcher::new(def, output_storage),
        TestReactionDispatcherDefinition::JsonlFile(def) => jsonl_file_dispatcher::JsonlFileReactionDataDispatcher::new(def, output_storage).await,
    }
}