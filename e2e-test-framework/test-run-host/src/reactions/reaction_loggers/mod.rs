use async_trait::async_trait;

use test_data_store::test_run_storage::{ReactionDataEvent, TestRunReactionStorage};

use super::TestRunReactionLoggerConfig;

pub mod console_logger;
pub mod jsonl_file_logger;

#[derive(Debug, thiserror::Error)]
pub enum ReactionLoggerError {
    Io(#[from] std::io::Error),
    Serde(#[from]serde_json::Error),
}

impl std::fmt::Display for ReactionLoggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
        }
    }
}

#[async_trait]
pub trait ReactionLogger : Send + Sync {
    async fn close(&mut self) -> anyhow::Result<()>;
    async fn log_reaction_data(&mut self, events: Vec<&ReactionDataEvent>) -> anyhow::Result<()>;
}

#[async_trait]
impl ReactionLogger for Box<dyn ReactionLogger + Send + Sync> {
    async fn close(&mut self) -> anyhow::Result<()> {
        (**self).close().await
    }
    async fn log_reaction_data(&mut self, events: Vec<&ReactionDataEvent>) -> anyhow::Result<()> {
        (**self).log_reaction_data(events).await
    }
}

pub async fn create_reaction_logger(def: &TestRunReactionLoggerConfig, output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionLogger + Send + Sync>> {
    match def {
        TestRunReactionLoggerConfig::Console(def) => console_logger::ConsoleReactionLogger::new(def, output_storage),
        TestRunReactionLoggerConfig::JsonlFile(def) => jsonl_file_logger::JsonlFileReactionLogger::new(def, output_storage).await,
    }
}