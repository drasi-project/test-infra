use async_trait::async_trait;
use otel_trace_logger::{OtelTraceReactionLogger, OtelTraceTestRunReactionLoggerConfig};
use serde::{Deserialize, Serialize};

use console_logger::{ConsoleReactionLogger, ConsoleTestRunReactionLoggerConfig};
use jsonl_file_logger::{JsonlFileReactionLogger, JsonlFileTestRunReactionLoggerConfig};
use test_data_store::test_run_storage::TestRunReactionStorage;

use super::reaction_handlers::ReactionOutputRecord;

pub mod console_logger;
pub mod jsonl_file_logger;
pub mod otel_trace_logger;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum TestRunReactionLoggerConfig {
    Console(ConsoleTestRunReactionLoggerConfig),
    JsonlFile(JsonlFileTestRunReactionLoggerConfig),
    OtelTrace(OtelTraceTestRunReactionLoggerConfig)
}

#[derive(Debug, thiserror::Error)]
pub enum ReactionLoggerError {
    Io(#[from] std::io::Error),
    Serde(#[from] serde_json::Error),
    Trace(#[from] opentelemetry::trace::TraceError),
}

impl std::fmt::Display for ReactionLoggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
            Self::Trace(e) => write!(f, "Trace error: {}:", e),
        }
    }
}

#[async_trait]
pub trait ReactionLogger : Send + Sync {
    async fn close(&mut self) -> anyhow::Result<()>;
    async fn log_reaction_record(&mut self, record: &ReactionOutputRecord) -> anyhow::Result<()>;
}

#[async_trait]
impl ReactionLogger for Box<dyn ReactionLogger + Send + Sync> {
    async fn close(&mut self) -> anyhow::Result<()> {
        (**self).close().await
    }
    async fn log_reaction_record(&mut self, record: &ReactionOutputRecord) -> anyhow::Result<()> {
        (**self).log_reaction_record(record).await
    }
}

pub async fn create_reaction_logger(def: &TestRunReactionLoggerConfig, output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionLogger + Send + Sync>> {
    match def {
        TestRunReactionLoggerConfig::Console(def) => ConsoleReactionLogger::new(def, output_storage),
        TestRunReactionLoggerConfig::JsonlFile(def) => JsonlFileReactionLogger::new(def, output_storage).await,
        TestRunReactionLoggerConfig::OtelTrace(def) => OtelTraceReactionLogger::new(def, output_storage),
    }
}