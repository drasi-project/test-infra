use async_trait::async_trait;
use otel_trace_logger::{OtelTraceResultStreamLogger, OtelTraceResultStreamLoggerConfig};
use serde::{Deserialize, Serialize};

use console_logger::{ConsoleResultStreamLogger, ConsoleResultStreamLoggerConfig};
use jsonl_file_logger::{JsonlFileResultStreamLogger, JsonlFileResultStreamLoggerConfig};
use test_data_store::test_run_storage::TestRunQueryStorage;

use super::result_stream_handlers::ResultStreamRecord;

pub mod console_logger;
pub mod jsonl_file_logger;
pub mod otel_trace_logger;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ResultStreamLoggerConfig {
    Console(ConsoleResultStreamLoggerConfig),
    JsonlFile(JsonlFileResultStreamLoggerConfig),
    OtelTrace(OtelTraceResultStreamLoggerConfig)
}

#[derive(Debug, thiserror::Error)]
pub enum ResultStreamLoggerError {
    Io(#[from] std::io::Error),
    Serde(#[from] serde_json::Error),
    Trace(#[from] opentelemetry::trace::TraceError),
}

impl std::fmt::Display for ResultStreamLoggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
            Self::Trace(e) => write!(f, "Trace error: {}:", e),
        }
    }
}

#[async_trait]
pub trait ResultStreamLogger : Send + Sync {
    async fn close(&mut self) -> anyhow::Result<()>;
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()>;
}

#[async_trait]
impl ResultStreamLogger for Box<dyn ResultStreamLogger + Send + Sync> {
    async fn close(&mut self) -> anyhow::Result<()> {
        (**self).close().await
    }
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {
        (**self).log_result_stream_record(record).await
    }
}

pub async fn create_result_stream_logger(def: &ResultStreamLoggerConfig, output_storage: &TestRunQueryStorage) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
    match def {
        ResultStreamLoggerConfig::Console(def) => ConsoleResultStreamLogger::new(def),
        ResultStreamLoggerConfig::JsonlFile(def) => JsonlFileResultStreamLogger::new(def, output_storage).await,
        ResultStreamLoggerConfig::OtelTrace(def) => OtelTraceResultStreamLogger::new(def, output_storage),
    }
}