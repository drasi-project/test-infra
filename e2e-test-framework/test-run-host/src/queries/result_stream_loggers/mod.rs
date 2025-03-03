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

use std::path::PathBuf;

use async_trait::async_trait;
use otel_metric_logger::{OtelMetricResultStreamLogger, OtelMetricResultStreamLoggerConfig};
use otel_trace_logger::{OtelTraceResultStreamLogger, OtelTraceResultStreamLoggerConfig};
use profiler_logger::{ProfilerResultStreamLogger, ProfilerResultStreamLoggerConfig};
use serde::{Deserialize, Serialize};

use console_logger::{ConsoleResultStreamLogger, ConsoleResultStreamLoggerConfig};
use jsonl_file_logger::{JsonlFileResultStreamLogger, JsonlFileResultStreamLoggerConfig};
use test_data_store::test_run_storage::{TestRunQueryId, TestRunQueryStorage};

use super::result_stream_handlers::ResultStreamRecord;

pub mod console_logger;
pub mod jsonl_file_logger;
pub mod otel_metric_logger;
pub mod otel_trace_logger;
pub mod profiler_logger;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ResultStreamLoggerConfig {
    Console(ConsoleResultStreamLoggerConfig),
    JsonlFile(JsonlFileResultStreamLoggerConfig),
    OtelMetric(OtelMetricResultStreamLoggerConfig),
    OtelTrace(OtelTraceResultStreamLoggerConfig),
    Profiler(ProfilerResultStreamLoggerConfig)
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

#[derive(Clone, Debug, Serialize)]
pub struct ResultStreamLoggerResult {
    pub has_output: bool,
    pub logger_name: String,
    pub output_folder_path: Option<PathBuf>,
}

#[async_trait]
pub trait ResultStreamLogger : Send + Sync {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult>;
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()>;
}

#[async_trait]
impl ResultStreamLogger for Box<dyn ResultStreamLogger + Send + Sync> {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult> {
        (**self).end_test_run().await
    }
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {
        (**self).log_result_stream_record(record).await
    }
}

pub async fn create_result_stream_logger(test_run_query_id: TestRunQueryId, config: &ResultStreamLoggerConfig, output_storage: &TestRunQueryStorage) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
    match config {
        ResultStreamLoggerConfig::Console(cfg) => ConsoleResultStreamLogger::new(test_run_query_id, cfg),
        ResultStreamLoggerConfig::JsonlFile(cfg) => JsonlFileResultStreamLogger::new(test_run_query_id, cfg, output_storage).await,
        ResultStreamLoggerConfig::OtelMetric(cfg) => OtelMetricResultStreamLogger::new(test_run_query_id, cfg).await,
        ResultStreamLoggerConfig::OtelTrace(cfg) => OtelTraceResultStreamLogger::new(test_run_query_id, cfg),
        ResultStreamLoggerConfig::Profiler(cfg) => ProfilerResultStreamLogger::new(test_run_query_id, cfg, output_storage).await,
    }
}

pub async fn create_result_stream_loggers(test_run_query_id: TestRunQueryId, configs: &Vec<ResultStreamLoggerConfig>, output_storage: &TestRunQueryStorage) -> anyhow::Result<Vec<Box<dyn ResultStreamLogger + Send + Sync>>> {
    let mut result = Vec::new();
    for config in configs {
        result.push(create_result_stream_logger(test_run_query_id.clone(), config, output_storage).await?);
    }
    Ok(result)
}