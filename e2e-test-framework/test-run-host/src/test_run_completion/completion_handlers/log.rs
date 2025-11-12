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

use super::CompletionHandler;
use crate::test_run_completion::types::ComponentCompletionSummary;

/// Logs test run completion summary.
///
/// This handler writes completion information to the log, including counts
/// of components that finished, stopped, or encountered errors.
pub struct LogCompletionHandler {
    log_level: LogLevel,
}

#[derive(Debug, Clone, Copy)]
enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogCompletionHandler {
    pub fn new(config: &test_data_store::test_repo_storage::models::LogHandlerConfig) -> Self {
        let log_level = config
            .log_level
            .as_ref()
            .and_then(|s| match s.to_lowercase().as_str() {
                "debug" => Some(LogLevel::Debug),
                "info" => Some(LogLevel::Info),
                "warn" | "warning" => Some(LogLevel::Warn),
                "error" => Some(LogLevel::Error),
                _ => None,
            })
            .unwrap_or(LogLevel::Info);

        Self { log_level }
    }

    fn log_summary(&self, test_run_id: &str, summary: &ComponentCompletionSummary) {
        let message = format!(
            "TestRun '{}' completed: DrasiServers(stopped={}, error={}), \
             Sources(finished={}, stopped={}, error={}), \
             Queries(stopped={}, error={}), Reactions(stopped={}, error={}), \
             Has Errors: {}",
            test_run_id,
            summary.drasi_servers_stopped,
            summary.drasi_servers_error,
            summary.sources_finished,
            summary.sources_stopped,
            summary.sources_error,
            summary.queries_stopped,
            summary.queries_error,
            summary.reactions_stopped,
            summary.reactions_error,
            summary.has_errors()
        );

        match self.log_level {
            LogLevel::Debug => log::debug!("{}", message),
            LogLevel::Info => log::info!("{}", message),
            LogLevel::Warn => log::warn!("{}", message),
            LogLevel::Error => log::error!("{}", message),
        }
    }
}

#[async_trait]
impl CompletionHandler for LogCompletionHandler {
    async fn handle_completion(
        &self,
        test_run_id: &str,
        completion_summary: &ComponentCompletionSummary,
    ) -> anyhow::Result<()> {
        self.log_summary(test_run_id, completion_summary);
        Ok(())
    }
}
