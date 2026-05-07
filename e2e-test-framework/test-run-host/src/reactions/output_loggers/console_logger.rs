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
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use test_data_store::test_run_storage::TestRunReactionId;

use crate::common::{HandlerPayload, HandlerRecord};

use super::{OutputLogger, OutputLoggerResult};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleOutputLoggerConfig {
    pub date_time_format: Option<String>,
}

#[derive(Debug)]
pub struct ConsoleOutputLoggerSettings {
    pub date_time_format: String,
    pub test_run_reaction_id: TestRunReactionId,
}

impl ConsoleOutputLoggerSettings {
    pub fn new(
        test_run_reaction_id: TestRunReactionId,
        def: &ConsoleOutputLoggerConfig,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            date_time_format: def
                .date_time_format
                .clone()
                .unwrap_or("%Y-%m-%d %H:%M:%S%.f".to_string()),
            test_run_reaction_id,
        })
    }
}

pub struct ConsoleOutputLogger {
    settings: ConsoleOutputLoggerSettings,
}

impl ConsoleOutputLogger {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        test_run_reaction_id: TestRunReactionId,
        def: &ConsoleOutputLoggerConfig,
    ) -> anyhow::Result<Box<dyn OutputLogger + Send + Sync>> {
        log::debug!("Creating ConsoleOutputLogger for {test_run_reaction_id} from {def:?}, ");

        let settings = ConsoleOutputLoggerSettings::new(test_run_reaction_id, def)?;
        log::trace!("Creating ConsoleOutputLogger with settings {settings:?}, ");

        Ok(Box::new(Self { settings }))
    }
}

#[async_trait]
impl OutputLogger for ConsoleOutputLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult> {
        Ok(OutputLoggerResult {
            has_output: false,
            logger_name: "Console".to_string(),
            output_folder_path: None,
        })
    }

    async fn log_handler_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()> {
        let time = Local::now().format(&self.settings.date_time_format);

        match &record.payload {
            HandlerPayload::ReactionInvocation {
                reaction_type,
                query_id,
                request_method,
                request_path,
                request_body,
                headers,
            } => {
                println!(
                    "[{}] Reaction Invocation - ID: {}, Seq: {}, Type: {}, Query: {}, Method: {} {}",
                    time, record.id, record.sequence, reaction_type, query_id, request_method, request_path
                );
                if !request_body.is_null() {
                    println!("  Request Body: {request_body}");
                }
                if !headers.is_empty() {
                    println!("  Headers: {headers:?}");
                }
            }
            HandlerPayload::ReactionOutput { reaction_output } => {
                println!(
                    "[{}] Reaction Output - ID: {}, Seq: {}, Output: {:?}",
                    time, record.id, record.sequence, reaction_output
                );
            }
            _ => {
                // Ignore other payload types (e.g., ResultStream for queries)
            }
        }

        Ok(())
    }
}
