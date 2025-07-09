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
use test_data_store::test_run_storage::TestRunQueryId;

use crate::queries::result_stream_handlers::ResultStreamRecord;

use super::{ResultStreamLogger, ResultStreamLoggerResult};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleResultStreamLoggerConfig {
    pub date_time_format: Option<String>,
}

#[derive(Debug)]
pub struct ConsoleResultStreamLoggerSettings {
    pub date_time_format: String,
    pub test_run_query_id: TestRunQueryId, 
}

impl ConsoleResultStreamLoggerSettings {
    pub fn new(test_run_query_id: TestRunQueryId, def: &ConsoleResultStreamLoggerConfig) -> anyhow::Result<Self> {
        Ok(Self {
            date_time_format: def.date_time_format.clone().unwrap_or("%Y-%m-%d %H:%M:%S%.f".to_string()),
            test_run_query_id,
        })
    }
}

pub struct ConsoleResultStreamLogger {
    settings: ConsoleResultStreamLoggerSettings,
}

impl ConsoleResultStreamLogger {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(test_run_query_id: TestRunQueryId, def: &ConsoleResultStreamLoggerConfig) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating ConsoleResultStreamLogger for {} from {:?}, ", test_run_query_id, def);

        let settings = ConsoleResultStreamLoggerSettings::new(test_run_query_id, def)?;
        log::trace!("Creating ConsoleResultStreamLogger with settings {:?}, ", settings);

        Ok(Box::new(Self { settings }))
    }
}  

#[async_trait]
impl ResultStreamLogger for ConsoleResultStreamLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult> {
        Ok(ResultStreamLoggerResult {
            has_output: false,
            logger_name: "Console".to_string(),
            output_folder_path: None,
        })
    }

    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {

        let time = Local::now().format(&self.settings.date_time_format);

        println!("ConsoleResultStreamLogger - Time: {}, ResultStreamRecord: {}", time, record);

        Ok(())
    }
}