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
        return Ok(Self {
            date_time_format: def.date_time_format.clone().unwrap_or("%Y-%m-%d %H:%M:%S%.f".to_string()),
            test_run_query_id,
        });
    }
}

pub struct ConsoleResultStreamLogger {
    settings: ConsoleResultStreamLoggerSettings,
}

impl ConsoleResultStreamLogger {
    pub fn new(test_run_query_id: TestRunQueryId, def: &ConsoleResultStreamLoggerConfig) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating ConsoleResultStreamLogger for {} from {:?}, ", test_run_query_id, def);

        let settings = ConsoleResultStreamLoggerSettings::new(test_run_query_id, &def)?;
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