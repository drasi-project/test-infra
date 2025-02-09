use async_trait::async_trait;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};

use crate::queries::result_stream_handlers::ResultStreamRecord;

use super::ResultStreamLogger;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleResultStreamLoggerConfig {
    pub date_time_format: Option<String>,
}

#[derive(Debug)]
pub struct ConsoleResultStreamLoggerSettings {
    pub date_time_format: String,
}

impl ConsoleResultStreamLoggerSettings {
    pub fn new(def: &ConsoleResultStreamLoggerConfig) -> anyhow::Result<Self> {
        return Ok(Self {
            date_time_format: def.date_time_format.clone().unwrap_or("%Y-%m-%d %H:%M:%S%.f".to_string()),
        });
    }
}

pub struct ConsoleResultStreamLogger {
    settings: ConsoleResultStreamLoggerSettings,
}

impl ConsoleResultStreamLogger {
    pub fn new(def: &ConsoleResultStreamLoggerConfig) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating ConsoleResultStreamLogger from {:?}, ", def);

        let settings = ConsoleResultStreamLoggerSettings::new(&def)?;
        log::trace!("Creating ConsoleResultStreamLogger with settings {:?}, ", settings);

        Ok(Box::new(Self { settings }))
    }
}  

#[async_trait]
impl ResultStreamLogger for ConsoleResultStreamLogger {
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {

        let time = Local::now().format(&self.settings.date_time_format);

        println!("ConsoleResultStreamLogger - Time: {}, ResultStreamRecord: {}", time, record);

        Ok(())
    }
}