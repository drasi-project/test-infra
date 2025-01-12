use async_trait::async_trait;
use chrono::prelude::*;

use test_data_store::test_run_storage::{ReactionDataEvent, TestRunReactionStorage};

use crate::reactions::ConsoleTestRunReactionLoggerConfig;

use super::ReactionLogger;

#[derive(Debug)]

pub struct ConsoleReactionLoggerSettings {
    pub date_time_format: String,
}

impl ConsoleReactionLoggerSettings {
    pub fn new(def: &ConsoleTestRunReactionLoggerConfig) -> anyhow::Result<Self> {
        return Ok(Self {
            date_time_format: def.date_time_format.clone().unwrap_or("%Y-%m-%d %H:%M:%S%.f".to_string()),
        });
    }
}

pub struct ConsoleReactionLogger {
    settings: ConsoleReactionLoggerSettings,
}

impl ConsoleReactionLogger {
    pub fn new(def: &ConsoleTestRunReactionLoggerConfig, _output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionLogger + Send + Sync>> {
        log::debug!("Creating ConsoleReactionLogger from {:?}, ", def);

        let settings = ConsoleReactionLoggerSettings::new(&def)?;
        log::trace!("Creating ConsoleReactionLogger with settings {:?}, ", settings);

        Ok(Box::new(Self { settings }))
    }
}  

#[async_trait]
impl ReactionLogger for ConsoleReactionLogger {
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn log_reaction_data(&mut self, events: Vec<&ReactionDataEvent>) -> anyhow::Result<()> {

        log::trace!("Dispatch reaction data");

        let time = Local::now().format(&self.settings.date_time_format);

        let event_list = events
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join(",");
        
        println!("ConsoleReactionLogger - Time: {}, ReactionDataEvents: [{}]", time, event_list);

        Ok(())
    }
}