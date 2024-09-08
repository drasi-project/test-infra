use chrono::prelude::*;

use async_trait::async_trait;

use crate::{config::ConsoleSourceChangeDispatcherConfig, script_source::SourceChangeEvent};
use super::SourceChangeDispatcher;

#[derive(Debug)]

pub struct ConsoleSourceChangeDispatcherSettings {
    pub date_time_format: String,
}

impl ConsoleSourceChangeDispatcherSettings {
    pub fn try_from_config(config: &ConsoleSourceChangeDispatcherConfig) -> anyhow::Result<Self> {
        return Ok(Self {
            date_time_format: config.date_time_format.clone().unwrap_or("%Y-%m-%d %H:%M:%S%.f".to_string()),
        });
    }
}

pub struct ConsoleSourceChangeDispatcher {
    settings: ConsoleSourceChangeDispatcherSettings,
}

impl ConsoleSourceChangeDispatcher {
    pub fn new(settings: ConsoleSourceChangeDispatcherSettings) -> anyhow::Result<Box<dyn SourceChangeDispatcher>> {

        log::info!("Initializing ConsoleSourceChangeDispatcher from {:?}", settings);

        Ok(Box::new(Self { settings }))
    }
}  

#[async_trait]
impl SourceChangeDispatcher for ConsoleSourceChangeDispatcher {
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {

        log::trace!("ConsoleSourceChangeDispatcher - dispatch_source_change_events");

        let time = Local::now().format(&self.settings.date_time_format);

        let event_list = events
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join(",");
        
        println!("ConsoleSourceChangeDispatcher - Time: {}, SourceChangeEvents: [{}]", time, event_list);

        Ok(())
    }
}