use async_trait::async_trait;
use chrono::prelude::*;

use test_data_store::{test_repo_storage::models::ConsoleTestReactionDispatcherDefinition, test_run_storage::{ReactionDataEvent, TestRunReactionStorage}};

use super::ReactionDataDispatcher;

#[derive(Debug)]

pub struct ConsoleReactionDataDispatcherSettings {
    pub date_time_format: String,
}

impl ConsoleReactionDataDispatcherSettings {
    pub fn new(def: &ConsoleTestReactionDispatcherDefinition) -> anyhow::Result<Self> {
        return Ok(Self {
            date_time_format: def.date_time_format.clone().unwrap_or("%Y-%m-%d %H:%M:%S%.f".to_string()),
        });
    }
}

pub struct ConsoleReactionDataDispatcher {
    settings: ConsoleReactionDataDispatcherSettings,
}

impl ConsoleReactionDataDispatcher {
    pub fn new(def: &ConsoleTestReactionDispatcherDefinition, _output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionDataDispatcher + Send + Sync>> {
        log::debug!("Creating ConsoleReactionDataDispatcher from {:?}, ", def);

        let settings = ConsoleReactionDataDispatcherSettings::new(&def)?;
        log::trace!("Creating ConsoleReactionDataDispatcher with settings {:?}, ", settings);

        Ok(Box::new(Self { settings }))
    }
}  

#[async_trait]
impl ReactionDataDispatcher for ConsoleReactionDataDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn dispatch_reaction_data(&mut self, events: Vec<&ReactionDataEvent>) -> anyhow::Result<()> {

        log::trace!("Dispatch reaction data");

        let time = Local::now().format(&self.settings.date_time_format);

        let event_list = events
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join(",");
        
        println!("ConsoleReactionDataDispatcher - Time: {}, ReactionDataEvents: [{}]", time, event_list);

        Ok(())
    }
}