use chrono::prelude::*;

use async_trait::async_trait;

use crate::test_script::SourceChangeEvent;
use super::SourceChangeEventDispatcher;

pub struct ConsoleSourceChangeEventDispatcher {}

impl ConsoleSourceChangeEventDispatcher {
    pub fn new() -> anyhow::Result<Box<dyn SourceChangeEventDispatcher>> {

        log::info!("Initializing ConsoleSourceChangeEventDispatcher...");

        Ok(Box::new(Self {}))
    }
}  

#[async_trait]
impl SourceChangeEventDispatcher for ConsoleSourceChangeEventDispatcher {
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {

        let time = Local::now().format("%Y-%m-%d %H:%M:%S%.f");

        let event_list = events
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join(",");
        
        println!("ConsoleSourceChangeEventDispatcher - Time: {}, SourceChangeEvents: [{}]", time, event_list);

        Ok(())
    }
}