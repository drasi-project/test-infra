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

        log::info!("Initializing ConsoleSourceChangeEventDispatcher...");

        println!("SourceChangeEvent: {:?}", events);
        Ok(())
    }
}