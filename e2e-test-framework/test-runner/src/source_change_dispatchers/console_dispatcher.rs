use crate::test_script::SourceChangeEvent;
use super::SourceChangeEventDispatcher;

pub struct ConsoleSourceChangeEventDispatcher {}

impl ConsoleSourceChangeEventDispatcher {
    pub fn new() -> anyhow::Result<Box<dyn SourceChangeEventDispatcher>> {

        log::info!("Initializing ConsoleSourceChangeEventDispatcher...");

        Ok(Box::new(Self {}))
    }
}  

impl SourceChangeEventDispatcher for ConsoleSourceChangeEventDispatcher {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> anyhow::Result<()> {

        log::info!("Initializing ConsoleSourceChangeEventDispatcher...");

        println!("SourceChangeEvent: {:?}", event);
        Ok(())
    }
}