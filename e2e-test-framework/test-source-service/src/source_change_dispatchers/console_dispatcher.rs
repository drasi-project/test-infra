use crate::test_script::SourceChangeEvent;
use super::{SourceChangeEventDispatcher, SourceChangeDispatcherResult};

pub struct ConsoleSourceChangeEventDispatcher {}

impl ConsoleSourceChangeEventDispatcher {
    pub fn new() -> Result<Box<dyn SourceChangeEventDispatcher>, SourceChangeDispatcherResult> {

        log::info!("Initializing ConsoleSourceChangeEventDispatcher...");

        Ok(Box::new(Self {}))
    }
}  

impl SourceChangeEventDispatcher for ConsoleSourceChangeEventDispatcher {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> Result<(), SourceChangeDispatcherResult> {

        log::info!("Initializing ConsoleSourceChangeEventDispatcher...");

        println!("SourceChangeEvent: {:?}", event);
        Ok(())
    }
}