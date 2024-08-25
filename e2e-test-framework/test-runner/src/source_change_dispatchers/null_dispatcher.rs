
use crate::test_script::SourceChangeEvent;
use super::SourceChangeEventDispatcher;

pub struct NullSourceChangeEventDispatcher {}

impl NullSourceChangeEventDispatcher {
    pub fn new() -> Box<dyn SourceChangeEventDispatcher> {

        log::info!("Initializing NullSourceChangeEventDispatcher...");

        Box::new(Self {})
    }
}  

impl SourceChangeEventDispatcher for NullSourceChangeEventDispatcher {
    fn dispatch_source_change_event(&mut self, _event: &SourceChangeEvent) -> anyhow::Result<()> {
        Ok(())
    }
}