use crate::test_script::{SourceChangeEvent, test_script_player::TestScriptPlayerConfig};
use super::{SourceChangeEventDispatcher, SourceChangeDispatcherResult};

pub struct DaprSourceChangeEventDispatcher {}

impl DaprSourceChangeEventDispatcher {
    pub fn new(_app_config: &TestScriptPlayerConfig) -> Result<Box<dyn SourceChangeEventDispatcher>, SourceChangeDispatcherResult> {
        Err(SourceChangeDispatcherResult::Error("DaprSourceChangeEventDispatcher not implemented.".to_string()))
    }
}  

impl SourceChangeEventDispatcher for DaprSourceChangeEventDispatcher {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> Result<(), SourceChangeDispatcherResult> {
        println!("SourceChangeEvent: {:?}", event);
        Ok(())
    }
}