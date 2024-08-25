use crate::test_script::{SourceChangeEvent, change_script_player::ChangeScriptPlayerConfig};
use super::{SourceChangeEventDispatcher, SourceChangeDispatcherError};

pub struct DaprSourceChangeEventDispatcher {}

impl DaprSourceChangeEventDispatcher {
    pub fn new(_app_config: &ChangeScriptPlayerConfig) -> anyhow::Result<Box<dyn SourceChangeEventDispatcher>> {
        Err(SourceChangeDispatcherError::NotImplemented("DaprSourceChangeEventDispatcher.".to_string()).into())
    }
}  

impl SourceChangeEventDispatcher for DaprSourceChangeEventDispatcher {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> anyhow::Result<()> {
        println!("SourceChangeEvent: {:?}", event);
        Ok(())
    }
}