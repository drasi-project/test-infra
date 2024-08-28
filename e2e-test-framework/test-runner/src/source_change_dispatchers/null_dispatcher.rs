
use async_trait::async_trait;

use crate::test_script::SourceChangeEvent;
use super::SourceChangeEventDispatcher;

pub struct NullSourceChangeEventDispatcher {}

impl NullSourceChangeEventDispatcher {
    pub fn new() -> Box<dyn SourceChangeEventDispatcher> {

        log::info!("Initializing NullSourceChangeEventDispatcher...");

        Box::new(Self {})
    }
}  

#[async_trait]
impl SourceChangeEventDispatcher for NullSourceChangeEventDispatcher {
    async fn dispatch_source_change_event(&mut self, _event: &SourceChangeEvent) -> anyhow::Result<()> {
        Ok(())
    }
}