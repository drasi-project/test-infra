use async_trait::async_trait;
use test_runner::script_source::SourceChangeEvent;

use super::SourceChangeQueueReader;

pub struct NoneSourceChangeQueueReader {}

impl NoneSourceChangeQueueReader {
    pub fn new() -> Box<dyn SourceChangeQueueReader + Send + Sync> {
        log::debug!("Creating NoneSourceChangeQueueReader");

        Box::new(NoneSourceChangeQueueReader {})
    }
}  

#[async_trait]
impl SourceChangeQueueReader for NoneSourceChangeQueueReader {
    async fn get_next_change(&mut self) -> anyhow::Result<SourceChangeEvent> {
        log::trace!("NoneSourceChangeQueueReader - get_next_change");

        tokio::time::sleep(tokio::time::Duration::MAX).await;

        anyhow::bail!("NoneSourceChangeQueueReader.get_next_change should never return");
    }
}