use async_trait::async_trait;

use test_runner::script_source::SourceChangeEvent;

pub mod none_change_queue_reader;
pub mod redis_change_queue_reader;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeQueueReaderError {
    Io(#[from] std::io::Error),
    Serde(#[from]serde_json::Error),
}

impl std::fmt::Display for SourceChangeQueueReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
        }
    }
}

#[async_trait]
pub trait SourceChangeQueueReader : Send + Sync {
    async fn get_next_change(&mut self) -> anyhow::Result<SourceChangeEvent>;
}

#[async_trait]
impl SourceChangeQueueReader for Box<dyn SourceChangeQueueReader + Send + Sync> {
    async fn get_next_change(&mut self) -> anyhow::Result<SourceChangeEvent> {
        (**self).get_next_change().await
    }
}