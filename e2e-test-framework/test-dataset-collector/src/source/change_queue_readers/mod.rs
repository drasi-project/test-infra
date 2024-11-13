use async_trait::async_trait;

use none_change_queue_reader::NoneSourceChangeQueueReader;
use redis_change_queue_reader::RedisSourceChangeQueueReader;
use test_beacon_change_queue_reader::TestBeaconSourceChangeQueueReader;
use test_runner::script_source::SourceChangeEvent;

use crate::config::SourceChangeQueueReaderConfig;

pub mod none_change_queue_reader;
pub mod redis_change_queue_reader;
pub mod test_beacon_change_queue_reader;

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
    // async fn cancel_get_next_change(&mut self) -> anyhow::Result<()>;
}

#[async_trait]
impl SourceChangeQueueReader for Box<dyn SourceChangeQueueReader + Send + Sync> {
    async fn get_next_change(&mut self) -> anyhow::Result<SourceChangeEvent> {
        (**self).get_next_change().await
    }
    // async fn cancel_get_next_change(&mut self) -> anyhow::Result<()> {
    //     (**self).cancel_get_next_change().await
    // }
}

pub async fn get_source_change_queue_reader<S: Into<String>>(config: Option<SourceChangeQueueReaderConfig>, source_id: S) -> anyhow::Result<Box<dyn SourceChangeQueueReader + Send + Sync>> {
    match config {
        Some(SourceChangeQueueReaderConfig::Redis(config)) => RedisSourceChangeQueueReader::new(config, source_id).await,
        Some(SourceChangeQueueReaderConfig::TestBeacon(config)) => TestBeaconSourceChangeQueueReader::new(config, source_id).await,
        None => Ok(NoneSourceChangeQueueReader::new())
    }
}