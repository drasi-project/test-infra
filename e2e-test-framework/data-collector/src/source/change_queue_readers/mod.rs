// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;

use none_change_queue_reader::NoneSourceChangeQueueReader;
use redis_change_queue_reader::RedisSourceChangeQueueReader;
use serde::{Deserialize, Serialize};
use test_beacon_change_queue_reader::TestBeaconSourceChangeQueueReader;
use tokio::sync::mpsc::Receiver;

use crate::config::SourceChangeQueueReaderConfig;

pub mod none_change_queue_reader;
pub mod redis_change_queue_reader;
pub mod test_beacon_change_queue_reader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceChangeQueueReaderStatus {
    Uninitialized,
    Running,
    Paused,
    Stopped,
    Error
}

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeQueueReaderError {
    #[error("Invalid {0} command, reader is currently in state: {1}")]
    InvalidCommand(String, String),
    #[error("Invalid queue data")]
    InvalidQueueData,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error: {0}")]
    Serde(#[from]serde_json::Error),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
}

// impl std::fmt::Display for SourceChangeQueueReaderError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Self::Io(e) => write!(f, "IO error: {}:", e),
//             Self::Serde(e) => write!(f, "Serde error: {}:", e),
//         }
//     }
// }

#[derive(Debug)]
pub enum SourceChangeQueueReaderMessage {
    QueueRecord(SourceChangeQueueRecord),
    Error(SourceChangeQueueReaderError),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeQueueRecord {
    pub change_events: Vec<serde_json::Value>,
    pub dequeue_time_ns: u64,
    pub enqueue_time_ns: u64,
    pub id: String,
    pub seq: usize,
    pub traceid: String,
    pub traceparent: String,
}

impl TryFrom<&str> for SourceChangeQueueRecord {
    type Error = serde_json::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

impl TryFrom<&String> for SourceChangeQueueRecord {
    type Error = serde_json::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

#[async_trait]
pub trait SourceChangeQueueReader : Send + Sync {
    async fn init(&self) -> anyhow::Result<Receiver<SourceChangeQueueReaderMessage>>;
    async fn pause(&self) -> anyhow::Result<()>;
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self) -> anyhow::Result<()>; 
}

#[async_trait]
impl SourceChangeQueueReader for Box<dyn SourceChangeQueueReader + Send + Sync> {
    async fn init(&self) -> anyhow::Result<Receiver<SourceChangeQueueReaderMessage>> {
        (**self).init().await
    }

    async fn pause(&self) -> anyhow::Result<()> {
        (**self).pause().await
    }

    async fn start(&self) -> anyhow::Result<()> {
        (**self).start().await
    }

    async fn stop(&self) -> anyhow::Result<()> {
        (**self).stop().await
    }
}

pub async fn get_source_change_queue_reader<S: Into<String>>(config: Option<SourceChangeQueueReaderConfig>, source_id: S) -> anyhow::Result<Box<dyn SourceChangeQueueReader + Send + Sync>> {
    match config {
        Some(SourceChangeQueueReaderConfig::Redis(config)) => RedisSourceChangeQueueReader::new(config, source_id).await,
        Some(SourceChangeQueueReaderConfig::TestBeacon(config)) => TestBeaconSourceChangeQueueReader::new(config, source_id).await,
        None => Ok(NoneSourceChangeQueueReader::new())
    }
}