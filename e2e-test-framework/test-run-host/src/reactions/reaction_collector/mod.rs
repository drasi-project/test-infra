use async_trait::async_trait;
use redis_result_queue_collector::RedisResultQueueCollector;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

use test_data_store::{test_repo_storage::models::TestReactionDefinition, test_run_storage::TestRunReactionId};

pub mod redis_result_queue_collector;
pub mod result_event;

#[derive(Debug, thiserror::Error)]
pub enum ReactionCollectorError {
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
    #[error("Coversion error")]
    ConversionError
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReactionCollectorStatus {
    Uninitialized,
    Running,
    Paused,
    Stopped,
    Error
}

impl Serialize for ReactionCollectorStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            ReactionCollectorStatus::Uninitialized => serializer.serialize_str("Uninitialized"),
            ReactionCollectorStatus::Running => serializer.serialize_str("Running"),
            ReactionCollectorStatus::Paused => serializer.serialize_str("Paused"),
            ReactionCollectorStatus::Stopped => serializer.serialize_str("Stopped"),
            ReactionCollectorStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Debug)]
pub enum ReactionCollectorMessage {
    Record(ReactionOutputRecord),
    Error(ReactionCollectorError),
    TestCompleted
}

#[derive(Clone, Debug, Serialize)]
pub struct ReactionOutputRecord {
    pub result_data: serde_json::Value,
    pub dequeue_time_ns: u64,
    pub enqueue_time_ns: u64,
    pub id: String,
    pub seq: usize,
    pub traceid: String,
    pub traceparent: String,
}

impl std::fmt::Display for ReactionOutputRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {

        match serde_json::to_string(self) {
            Ok(json_data) => {
                let json_data_unescaped = json_data
                    .replace("\\\"", "\"") 
                    .replace("\\'", "'"); 

                write!(f, "{}", json_data_unescaped)
            },
            Err(e) => return write!(f, "Error serializing ReactionOutputRecord: {:?}. Error: {}", self, e),
        }
    }
}

#[async_trait]
pub trait ReactionCollector : Send + Sync {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionCollectorMessage>>;
    async fn pause(&self) -> anyhow::Result<()>;
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self) -> anyhow::Result<()>;
}

#[async_trait]
impl ReactionCollector for Box<dyn ReactionCollector + Send + Sync> {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionCollectorMessage>> {
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

pub async fn create_reaction_collector (
    id: TestRunReactionId, 
    definition: TestReactionDefinition
) -> anyhow::Result<Box<dyn ReactionCollector + Send + Sync>> {
    match definition {
        TestReactionDefinition::RedisResultQueue{common_def, unique_def} => {
            Ok(Box::new(RedisResultQueueCollector::new(id, common_def, unique_def).await?))            
        },
        TestReactionDefinition::DaprResultQueue { .. } => {
            unimplemented!("DaprResultQueue is not implemented yet")
        }
    }
}