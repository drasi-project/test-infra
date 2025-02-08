use async_trait::async_trait;
use redis_result_stream_handler::RedisResultQueueHandler;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

use test_data_store::{test_repo_storage::models::TestReactionDefinition, test_run_storage::TestRunReactionId};

pub mod redis_result_stream_handler;

#[derive(Debug, thiserror::Error)]
pub enum ReactionHandlerError {
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
    ConversionError,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReactionHandlerStatus {
    Uninitialized,
    Running,
    Paused,
    Stopped,
    Error
}

impl Serialize for ReactionHandlerStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            ReactionHandlerStatus::Uninitialized => serializer.serialize_str("Uninitialized"),
            ReactionHandlerStatus::Running => serializer.serialize_str("Running"),
            ReactionHandlerStatus::Paused => serializer.serialize_str("Paused"),
            ReactionHandlerStatus::Stopped => serializer.serialize_str("Stopped"),
            ReactionHandlerStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Debug)]
pub enum ReactionHandlerMessage {
    Record(ReactionOutputRecord),
    Error(ReactionHandlerError),
    TestCompleted
}

#[derive(Clone, Debug, Serialize)]
pub struct ReactionOutputRecord {
    pub reaction_output_data: serde_json::Value,
    pub dequeue_time_ns: u64,
    pub enqueue_time_ns: u64,
    pub id: String,
    pub seq: usize,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

impl opentelemetry::propagation::Extractor for ReactionOutputRecord {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.traceparent.as_deref(),
            "tracestate" => self.tracestate.as_deref(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent", "tracestate"]
    }
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
pub trait ReactionHandler : Send + Sync {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>>;
    async fn pause(&self) -> anyhow::Result<()>;
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self) -> anyhow::Result<()>;
}

#[async_trait]
impl ReactionHandler for Box<dyn ReactionHandler + Send + Sync> {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
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

pub async fn create_reaction_handler (
    id: TestRunReactionId, 
    definition: TestReactionDefinition
) -> anyhow::Result<Box<dyn ReactionHandler + Send + Sync>> {
    match definition {
        TestReactionDefinition::RedisResultQueue{common_def, unique_def} => {
            Ok(Box::new(RedisResultQueueHandler::new(id, common_def, unique_def).await?))            
        },
        TestReactionDefinition::DaprResultQueue { .. } => {
            unimplemented!("DaprResultQueue is not implemented yet")
        }
    }
}