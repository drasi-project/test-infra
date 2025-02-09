use async_trait::async_trait;
use redis_result_stream_handler::RedisResultStreamHandler;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

use test_data_store::{test_repo_storage::models::TestQueryDefinition, test_run_storage::TestRunQueryId};

pub mod redis_result_stream_handler;

#[derive(Debug, thiserror::Error)]
pub enum ResultStreamHandlerError {
    #[error("Invalid {0} command, reader is currently in state: {1}")]
    InvalidCommand(String, String),
    #[error("Invalid stream data")]
    InvalidStreamData,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error: {0}")]
    Serde(#[from]serde_json::Error),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("Conversion error")]
    ConversionError,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResultStreamHandlerStatus {
    Uninitialized,
    Running,
    Paused,
    Stopped,
    Error
}

impl Serialize for ResultStreamHandlerStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            ResultStreamHandlerStatus::Uninitialized => serializer.serialize_str("Uninitialized"),
            ResultStreamHandlerStatus::Running => serializer.serialize_str("Running"),
            ResultStreamHandlerStatus::Paused => serializer.serialize_str("Paused"),
            ResultStreamHandlerStatus::Stopped => serializer.serialize_str("Stopped"),
            ResultStreamHandlerStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Debug)]
pub enum ResultStreamHandlerMessage {
    Record(ResultStreamRecord),
    Error(ResultStreamHandlerError),
    TestCompleted
}

#[derive(Clone, Debug, Serialize)]
pub struct ResultStreamRecord {
    pub record_data: serde_json::Value,
    pub dequeue_time_ns: u64,
    pub enqueue_time_ns: u64,
    pub id: String,
    pub seq: usize,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

impl opentelemetry::propagation::Extractor for ResultStreamRecord {
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

impl std::fmt::Display for ResultStreamRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {

        match serde_json::to_string(self) {
            Ok(json_data) => {
                let json_data_unescaped = json_data
                    .replace("\\\"", "\"") 
                    .replace("\\'", "'"); 

                write!(f, "{}", json_data_unescaped)
            },
            Err(e) => return write!(f, "Error serializing ResultStreamRecord: {:?}. Error: {}", self, e),
        }
    }
}

#[async_trait]
pub trait ResultStreamHandler : Send + Sync {
    async fn init(&self) -> anyhow::Result<Receiver<ResultStreamHandlerMessage>>;
    async fn pause(&self) -> anyhow::Result<()>;
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self) -> anyhow::Result<()>;
}

#[async_trait]
impl ResultStreamHandler for Box<dyn ResultStreamHandler + Send + Sync> {
    async fn init(&self) -> anyhow::Result<Receiver<ResultStreamHandlerMessage>> {
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

pub async fn create_result_stream_handler (
    id: TestRunQueryId, 
    definition: TestQueryDefinition
) -> anyhow::Result<Box<dyn ResultStreamHandler + Send + Sync>> {
    match definition {
        TestQueryDefinition::RedisStream{common_def, unique_def} => {
            Ok(Box::new(RedisResultStreamHandler::new(id, common_def, unique_def).await?))            
        },
        TestQueryDefinition::DaprPubSub { .. } => {
            unimplemented!("DaprResultStreamHandler is not implemented yet")
        }
    }
}