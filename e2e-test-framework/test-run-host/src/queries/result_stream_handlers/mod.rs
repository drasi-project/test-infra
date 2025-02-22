use async_trait::async_trait;
use redis_result_stream_handler::RedisResultStreamHandler;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

use test_data_store::{test_repo_storage::models::ResultStreamHandlerDefinition, test_run_storage::TestRunQueryId};

use super::result_stream_record::QueryResultRecord;

pub mod redis_result_stream_handler;


#[derive(Debug, thiserror::Error)]
pub enum ResultStreamHandlerError {
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
pub enum ResultStreamStatus {
    Unknown,
    BootstrapStarted,
    BootstrapComplete,
    Running,
    Stopped,
    Deleted
}

impl Serialize for ResultStreamStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            ResultStreamStatus::Unknown => serializer.serialize_str("Unknown"),
            ResultStreamStatus::BootstrapStarted => serializer.serialize_str("BootstrapStarted"),
            ResultStreamStatus::BootstrapComplete => serializer.serialize_str("BootstrapComplete"),
            ResultStreamStatus::Running => serializer.serialize_str("Running"),
            ResultStreamStatus::Stopped => serializer.serialize_str("Stopped"),
            ResultStreamStatus::Deleted => serializer.serialize_str("Deleted"),
        }
    }
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
}

#[derive(Clone, Debug, Serialize)]
pub struct ResultStreamRecord {
    pub record_data: QueryResultRecord,
    pub dequeue_time_ns: u64,
    pub enqueue_time_ns: u64,
    pub id: String,
    pub seq: usize,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

impl opentelemetry_api::propagation::Extractor for ResultStreamRecord {
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
    definition: ResultStreamHandlerDefinition
) -> anyhow::Result<Box<dyn ResultStreamHandler + Send + Sync>> {
    match definition {
        ResultStreamHandlerDefinition::RedisStream(definition) => {
            Ok(Box::new(RedisResultStreamHandler::new(id, definition).await?))            
        },
        ResultStreamHandlerDefinition::DaprPubSub(_) => {
            unimplemented!("DaprResultStreamHandler is not implemented yet")
        }
    }
}