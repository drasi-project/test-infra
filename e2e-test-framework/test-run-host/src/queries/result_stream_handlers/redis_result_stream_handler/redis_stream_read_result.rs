use serde::{Deserialize, Serialize};

use crate::queries::{result_stream_handlers::ResultStreamRecord, result_stream_record::QueryResultRecord};

use super::{ResultStreamHandlerError, ResultStreamHandlerMessage};

pub struct RedisStreamReadResult {
    pub dequeue_time_ns: u64,
    pub enqueue_time_ns: u64,
    pub error: Option<ResultStreamHandlerError>,
    pub id: String,
    pub record: Option<RedisStreamRecordData>,
    pub seq: usize,
}

impl TryInto<ResultStreamHandlerMessage> for RedisStreamReadResult {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ResultStreamHandlerMessage, Self::Error> {
        match self.record {
            Some(record) => {
                let result_stream_record = ResultStreamRecord {
                    record_data: record.data,
                    dequeue_time_ns: self.dequeue_time_ns,
                    enqueue_time_ns: self.enqueue_time_ns,
                    id: record.id,
                    seq: self.seq,
                    traceparent: record.traceparent,
                    tracestate: record.tracestate
                };

                Ok(ResultStreamHandlerMessage::Record(result_stream_record))
            },
            None => {
                match self.error {
                    Some(e) => {
                        Ok(ResultStreamHandlerMessage::Error(e))
                    },
                    None => {
                        Err(anyhow::anyhow!("No record or error found in stream entry"))
                    }
                }
            }
        }
    }
}    

#[derive(Debug, Serialize, Deserialize)]
pub struct RedisStreamRecordData {
    pub data: QueryResultRecord,
    pub id: String,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>
}

impl TryFrom<&str> for RedisStreamRecordData {
    type Error = serde_json::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

impl TryFrom<&String> for RedisStreamRecordData {
    type Error = serde_json::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}