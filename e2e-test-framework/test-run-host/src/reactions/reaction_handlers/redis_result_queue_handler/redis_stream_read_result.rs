use serde::{Deserialize, Serialize};

use crate::reactions::reaction_handlers::ReactionOutputRecord;

use super::{query_result_record::QueryResultRecord, ReactionHandlerError, ReactionHandlerMessage};

pub struct RedisStreamReadResult {
    pub dequeue_time_ns: u64,
    pub enqueue_time_ns: u64,
    pub error: Option<ReactionHandlerError>,
    pub id: String,
    pub record: Option<RedisStreamRecordData>,
    pub seq: usize,
}

impl TryInto<ReactionHandlerMessage> for RedisStreamReadResult {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ReactionHandlerMessage, Self::Error> {
        match self.record {
            Some(record) => {
                let reaction_collector_event = ReactionOutputRecord {
                    reaction_output_data: serde_json::to_value(&record.data).unwrap(),
                    dequeue_time_ns: self.dequeue_time_ns,
                    enqueue_time_ns: self.enqueue_time_ns,
                    id: record.id,
                    seq: self.seq,
                    traceparent: record.traceparent,
                    tracestate: record.tracestate
                };

                Ok(ReactionHandlerMessage::Record(reaction_collector_event))
            },
            None => {
                match self.error {
                    Some(e) => {
                        Ok(ReactionHandlerMessage::Error(e))
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