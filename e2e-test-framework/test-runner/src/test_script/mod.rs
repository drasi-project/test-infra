pub mod bootstrap_script_reader;
pub mod change_script_player;
pub mod change_script_reader;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEvent {
    pub payload: SourceChangeEventPayload,
    pub schema: serde_json::Value, // Assuming schema can be any valid JSON
}

type SourceChangeEventBefore = serde_json::Value; // Arbitrary JSON object for before
type SourceChangeEventAfter = serde_json::Value; // Arbitrary JSON object for after

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEventPayload {
    pub op: String,
    pub ts_ms: u64,
    pub ts_us: u64,
    pub ts_ns: u64,
    pub source: SourceChangeEventSourceInfo,
    pub before: SourceChangeEventBefore, 
    pub after: SourceChangeEventAfter,  
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEventSourceInfo {
    pub db: String,
    pub table: String,
    pub ts_ms: u64,
    pub ts_us: u64,
    pub ts_ns: u64,
    pub lsn: u64,
    #[serde(rename = "txId")]
    pub tx_id: u64,
    pub schema: String,
}