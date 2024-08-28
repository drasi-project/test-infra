pub mod bootstrap_script_reader;
pub mod change_script_player;
pub mod change_script_reader;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEvent {
    pub op: String,
    pub ts_ms: u64,
    pub schema: String,
    pub payload: SourceChangeEventPayload,
}

type SourceChangeEventBefore = serde_json::Value; // Arbitrary JSON object for before
type SourceChangeEventAfter = serde_json::Value; // Arbitrary JSON object for after

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEventPayload {
    pub source: SourceChangeEventSourceInfo,
    pub before: SourceChangeEventBefore, 
    pub after: SourceChangeEventAfter,  
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEventSourceInfo {
    pub db: String,
    pub table: String,
    pub ts_ms: u64,
    pub ts_sec: u64,
    pub lsn: u64,
}