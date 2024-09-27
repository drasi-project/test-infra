pub mod bootstrap_script_file_reader;
pub mod change_script_file_reader;

use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEvent {
    pub op: String,
    pub ts_ms: u64,
    pub schema: String,
    pub payload: SourceChangeEventPayload,
}

impl std::fmt::Display for SourceChangeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {

        match serde_json::to_string(self) {
            Ok(json_data) => {
                let json_data_unescaped = json_data
                    .replace("\\\"", "\"") 
                    .replace("\\'", "'"); 

                write!(f, "{}", json_data_unescaped)
            },
            Err(e) => return write!(f, "Error serializing SourceChangeEvent: {:?}. Error: {}", self, e),
        }
    }
}