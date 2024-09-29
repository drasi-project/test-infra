use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

pub mod bootstrap_script_file_reader;
pub mod change_script_file_reader;
pub mod change_script_file_writer;

type SourceChangeEventBefore = serde_json::Value; // Arbitrary JSON object for before
type SourceChangeEventAfter = serde_json::Value; // Arbitrary JSON object for after

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEvent {
    pub op: String,
    pub ts_ms: u64,
    pub schema: String,
    pub payload: SourceChangeEventPayload,
}

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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")] // This will use the "type" field to determine the enum variant
pub enum ChangeScriptRecord {
    Comment(CommentRecord),
    Header(HeaderRecord),
    Label(LabelRecord),
    PauseCommand(PauseCommandRecord),
    SourceChange(SourceChangeRecord),
    Finish(FinishRecord),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommentRecord {
    pub comment: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeaderRecord {
    pub start_time: DateTime<FixedOffset>,
    #[serde(default)]
    pub description: String,
}

impl Default for HeaderRecord {
    fn default() -> Self {
        HeaderRecord {
            start_time: DateTime::parse_from_rfc3339("1970-01-01T00:00:00.000-00:00").unwrap(),
            description: "Error: Header record not found.".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelRecord {
    #[serde(default)]
    pub offset_ns: u64,
    pub label: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PauseCommandRecord {
    #[serde(default)]
    pub offset_ns: u64,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FinishRecord {
    #[serde(default)]
    pub offset_ns: u64,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeRecord {
    #[serde(default)]
    pub offset_ns: u64,
    pub source_change_event: SourceChangeEvent,
}

// The SequencedChangeScriptRecord struct wraps a ChangeScriptRecord and ensures that each record has a 
// sequence number and an offset_ns field. The sequence number is the order in which the record was read
// from the script files. The offset_ns field the nanos since the start of the script starting time, 
// which is the start_time field in the Header record.
#[derive(Clone, Debug, Serialize)]
pub struct SequencedChangeScriptRecord {
    pub seq: u64,
    pub offset_ns: u64,
    pub record: ChangeScriptRecord,
}
