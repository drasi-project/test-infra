// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

pub mod bootstrap_script_file_reader;
pub mod bootstrap_script_file_writer;
pub mod change_script_file_reader;
pub mod change_script_file_writer;

pub type SourceChangeEventBefore = serde_json::Value; // Arbitrary JSON object for before
pub type SourceChangeEventAfter = serde_json::Value; // Arbitrary JSON object for after

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEvent {
    #[serde(rename = "reactivatorEnd_ns")]
    pub reactivator_end_ns: u64,
    pub op: String,
    #[serde(rename = "reactivatorStart_ns")]
    pub reactivator_start_ns: u64,
    pub payload: SourceChangeEventPayload,
}

impl TryFrom<&str> for SourceChangeEvent {
    type Error = serde_json::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

impl TryFrom<&String> for SourceChangeEvent {
    type Error = serde_json::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
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
    pub ts_ns: u64,
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
#[serde(tag = "kind")] // This will use the "kind" field to determine the enum variant
pub enum BootstrapScriptRecord {
    Comment(CommentRecord),
    Header(BootstrapHeaderRecord),
    Label(LabelRecord),
    Node(NodeRecord),
    Relation(RelationRecord),
    Finish(BootstrapFinishRecord),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")] // This will use the "kind" field to determine the enum variant
pub enum ChangeScriptRecord {
    Comment(CommentRecord),
    Header(ChangeHeaderRecord),
    Label(LabelRecord),
    PauseCommand(PauseCommandRecord),
    SourceChange(SourceChangeRecord),
    Finish(ChangeFinishRecord),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommentRecord {
    pub comment: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapHeaderRecord {
    pub start_time: DateTime<FixedOffset>,
    #[serde(default)]
    pub description: String,
}

impl Default for BootstrapHeaderRecord {
    fn default() -> Self {
        BootstrapHeaderRecord {
            start_time: DateTime::parse_from_rfc3339("1970-01-01T00:00:00.000-00:00").unwrap(),
            description: "Error: Header record not found.".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeHeaderRecord {
    pub start_time: DateTime<FixedOffset>,
    #[serde(default)]
    pub description: String,
}

impl Default for ChangeHeaderRecord {
    fn default() -> Self {
        ChangeHeaderRecord {
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
pub struct BootstrapFinishRecord {
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeFinishRecord {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SequencedChangeScriptRecord {
    pub seq: u64,
    pub offset_ns: u64,
    pub record: ChangeScriptRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: String,
    pub labels: Vec<String>,
    #[serde(default)]
    pub properties: serde_json::Value
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationRecord {
    pub id: String,
    pub labels: Vec<String>,
    pub start_id: String,
    pub start_label: Option<String>,
    pub end_id: String,
    pub end_label: Option<String>,    
    #[serde(default)]
    pub properties: serde_json::Value
}