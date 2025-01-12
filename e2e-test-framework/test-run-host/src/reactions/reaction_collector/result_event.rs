use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// Enum to represent versions
#[derive(Debug, Serialize, Deserialize)]
pub enum Versions {
    V1,
}

// Base struct for ResultEvent
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ResultEvent {
    Change(ChangeEvent),
    Control(ControlEvent),
}

// Base fields for ResultEvent
#[derive(Debug, Serialize, Deserialize)]
pub struct BaseResultEvent {
    #[serde(rename = "queryId")]
    pub query_id: String,

    pub sequence: i64,

    #[serde(rename = "sourceTimeMs")]
    pub source_time_ms: i64,

    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

// ChangeEvent struct
#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeEvent {
    #[serde(flatten)]
    pub base: BaseResultEvent,

    #[serde(rename = "addedResults")]
    pub added_results: Vec<HashMap<String, serde_json::Value>>,

    #[serde(rename = "updatedResults")]
    pub updated_results: Vec<UpdatePayload>,

    #[serde(rename = "deletedResults")]
    pub deleted_results: Vec<HashMap<String, serde_json::Value>>,
}

// UpdatePayload struct
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatePayload {
    pub before: HashMap<String, serde_json::Value>,
    pub after: HashMap<String, serde_json::Value>,
}

// ControlEvent struct
#[derive(Debug, Serialize, Deserialize)]
pub struct ControlEvent {
    #[serde(flatten)]
    pub base: BaseResultEvent,

    #[serde(rename = "controlSignal")]
    pub control_signal: ControlSignal,
}

// ControlSignal enum
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ControlSignal {
    BootstrapStarted(BootstrapStartedSignal),
    BootstrapCompleted(BootstrapCompletedSignal),
    Running(RunningSignal),
    Stopped(StoppedSignal),
    Deleted(DeletedSignal),
}

// Individual signal structs
#[derive(Debug, Serialize, Deserialize)]
pub struct BootstrapStartedSignal {
    // Additional fields can be added if necessary
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BootstrapCompletedSignal {
    // Additional fields can be added if necessary
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunningSignal {
    // Additional fields can be added if necessary
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoppedSignal {
    // Additional fields can be added if necessary
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeletedSignal {
    // Additional fields can be added if necessary
}

impl TryFrom<&str> for ResultEvent {
    type Error = serde_json::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

impl TryFrom<&String> for ResultEvent {
    type Error = serde_json::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}