use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum QueryResultRecord {
    #[serde(rename = "change")]
    Change(ChangeEvent),
    #[serde(rename = "control")]
    Control(ControlEvent),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BaseResultEvent {
    #[serde(rename = "queryId")]
    pub query_id: String,

    pub sequence: i64,

    #[serde(rename = "sourceTimeMs")]
    pub source_time_ms: i64,

    pub metadata: Option<BaseResultMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BaseResultMetadata {
    #[serde(rename = "changeEvent")]
    pub change_event: Option<HashMap<String, serde_json::Value>>,
    pub tracking: TrackingMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrackingMetadata {
    pub query: QueryTrackingMetadata,
    pub source: SourceTrackingMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryTrackingMetadata {
    pub dequeue_ms: u64,
    #[serde(rename = "queryEnd_ms")]
    pub query_end_ms: u64,
    #[serde(rename = "queryStart_ms")]
    pub query_start_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceTrackingMetadata {
    #[serde(rename = "changeDispatcherEnd_ms")]
    pub change_dispatcher_end_ms: u64,
    #[serde(rename = "changeDispatcherStart_ms")]
    pub change_dispatcher_start_ms: u64,
    #[serde(rename = "changeSvcEnd_ms")]
    pub change_svc_end_ms: u64,
    #[serde(rename = "changeSvcStart_ms")]
    pub change_svc_start_ms: u64,
    pub reactivator_ms: u64,
    pub seq: u64,
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatePayload {
    pub before: HashMap<String, serde_json::Value>,
    pub after: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ControlEvent {
    #[serde(flatten)]
    pub base: BaseResultEvent,

    #[serde(rename = "controlSignal")]
    pub control_signal: ControlSignal,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ControlSignal {
    #[serde(rename = "bootstrapStarted")]
    BootstrapStarted(BootstrapStartedSignal),
    #[serde(rename = "bootstrapCompleted")]
    BootstrapCompleted(BootstrapCompletedSignal),
    #[serde(rename = "running")]
    Running(RunningSignal),
    #[serde(rename = "stopped")]
    Stopped(StoppedSignal),
    #[serde(rename = "deleted")]
    Deleted(DeletedSignal),
}

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