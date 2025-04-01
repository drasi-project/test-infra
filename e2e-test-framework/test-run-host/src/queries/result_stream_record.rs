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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum QueryResultRecord {
    #[serde(rename = "change")]
    Change(ChangeEvent),
    #[serde(rename = "control")]
    Control(ControlEvent),
}

impl QueryResultRecord {
    pub fn get_source_seq(&self) -> i64 {
        match self {
            QueryResultRecord::Change(change) => {
                change.base.sequence
            },
            QueryResultRecord::Control(control) => {
                control.base.sequence
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BaseResultEvent {
    #[serde(rename = "queryId")]
    pub query_id: String,

    pub sequence: i64,

    #[serde(rename = "sourceTimeMs")]
    pub source_time_ms: i64,

    pub metadata: Option<BaseResultMetadata>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BaseResultMetadata {
    // #[serde(rename = "changeEvent")]
    // pub change_event: Option<HashMap<String, serde_json::Value>>,
    pub tracking: TrackingMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrackingMetadata {
    pub query: QueryTrackingMetadata,
    pub source: SourceTrackingMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryTrackingMetadata {
    pub dequeue_ns: u64,
    #[serde(rename = "queryEnd_ns")]
    pub query_end_ns: u64,
    #[serde(rename = "queryStart_ns")]
    pub query_start_ns: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceTrackingMetadata {
    #[serde(rename = "changeDispatcherEnd_ns")]
    pub change_dispatcher_end_ns: u64,
    #[serde(rename = "changeDispatcherStart_ns")]
    pub change_dispatcher_start_ns: u64,
    #[serde(rename = "changeRouterEnd_ns")]
    pub change_router_end_ns: u64,
    #[serde(rename = "changeRouterStart_ms")]
    pub change_router_start_ns: u64,
    #[serde(rename = "reactivatorStart_ns")]
    pub reactivator_start_ns: u64,
    #[serde(rename = "reactivatorEnd_ns")]
    pub reactivator_end_ns: u64,
    pub source_ns: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UpdatePayload {
    pub before: HashMap<String, serde_json::Value>,
    pub after: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlEvent {
    #[serde(flatten)]
    pub base: BaseResultEvent,

    #[serde(rename = "controlSignal")]
    pub control_signal: ControlSignal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapStartedSignal {
    // Additional fields can be added if necessary
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapCompletedSignal {
    // Additional fields can be added if necessary
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunningSignal {
    // Additional fields can be added if necessary
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoppedSignal {
    // Additional fields can be added if necessary
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeletedSignal {
    // Additional fields can be added if necessary
}