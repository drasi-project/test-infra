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

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DataCollectorConfig {
    #[serde(default)]
    pub data_collections: Vec<DataCollectionConfig>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataCollectionConfig {
    pub id: String,
    pub queries: Vec<DataCollectionQueryConfig>,
    pub sources: Vec<DataCollectionSourceConfig>,
}

impl Default for DataCollectionConfig {
    fn default() -> Self {
        DataCollectionConfig {
            id: "default_data_collection".to_string(),
            queries: Vec::new(),
            sources: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataCollectionQueryConfig {
    pub query_id: Option<String>,
    pub result_event_recorders: Vec<QueryResultEventRecorderConfig>,
    pub result_set_loggers: Vec<QueryResultSetLoggerConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum QueryResultEventRecorderConfig {
    Console(ConsoleQueryResultEventRecorderConfig),
    JsonlFile(JsonlFileQueryResultEventRecorderConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleQueryResultEventRecorderConfig {
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileQueryResultEventRecorderConfig {
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum QueryResultSetLoggerConfig {
    Console(ConsoleQueryResultSetLoggerConfig),
    JsonlFile(JsonlFileQueryResultSetLoggerConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleQueryResultSetLoggerConfig {
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileQueryResultSetLoggerConfig {
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataCollectionSourceConfig {
    pub source_id: String,
    #[serde(default = "default_start_immediately")]
    pub start_immediately: bool,
    #[serde(default)]
    pub bootstrap_data_recorder: Option<SourceBootstrapDataRecorderConfig>,
    pub source_change_recorder: Option<SourceChangeRecorderConfig>,
}
fn default_start_immediately() -> bool { false }

impl Default for DataCollectionSourceConfig {
    fn default() -> Self {
        DataCollectionSourceConfig {
            source_id: "default_source".to_string(),
            start_immediately: false,
            bootstrap_data_recorder: None,
            source_change_recorder: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceBootstrapDataRecorderConfig
{
    pub node_labels: Option<Vec<String>>,
    pub relation_labels: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceBootstrapDataLoggerConfig {
    Console(ConsoleSourceBootstrapDataLoggerConfig),
    Script(ScriptSourceBootstrapDataLoggerConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleSourceBootstrapDataLoggerConfig {
    pub date_time_format: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptSourceBootstrapDataLoggerConfig {
    pub max_record_count: Option<u32>,
    pub start_time_mode: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceChangeRecorderConfig {
    pub drain_queue_on_stop: Option<bool>,
    #[serde(default)]
    pub change_queue_reader: Option<SourceChangeQueueReaderConfig>,
    #[serde(default)]
    pub change_event_loggers: Vec<SourceChangeEventLoggerConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceChangeQueueReaderConfig {
    Redis(RedisSourceChangeQueueReaderConfig),
    TestBeacon(TestBeaconSourceChangeQueueReaderConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisSourceChangeQueueReaderConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub queue_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestBeaconSourceChangeQueueReaderConfig {
    pub interval_ns: Option<u32>,
    pub record_count: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceChangeEventLoggerConfig {
    Console(ConsoleSourceChangeEventLoggerConfig),
    Script(ScriptSourceChangeEventLoggerConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleSourceChangeEventLoggerConfig {
    pub date_time_format: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptSourceChangeEventLoggerConfig {
    pub max_record_count: Option<u32>,
    pub start_time_mode: Option<String>,
}