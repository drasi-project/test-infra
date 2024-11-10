use serde::{Deserialize, Serialize};
use test_runner::config::TestRepoConfig;

#[derive(Debug, Deserialize, Serialize)]
pub struct TestDatasetCollectorConfig {
    #[serde(default = "default_data_store_path")]
    pub data_store_path: String,
    #[serde(default = "default_prune_data_store_path")]
    pub prune_data_store_path: bool,
    #[serde(default)]
    pub test_repos: Vec<TestRepoConfig>,
    #[serde(default)]
    pub datasets: Vec<DatasetConfig>,
}
fn default_data_store_path() -> String { "./test_dataset_collector_data".to_string() }
fn default_prune_data_store_path() -> bool { false }

impl Default for TestDatasetCollectorConfig {
    fn default() -> Self {
        TestDatasetCollectorConfig {
            data_store_path: default_data_store_path(),
            prune_data_store_path: default_prune_data_store_path(),
            test_repos: Vec::new(),
            datasets: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DatasetConfig {
    pub dataset_id: String,
    pub queries: Vec<QueryConfig>,
    pub sources: Vec<SourceConfig>,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        DatasetConfig {
            dataset_id: "default_dataset".to_string(),
            queries: Vec::new(),
            sources: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryConfig {
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

#[derive(Debug, Deserialize, Serialize)]
pub struct SourceConfig {
    pub source_id: String,
    #[serde(default = "default_start_immediately")]
    pub start_immediately: bool,
    #[serde(default)]
    pub bootstrap_data_recorder: Option<SourceBootstrapDataRecorderConfig>,
    pub source_change_recorder: Option<SourceChangeRecorderConfig>,
}
fn default_start_immediately() -> bool { false }

impl Default for SourceConfig {
    fn default() -> Self {
        SourceConfig {
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

#[derive(Debug, Deserialize, Serialize)]
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