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

use std::{num::NonZeroU32, str::FromStr};

use serde::{Deserialize, Serialize, de::{self, Deserializer}};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum TimeMode {
    Live,
    Recorded,
    Rebased(u64),
}

impl Default for TimeMode {
    fn default() -> Self {
        Self::Recorded
    }
}

impl FromStr for TimeMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "recorded" => Ok(Self::Recorded),
            _ => {
                match chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(t) => Ok(Self::Rebased(t.timestamp_nanos_opt().unwrap() as u64)),
                    Err(e) => {
                        anyhow::bail!("Error parsing TimeMode - value:{}, error:{}", s, e);
                    }
                }
            }
        }
    }
}

impl std::fmt::Display for TimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Recorded => write!(f, "recorded"),
            Self::Rebased(time) => write!(f, "{}", time),
        }
    }
}

impl<'de> Deserialize<'de> for TimeMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: String = Deserialize::deserialize(deserializer)?;
        value.parse::<TimeMode>().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SpacingMode {
    None,
    Rate(NonZeroU32),
    Recorded,
}

impl Default for SpacingMode {
    fn default() -> Self {
        Self::Recorded
    }
}

impl FromStr for SpacingMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "recorded" => Ok(Self::Recorded),
            _ => {
                // Parse the string as a NonZero<u32>.
                match s.parse::<u32>() {
                    Ok(num) => match NonZeroU32::new(num) {
                        Some(rate) => Ok(Self::Rate(rate)),
                        None => anyhow::bail!("Invalid SpacingMode: {}", s),
                    },
                    Err(e) => {
                        anyhow::bail!("Error parsing SpacingMode: {}", e);
                    }
                }
            }
        }
    }
}

impl std::fmt::Display for SpacingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Recorded => write!(f, "recorded"),
            Self::Rate(r) => write!(f, "{}", r),
        }
    }
}

impl<'de> Deserialize<'de> for SpacingMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: String = Deserialize::deserialize(deserializer)?;
        value.parse::<SpacingMode>().map_err(de::Error::custom)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LocalTestDefinition {
    pub test_id: String,
    pub version: u32,
    pub description: Option<String>,
    pub test_folder: Option<String>,
    #[serde(default)]
    pub queries: Vec<TestQueryDefinition>,
    #[serde(default)]
    pub sources: Vec<TestSourceDefinition>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TestDefinition {
    #[serde(skip_deserializing)]
    #[serde(default)]
    pub test_id: String,
    pub version: u32,
    pub description: Option<String>,
    pub test_folder: Option<String>,
    #[serde(default)]
    pub queries: Vec<TestQueryDefinition>,
    #[serde(default)]
    pub sources: Vec<TestSourceDefinition>,    
}

impl TestDefinition {
    pub fn get_test_query(&self, query_id: &str) -> anyhow::Result<TestQueryDefinition> {
        let test_query_definition = self.queries.iter()
            .find(|query| query.test_query_id == query_id)
            .ok_or_else(|| {
                anyhow::anyhow!("Test Query with ID {:?} not found", query_id)
            })?;

        Ok(test_query_definition.clone())
    }

    
    pub fn get_test_source(&self, source_id: &str) -> anyhow::Result<TestSourceDefinition> {
        let test_source_definition = self.sources.iter().find(|source| {
            match source {
                TestSourceDefinition::Model(def) => { def.common.test_source_id == source_id },
                TestSourceDefinition::Script(def) => def.common.test_source_id == source_id,
            }
        }).ok_or_else(|| {
            anyhow::anyhow!("Test Source with ID {:?} not found", source_id)
        })?;        

        Ok(test_source_definition.clone())
    }

}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]

pub enum TestSourceDefinition {
    Model(ModelTestSourceDefinition),
    Script(ScriptTestSourceDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonTestSourceDefinition {
    pub test_source_id: String,
    #[serde(default)]
    pub source_change_dispatchers: Vec<SourceChangeDispatcherDefinition>,
    #[serde(default)]
    pub subscribers: Vec<QueryId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptTestSourceDefinition {
    pub bootstrap_data_generator: Option<BootstrapDataGeneratorDefinition>,
    #[serde(flatten)]
    pub common: CommonTestSourceDefinition,
    pub source_change_generator: Option<SourceChangeGeneratorDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelTestSourceDefinition {
    #[serde(flatten)]
    pub common: CommonTestSourceDefinition,
    pub model_data_generator: Option<ModelDataGeneratorDefinition>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum BootstrapDataGeneratorDefinition {
    Script(ScriptBootstrapDataGeneratorDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonBootstrapDataGeneratorDefinition {
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptBootstrapDataGeneratorDefinition {
    #[serde(flatten)]
    pub common: CommonBootstrapDataGeneratorDefinition,
    pub script_file_folder: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ModelDataGeneratorDefinition {
    BuildingHierarchy(BuildingHierarchyDataGeneratorDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonModelDataGeneratorDefinition {
    pub change_count: Option<u64>,
    pub seed: Option<u64>,
    #[serde(default)]
    pub spacing_mode: SpacingMode,
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildingHierarchyDataGeneratorDefinition {
    #[serde(flatten)]
    pub common: CommonModelDataGeneratorDefinition,
    pub initialization: BuildingHierarchyDataGeneratorInitializationDefinition,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildingHierarchyDataGeneratorInitializationDefinition {
    pub building_count: Option<(u32, f64)>,
    pub floor_count: Option<(u32, f64)>,
    pub room_count: Option<(u32, f64)>,
    pub sensor_co2: Option<(f64, f64)>,
    pub sensor_humidity: Option<(f64, f64)>,
    pub sensor_light: Option<(f64, f64)>,
    pub sensor_noise: Option<(f64, f64)>,
    pub sensor_temperature: Option<(f64, f64)>,
    pub sensor_occupancy: Option<(u32, f64)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SourceChangeGeneratorDefinition {
    Script(ScriptSourceChangeGeneratorDefinition),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonSourceChangeGeneratorDefinition {
    #[serde(default)]
    pub spacing_mode: SpacingMode,
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptSourceChangeGeneratorDefinition {
    #[serde(flatten)]
    pub common: CommonSourceChangeGeneratorDefinition,
    #[serde(default = "is_false")]
    pub ignore_scripted_pause_commands: bool,
    pub script_file_folder: String,
}
fn is_false() -> bool { false }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SourceChangeDispatcherDefinition {
    Console(ConsoleSourceChangeDispatcherDefinition),
    Dapr(DaprSourceChangeDispatcherDefinition),
    JsonlFile(JsonlFileSourceChangeDispatcherDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleSourceChangeDispatcherDefinition {
    pub date_time_format: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaprSourceChangeDispatcherDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pubsub_name: Option<String>,
    pub pubsub_topic: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileSourceChangeDispatcherDefinition {
    pub max_events_per_file: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestQueryDefinition {
    #[serde(default)]
    pub test_query_id: String,
    pub result_stream_handler: ResultStreamHandlerDefinition,
    pub stop_trigger: StopTriggerDefinition,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ResultStreamHandlerDefinition {
    DaprPubSub(DaprPubSubResultStreamHandlerDefinition),
    RedisStream(RedisStreamResultStreamHandlerDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaprPubSubResultStreamHandlerDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pubsub_name: Option<String>,
    pub pubsub_topic: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisStreamResultStreamHandlerDefinition {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub stream_name: Option<String>,
    pub process_old_entries: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum StopTriggerDefinition {
    RecordSequenceNumber(RecordSequenceNumberStopTriggerDefinition),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordSequenceNumberStopTriggerDefinition {
    pub record_sequence_number: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryId {
    #[serde(default = "default_query_node_id")]
    pub node_id: String,
    #[serde(default = "default_query_id")]
    pub query_id: String,
}
fn default_query_node_id() -> String { "default".to_string() }
fn default_query_id() -> String { "test_query".to_string() }

#[derive(Debug, thiserror::Error)]
pub enum ParseQueryIdError {
    #[error("Invalid format for QueryId - {0}")]
    InvalidFormat(String),
    #[error("Invalid values for QueryId - {0}")]
    InvalidValues(String),
}

impl TryFrom<&str> for QueryId {
    type Error = ParseQueryIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() == 2 {
            Ok(QueryId {
                node_id: parts[0].to_string(), 
                query_id: parts[1].to_string(), 
            })
        } else {
            Err(ParseQueryIdError::InvalidFormat(value.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::{BufReader, Write}};

    use tempfile::tempdir;

    use super::*;

    fn create_test_file(content: &str) -> File {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("unit_test.test.json");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "{}", content).unwrap();
        file.sync_all().unwrap();
        File::open(file_path).unwrap()
    }

    #[test]
    fn test_read_bootstrap_data_generator() {
        let content = r#"
        {
            "kind": "Script",
            "script_file_folder": "bootstrap_data_scripts",
            "script_file_list": ["init*.jsonl", "deploy*.jsonl"],
            "time_mode": "recorded"
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let bootstrap_data_generator: BootstrapDataGeneratorDefinition = serde_json::from_reader(reader).unwrap();

        match bootstrap_data_generator {
            BootstrapDataGeneratorDefinition::Script(definition) => {
                assert_eq!(definition.common.time_mode, TimeMode::Recorded);
                assert_eq!(definition.script_file_folder, "bootstrap_data_scripts");
            }
        }
    }

    #[test]
    fn test_read_source_change_generator() {
        let content = r#"
        {
            "kind": "Script",
            "script_file_folder": "source_change_scripts",
            "script_file_list": ["change01.jsonl", "change02.jsonl"],
            "spacing_mode": "100",
            "time_mode": "recorded"
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source_change_generator: SourceChangeGeneratorDefinition = serde_json::from_reader(reader).unwrap();

        match source_change_generator {
            SourceChangeGeneratorDefinition::Script(definition) => {
                assert_eq!(definition.common.spacing_mode, SpacingMode::Rate(NonZeroU32::new(100).unwrap()));
                assert_eq!(definition.common.time_mode, TimeMode::Recorded);
                assert_eq!(definition.script_file_folder, "source_change_scripts");
            }
        }
    }

    #[test]
    fn test_read_script_source() {
        let content = r#"
        {
            "test_source_id": "source1",
            "kind": "Script",
            "bootstrap_data_generator": {
                "kind": "Script",
                "script_file_folder": "bootstrap_data_scripts",
                "time_mode": "live"
            },
            "source_data_generator": {
                "kind": "Script",
                "script_file_folder": "source_change_scripts",
                "spacing_mode": "recorded",
                "time_mode": "live"
            }
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source: TestSourceDefinition = serde_json::from_reader(reader).unwrap();

        match source {
            TestSourceDefinition::Script(source) => {
                assert_eq!(source.common.test_source_id, "source1");

                match source.bootstrap_data_generator.as_ref().unwrap() {
                    BootstrapDataGeneratorDefinition::Script(definition)=> {
                        assert_eq!(definition.common.time_mode, TimeMode::Live);
                        assert_eq!(definition.script_file_folder, "bootstrap_data_scripts");
                    }
                }
        
                match source.source_change_generator.as_ref().unwrap() {
                    SourceChangeGeneratorDefinition::Script(definition) => {
                        assert_eq!(definition.common.spacing_mode, SpacingMode::Recorded);
                        assert_eq!(definition.common.time_mode, TimeMode::Live);
                        assert_eq!(definition.script_file_folder, "source_change_scripts");
                    }
                }
        
            }
            _ => panic!("Expected ScriptTestSourceDefinition"),
        }
    }

    #[test]
    fn test_read_script_source_with_no_data_generators() {
        let content = r#"
        {
            "test_source_id": "source1"
            "kind": "Script"
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source: TestSourceDefinition = serde_json::from_reader(reader).unwrap();

        match source {
            TestSourceDefinition::Script(source) => {
                assert_eq!(source.common.test_source_id, "source1");
                assert_eq!(source.bootstrap_data_generator.is_none(), true);
                assert_eq!(source.source_change_generator.is_none(), true);
        }
            _ => panic!("Expected ScriptTestSourceDefinition"),
        }
    }

    #[test]
    fn test_read_test_definition() {
        let content = r#"
        {
            "test_id": "test1",
            "version": 1,
            "description": "A test definition",
            "test_folder": "test1",
            "sources": [
                {
                    "test_source_id": "source1",
                    "kind": "Script",
                    "bootstrap_data_generator": {
                        "kind": "Script",
                        "script_file_folder": "bootstrap_data_scripts",
                        "time_mode": "live"
                    },
                    "source_data_generator": {
                        "kind": "Script",
                        "script_file_folder": "source_change_scripts",
                        "spacing_mode": "recorded",
                        "time_mode": "live"
                    }
                }
            ],
            "queries": [],
            "clients": []
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let test_definition: TestDefinition = serde_json::from_reader(reader).unwrap();
        
        assert_eq!(test_definition.test_id, "test1");
        assert_eq!(test_definition.version, 1);
        assert_eq!(test_definition.description.unwrap(), "A test definition");
        assert_eq!(test_definition.test_folder.unwrap(), "test1");
        assert_eq!(test_definition.sources.len(), 1);
        let source = &test_definition.sources[0];

        match source {
            TestSourceDefinition::Script(source) => {
                assert_eq!(source.common.test_source_id, "source1");

                match source.bootstrap_data_generator.as_ref().unwrap() {
                    BootstrapDataGeneratorDefinition::Script(definition) => {
                        assert_eq!(definition.common.time_mode, TimeMode::Live);
                        assert_eq!(definition.script_file_folder, "bootstrap_data_scripts");
                    }
                }
        
                match source.source_change_generator.as_ref().unwrap() {
                    SourceChangeGeneratorDefinition::Script(definition) => {
                        assert_eq!(definition.common.spacing_mode, SpacingMode::Recorded);
                        assert_eq!(definition.common.time_mode, TimeMode::Live);
                        assert_eq!(definition.script_file_folder, "source_change_scripts");
                    }
                }
            }
            _ => panic!("Expected ScriptTestSourceDefinition"),
        }
    }

    #[test]
    fn test_spacing_mode_from_str() {
        assert_eq!("none".parse::<SpacingMode>().unwrap(), SpacingMode::None);
        assert_eq!("recorded".parse::<SpacingMode>().unwrap(), SpacingMode::Recorded);
        assert_eq!("100".parse::<SpacingMode>().unwrap(), SpacingMode::Rate(NonZeroU32::new(100).unwrap()));
        assert_eq!("1000".parse::<SpacingMode>().unwrap(), SpacingMode::Rate(NonZeroU32::new(1000).unwrap()));
    }

    #[test]
    fn test_spacing_mode_display() {
        assert_eq!(SpacingMode::None.to_string(), "none");
        assert_eq!(SpacingMode::Recorded.to_string(), "recorded");
        assert_eq!(SpacingMode::Rate(NonZeroU32::new(1000).unwrap()).to_string(), "1000");
    }

    #[test]
    fn test_spacing_mode_deserialize() {
        let json = r#""none""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(spacing_mode, SpacingMode::None);

        let json = r#""recorded""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(spacing_mode, SpacingMode::Recorded);

        let json = r#""1000""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(spacing_mode, SpacingMode::Rate(NonZeroU32::new(1000).unwrap()));
    }

    #[test]
    fn test_time_mode_from_str() {
        assert_eq!("live".parse::<TimeMode>().unwrap(), TimeMode::Live);
        assert_eq!("recorded".parse::<TimeMode>().unwrap(), TimeMode::Recorded);
        let timestamp = "2021-09-14T14:12:00Z";
        let parsed_time = chrono::DateTime::parse_from_rfc3339(timestamp).unwrap().timestamp_nanos_opt().unwrap() as u64;
        assert_eq!(timestamp.parse::<TimeMode>().unwrap(), TimeMode::Rebased(parsed_time));
    }

    #[test]
    fn test_time_mode_display() {
        assert_eq!(TimeMode::Live.to_string(), "live");
        assert_eq!(TimeMode::Recorded.to_string(), "recorded");
        assert_eq!(TimeMode::Rebased(1631629920000000000).to_string(), "1631629920000000000");
    }

    #[test]
    fn test_time_mode_deserialize() {
        let json = r#""live""#;
        let time_mode: TimeMode = serde_json::from_str(json).unwrap();
        assert_eq!(time_mode, TimeMode::Live);

        let json = r#""recorded""#;
        let time_mode: TimeMode = serde_json::from_str(json).unwrap();
        assert_eq!(time_mode, TimeMode::Recorded);

        let json = r#""2021-09-14T14:12:00Z""#;
        let parsed_time = chrono::DateTime::parse_from_rfc3339("2021-09-14T14:12:00Z").unwrap().timestamp_nanos_opt().unwrap() as u64;
        let time_mode: TimeMode = serde_json::from_str(json).unwrap();
        assert_eq!(time_mode, TimeMode::Rebased(parsed_time));
    }
}

