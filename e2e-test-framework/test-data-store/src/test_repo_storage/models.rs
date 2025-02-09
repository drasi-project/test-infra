use std::str::FromStr;

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
    Recorded,
    Fixed(u64),
}

impl Default for SpacingMode {
    fn default() -> Self {
        Self::Recorded
    }
}

// Implementation of FromStr for ReplayEventSpacingMode.
// For the Fixed variant, the spacing is specified as a string duration such as '5s' or '100n'.
// Supported units are seconds ('s'), milliseconds ('m'), microseconds ('u'), and nanoseconds ('n').
// If the string can't be parsed as a TimeDelta, an error is returned.
impl FromStr for SpacingMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Self::None),
            "recorded" => Ok(Self::Recorded),
            _ => {
                // Parse the string as a number, followed by a time unit character.
                let (num_str, unit_str) = s.split_at(s.len() - 1);
                let num = match num_str.parse::<u64>() {
                    Ok(num) => num,
                    Err(e) => {
                        anyhow::bail!("Error parsing SpacingMode: {}", e);
                    }
                };
                match unit_str {
                    "s" => Ok(Self::Fixed(num * 1000000000)),
                    "m" => Ok(Self::Fixed(num * 1000000)),
                    "u" => Ok(Self::Fixed(num * 1000)),
                    "n" => Ok(Self::Fixed(num)),
                    _ => {
                        anyhow::bail!("Invalid SpacingMode: {}", s);
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
            Self::Fixed(d) => write!(f, "{}", d),
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
    pub reactions: Vec<TestReactionDefinition>,
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
    pub reactions: Vec<TestReactionDefinition>,
    #[serde(default)]
    pub sources: Vec<TestSourceDefinition>,    
}

impl TestDefinition {
    pub fn new(test_id: &str, source: TestSourceDefinition) -> Self {
        Self {
            test_id: test_id.to_string(),
            version: 0,
            description: format!("A local test definition to hold source: {}", source.test_source_id).into(),
            test_folder: None,
            sources: vec![source],
            queries: Vec::new(),
            reactions: Vec::new()
        }
    }

    pub fn get_source(&self, source_id: &str) -> Option<TestSourceDefinition> {
        self.sources.iter().find(|source| source.test_source_id == source_id).cloned()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TestSourceDefinition {
    pub test_source_id: String,
    #[serde(alias = "bootstrap_data_generator")]
    pub bootstrap_data_generator_def: Option<BootstrapDataGeneratorDefinition>,
    #[serde(default, alias = "source_change_dispatchers")]
    pub source_change_dispatcher_defs: Vec<SourceChangeDispatcherDefinition>,
    #[serde(alias = "source_change_generator")]
    pub source_change_generator_def: Option<SourceChangeGeneratorDefinition>,
    #[serde(default)]
    pub subscribers: Vec<QueryId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum BootstrapDataGeneratorDefinition {
    Script {
        #[serde(flatten)]
        common_config: CommonBootstrapDataGeneratorDefinition,
        #[serde(flatten)]
        unique_config: ScriptBootstrapDataGeneratorDefinition,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonBootstrapDataGeneratorDefinition {
    #[serde(default)]
    pub time_mode: TimeMode,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScriptBootstrapDataGeneratorDefinition {
    pub script_file_folder: String,
    // pub script_file_list: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SourceChangeGeneratorDefinition {
    Script {
        #[serde(flatten)]
        common_config: CommonSourceChangeGeneratorDefinition,
        #[serde(flatten)]
        unique_config: ScriptSourceChangeGeneratorDefinition,
    },
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
    #[serde(default = "is_false")]
    pub ignore_scripted_pause_commands: bool,
    pub script_file_folder: String,
    // pub script_file_list: Vec<String>,
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
#[serde(tag = "kind")]
pub enum TestReactionDefinition {
    AzureEventGrid {
        #[serde(flatten)]
        common_def: CommonTestReactionDefinition,
        #[serde(flatten)]
        unique_def: AzureEventGridTestReactionDefinition,
    },
    SignalR {
        #[serde(flatten)]
        common_def: CommonTestReactionDefinition,
        #[serde(flatten)]
        unique_def: SignalRTestReactionDefinition,
    },
}

impl TestReactionDefinition {
    pub fn get_id(&self) -> String {
        match self {
            TestReactionDefinition::AzureEventGrid { common_def, .. } => common_def.test_reaction_id.clone(),
            TestReactionDefinition::SignalR { common_def, .. } => common_def.test_reaction_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonTestReactionDefinition {
    #[serde(default)]
    pub queries: Vec<QueryId>,
    pub test_reaction_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AzureEventGridTestReactionDefinition {
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignalRTestReactionDefinition {
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestQueryDefinition {
    #[serde(default)]
    pub test_query_id: String,
    pub result_stream_handler: ResultStreamHandlerDefinition
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
            BootstrapDataGeneratorDefinition::Script { common_config, unique_config } => {
                assert_eq!(common_config.time_mode, TimeMode::Recorded);
                assert_eq!(unique_config.script_file_folder, "bootstrap_data_scripts");
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
            "spacing_mode": "100m",
            "time_mode": "recorded"
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source_change_generator: SourceChangeGeneratorDefinition = serde_json::from_reader(reader).unwrap();

        match source_change_generator {
            SourceChangeGeneratorDefinition::Script { common_config, unique_config } => {
                assert_eq!(common_config.spacing_mode, SpacingMode::Fixed(100000000));
                assert_eq!(common_config.time_mode, TimeMode::Recorded);
                assert_eq!(unique_config.script_file_folder, "source_change_scripts");
            }
        }
    }

    #[test]
    fn test_read_source() {
        let content = r#"
        {
            "test_source_id": "source1",
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

        assert_eq!(source.test_source_id, "source1");

        match source.bootstrap_data_generator_def.as_ref().unwrap() {
            BootstrapDataGeneratorDefinition::Script { common_config, unique_config } => {
                assert_eq!(common_config.time_mode, TimeMode::Live);
                assert_eq!(unique_config.script_file_folder, "bootstrap_data_scripts");
            }
        }

        match source.source_change_generator_def.as_ref().unwrap() {
            SourceChangeGeneratorDefinition::Script { common_config, unique_config } => {
                assert_eq!(common_config.spacing_mode, SpacingMode::Recorded);
                assert_eq!(common_config.time_mode, TimeMode::Live);
                assert_eq!(unique_config.script_file_folder, "source_change_scripts");
            }
        }
    }

    #[test]
    fn test_read_source_with_no_data_generators() {
        let content = r#"
        {
            "test_source_id": "source1"
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source: TestSourceDefinition = serde_json::from_reader(reader).unwrap();

        assert_eq!(source.test_source_id, "source1");
        assert_eq!(source.bootstrap_data_generator_def.is_none(), true);
        assert_eq!(source.source_change_generator_def.is_none(), true);
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
            "reactions": [],
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
        assert_eq!(source.test_source_id, "source1");

        match source.bootstrap_data_generator_def.as_ref().unwrap() {
            BootstrapDataGeneratorDefinition::Script { common_config, unique_config } => {
                assert_eq!(common_config.time_mode, TimeMode::Live);
                assert_eq!(unique_config.script_file_folder, "bootstrap_data_scripts");
            }
        }

        match source.source_change_generator_def.as_ref().unwrap() {
            SourceChangeGeneratorDefinition::Script { common_config, unique_config } => {
                assert_eq!(common_config.spacing_mode, SpacingMode::Recorded);
                assert_eq!(common_config.time_mode, TimeMode::Live);
                assert_eq!(unique_config.script_file_folder, "source_change_scripts");
            }
        }
    }

    #[test]
    fn test_spacing_mode_from_str() {
        assert_eq!("none".parse::<SpacingMode>().unwrap(), SpacingMode::None);
        assert_eq!("recorded".parse::<SpacingMode>().unwrap(), SpacingMode::Recorded);
        assert_eq!("100s".parse::<SpacingMode>().unwrap(), SpacingMode::Fixed(100000000000));
        assert_eq!("100m".parse::<SpacingMode>().unwrap(), SpacingMode::Fixed(100000000));
        assert_eq!("100u".parse::<SpacingMode>().unwrap(), SpacingMode::Fixed(100000));
        assert_eq!("100n".parse::<SpacingMode>().unwrap(), SpacingMode::Fixed(100));
    }

    #[test]
    fn test_spacing_mode_display() {
        assert_eq!(SpacingMode::None.to_string(), "none");
        assert_eq!(SpacingMode::Recorded.to_string(), "recorded");
        assert_eq!(SpacingMode::Fixed(100000000000).to_string(), "100000000000");
    }

    #[test]
    fn test_spacing_mode_deserialize() {
        let json = r#""none""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(spacing_mode, SpacingMode::None);

        let json = r#""recorded""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(spacing_mode, SpacingMode::Recorded);

        let json = r#""100s""#;
        let spacing_mode: SpacingMode = serde_json::from_str(json).unwrap();
        assert_eq!(spacing_mode, SpacingMode::Fixed(100000000000));
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

