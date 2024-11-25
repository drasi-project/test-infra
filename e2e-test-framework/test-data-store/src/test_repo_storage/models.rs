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
        match s {
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
        match s {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum BootstrapDataGeneratorKind {
    None,
    Script,
}

impl Default for BootstrapDataGeneratorKind {
    fn default() -> Self {
        Self::None
    }
}

impl FromStr for BootstrapDataGeneratorKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "None" => Ok(Self::None),
            "Script" => Ok(Self::Script),
            _ => {
                anyhow::bail!("Invalid BootstrapDataGeneratorKind: {}", s);
            }
        }
    }
}

impl std::fmt::Display for BootstrapDataGeneratorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Script => write!(f, "Script"),
        }
    }
}

impl<'de> Deserialize<'de> for BootstrapDataGeneratorKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: String = Deserialize::deserialize(deserializer)?;
        value.parse::<BootstrapDataGeneratorKind>().map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SourceChangeGeneratorKind {
    None,
    Script,
}

impl Default for SourceChangeGeneratorKind {
    fn default() -> Self {
        Self::None
    }
}

impl FromStr for SourceChangeGeneratorKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "None" => Ok(Self::None),
            "Script" => Ok(Self::Script),
            _ => {
                anyhow::bail!("Invalid SourceChangeGeneratorKind: {}", s);
            }
        }
    }
}

impl std::fmt::Display for SourceChangeGeneratorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Script => write!(f, "Script"),
        }
    }
}

impl<'de> Deserialize<'de> for SourceChangeGeneratorKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: String = Deserialize::deserialize(deserializer)?;
        value.parse::<SourceChangeGeneratorKind>().map_err(de::Error::custom)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TestDefinition {
    pub id: String,
    pub version: u32,
    pub description: Option<String>,
    pub test_folder: Option<String>,
    pub sources: Vec<SourceDefinition>,
    pub queries: Vec<QueryDefinition>,
    pub reactions: Vec<ReactionDefinition>,
    pub clients: Vec<ClientDefinition>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SourceDefinition {
    pub id: String,
    pub bootstrap_data_generator: BootstrapDataGeneratorDefinition,
    pub source_change_generator: SourceChangeGeneratorDefinition,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BootstrapDataGeneratorDefinition {    
    pub kind: BootstrapDataGeneratorKind,
    pub script_file_folder: String,
    pub script_file_list: Vec<String>,
    pub time_mode: TimeMode,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SourceChangeGeneratorDefinition {
    pub kind: SourceChangeGeneratorKind,
    pub script_file_folder: String,
    pub script_file_list: Vec<String>,
    pub spacing_mode: SpacingMode,
    pub time_mode: TimeMode,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QueryDefinition {
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReactionDefinition {
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientDefinition {
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::{BufReader, Write}};

    use tempfile::tempdir;

    use super::*;

    fn create_test_file(content: &str) -> File {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("unit_test.test");
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
        
        assert_eq!(bootstrap_data_generator.kind, BootstrapDataGeneratorKind::Script);
        assert_eq!(bootstrap_data_generator.script_file_folder, "bootstrap_data_scripts");
        assert_eq!(bootstrap_data_generator.script_file_list, vec!["init*.jsonl", "deploy*.jsonl"]);
        assert_eq!(bootstrap_data_generator.time_mode, TimeMode::Recorded);
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
        
        assert_eq!(source_change_generator.kind, SourceChangeGeneratorKind::Script);
        assert_eq!(source_change_generator.script_file_folder, "source_change_scripts");
        assert_eq!(source_change_generator.script_file_list, vec!["change01.jsonl", "change02.jsonl"]);
        assert_eq!(source_change_generator.spacing_mode, SpacingMode::Fixed(100000000));
        assert_eq!(source_change_generator.time_mode, TimeMode::Recorded);
    }

    #[test]
    fn test_read_source() {
        let content = r#"
        {
            "id": "source1",
            "bootstrap_data_generator": {
                "kind": "None",
                "script_file_folder": "bootstrap_data_scripts",
                "script_file_list": ["init*.jsonl", "deploy*.jsonl"],
                "time_mode": "live"
            },
            "source_change_generator": {
                "kind": "Script",
                "script_file_folder": "source_change_scripts",
                "script_file_list": ["change01.jsonl", "change02.jsonl"],
                "spacing_mode": "recorded",
                "time_mode": "live"
            }
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source: SourceDefinition = serde_json::from_reader(reader).unwrap();

        assert_eq!(source.id, "source1");
        assert_eq!(source.bootstrap_data_generator.kind, BootstrapDataGeneratorKind::None);
        assert_eq!(source.bootstrap_data_generator.script_file_folder, "bootstrap_data_scripts");
        assert_eq!(source.bootstrap_data_generator.script_file_list, vec!["init*.jsonl", "deploy*.jsonl"]);
        assert_eq!(source.bootstrap_data_generator.time_mode, TimeMode::Live);
        assert_eq!(source.source_change_generator.kind, SourceChangeGeneratorKind::Script);
        assert_eq!(source.source_change_generator.script_file_folder, "source_change_scripts");
        assert_eq!(source.source_change_generator.script_file_list, vec!["change01.jsonl", "change02.jsonl"]);
        assert_eq!(source.source_change_generator.spacing_mode, SpacingMode::Recorded);
        assert_eq!(source.source_change_generator.time_mode, TimeMode::Live);
    }

    #[test]
    fn test_read_test_definition() {
        let content = r#"
        {
            "id": "test1",
            "version": 1,
            "description": "A test definition",
            "test_folder": "test1",
            "sources": [
                {
                    "id": "source1",
                    "bootstrap_data_generator": {
                        "kind": "Script",
                        "script_file_folder": "bootstrap_data_scripts",
                        "script_file_list": ["init*.jsonl", "deploy*.jsonl"],
                        "time_mode": "live"
                    },
                    "source_change_generator": {
                        "kind": "None",
                        "script_file_folder": "source_change_scripts",
                        "script_file_list": ["change01.jsonl", "change02.jsonl"],
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
        
        assert_eq!(test_definition.id, "test1");
        assert_eq!(test_definition.version, 1);
        assert_eq!(test_definition.description.unwrap(), "A test definition");
        assert_eq!(test_definition.test_folder.unwrap(), "test1");
        assert_eq!(test_definition.sources.len(), 1);
        let source = &test_definition.sources[0];
        assert_eq!(source.id, "source1");
        assert_eq!(source.bootstrap_data_generator.kind, BootstrapDataGeneratorKind::Script);
        assert_eq!(source.bootstrap_data_generator.script_file_folder, "bootstrap_data_scripts");
        assert_eq!(source.bootstrap_data_generator.script_file_list, vec!["init*.jsonl", "deploy*.jsonl"]);
        assert_eq!(source.bootstrap_data_generator.time_mode, TimeMode::Live);
        assert_eq!(source.source_change_generator.kind, SourceChangeGeneratorKind::None);
        assert_eq!(source.source_change_generator.script_file_folder, "source_change_scripts");
        assert_eq!(source.source_change_generator.script_file_list, vec!["change01.jsonl", "change02.jsonl"]);
        assert_eq!(source.source_change_generator.spacing_mode, SpacingMode::Recorded);
        assert_eq!(source.source_change_generator.time_mode, TimeMode::Live);        
    }

    #[test]
    fn test_read_single_source_hierarchical_query() {
        let file_path = "tests/test_definitions/test_single_source_hierarchical_query.test";
        let file = File::open(file_path).unwrap();
        let reader = BufReader::new(file);
        let test_definition: TestDefinition = serde_json::from_reader(reader).unwrap();
        
        assert_eq!(test_definition.id, "single_source_hierarchical_query");
        assert_eq!(test_definition.version, 1);
        assert_eq!(test_definition.description.unwrap(), "A single source hierarchical query using facilities data.");
        assert_eq!(test_definition.test_folder.unwrap(), "single_source_hierarchical_query");
        assert_eq!(test_definition.sources.len(), 1);
        let source = &test_definition.sources[0];
        assert_eq!(source.id, "facilities");
        assert_eq!(source.bootstrap_data_generator.kind, BootstrapDataGeneratorKind::Script);
        assert_eq!(source.bootstrap_data_generator.script_file_folder, "bootstrap_data_scripts");
        assert_eq!(source.bootstrap_data_generator.script_file_list, Vec::<String>::new());
        assert_eq!(source.bootstrap_data_generator.time_mode, TimeMode::Recorded);
        assert_eq!(source.source_change_generator.kind, SourceChangeGeneratorKind::Script);
        assert_eq!(source.source_change_generator.script_file_folder, "source_change_scripts");
        assert_eq!(source.source_change_generator.script_file_list, Vec::<String>::new());
        assert_eq!(source.source_change_generator.spacing_mode, SpacingMode::None);
        assert_eq!(source.source_change_generator.time_mode, TimeMode::Recorded);
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
