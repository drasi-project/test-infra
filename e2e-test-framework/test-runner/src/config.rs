use clap::Parser;
use std::{fmt, str::FromStr};
use serde::{Deserialize, Serialize, Serializer};

// The OutputType enum is used to specify the destination for output of various sorts including:
//   - the SourceChangeEvents generated during the run
//   - the Telemetry data generated during the run
//   - the Log data generated during the run
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Copy)]
pub enum OutputType {
    Console,
    File,
    None,
    Publish,
}

// Implement the FromStr trait for OutputType to allow parsing from a string.
impl FromStr for OutputType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "console" | "c" => Ok(OutputType::Console),
            "file" | "f" => Ok(OutputType::File),
            "none" | "n" => Ok(OutputType::None),
            "publish" | "p" => Ok(OutputType::Publish),
            _ => Err(format!("Invalid OutputType: {}", s))
        }
    }
}

// Implement the Display trait on OutputType for better error messages
impl fmt::Display for OutputType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OutputType::Console => write!(f, "console"),
            OutputType::File => write!(f, "file"),
            OutputType::None => write!(f, "none"),
            OutputType::Publish => write!(f, "publish"),
        }
    }
}

// A struct to hold Service Parameters obtained from env vars and/or command line arguments.
// Command line args will override env vars. If neither is provided, default values are used.
#[derive(Parser, Debug, Clone, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct ServiceParams {
    // The path of the Service config file.
    // If not provided, the Service will start and wait for Change Script Players to be started through the Web API.
    #[arg(short = 'c', long = "config", env = "DRASI_CONFIG_FILE")]
    pub config_file_path: Option<String>,

    // The path where data used and generated in the Change Script Players gets stored.
    // If not provided, the default_value is used.
    #[arg(short = 'd', long = "data", env = "DRASI_DATA_CACHE", default_value = "./source_data_cache")]
    pub data_cache_path: String,

    // The port number the Web API will listen on.
    // If not provided, the default_value is used.
    #[arg(short = 'p', long = "port", env = "DRASI_PORT", default_value_t = 4000)]
    pub port: u16,

    // The OutputType for Source Change Events.
    // If not provided, the default_value "publish" is used. ensuring that SourceChangeEvents are published
    // to the Change Queue for downstream processing.
    #[arg(short = 'e', long = "event_out", env = "DRASI_EVENT_OUTPUT", default_value_t = OutputType::Publish)]
    pub event_output: OutputType,

    // The OutputType for Change Script Player Telemetry data.
    // If not provided, the default_value "publish" is used ensuring that Telemetry data is published
    // so it can be captured and logged against the test run for analysis.
    #[arg(short = 't', long = "telem_out", env = "DRASI_TELEM_OUTPUT", default_value_t = OutputType::None)]
    pub telemetry_output: OutputType,

    // The OutputType for Change Script Player Log data.
    // If not provided, the default_value "none" is used ensuring that Log data is not generated.
    #[arg(short = 'l', long = "log_out", env = "DRASI_LOG_OUTPUT", default_value_t = OutputType::None)]
    pub log_output: OutputType,
}

// The ServiceConfig is what is loaded from the Service config File. It can contain the configurations for multiple Change Script Players,
// as well as default values that are used when a Change Script Player doesn't specify a value.
#[derive(Debug, Deserialize, Serialize)]
pub struct ServiceConfig {
    #[serde(default)]
    pub source_defaults: SourceConfig,
    #[serde(default)]
    pub sources: Vec<SourceConfig>,
    #[serde(default)]
    pub test_repos: Vec<TestRepoConfig>,
}

impl ServiceConfig {
    pub fn from_file_path(config_file_path: &str) -> anyhow::Result<Self> {
        // Validate that the file exists and if not return an error.
        if !std::path::Path::new(config_file_path).exists() {
            anyhow::bail!("Service Config file not found: {}", config_file_path);
        }

        // Read the file content into a string.
        let config_file_json = std::fs::read_to_string(config_file_path)?;

        // Parse the string into a ServiceConfig struct.
        let service_config_file = serde_json::from_str(&config_file_json)?;

        Ok(service_config_file)
    }
}

impl Default for ServiceConfig {
    fn default() -> Self {
        ServiceConfig {
            source_defaults: SourceConfig::default(),
            test_repos: Vec::new(),
            sources: Vec::new(),
        }
    }
}

// The SourceConfig is what is loaded from the Service config file or passed in to the Web API 
// to create a new Change Script Player.
#[derive(Debug, Deserialize, Serialize)]
pub struct SourceConfig {
    pub test_repo_id: Option<String>,
    pub test_id: Option<String>,
    pub test_run_id: Option<String>,
    pub source_id: Option<String>,
    pub proxy: Option<ProxyConfig>,
    pub reactivator: Option<ReactivatorConfig>,
}

impl Default for SourceConfig {
    fn default() -> Self {
        SourceConfig {
            test_repo_id: None,
            test_id: None,
            test_run_id: None,
            source_id: None,
            proxy: None,
            reactivator: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProxyConfig {
    pub time_mode: Option<String>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            time_mode: Some("Recorded".to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReactivatorConfig {
    pub dispatchers: Option<Vec<SourceChangeDispatcherConfig>>,
    pub ignore_scripted_pause_commands: Option<bool>,
    pub spacing_mode: Option<String>,
    pub start_immediately: Option<bool>,
    pub time_mode: Option<String>,
}

impl Default for ReactivatorConfig {
    fn default() -> Self {
        ReactivatorConfig {
            dispatchers: None,
            ignore_scripted_pause_commands: Some(false),
            spacing_mode: Some("Recorded".to_string()),
            start_immediately: Some(true),
            time_mode: Some("Recorded".to_string()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceChangeDispatcherConfig {
    Console(ConsoleSourceChangeDispatcherConfig),
    Dapr(DaprSourceChangeDispatcherConfig),
    JsonlFile(JsonlFileSourceChangeDispatcherConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConsoleSourceChangeDispatcherConfig {
    pub date_time_format: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaprSourceChangeDispatcherConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pubsub_name: Option<String>,
    pub pubsub_topic: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileSourceChangeDispatcherConfig {
    pub folder_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TestRepoConfig {
    AzureStorageBlob {
        #[serde(flatten)]
        common_config: CommonTestRepoConfig,
        #[serde(flatten)]
        unique_config: AzureStorageBlobTestRepoConfig,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommonTestRepoConfig {
    #[serde(default = "is_false")]
    pub force_cache_refresh: bool,
    pub id: String,
}
fn is_false() -> bool { false }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AzureStorageBlobTestRepoConfig {
    pub account_name: String,
    #[serde(serialize_with = "mask_secret")]
    pub access_key: String,
    pub container: String,
    pub root_path: String,
}
pub fn mask_secret<S>(_: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("******")
}

#[cfg(test)]
mod tests {
    use super::*;
 
    #[test]
    fn test_output_type_from_str() {
        assert_eq!(OutputType::from_str("console").unwrap(), OutputType::Console);
        assert_eq!(OutputType::from_str("c").unwrap(), OutputType::Console);
        assert_eq!(OutputType::from_str("file").unwrap(), OutputType::File);
        assert_eq!(OutputType::from_str("f").unwrap(), OutputType::File);
        assert_eq!(OutputType::from_str("none").unwrap(), OutputType::None);
        assert_eq!(OutputType::from_str("n").unwrap(), OutputType::None);
        assert_eq!(OutputType::from_str("publish").unwrap(), OutputType::Publish);
        assert_eq!(OutputType::from_str("p").unwrap(), OutputType::Publish);
        assert!(OutputType::from_str("invalid").is_err());
    }

    #[test]
    fn test_output_type_display() {
        assert_eq!(OutputType::Console.to_string(), "console");
        assert_eq!(OutputType::File.to_string(), "file");
        assert_eq!(OutputType::None.to_string(), "none");
        assert_eq!(OutputType::Publish.to_string(), "publish");
    }

    #[test]
    fn test_service_config_file_from_file_path() {
        // Create a temporary file with valid JSON content
        let temp_file_path = "/tmp/test_service_config.json";
        let json_content = r#"
        {
            "test_repos": {
                "repo1": {
                    "type": "AzureStorageBlob",
                    "id": "repo1"
                }
            },
            "defaults": {},
            "sources": []
        }
        "#;
        std::fs::write(temp_file_path, json_content).unwrap();

        // Test loading the config file
        let config = ServiceConfig::from_file_path(temp_file_path);
        assert!(config.is_ok());

        // Clean up the temporary file
        std::fs::remove_file(temp_file_path).unwrap();
    }

    #[test]
    fn test_service_config_file_from_file_path_not_found() {
        let result = ServiceConfig::from_file_path("/path/does/not/exist.json");
        assert!(result.is_err());
    }
}