use clap::Parser;
use std::{fmt, str::FromStr};
use serde::{Deserialize, Serialize};

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

// A struct to hold Service Settings obtained from env vars and/or command line arguments.
// Command line args will override env vars. If neither is provided, default values are used.
#[derive(Parser, Debug, Clone, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct ServiceSettings {
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

// The ServiceConfigFile is what is loaded from the Service config File. It can contain the configurations for multiple Change Script Players,
// as well as default values that are used when a Change Script Player doesn't specify a value.
#[derive(Debug, Deserialize, Serialize)]
pub struct ServiceConfigFile {
    #[serde(default)]
    pub defaults: SourceConfigDefaults,
    #[serde(default = "default_source_config_list")]
    pub sources: Vec<SourceConfig>,
}
fn default_source_config_list() -> Vec<SourceConfig> { Vec::new() }

impl ServiceConfigFile {
    pub fn from_file_path(config_file_path: &str) -> anyhow::Result<Self> {
        // Validate that the file exists and if not return an error.
        if !std::path::Path::new(config_file_path).exists() {
            anyhow::bail!("Service Config file not found: {}", config_file_path);
        }

        // Read the file content into a string.
        let config_file_json = std::fs::read_to_string(config_file_path)?;

        // Parse the string into a ServiceConfigFile struct.
        let source_config_file = serde_json::from_str(&config_file_json)?;

        Ok(source_config_file)
    }
}

impl Default for ServiceConfigFile {
    fn default() -> Self {
        Self {
            defaults: SourceConfigDefaults::default(),
            sources: default_source_config_list(),
        }
    }
}

// The SourceConfigDefaults struct holds the default values read from the Service config file that are used when a Player COnfig doesn't specify a value.
#[derive(Debug, Deserialize, Serialize)]
pub struct SourceConfigDefaults {
    #[serde(default = "is_false")]
    pub force_test_repo_cache_refresh: bool,
    pub test_storage_account: Option<String>,
    pub test_storage_access_key: Option<String>,
    pub test_storage_container: Option<String>,
    pub test_storage_path: Option<String>,
    pub test_id: Option<String>,
    pub test_run_id: Option<String>,
    pub source_id: Option<String>,
    #[serde(default)]
    pub proxy: ProxyConfigDefaults,
    #[serde(default)]
    pub reactivator: ReactivatorConfigDefaults,
}

impl Default for SourceConfigDefaults {
    fn default() -> Self {
        SourceConfigDefaults {
            force_test_repo_cache_refresh: false,
            test_storage_account: None,
            test_storage_access_key: None,
            test_storage_container: None,
            test_storage_path: None,
            test_id: None,
            test_run_id: None,
            source_id: None,
            proxy: ProxyConfigDefaults::default(),
            reactivator: ReactivatorConfigDefaults::default(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProxyConfigDefaults {
    #[serde(default = "is_recorded")]
    pub time_mode: String,
}

impl Default for ProxyConfigDefaults {
    fn default() -> Self {
        ProxyConfigDefaults {
            time_mode: is_recorded(),
        }
    }
}
fn is_recorded() -> String { "recorded".to_string() }

#[derive(Debug, Deserialize, Serialize)]
pub struct ReactivatorConfigDefaults {
    #[serde(default = "is_false")]
    pub ignore_scripted_pause_commands: bool,
    #[serde(default = "is_recorded")]
    pub spacing_mode: String,
    #[serde(default = "is_true")]
    pub start_immediately: bool,
    #[serde(default = "is_recorded")]
    pub time_mode: String,
    #[serde(default)]
    pub dispatchers: Option<Vec<SourceChangeDispatcherConfig>>,
}

impl Default for ReactivatorConfigDefaults {
    fn default() -> Self {
        ReactivatorConfigDefaults {
            ignore_scripted_pause_commands: is_false(),
            spacing_mode: is_recorded(),
            start_immediately: is_true(),
            time_mode: is_recorded(),
            dispatchers: None,
        }
    }
}
fn is_true() -> bool { true }
fn is_false() -> bool { false }

// The SourceConfig is what is loaded from the Service config file or passed in to the Web API 
// to create a new Change Script Player.
#[derive(Debug, Deserialize, Serialize)]
pub struct SourceConfig {
    pub force_test_repo_cache_refresh: Option<bool>,

    // The Test Storage Account where the Test Repo is located.
    pub test_storage_account: Option<String>,

    // The Test Storage Access Key where the Test Repo is located.
    pub test_storage_access_key: Option<String>,

    // The Test Storage Container where the Test Repo is located.
    pub test_storage_container: Option<String>,

    // The Test Storage Path where the Test Repo is located.
    pub test_storage_path: Option<String>,

    // The Test ID.
    pub test_id: Option<String>,

    // The Test Run ID.
    pub test_run_id: Option<String>,

    // The Source ID for the Change Script Player.
    pub source_id: Option<String>,

    pub proxy: Option<ProxyConfig>,

    pub reactivator: Option<ReactivatorConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProxyConfig {
    pub time_mode: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReactivatorConfig {
    // Whether the player should ignore scripted pause commands.
    pub ignore_scripted_pause_commands: Option<bool>,

    // Spacing Mode for the Change Script Player as a string.
    // Either "none", "recorded", or a fixed spacing in the format "Xs", "Xm", "Xu", or "Xn".
    pub spacing_mode: Option<String>,

    // Flag to indicate if the Service should start the Change Script Player immediately after initialization.
    pub start_immediately: Option<bool>,

    // Time Mode for the Change Script Player as a string.
    // Either "live", "recorded", or a specific time in the format "YYYY-MM-DDTHH:MM:SS:SSS Z".
    // If not provided, "recorded" is used.
    pub time_mode: Option<String>,

    pub dispatchers: Option<Vec<SourceChangeDispatcherConfig>>,
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