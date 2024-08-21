use clap::Parser;
use std::{fmt, str::FromStr};
use serde::{Deserialize, Serialize};

// The OutputType enum is used to specify the destination for Test Script Player output of various sorts including:
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
            "publish" | "qp" => Ok(OutputType::Publish),
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
    // If not provided, the Service will start and wait for Test Script Players to be started through the Web API.
    #[arg(short = 'c', long = "config", env = "DRASI_CONFIG_FILE")]
    pub config_file_path: Option<String>,

    // The path where data used and generated in the Test Script Players gets stored.
    // If not provided, the default_value is used.
    #[arg(short = 'd', long = "data", env = "DRASI_DATA_CACHE", default_value = "./source_data_cache")]
    pub data_cache_path: String,

    // The port number the Web API will listen on.
    // If not provided, the default_value is used.
    #[arg(short = 'p', long = "port", env = "DRASI_PORT", default_value = "4000")]
    pub port: String,

    // The OutputType for Source Change Events.
    // If not provided, the default_value "publish" is used. ensuring that SourceChangeEvents are published
    // to the Change Queue for downstream processing.
    #[arg(short = 'e', long = "event_out", env = "DRASI_EVENT_OUTPUT", default_value = "none")]
    pub event_output: OutputType,

    // The OutputType for Test Script Player Telemetry data.
    // If not provided, the default_value "publish" is used ensuring that Telemetry data is published
    // so it can be captured and logged against the test run for analysis.
    #[arg(short = 't', long = "telem_out", env = "DRASI_TELEM_OUTPUT", default_value = "none")]
    pub telemetry_output: OutputType,

    // The OutputType for Test Script Player Log data.
    // If not provided, the default_value "none" is used ensuring that Log data is not generated.
    #[arg(short = 'l', long = "log_out", env = "DRASI_LOG_OUTPUT", default_value = "none")]
    pub log_output: OutputType,
}

// The PlayerConfig is what is loaded from the Service config File. It can contain the configurations for multiple Test Script Players,
// as well as default values that are used when a Test Script Player doesn't specify a value.
#[derive(Debug, Deserialize, Serialize)]
pub struct ServiceConfigFile {
    #[serde(default)]
    pub player_defaults: PlayerConfigDefaults,
    #[serde(default = "default_player_config_list")]
    pub players: Vec<PlayerConfig>,
}
fn default_player_config_list() -> Vec<PlayerConfig> { Vec::new() }

impl ServiceConfigFile {
    pub fn from_file_path(config_file_path: &str) -> Result<Self, String> {
        // Validate that the file exists and if not return an error.
        if !std::path::Path::new(config_file_path).exists() {
            return Err(format!("Service Config file not found: {}", config_file_path));
        }

        // Read the file content into a string.
        let config_file_json = match std::fs::read_to_string(config_file_path) {
            Ok(config_file_json) => config_file_json,
            Err(e) => return Err(format!("Error reading Service Config file: {}", e))
        };

        // Parse the string into a ServiceConfigFile struct.
        match serde_json::from_str(&config_file_json) {
            Ok(config_file) => Ok(config_file),
            Err(e) => Err(format!("Error parsing Service Config JSON: {}", e))
        }
    }
}

impl Default for ServiceConfigFile {
    fn default() -> Self {
        Self {
            player_defaults: PlayerConfigDefaults::default(),
            players: default_player_config_list(),
        }
    }
}

// The PlayerConfigDefaults struct holds the default values read from the Service config file that are used when a Player COnfig doesn't specify a value.
#[derive(Debug, Deserialize, Serialize)]
pub struct PlayerConfigDefaults {
    pub test_storage_account: Option<String>,
    pub test_storage_access_key: Option<String>,
    pub test_storage_container: Option<String>,
    pub test_storage_path: Option<String>,
    pub source_id: Option<String>,
    pub change_queue_address: Option<String>,
    pub change_queue_port: Option<u16>,
    pub change_queue_topic: Option<String>,
    #[serde(default = "is_true")]
    pub start_immediately: bool,
    #[serde(default = "is_false")]
    pub ignore_scripted_pause_commands: bool,
    #[serde(default)]
    pub source_change_event_time_mode: Option<String>,
    #[serde(default)]
    pub source_change_event_spacing_mode: Option<String>,
}
fn is_true() -> bool { true }
fn is_false() -> bool { false }

impl Default for PlayerConfigDefaults {
    fn default() -> Self {
        PlayerConfigDefaults {
            test_storage_account: None,
            test_storage_access_key: None,
            test_storage_container: None,
            test_storage_path: None,
            source_id: None,
            change_queue_address: None,
            change_queue_port: None,
            change_queue_topic: None,
            start_immediately: true,
            ignore_scripted_pause_commands: false,
            source_change_event_time_mode: None,
            source_change_event_spacing_mode: None,
        }
    }
}

// The PlayerConfig is what is loaded from the Service config file or passed in to the Web API 
// to create a new Test Script Player.
#[derive(Debug, Deserialize, Serialize)]
pub struct PlayerConfig {
    // The Test ID.
    pub test_id: String,

    // The Test Run ID.
    pub test_run_id: Option<String>,

    // The Test Storage Account where the Test Repo is located.
    pub test_storage_account: Option<String>,

    // The Test Storage Access Key where the Test Repo is located.
    pub test_storage_access_key: Option<String>,

    // The Test Storage Container where the Test Repo is located.
    pub test_storage_container: Option<String>,

    // The Test Storage Path where the Test Repo is located.
    pub test_storage_path: Option<String>,

    // The Source ID for the Test Script Player.
    pub source_id: Option<String>,

    // Flag to indicate if the Service should start the Test Script Player immediately after initialization.
    pub start_immediately: Option<bool>,

    // *** The following fields are used to configure the Change Queue output destination. ***
    // *** These are used by default if the output destination is "queue". ***
    // The address of the change queue
    pub change_queue_address: Option<String>,

    // The port of the change queue
    pub change_queue_port: Option<u16>,

    // The PubSub topic for the change queue
    pub change_queue_topic: Option<String>,

    // SourceChangeEvent Time Mode for the Test Script Player as a string.
    // Either "live", "recorded", or a specific time in the format "YYYY-MM-DDTHH:MM:SS:SSS Z".
    // If not provided, "recorded" is used.
    pub source_change_event_time_mode: Option<String>,

    // SourceChangeEvent Spacing Mode for the Test Script Player as a string.
    // Either "none", "recorded", or a fixed spacing in the format "Xs", "Xm", "Xu", or "Xn".
    pub source_change_event_spacing_mode: Option<String>,

    // Whether the player should ignore scripted pause commands.
    pub ignore_scripted_pause_commands: Option<bool>,
}