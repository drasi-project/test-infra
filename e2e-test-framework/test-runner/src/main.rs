use std::{fmt, str::FromStr};

use clap::Parser;
use runner::{ServiceStatus, TestRunner};
use serde::{Deserialize, Serialize};

mod runner;
mod script_source;
mod source_change_dispatchers;
mod test_repo;
mod web_api;

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

// The main function that starts the starts the Service.
// If the Test Runner is started with a config file, it will initialize the Test Runner with the settings in the file.
// If the Test Runner is started with no config file, it will wait to be managed through the Web API.
#[tokio::main]
async fn main() {
     
     env_logger::init();

    // Parse the command line and env var args into an ServiceParams struct. If the args are invalid, return an error.
    let service_params = ServiceParams::parse();
    log::trace!("{:#?}", service_params);

    // Create the initial ServiceState
    let mut service_state = TestRunner::new(service_params).await;
    log::debug!("Initial ServiceState {:?}", &service_state);

    // Iterate over the initial Change Script Players and start each one if it is configured to start immediately.
    for (_, player) in service_state.reactivators.iter() {
        if player.get_settings().reactivator.start_immediately {
            match player.start().await {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("Error starting ChangeScriptPlayer: {}", e);
                    log::error!("{}", msg);
                    service_state.service_status = ServiceStatus::Error(msg);
                    break;
                }
            }
        }
    }

    // Set the ServiceStatus to Ready if it is not already in an Error state.
    match &service_state.service_status {
        ServiceStatus::Error(_) => {
            log::error!("Test Runner failed to initialize correctly, ServiceState: {:?}", &service_state.service_status);            
        },
        _ => {
            service_state.service_status = ServiceStatus::Ready;
            log::info!("Test Runner initialized successfully, ServiceState: {:?}", &service_state.service_status);            
        }
    }

    // Start the Web API.
    web_api::start_web_api(service_state).await;

}