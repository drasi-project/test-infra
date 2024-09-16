use clap::Parser;
use serde::Serialize;

use runner::{config::TestRunnerConfig, TestRunner};

mod runner;
mod script_source;
mod source_change_dispatchers;
mod test_repo;
mod web_api;

// A struct to hold parameters obtained from env vars and/or command line arguments.
// Command line args will override env vars. If neither is provided, default values are used.
#[derive(Parser, Debug, Clone, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct HostParams {
    // The path of the config file.
    // If not provided, the the TestRunner will be stared with no active configuration
    // and wait to be configured through the Web API.
    #[arg(short = 'c', long = "config", env = "DRASI_CONFIG_FILE")]
    pub config_file_path: Option<String>,

    // The path where data used and generated in the TestRunner gets stored.
    // This will override the value in the config file if it is present.
    #[arg(short = 'd', long = "data", env = "DRASI_DATA_STORE_PATH")]
    pub data_store_path: Option<String>,

    // Flag to enable pruning of the data store at startup.
    #[arg(short = 'x', long = "prune", env = "DRASI_PRUNE_DATA_STORE")]
    pub prune_data_store : bool,

    // The port number the Web API will listen on.
    // If not provided, the default_value is used.
    #[arg(short = 'p', long = "port", env = "DRASI_PORT", default_value_t = 4000)]
    pub port: u16
}

// The main function that starts the starts the Test Runner Host.
#[tokio::main]
async fn main() {
     
     env_logger::init();

    // Parse the command line and env var args. If the args are invalid, return an error.
    let host_params = HostParams::parse();
    log::info!("Using settings - {:#?}", host_params);

    // Load the config from a file if a path is specified in the HostParams.
    // If the specified file does not exist, return an error.
    // If no config file is specified, create the TestRunner with a default configuration.
    let mut test_runner_config = match host_params.config_file_path.as_ref() {
        Some(config_file_path) => {
            log::info!("Loading Test Runner config from {:#?}", config_file_path);

            // Validate that the file exists and if not return an error.
            if !std::path::Path::new(config_file_path).exists() {
                panic!("Config file not found: {}", config_file_path);
            }

            // Read the file content into a string.
            let config_file_json = std::fs::read_to_string(config_file_path).unwrap_or_else(|err| {
                panic!("Error reading config file: {}", err);
            });

            serde_json::from_str::<TestRunnerConfig>(&config_file_json).unwrap_or_else(|err| {
                panic!("Error parsing config TestRunner: {}", err);
            })
        },
        None => {
            log::info!("No config file specified.; using default configuration.");
            TestRunnerConfig::default()
        }
    };

    // If a data_store_path is specified in the HostParams, update the TestRunnerConfig.
    if host_params.data_store_path.is_some() {
        test_runner_config.data_store_path = host_params.data_store_path.unwrap();
    }

    // If the prune_data_store flag is set, update the TestRunnerConfig
    if host_params.prune_data_store {
        test_runner_config.prune_data_store_path = true;
    }

    log::debug!("Creating Test Runner with config {:?}", test_runner_config);
    let mut test_runner = TestRunner::new(test_runner_config).await.unwrap_or_else(|err| {
        panic!("Error creating TestRunner: {}", err);
    });

    // Start the TestRunner. This will start any players that are configured to start on launch.
    test_runner.start().await.unwrap_or_else(|err| {
        panic!("Error starting TestRunner: {}", err);
    });

    // Start the Web API.
    web_api::start_web_api(host_params.port, test_runner).await;
}