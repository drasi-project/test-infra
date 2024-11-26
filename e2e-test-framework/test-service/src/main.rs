use std::sync::Arc;

use clap::Parser;
use serde::{Deserialize, Serialize};
use data_collector::{config::DataCollectorConfig, DataCollector};
use test_data_store::{TestDataStoreConfig, TestDataStore};
use test_runner::{TestRunnerConfig, TestRunner};
use tokio::sync::RwLock;

mod web_api;

// A struct to hold parameters obtained from env vars and/or command line arguments.
// Command line args will override env vars. If neither is provided, default values are used.
#[derive(Parser, Debug, Clone, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct HostParams {
    // The path of the config file.
    // If not provided, the TestService will be stared with no active configuration
    // and wait to be configured through the Web API.
    #[arg(short = 'c', long = "config", env = "DRASI_CONFIG_FILE")]
    pub config_file_path: Option<String>,

    // The path where data used and generated in the TestService gets stored.
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

#[derive(Debug, Deserialize, Serialize)]
pub struct TestServiceConfig {
    #[serde(default)]
    pub data_store: TestDataStoreConfig,
    #[serde(default)]
    pub test_runner: TestRunnerConfig,
    #[serde(default)]
    pub data_collector: DataCollectorConfig,
}

impl Default for TestServiceConfig {
    fn default() -> Self {
        TestServiceConfig {
            data_store: TestDataStoreConfig::default(),
            test_runner: TestRunnerConfig::default(),
            data_collector: DataCollectorConfig::default(),
        }
    }
}

// The main function that starts the starts the Test Runner Host.
#[tokio::main]
async fn main() {
     
     env_logger::init();

    // Parse the command line and env var args. If the args are invalid, return an error.
    let host_params = HostParams::parse();
    log::info!("Started Test Service with - {:?}", host_params);

    // Load the config from a file if a path is specified in the HostParams.
    // If the specified file does not exist, return an error.
    // If no config file is specified, create the TestService with a default configuration.
    let mut test_service_config = match host_params.config_file_path.as_ref() {
        Some(config_file_path) => {
            log::info!("Loading Test Service config from {:#?}", config_file_path);

            // Validate that the file exists and if not return an error.
            if !std::path::Path::new(config_file_path).exists() {
                panic!("Config file not found: {}", config_file_path);
            }

            // Read the file content into a string.
            let config_file_json = std::fs::read_to_string(config_file_path).unwrap_or_else(|err| {
                panic!("Error reading config file: {}", err);
            });

            serde_json::from_str::<TestServiceConfig>(&config_file_json).unwrap_or_else(|err| {
                panic!("Error parsing TestServiceConfig: {}", err);
            })
        },
        None => {
            log::info!("No config file specified; using default configuration.");
            TestServiceConfig::default()
        }
    };

    if host_params.data_store_path.is_some() {
        test_service_config.data_store.data_store_path = host_params.data_store_path;
    };

    if host_params.prune_data_store {
        test_service_config.data_store.delete_data_store = Some(true);
    };

    // Create the TestDataStore.
    let test_data_store = Arc::new(TestDataStore::new(test_service_config.data_store).await.unwrap_or_else(|err| {
        panic!("Error creating TestDataStore: {}", err);
    }));

    let mut data_collector = DataCollector::new(test_service_config.data_collector, test_data_store.clone()).await.unwrap_or_else(|err| {
        panic!("Error creating DataCollector: {}", err);
    });

    // Start the DataCollector. This will start any collectors that are configured to start on launch.
    data_collector.start().await.unwrap_or_else(|err| {
        panic!("Error starting DataCollector: {}", err);
    });

    let mut test_runner = TestRunner::new(test_service_config.test_runner, test_data_store.clone()).await.unwrap_or_else(|err| {
        panic!("Error creating TestRunner: {}", err);
    });

    // Start the TestRunner. This will start any players that are configured to start on launch.
    test_runner.start().await.unwrap_or_else(|err| {
        panic!("Error starting TestRunner: {}", err);
    });
    
    // Start the Web API.
    web_api::start_web_api(
        host_params.port, 
        test_data_store, 
        Arc::new(RwLock::new(test_runner)), 
        Arc::new(RwLock::new(data_collector))
    ).await;
}
