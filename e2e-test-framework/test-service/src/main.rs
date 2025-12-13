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

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use data_collector::{config::DataCollectorConfig, DataCollector};
use serde::{Deserialize, Serialize};
use test_data_store::{TestDataStore, TestDataStoreConfig};
use test_run_host::{TestRunHost, TestRunHostConfig};

mod openapi;
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
    pub prune_data_store: bool,

    // The port number the Web API will listen on.
    // If not provided, the default_value is used.
    #[arg(
        short = 'p',
        long = "port",
        env = "DRASI_PORT",
        default_value_t = 63123
    )]
    pub port: u16,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct TestServiceConfig {
    #[serde(default)]
    pub data_store: TestDataStoreConfig,
    #[serde(default)]
    pub test_run_host: TestRunHostConfig,
    #[serde(default)]
    pub data_collector: DataCollectorConfig,
}

/// Loads configuration from a file, supporting both YAML and JSON formats.
///
/// This function uses a content-based parsing approach:
/// 1. Attempts to parse as YAML first (since YAML is a superset of JSON)
/// 2. Falls back to JSON parsing if YAML fails
/// 3. Returns detailed error messages from both parsers if both fail
///
/// # Arguments
/// * `path` - Path to the configuration file
///
/// # Returns
/// * `Result<TestServiceConfig>` - The parsed configuration or an error with details
fn load_config_from_file(path: &str) -> Result<TestServiceConfig> {
    // Read file content
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {path}"))?;

    // Try YAML first, then JSON
    match serde_yaml::from_str::<TestServiceConfig>(&content) {
        Ok(config) => {
            log::debug!("Successfully parsed config as YAML from: {path}");
            Ok(config)
        }
        Err(yaml_err) => {
            // If YAML fails, try JSON
            match serde_json::from_str::<TestServiceConfig>(&content) {
                Ok(config) => {
                    log::debug!("Successfully parsed config as JSON from: {path}");
                    Ok(config)
                }
                Err(json_err) => {
                    // Both failed, return detailed error
                    Err(anyhow::anyhow!(
                        "Failed to parse config file '{path}':\n  YAML: {yaml_err}\n  JSON: {json_err}"
                    ))
                }
            }
        }
    }
}

// The main function that starts the starts the Test Service.
#[tokio::main]
async fn main() {
    // Initialize env_logger - back to simple init to respect RUST_LOG env var
    env_logger::init();

    // Parse the command line and env var args. If the args are invalid, return an error.
    let host_params = HostParams::parse();
    log::info!("Started Test Service with - {host_params:?}");

    // Load the config from a file if a path is specified in the HostParams.
    // If the specified file does not exist, return an error.
    // If no config file is specified, create the TestService with a default configuration.
    let mut test_service_config = match host_params.config_file_path.as_ref() {
        Some(config_file_path) => {
            log::info!("Loading Test Service config from {config_file_path:#?}");

            // Load config using the new dual-format parser
            load_config_from_file(config_file_path).unwrap_or_else(|err| {
                panic!("Error loading config file: {err}");
            })
        }
        None => {
            log::info!("No config file specified; using default configuration.");
            TestServiceConfig::default()
        }
    };

    if host_params.data_store_path.is_some() {
        test_service_config.data_store.data_store_path = host_params.data_store_path;
    };

    if host_params.prune_data_store {
        test_service_config.data_store.delete_on_start = Some(true);
    };

    // Create the TestDataStore.
    let test_data_store = Arc::new(
        TestDataStore::new(test_service_config.data_store)
            .await
            .unwrap_or_else(|err| {
                panic!("Error creating TestDataStore: {err}");
            }),
    );

    let data_collector = Arc::new(
        DataCollector::new(test_service_config.data_collector, test_data_store.clone())
            .await
            .unwrap_or_else(|err| {
                panic!("Error creating DataCollector: {err}");
            }),
    );

    // Start the DataCollector. This will start any collectors that are configured to start on launch.
    data_collector.start().await.unwrap_or_else(|err| {
        panic!("Error starting DataCollector: {err}");
    });

    let test_run_host = Arc::new(
        TestRunHost::new(test_service_config.test_run_host, test_data_store.clone())
            .await
            .unwrap_or_else(|err| {
                panic!("Error creating TestRunHost: {err}");
            }),
    );

    // Now that TestRunHost is wrapped in Arc, set it on sources and start auto-start sources
    test_run_host
        .initialize_sources(test_run_host.clone())
        .await
        .unwrap_or_else(|err| {
            panic!("Error initializing sources: {err}");
        });

    // Start the Web API.
    web_api::start_web_api(
        host_params.port,
        test_data_store,
        test_run_host,
        data_collector,
    )
    .await;
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_valid_json_config() {
        let json_content = r#"{
            "data_store": {
                "data_store_path": "/tmp/test"
            },
            "test_run_host": {
                "test_runs": []
            },
            "data_collector": {
                "data_collections": []
            }
        }"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = load_config_from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(
            config.data_store.data_store_path,
            Some("/tmp/test".to_string())
        );
    }

    #[test]
    fn test_load_valid_yaml_config() {
        let yaml_content = r#"
data_store:
  data_store_path: /tmp/test
test_run_host:
  test_runs: []
data_collector:
  data_collections: []
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = load_config_from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(
            config.data_store.data_store_path,
            Some("/tmp/test".to_string())
        );
    }

    #[test]
    fn test_load_minimal_config_with_defaults() {
        let yaml_content = "{}";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = load_config_from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert!(config.data_store.data_store_path.is_none());
        assert!(config.test_run_host.test_runs.is_empty());
    }

    #[test]
    fn test_load_json_with_yaml_features() {
        // JSON that's valid YAML but not strict JSON (has comments)
        let content = r#"
# This is a YAML comment
data_store:
  data_store_path: /tmp/test
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = load_config_from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(
            config.data_store.data_store_path,
            Some("/tmp/test".to_string())
        );
    }

    #[test]
    fn test_load_invalid_config_both_formats() {
        // This content is invalid for TestServiceConfig structure
        let invalid_content = r#"
data_store: "this should be an object, not a string"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(invalid_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = load_config_from_file(temp_file.path().to_str().unwrap());
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        // Should contain both YAML and JSON error messages
        assert!(err_msg.contains("YAML:"));
        assert!(err_msg.contains("JSON:"));
    }

    #[test]
    fn test_load_missing_file() {
        let result = load_config_from_file("/nonexistent/path/to/config.json");
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("Failed to read config file"));
    }

    #[test]
    fn test_load_empty_file() {
        // Empty file parses as YAML null, which gets default config
        let empty_content = "";

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(empty_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = load_config_from_file(temp_file.path().to_str().unwrap()).unwrap();
        // Empty file should produce default config
        assert!(config.data_store.data_store_path.is_none());
    }

    #[test]
    fn test_load_complex_nested_config() {
        let yaml_content = r#"
data_store:
  data_store_path: /tmp/complex/test
  delete_on_start: true
  delete_on_stop: false
  test_repos:
    - id: repo1
      kind: LocalStorage
      source_path: /path/to/repo
test_run_host:
  test_runs:
    - test_id: test1
      test_repo_id: repo1
      test_run_id: run001
      queries: []
      reactions: []
      sources: []
      drasi_servers: []
data_collector:
  data_collections: []
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = load_config_from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(
            config.data_store.data_store_path,
            Some("/tmp/complex/test".to_string())
        );
        assert_eq!(config.data_store.delete_on_start, Some(true));
        assert_eq!(config.data_store.delete_on_stop, Some(false));
        assert_eq!(config.test_run_host.test_runs.len(), 1);
        assert_eq!(config.test_run_host.test_runs[0].test_id, "test1");
    }

    #[test]
    fn test_load_json_that_fails_yaml_parse() {
        // Some edge case JSON that might not parse well as YAML
        let json_content = r#"{"data_store":{"data_store_path":"/tmp/test"}}"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(json_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let config = load_config_from_file(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(
            config.data_store.data_store_path,
            Some("/tmp/test".to_string())
        );
    }
}
