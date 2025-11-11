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

//! Integration tests for YAML and JSON configuration file loading

use std::fs;
use std::path::Path;

/// Test that example JSON config files can be parsed
#[test]
fn test_example_json_configs_parse() {
    let json_configs = vec![
        "examples/building_comfort/drasi_embedded/config.json",
        "examples/population/local/config.json",
    ];

    for config_path in json_configs {
        if Path::new(config_path).exists() {
            let content = fs::read_to_string(config_path)
                .unwrap_or_else(|e| panic!("Failed to read {}: {}", config_path, e));

            // Try parsing as JSON first (original format)
            let json_result: Result<serde_json::Value, _> = serde_json::from_str(&content);
            assert!(
                json_result.is_ok(),
                "Failed to parse {} as JSON: {:?}",
                config_path,
                json_result.err()
            );

            // Should also parse as YAML (since YAML is superset of JSON)
            let yaml_result: Result<serde_yaml::Value, _> = serde_yaml::from_str(&content);
            assert!(
                yaml_result.is_ok(),
                "Failed to parse {} as YAML: {:?}",
                config_path,
                yaml_result.err()
            );
        }
    }
}

/// Test that example YAML config files can be parsed
#[test]
fn test_example_yaml_configs_parse() {
    let yaml_configs = vec![
        "examples/building_comfort/drasi_embedded/config.yaml",
        "examples/population/local/config.yaml",
    ];

    for config_path in yaml_configs {
        if Path::new(config_path).exists() {
            let content = fs::read_to_string(config_path)
                .unwrap_or_else(|e| panic!("Failed to read {}: {}", config_path, e));

            // Should parse as YAML
            let yaml_result: Result<serde_yaml::Value, _> = serde_yaml::from_str(&content);
            assert!(
                yaml_result.is_ok(),
                "Failed to parse {} as YAML: {:?}",
                config_path,
                yaml_result.err()
            );
        }
    }
}

/// Test that JSON and YAML versions of the same config produce equivalent data structures
#[test]
fn test_json_yaml_equivalence() {
    let config_pairs = vec![
        (
            "examples/building_comfort/drasi_embedded/config.json",
            "examples/building_comfort/drasi_embedded/config.yaml",
        ),
        (
            "examples/population/local/config.json",
            "examples/population/local/config.yaml",
        ),
    ];

    for (json_path, yaml_path) in config_pairs {
        if Path::new(json_path).exists() && Path::new(yaml_path).exists() {
            let json_content = fs::read_to_string(json_path)
                .unwrap_or_else(|e| panic!("Failed to read {}: {}", json_path, e));
            let yaml_content = fs::read_to_string(yaml_path)
                .unwrap_or_else(|e| panic!("Failed to read {}: {}", yaml_path, e));

            // Parse both as YAML (since YAML can parse JSON)
            let json_as_yaml: serde_yaml::Value = serde_yaml::from_str(&json_content)
                .unwrap_or_else(|e| panic!("Failed to parse {} as YAML: {}", json_path, e));
            let yaml_value: serde_yaml::Value = serde_yaml::from_str(&yaml_content)
                .unwrap_or_else(|e| panic!("Failed to parse {}: {}", yaml_path, e));

            // Convert to JSON strings for comparison (normalizes formatting)
            let json_normalized = serde_json::to_string_pretty(&json_as_yaml).unwrap();
            let yaml_normalized = serde_json::to_string_pretty(&yaml_value).unwrap();

            assert_eq!(
                json_normalized, yaml_normalized,
                "JSON and YAML configs produce different data structures:\nJSON: {}\nYAML: {}",
                json_path, yaml_path
            );
        }
    }
}
