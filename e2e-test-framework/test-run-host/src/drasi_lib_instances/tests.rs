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

use crate::drasi_lib_instances::{
    TestRunDrasiLibInstanceConfig, TestRunDrasiLibInstanceDefinition,
    TestRunDrasiLibInstanceOverrides, TestRunDrasiLibInstanceState,
};
use test_data_store::test_repo_storage::models::{
    DrasiLibInstanceConfig, TestDrasiLibInstanceDefinition,
};

fn empty_instance_def(id: &str, log_level: Option<&str>) -> TestDrasiLibInstanceDefinition {
    TestDrasiLibInstanceDefinition {
        test_drasi_lib_instance_id: id.to_string(),
        name: Some(format!("instance-{id}")),
        description: None,
        config: DrasiLibInstanceConfig {
            log_level: log_level.map(|s| s.to_string()),
            sources: Vec::new(),
            queries: Vec::new(),
            reactions: Vec::new(),
        },
    }
}

fn run_config(test_drasi_lib_instance_id: &str) -> TestRunDrasiLibInstanceConfig {
    TestRunDrasiLibInstanceConfig {
        start_immediately: true,
        test_drasi_lib_instance_id: test_drasi_lib_instance_id.to_string(),
        test_run_overrides: None,
        test_id: Some("test".to_string()),
        test_repo_id: Some("test_repo".to_string()),
        test_run_id: Some("test_run_001".to_string()),
    }
}

#[test]
fn run_config_start_immediately_defaults_to_true() {
    let json = r#"{
        "test_id": "test",
        "test_repo_id": "test_repo",
        "test_drasi_lib_instance_id": "instance1"
    }"#;

    let config: TestRunDrasiLibInstanceConfig = serde_json::from_str(json).unwrap();
    assert!(config.start_immediately);
}

#[test]
fn run_config_start_immediately_can_be_overridden() {
    let json = r#"{
        "test_id": "test",
        "test_repo_id": "test_repo",
        "test_drasi_lib_instance_id": "instance1",
        "start_immediately": false
    }"#;

    let config: TestRunDrasiLibInstanceConfig = serde_json::from_str(json).unwrap();
    assert!(!config.start_immediately);
}

#[test]
fn state_display_uses_human_readable_strings() {
    assert_eq!(
        TestRunDrasiLibInstanceState::Uninitialized.to_string(),
        "Uninitialized"
    );
    assert_eq!(TestRunDrasiLibInstanceState::Running.to_string(), "Running");
    assert_eq!(TestRunDrasiLibInstanceState::Stopped.to_string(), "Stopped");
    assert_eq!(
        TestRunDrasiLibInstanceState::Error("boom".to_string()).to_string(),
        "Error: boom"
    );
}

#[test]
fn effective_config_returns_definition_log_level_when_no_override() {
    let definition = TestRunDrasiLibInstanceDefinition::new(
        run_config("instance1"),
        empty_instance_def("instance1", Some("debug")),
    )
    .unwrap();

    assert_eq!(
        definition.effective_config().log_level.as_deref(),
        Some("debug")
    );
}

#[test]
fn effective_config_log_level_override_takes_precedence() {
    let mut config = run_config("instance1");
    config.test_run_overrides = Some(TestRunDrasiLibInstanceOverrides {
        log_level: Some("trace".to_string()),
    });

    let definition = TestRunDrasiLibInstanceDefinition::new(
        config,
        empty_instance_def("instance1", Some("info")),
    )
    .unwrap();

    assert_eq!(
        definition.effective_config().log_level.as_deref(),
        Some("trace")
    );
}

#[test]
fn effective_config_log_level_none_when_unset() {
    let definition = TestRunDrasiLibInstanceDefinition::new(
        run_config("instance1"),
        empty_instance_def("instance1", None),
    )
    .unwrap();

    assert!(definition.effective_config().log_level.is_none());
}
