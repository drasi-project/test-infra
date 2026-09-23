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

#[test]
fn removed_execution_mode_overrides_are_rejected() {
    for key in ["execution_mode", "executionMode"] {
        for value in [
            serde_json::json!("componentGraph"),
            serde_json::json!("computationGraph"),
            serde_json::json!("invalid"),
            serde_json::Value::Null,
        ] {
            let mut config = serde_json::to_value(run_config("instance1")).unwrap();
            config["test_run_overrides"] = serde_json::json!({ key: value });
            let error = serde_json::from_value::<TestRunDrasiLibInstanceConfig>(config)
                .unwrap_err()
                .to_string();
            assert!(error.contains("unknown field"), "{error}");
            assert!(error.contains(key), "{error}");
        }
    }
}

#[test]
fn runtime_overrides_serialize_without_an_engine_selector() {
    let overrides: TestRunDrasiLibInstanceOverrides =
        serde_json::from_value(serde_json::json!({ "log_level": "debug" })).unwrap();
    assert_eq!(
        serde_json::to_value(overrides).unwrap(),
        serde_json::json!({ "log_level": "debug" })
    );
}

#[tokio::test]
async fn embedded_instances_report_the_single_live_runtime() {
    use super::TestRunDrasiLibInstance;
    use test_data_store::test_run_storage::TestRunDrasiLibInstanceStorage;

    let directory = tempfile::tempdir().unwrap();
    let mut config = run_config("instance1");
    config.start_immediately = false;
    let definition =
        TestRunDrasiLibInstanceDefinition::new(config, empty_instance_def("instance1", None))
            .unwrap();
    let storage = TestRunDrasiLibInstanceStorage {
        id: definition.id.clone(),
        path: directory.path().to_path_buf(),
    };
    let instance = TestRunDrasiLibInstance::new(
        definition,
        storage,
        crate::test_run_completion::LifecycleTx::disabled(),
    )
    .await
    .unwrap();

    assert!(instance.get_runtime_info().await.is_err());
    for _ in 0..2 {
        instance.start().await.unwrap();
        let runtime = instance.get_runtime_info().await.unwrap();
        assert_eq!(runtime.execution_mode, "computationGraph");
        assert!(runtime.running);
        instance.stop().await.unwrap();
        assert!(instance.get_runtime_info().await.is_err());
    }
}

#[tokio::test]
async fn embedded_application_adapters_preserve_result_order_and_count() {
    use super::TestRunDrasiLibInstance;
    use drasi_source_application::PropertyMapBuilder;
    use test_data_store::test_run_storage::TestRunDrasiLibInstanceStorage;

    let directory = tempfile::tempdir().unwrap();
    let instance_definition = serde_json::from_value(serde_json::json!({
        "test_drasi_lib_instance_id": "instance1",
        "config": {
            "sources": [{ "id": "source1", "kind": "application" }],
            "queries": [{
                "id": "query1",
                "query": "MATCH (n:TestNode) RETURN n.marker AS marker",
                "sources": ["source1"]
            }],
            "reactions": [{
                "id": "reaction1", "kind": "application", "queries": ["query1"]
            }]
        }
    }))
    .unwrap();
    let definition =
        TestRunDrasiLibInstanceDefinition::new(run_config("instance1"), instance_definition)
            .unwrap();
    let storage = TestRunDrasiLibInstanceStorage {
        id: definition.id.clone(),
        path: directory.path().to_path_buf(),
    };
    let instance = TestRunDrasiLibInstance::new(
        definition,
        storage,
        crate::test_run_completion::LifecycleTx::disabled(),
    )
    .await
    .unwrap();
    let source = instance.get_source_handle("source1").await.unwrap();
    let reaction = instance.get_reaction_handle("reaction1").await.unwrap();
    let mut subscription = reaction
        .subscribe_with_options(Default::default())
        .await
        .unwrap();

    for index in 0..3 {
        let marker = format!("ordered-node-{index}");
        source
            .send_node_insert(
                marker.as_str(),
                vec!["TestNode".to_string()],
                PropertyMapBuilder::new()
                    .with_string("marker", &marker)
                    .build(),
            )
            .await
            .unwrap();
    }

    let mut previous_sequence = None;
    for index in 0..3 {
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let result = subscription
                    .recv()
                    .await
                    .expect("reaction subscription closed");
                if !result.results.is_empty() {
                    break result;
                }
            }
        })
        .await
        .expect("application reaction did not receive the source change");
        assert_eq!(result.query_id, "query1");
        assert_eq!(result.results.len(), 1);
        if let Some(previous) = previous_sequence {
            assert!(result.sequence > previous);
        }
        previous_sequence = Some(result.sequence);
        let payload = serde_json::to_string(&result.results).unwrap();
        assert!(
            payload.contains(&format!("ordered-node-{index}")),
            "{payload}"
        );
    }
    instance.stop().await.unwrap();
}
