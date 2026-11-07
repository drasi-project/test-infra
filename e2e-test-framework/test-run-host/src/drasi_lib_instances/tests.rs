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
use anyhow::{Context, Result};
use drasi_lib::computation::v1::*;
use drasi_source_application::PropertyMapBuilder;
use serde_json::{json, Value};
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};
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
    assert_eq!(
        TestRunDrasiLibInstanceState::Stopping.to_string(),
        "Stopping"
    );
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
    );

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
async fn embedded_native_application_reaction_preserves_result_order_and_count() {
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
                "inputs": ["source1"]
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
    );
    instance.start().await.unwrap();
    assert_eq!(
        instance
            .get_drasi_lib()
            .await
            .expect("running instance")
            .query_factory_implementation()
            .expect("registered native query factory")
            .name
            .as_ref(),
        "drasi.experimental/indexed-query/ordinary"
    );
    wait_named_started(&instance, &["source1", "query1", "reaction1"])
        .await
        .unwrap();
    let query_id = instance.get_query("query1").await.unwrap().id;
    let source = instance.get_source_handle("source1").await.unwrap();
    let mut subscription = instance.take_reaction_receiver("reaction1").await.unwrap();
    assert!(instance.take_reaction_receiver("reaction1").await.is_err());

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
                let input = subscription
                    .recv()
                    .await
                    .expect("reaction subscription closed");
                let result =
                    drasi_lib::computation::v1::QueryChangeCodec::to_legacy_result(&input.envelope)
                        .unwrap();
                if !result.results.is_empty() {
                    break result;
                }
            }
        })
        .await
        .expect("application reaction did not receive the source change");
        assert_eq!(result.query_id, query_id);
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

pub(crate) fn native_instance(
    mut config: Value,
) -> Result<(super::TestRunDrasiLibInstance, tempfile::TempDir)> {
    let directory = tempfile::tempdir()?;
    let fields = config
        .as_object_mut()
        .context("test configuration must be an object")?;
    for field in ["sources", "queries", "reactions"] {
        fields.entry(field).or_insert_with(|| json!([]));
    }
    let definition = TestRunDrasiLibInstanceDefinition::new(
        run_config("instance1"),
        serde_json::from_value(json!({
            "test_drasi_lib_instance_id": "instance1",
            "config": config,
        }))?,
    )?;
    let storage = test_data_store::test_run_storage::TestRunDrasiLibInstanceStorage {
        id: definition.id.clone(),
        path: directory.path().to_path_buf(),
    };
    Ok((
        super::TestRunDrasiLibInstance::new(
            definition,
            storage,
            crate::test_run_completion::LifecycleTx::disabled(),
        ),
        directory,
    ))
}

async fn next_result(
    receiver: &mut tokio::sync::mpsc::Receiver<InputEnvelope>,
) -> Result<drasi_lib::channels::QueryResult> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let input = receiver.recv().await.context("native reaction closed")?;
            let result = QueryChangeCodec::to_legacy_result(&input.envelope)?;
            if !result.results.is_empty() {
                return Ok(result);
            }
        }
    })
    .await?
}

async fn expect_row(
    receiver: &mut tokio::sync::mpsc::Receiver<InputEnvelope>,
    query: &str,
    row: Value,
) -> Result<()> {
    let result = next_result(receiver).await?;
    assert_eq!(result.query_id, query);
    assert!(
        matches!(
            &result.results[..],
            [drasi_lib::channels::ResultDiff::Add { data, .. }] if data == &row
        ),
        "{result:?}"
    );
    Ok(())
}

async fn wait_named_started(
    instance: &super::TestRunDrasiLibInstance,
    names: &[&str],
) -> Result<()> {
    let core = instance.get_drasi_lib().await.context("instance owner")?;
    for name in names {
        let handle = core.component_by_name(name)?;
        tokio::time::timeout(Duration::from_secs(5), handle.wait_started())
            .await
            .with_context(|| {
                let control = core.computation_control().expect("test graph");
                let topology = control.desired_snapshot();
                let nodes: Vec<_> = topology.nodes.iter().map(|node| (
                    node.name.to_string(),
                    node.descriptor.id().to_string(),
                    node.descriptor.ports().iter().map(|port| port.id().to_string()).collect::<Vec<_>>(),
                )).collect();
                let edges: Vec<_> = topology.edges.iter().map(|edge| edge.definition.clone()).collect();
                let accepted = core.desired_configuration().expect("test configuration");
                format!("{name} did not start: nodes={nodes:?}; edges={edges:?}; unbound={:?}; accepted_native_edges={:?}; ordinary={:?}",
                    topology.unbound_relationships, accepted.desired.topology.relationships, accepted.desired.ordinary)
            })??;
    }
    Ok(())
}

async fn native_identity_bindings_do_not_follow_reused_names() -> Result<()> {
    use super::api_models::*;
    let (instance, _directory) = native_instance(json!({}))?;
    instance.start().await?;
    let result = async {
        let name = "e91ec818-e5c0-457b-b8a0-4b0803fcb13b";
        let first = instance
            .create_source(CreateSourceRequest {
                name: name.into(),
                source_type: "application".into(),
                auto_start: false,
                properties: json!({"tag": "first"}),
            })
            .await?;
        ComponentId::try_new(first.id.as_str())?;
        assert_eq!(first.name, name);
        assert_ne!(
            first.id, name,
            "a UUID-looking name is not an external ID request"
        );
        let core = instance.get_drasi_lib().await.context("instance owner")?;
        assert!(!core.allow_external_component_ids());
        assert_eq!(core.component_by_name(name)?.id().as_str(), first.id);
        let old_handle = instance.get_source_handle(name).await?;
        assert!(instance
            .update_source(
                name,
                UpdateSourceRequest {
                    source_type: None,
                    auto_start: None,
                    properties: Some(json!({"tag": "changed"})),
                }
            )
            .await
            .is_err());
        let unchanged = instance.get_source(name).await?;
        assert_eq!(unchanged.id, first.id);
        assert_eq!(unchanged.properties, json!({"tag": "first"}));
        instance.delete_source(name).await?;
        let second = instance
            .create_source(CreateSourceRequest {
                name: name.into(),
                source_type: "application".into(),
                auto_start: false,
                properties: json!({"tag": "second"}),
            })
            .await?;
        assert_ne!(first.id, second.id);
        assert_eq!(second.name, name);
        assert!(
            old_handle
                .send_node_insert(
                    "old-handle",
                    vec!["Item"],
                    PropertyMapBuilder::new().build(),
                )
                .await
                .is_err(),
            "an old handle must not reach a replacement object"
        );
        instance.delete_source(name).await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let cleanup = instance.stop().await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(error.context(format!("cleanup also failed: {cleanup}"))),
    }
}

#[test]
fn native_instance_uuid_assignment_and_stale_handle_fencing() -> Result<()> {
    both_runtimes(native_identity_bindings_do_not_follow_reused_names)
}

fn both_runtimes<F, Fut>(case: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    for current_thread in [true, false] {
        let mut builder = if current_thread {
            tokio::runtime::Builder::new_current_thread()
        } else {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.worker_threads(2);
            builder
        };
        builder
            .enable_all()
            .build()?
            .block_on(async { tokio::time::timeout(Duration::from_secs(20), case()).await? })?;
    }
    Ok(())
}

async fn native_programmatic_lifecycle() -> Result<()> {
    use super::api_models::*;
    let (instance, _directory) = native_instance(json!({}))?;
    assert!(instance.get_drasi_lib().await.is_none());
    instance.start().await?;
    let result = async {
        assert!(instance
            .create_reaction(CreateReactionRequest {
                name: "unbound-input".into(),
                reaction_type: "application".into(),
                queries: vec!["names".into()],
                auto_start: true,
                properties: json!({}),
            })
            .await
            .is_err());
        assert!(instance.list_reactions().await?.is_empty());
        instance
            .create_reaction(CreateReactionRequest {
                name: "sink".into(),
                reaction_type: "application".into(),
                queries: vec![],
                auto_start: true,
                properties: json!({"tag":"observer"}),
            })
            .await?;
        let reaction = instance.get_reaction("sink").await?;
        assert!(reaction.queries.is_empty());
        assert_eq!(reaction.reaction_type, "application");
        assert_eq!(reaction.properties, json!({"tag":"observer"}));
        assert!(reaction.auto_start);
        let mut receiver = instance.take_reaction_receiver("sink").await?;
        assert!(instance.take_reaction_receiver("sink").await.is_err());
        instance
            .create_query(CreateQueryRequest {
                name: "names".into(),
                query: "MATCH (n:Person) RETURN n.name AS name".into(),
                inputs: vec!["input".into()],
                auto_start: true,
                profiling: false,
            })
            .await?;
        instance
            .create_source(CreateSourceRequest {
                name: "input".into(),
                source_type: "application".into(),
                auto_start: false,
                properties: json!({"tag":"producer"}),
            })
            .await?;
        let source = instance.get_source("input").await?;
        assert_eq!(source.source_type, "application");
        assert_eq!(source.properties, json!({"tag":"producer"}));
        assert!(!source.auto_start);
        assert!(instance
            .list_sources()
            .await?
            .iter()
            .any(|source| { source.name == "input" && source.source_type == "application" }));
        assert_eq!(instance.list_queries().await?.len(), 1);
        assert_eq!(instance.list_reactions().await?.len(), 1);
        let core = instance.get_drasi_lib().await.context("instance owner")?;
        assert!(!core.allow_external_component_ids());
        let query_id = core.component_by_name("names")?.id().clone();
        let reaction_id = core.component_by_name("sink")?.id().clone();
        let mut current = core.desired_configuration()?;
        current
            .desired
            .topology
            .relationships
            .push(DesiredRelationship {
                definition: EdgeDefinition::new(
                    Endpoint::new(query_id.clone(), PortId::try_new("out")?),
                    Endpoint::new(reaction_id, PortId::try_new("in")?),
                ),
                policy: RelationshipPolicy::default(),
                pipe: DesiredPipe::Bounded { capacity: 1000 },
            });
        core.apply_desired_state(current.revision, "connect-native-observer", current.desired)
            .await?;
        core.reconcile_desired_state().await?;
        assert_eq!(instance.get_reaction("sink").await?.queries, ["names"]);
        let snapshot = core.computation_control()?.desired_snapshot();
        let legacy_source = SourcePluginAdapterFactory::default()
            .descriptor()
            .implementation
            .clone();
        let legacy_reaction = ReactionPluginAdapterFactory::default()
            .descriptor()
            .implementation
            .clone();
        assert!(!snapshot.specifications.values().any(|spec| {
            spec.implementation == legacy_source || spec.implementation == legacy_reaction
        }));
        let source_generation = core.component_by_name("input")?.generation();
        let input = instance.get_source_handle("input").await?;
        input
            .send_node_insert(
                "before-start",
                vec!["Person"],
                PropertyMapBuilder::new()
                    .with_string("name", "Buffered")
                    .build(),
            )
            .await?;
        assert!(receiver.try_recv().is_err());
        instance.start_source("input").await?;
        expect_row(&mut receiver, query_id.as_str(), json!({"name":"Buffered"})).await?;
        let update = instance
            .update_query(
                "names",
                UpdateQueryRequest {
                    query: Some("MATCH (n:Person) RETURN n.name AS name, true AS updated".into()),
                    inputs: None,
                    auto_start: None,
                    profiling: None,
                },
            )
            .await;
        assert!(
            update.is_err(),
            "semantic changes cannot silently reset live-only inputs"
        );
        assert!(!instance.get_query("names").await?.query.contains("updated"));
        assert!(
            !core
                .get_query_config(query_id.as_str())
                .await?
                .enable_bootstrap,
            "an update must preserve the explicit live-only input"
        );
        assert_eq!(
            core.component_by_name("input")?.generation(),
            source_generation
        );
        input
            .send_node_insert(
                "updated",
                vec!["Person"],
                PropertyMapBuilder::new()
                    .with_string("name", "Updated")
                    .build(),
            )
            .await?;
        expect_row(&mut receiver, query_id.as_str(), json!({"name":"Updated"})).await?;
        instance.stop_query("names").await?;
        input
            .send_node_insert(
                "paused",
                vec!["Person"],
                PropertyMapBuilder::new()
                    .with_string("name", "Paused")
                    .build(),
            )
            .await?;
        instance.start_query("names").await?;
        expect_row(&mut receiver, query_id.as_str(), json!({"name":"Paused"})).await?;
        instance.stop_source("input").await?;
        input
            .send_node_insert(
                "source-stopped",
                vec!["Person"],
                PropertyMapBuilder::new()
                    .with_string("name", "Restarted")
                    .build(),
            )
            .await?;
        instance.start_source("input").await?;
        expect_row(
            &mut receiver,
            query_id.as_str(),
            json!({"name":"Restarted"}),
        )
        .await?;
        assert!(instance.delete_source("input").await.is_err());
        assert!(instance.get_source_handle("input").await.is_ok());
        instance.delete_reaction("sink").await?;
        instance.delete_query("names").await?;
        instance.delete_source("input").await?;
        assert!(instance.get_source_handle("input").await.is_err());
        assert!(instance.list_queries().await?.is_empty());
        assert!(instance.list_reactions().await?.is_empty());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = instance.stop().await;
    result?;
    cleanup
}

async fn native_multi_query_results() -> Result<()> {
    let (instance, _directory) = native_instance(json!({
        "sources":[{"id":"input","kind":"application"}],
        "queries":[
            {"id":"first","query":"MATCH (n:Item) RETURN n.marker AS marker","inputs":["input"]},
            {"id":"second","query":"MATCH (n:Item) RETURN n.marker AS marker","inputs":["input"]}
        ],
        "reactions":[{"id":"sink","kind":"application","queries":["first","second"]}]
    }))?;
    instance.start().await?;
    let result = async {
        wait_named_started(&instance, &["input", "first", "second", "sink"]).await?;
        let first = instance.get_query("first").await?.id;
        let second = instance.get_query("second").await?.id;
        let input = instance.get_source_handle("input").await?;
        let mut receiver = instance.take_reaction_receiver("sink").await?;
        for index in 0..5 {
            input
                .send_node_insert(
                    index.to_string(),
                    vec!["Item"],
                    PropertyMapBuilder::new()
                        .with_integer("marker", index)
                        .build(),
                )
                .await?;
        }
        let mut rows = std::collections::BTreeMap::<String, Vec<Value>>::new();
        let mut sequences = HashMap::<String, u64>::new();
        for _ in 0..10 {
            let result = next_result(&mut receiver).await?;
            if let Some(previous) = sequences.insert(result.query_id.clone(), result.sequence) {
                assert!(result.sequence > previous);
            }
            let [drasi_lib::channels::ResultDiff::Add { data, .. }] = &result.results[..] else {
                anyhow::bail!("unexpected result: {result:?}");
            };
            rows.entry(result.query_id)
                .or_default()
                .push(data["marker"].clone());
        }
        let expected = (0..5).map(|index| json!(index)).collect::<Vec<_>>();
        assert_eq!(
            rows,
            std::collections::BTreeMap::from([(first, expected.clone()), (second, expected),])
        );
        assert!(receiver.try_recv().is_err(), "unexpected duplicate result");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = instance.stop().await;
    result?;
    cleanup
}

async fn native_failed_start_retains_owner() -> Result<()> {
    let (instance, _directory) = native_instance(json!({
        "sources":[
            {"id":"input","kind":"application"},
            {"id":"unsupported","kind":"not-a-source"}
        ]
    }))?;
    assert!(instance.get_drasi_lib().await.is_none());
    let error = instance
        .start()
        .await
        .expect_err("unsupported source must fail visibly");
    assert!(error.to_string().contains("Unsupported"), "{error}");
    assert!(matches!(
        instance.get_state().await,
        TestRunDrasiLibInstanceState::Error(_)
    ));
    let owner = instance
        .get_drasi_lib()
        .await
        .context("failed startup lost the owner")?;
    assert!(!owner.is_running().await);
    assert!(instance
        .start()
        .await
        .expect_err("restart cannot replace retained ownership")
        .to_string()
        .contains("must be stopped"));
    assert!(Arc::ptr_eq(
        &owner,
        &instance.get_drasi_lib().await.context("retained owner")?
    ));
    instance.stop().await?;
    assert!(instance.get_drasi_lib().await.is_none());
    assert!(instance.get_source_handle("input").await.is_err());
    assert_eq!(
        instance.get_state().await,
        TestRunDrasiLibInstanceState::Stopped
    );
    Ok(())
}

struct StopGate {
    descriptor: ComponentDescriptor,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

struct StopGateFactory {
    descriptor: FactoryDescriptor,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ComponentFactory for StopGateFactory {
    fn descriptor(&self) -> &FactoryDescriptor {
        &self.descriptor
    }

    fn validate(&self, _: &ComponentSpecification) -> Result<()> {
        Ok(())
    }

    async fn create(
        &self,
        context: ConstructionContext,
    ) -> Result<ConstructedComponent, ComponentCreationError> {
        Ok(ConstructedComponent::service(Box::new(StopGate {
            descriptor: context.specification.descriptor.clone(),
            entered: self.entered.clone(),
            release: self.release.clone(),
        })))
    }
}

#[async_trait::async_trait]
impl ComputationComponent for StopGate {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }
    async fn start(&mut self) -> Result<()> {
        Ok(())
    }
    async fn stop(&mut self) -> Result<()> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ComputationService for StopGate {
    async fn run(&mut self) -> Result<()> {
        std::future::pending().await
    }
}

async fn native_cancelled_stop_retains_owner() -> Result<()> {
    let (instance, _directory) = native_instance(json!({}))?;
    instance.start().await?;
    let core = instance.get_drasi_lib().await.context("instance owner")?;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let factory = Arc::new(StopGateFactory {
        descriptor: FactoryDescriptor {
            implementation: ImplementationIdentity::try_new("test-infra/stop-gate", "1")?,
            role: ComponentRole::Service,
            configuration_version: 1,
            configuration: ConfigurationSchema::default(),
            dependencies: Default::default(),
        },
        entered: entered.clone(),
        release: release.clone(),
    });
    let receipt = core
        .create_components(move |ids| {
            let id = ids.allocate()?;
            let specification = ComponentSpecification {
                descriptor: ComponentDescriptor::try_new(id, vec![])?,
                implementation: factory.descriptor.implementation.clone(),
                configuration_version: 1,
                role: ComponentRole::Service,
                completion: None,
                configuration: Default::default(),
                dependencies: Default::default(),
            };
            ComponentBatch::builder()
                .component(ComponentName::try_new("stop-gate")?, specification, factory)
                .build()
        })
        .await?;
    let id = &receipt
        .creation
        .as_ref()
        .context("missing gate identity")?
        .components[0]
        .id;
    core.reconcile_desired_state().await?;
    let service = core.computation_component(id.as_str())?;
    service.wait_started().await?;
    let mut stopping = Box::pin(instance.stop());
    tokio::select! {
        _ = entered.notified() => {},
        result = &mut stopping => anyhow::bail!("stop completed before the gate: {result:?}"),
    }
    drop(stopping);
    assert!(Arc::ptr_eq(
        &core,
        &instance
            .get_drasi_lib()
            .await
            .context("stop lost the owner")?
    ));
    assert_eq!(
        instance.get_state().await,
        TestRunDrasiLibInstanceState::Stopping
    );
    assert!(
        instance.start().await.is_err(),
        "cancelled shutdown must not report a successful restart"
    );
    release.notify_one();
    instance.stop().await?;
    assert!(instance.get_drasi_lib().await.is_none());
    assert!(!core.is_running().await);
    Ok(())
}

#[test]
fn native_instance_programmatic_dependencies_metadata_and_lifecycle() -> Result<()> {
    both_runtimes(native_programmatic_lifecycle)
}

#[test]
fn native_instance_fanout_keeps_exact_per_query_order_and_count() -> Result<()> {
    both_runtimes(native_multi_query_results)
}

#[test]
fn native_instance_failed_start_keeps_cleanup_ownership() -> Result<()> {
    both_runtimes(native_failed_start_retains_owner)
}

#[test]
fn native_instance_cancelled_shutdown_can_be_awaited_again() -> Result<()> {
    both_runtimes(native_cancelled_stop_retains_owner)
}
