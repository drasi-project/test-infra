use super::*;
use drasi_lib::profiling::ProfilingMetadata;
use test_data_store::test_run_storage::TestRunId;

#[test]
fn profiling_capture_is_opt_in_and_preserves_query_identity_and_timestamps() {
    let definition: DrasiLibInstanceChannelReactionHandlerDefinition =
        serde_json::from_value(serde_json::json!({
            "drasi_lib_instance_id": "instance", "reaction_id": "reaction"
        }))
        .unwrap();
    assert!(!definition.include_profiling);
    let mut result = QueryResult::with_profiling(
        "query".into(),
        7,
        chrono::Utc::now(),
        Vec::new(),
        Default::default(),
        ProfilingMetadata {
            query_core_call_ns: Some(100),
            query_core_return_ns: Some(140),
            ..Default::default()
        },
    );
    assert!(result_profiling(&result, false).unwrap().is_none());
    let profile = result_profiling(&result, true).unwrap().unwrap();
    assert_eq!(profile["query_id"], "query");
    assert_eq!(profile["query_sequence"], 7);
    assert_eq!(
        profile["timestamps"],
        serde_json::to_value(result.profiling.as_ref().unwrap()).unwrap()
    );
    assert!(profile["handler_received_ns"].as_u64().unwrap() > 0);
    result.profiling = None;
    assert!(result_profiling(&result, true).unwrap().unwrap()["timestamps"].is_null());
}

#[tokio::test]
async fn pausing_retains_the_message_until_resume() {
    let (state, mut status) = watch::channel(ReactionHandlerStatus::Paused);
    let (tx, mut rx) = mpsc::channel(1);
    let pending = send(
        &tx,
        &mut status,
        ReactionHandlerMessage::Control(ReactionControlSignal::Start),
    );
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    assert!(rx.try_recv().is_err());
    state.send_replace(ReactionHandlerStatus::Running);
    assert!(pending.await.unwrap());
    assert!(matches!(
        rx.recv().await,
        Some(ReactionHandlerMessage::Control(
            ReactionControlSignal::Start
        ))
    ));
}

#[tokio::test]
async fn full_output_backpressures_without_dropping_the_next_message() {
    let (_state, mut status) = watch::channel(ReactionHandlerStatus::Running);
    let (tx, mut rx) = mpsc::channel(1);
    tx.send(ReactionHandlerMessage::Control(
        ReactionControlSignal::Start,
    ))
    .await
    .unwrap();
    let pending = send(
        &tx,
        &mut status,
        ReactionHandlerMessage::Control(ReactionControlSignal::Pause),
    );
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    assert!(matches!(
        rx.recv().await,
        Some(ReactionHandlerMessage::Control(
            ReactionControlSignal::Start
        ))
    ));
    assert!(pending.await.unwrap());
    assert!(matches!(
        rx.recv().await,
        Some(ReactionHandlerMessage::Control(
            ReactionControlSignal::Pause
        ))
    ));
}

#[tokio::test]
async fn stop_interrupts_a_full_output_wait() {
    let (state, mut status) = watch::channel(ReactionHandlerStatus::Running);
    let (tx, _rx) = mpsc::channel(1);
    tx.send(ReactionHandlerMessage::Control(
        ReactionControlSignal::Start,
    ))
    .await
    .unwrap();
    let pending = send(
        &tx,
        &mut status,
        ReactionHandlerMessage::Control(ReactionControlSignal::Pause),
    );
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    state.send_replace(ReactionHandlerStatus::Stopped);
    assert!(!tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap());
}

#[tokio::test]
async fn stopping_before_host_binding_joins_the_only_worker() {
    let handler = DrasiLibInstanceHandler::create_channel(
        TestRunQueryId::new(&TestRunId::new("repo", "test", "run"), "query"),
        serde_json::from_value(serde_json::json!({
            "drasi_lib_instance_id": "instance", "reaction_id": "reaction", "buffer_size": 1
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    let mut output = handler.init().await.unwrap();
    assert!(matches!(
        output.recv().await,
        Some(ReactionHandlerMessage::Control(
            ReactionControlSignal::Start
        ))
    ));
    handler.pause().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), handler.stop())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(handler.status().await, ReactionHandlerStatus::Stopped);
    assert!(matches!(
        output.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
    assert!(handler.init().await.is_err());
}

#[tokio::test]
async fn zero_handler_capacity_is_rejected_before_channel_creation() {
    assert!(DrasiLibInstanceHandler::create_channel(
        TestRunQueryId::new(&TestRunId::new("repo", "test", "run"), "query"),
        serde_json::from_value(serde_json::json!({
            "drasi_lib_instance_id": "instance", "reaction_id": "reaction", "buffer_size": 0
        }))
        .unwrap(),
    )
    .await
    .is_err());
}

async fn native_handler_projection(batch: bool, include_profiling: bool) -> Result<()> {
    use crate::drasi_lib_instances::{TestRunDrasiLibInstance, TestRunDrasiLibInstanceDefinition};
    use drasi_source_application::PropertyMapBuilder;
    use futures::StreamExt;
    use std::collections::HashMap;
    use test_data_store::test_run_storage::TestRunDrasiLibInstanceStorage;

    let directory = tempfile::tempdir()?;
    let definition = TestRunDrasiLibInstanceDefinition::new(
        serde_json::from_value(serde_json::json!({
            "test_repo_id":"repo", "test_id":"test", "test_run_id":"run",
            "test_drasi_lib_instance_id":"instance"
        }))?,
        serde_json::from_value(serde_json::json!({
            "test_drasi_lib_instance_id":"instance",
            "config":{
                "sources":[{"id":"input","kind":"application"}],
                "queries":[{"id":"names","query":"MATCH (n:Item) RETURN n.value AS value","sources":["input"]}],
                "reactions":[{"id":"sink","kind":"application","queries":["names"]}]
            }
        }))?,
    )?;
    let run_id = definition.id.test_run_id.clone();
    let storage = TestRunDrasiLibInstanceStorage {
        id: definition.id.clone(),
        path: directory.path().to_path_buf(),
    };
    let instance = TestRunDrasiLibInstance::new(
        definition,
        storage,
        crate::test_run_completion::LifecycleTx::disabled(),
    );
    let host = Arc::new(
        crate::TestRunHost::new(
            crate::TestRunHostConfig::default(),
            Arc::new(test_data_store::TestDataStore::new_temp(None).await?),
        )
        .await?,
    );
    host.test_runs.write().await.insert(
        run_id.clone(),
        crate::TestRun {
            id: run_id.clone(),
            drasi_lib_instances: HashMap::from([("instance".into(), instance)]),
            queries: HashMap::new(),
            reactions: HashMap::new(),
            sources: HashMap::new(),
            status: crate::TestRunStatus::Initialized,
            lifecycle_tx: crate::test_run_completion::LifecycleTx::disabled(),
            completion_monitoring_task: None,
        },
    );
    let (core, input) = {
        let runs = host.test_runs.read().await;
        let instance = &runs[&run_id].drasi_lib_instances["instance"];
        instance.start().await?;
        (
            instance
                .get_drasi_lib()
                .await
                .context("native instance owner")?,
            instance.get_source_handle("input").await?,
        )
    };
    let id = TestRunQueryId::new(&run_id, "observer");
    let handler = if batch {
        DrasiLibInstanceHandler::create_callback(
            id,
            serde_json::from_value(serde_json::json!({
                "drasi_lib_instance_id":"instance", "reaction_id":"sink"
            }))?,
        )
        .await?
    } else {
        DrasiLibInstanceHandler::create_channel(
            id,
            serde_json::from_value(serde_json::json!({
                "drasi_lib_instance_id":"instance", "reaction_id":"sink",
                "buffer_size":1, "include_profiling":include_profiling
            }))?,
        )
        .await?
    };
    handler.set_test_run_host(host.clone()).await;
    let mut output = handler.init().await?;
    let result = async {
        assert!(matches!(
            output.recv().await,
            Some(ReactionHandlerMessage::Control(
                ReactionControlSignal::Start
            ))
        ));
        let mut query = core.subscribe_query_results("names").await?.stream;
        handler.pause().await?;
        let mut expected = Vec::new();
        for value in 0..3 {
            input
                .send_node_insert(
                    value.to_string(),
                    vec!["Item"],
                    PropertyMapBuilder::new()
                        .with_integer("value", value)
                        .build(),
                )
                .await?;
            expected.push(
                tokio::time::timeout(Duration::from_secs(5), query.next())
                    .await?
                    .context("query reader ended")??,
            );
        }
        assert!(
            output.try_recv().is_err(),
            "paused handler emitted a result"
        );
        handler.start().await?;
        for result in expected {
            let message = tokio::time::timeout(Duration::from_secs(5), output.recv())
                .await?
                .context("handler closed before delivery")?;
            let ReactionHandlerMessage::Invocation(invocation) = message else {
                anyhow::bail!("expected an invocation: {message:?}");
            };
            if batch {
                assert!(matches!(invocation.handler_type, ReactionHandlerType::Grpc));
                assert_eq!(
                    invocation.payload.value,
                    serde_json::json!({
                        "query_id": result.query_id, "results": result.results,
                    })
                );
                assert!(invocation.payload.invocation_id.is_none());
                assert!(invocation.payload.metadata.is_none());
            } else {
                assert!(matches!(invocation.handler_type, ReactionHandlerType::Http));
                assert_eq!(result.results.len(), 1);
                assert_eq!(
                    invocation.payload.value,
                    serde_json::json!({
                        "query_id": result.query_id, "result": result.results[0],
                    })
                );
                assert!(invocation.payload.invocation_id.is_some());
                let metadata = invocation.payload.metadata.context("channel metadata")?;
                assert_eq!(metadata["reaction_id"], "sink");
                if include_profiling {
                    assert_eq!(metadata["profiling"]["query_id"], "names");
                    assert_eq!(metadata["profiling"]["query_sequence"], result.sequence);
                    assert_eq!(
                        metadata["profiling"]["timestamps"],
                        serde_json::to_value(&result.profiling)?
                    );
                    assert!(
                        metadata["profiling"]["handler_received_ns"]
                            .as_u64()
                            .unwrap()
                            > 0
                    );
                } else {
                    assert!(metadata.get("profiling").is_none());
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let stopped = handler.stop().await;
    let cleanup = {
        let runs = host.test_runs.read().await;
        runs[&run_id].drasi_lib_instances["instance"].stop().await
    };
    result?;
    stopped?;
    cleanup
}

#[tokio::test]
async fn real_native_input_preserves_channel_batch_and_profiling_projections() -> Result<()> {
    for (batch, profiling) in [(false, false), (false, true), (true, false)] {
        tokio::time::timeout(
            Duration::from_secs(20),
            native_handler_projection(batch, profiling),
        )
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn unavailable_instance_reports_an_error_and_joins_the_worker() -> Result<()> {
    let handler = DrasiLibInstanceHandler::create_channel(
        TestRunQueryId::new(&TestRunId::new("repo", "test", "run"), "query"),
        serde_json::from_value(serde_json::json!({
            "drasi_lib_instance_id":"missing", "reaction_id":"sink"
        }))?,
    )
    .await?;
    let host = Arc::new(
        crate::TestRunHost::new(
            crate::TestRunHostConfig::default(),
            Arc::new(test_data_store::TestDataStore::new_temp(None).await?),
        )
        .await?,
    );
    handler.set_test_run_host(host).await;
    let mut output = handler.init().await?;
    assert!(matches!(
        output.recv().await,
        Some(ReactionHandlerMessage::Control(
            ReactionControlSignal::Start
        ))
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), output.recv()).await?,
        Some(ReactionHandlerMessage::Error(_)),
    ));
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), output.recv()).await?,
        Some(ReactionHandlerMessage::Control(ReactionControlSignal::Stop)),
    ));
    assert!(
        handler.stop().await.is_err(),
        "cleanup must retain the worker's original failure"
    );
    assert!(output.recv().await.is_none());
    Ok(())
}
