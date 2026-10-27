use super::*;
use drasi_lib::computation::v1::{
    ComputationComponent, EnvelopeSource, GraphChangeCodec, QueryChangeCodec, StreamId,
};
use drasi_source_application::NativeApplicationSource;
use serde_json::{json, Value};
use std::{collections::HashMap, num::NonZeroUsize};
use test_data_store::test_run_storage::TestRunId;

fn event(value: usize) -> SourceChangeEvent {
    serde_json::from_value(json!({
        "op":"i", "reactivatorStart_ns":1, "reactivatorEnd_ns":2,
        "payload":{
            "source":{"db":"test", "table":"node", "ts_ns":12345678900u64, "lsn":value},
            "before":null,
            "after":{"id":value.to_string(),"labels":["Item"],"properties":{"value":value}}
        }
    }))
    .unwrap()
}

fn dispatcher() -> DrasiLibInstanceChannelSourceChangeDispatcher {
    DrasiLibInstanceChannelSourceChangeDispatcher {
        settings: DrasiLibInstanceChannelSourceChangeDispatcherSettings {
            drasi_lib_instance_id: TestRunDrasiLibInstanceId::new(
                &TestRunId::new("test_repo", "test", "test_run_001"),
                "instance1",
            ),
            source_id: "input".into(),
            buffer_size: 1,
        },
        test_run_host: None,
        sender: None,
        queued_events: Vec::new(),
        receiver_task: RwLock::new(None),
        closing: false,
        failure: None,
    }
}

fn input() -> Result<(NativeApplicationSource, ApplicationSourceHandle)> {
    NativeApplicationSource::channel(
        "test",
        "graph",
        "input",
        StreamId::try_new("input/out")?,
        NonZeroUsize::new(1).context("test capacity")?,
    )
}

#[test]
fn converts_complete_node_relation_update_and_delete_records() -> Result<()> {
    let mut node = event(1);
    node.payload.after["properties"]["nested"] = json!({"array":[1,true,null,"value"]});
    let SourceChange::Insert {
        element: Element::Node {
            metadata,
            properties,
        },
    } = source_change(&node, "input")?
    else {
        anyhow::bail!("expected a node");
    };
    assert_eq!(metadata.reference, ElementReference::new("input", "1"));
    assert_eq!(metadata.effective_from, 12345);
    assert_eq!(metadata.labels.as_ref(), &[Arc::<str>::from("Item")]);
    assert_eq!(
        properties.get("nested").map(Value::from),
        Some(json!({"array":[1,true,null,"value"]})),
    );
    let mut relation = event(2);
    relation.op = "u".into();
    relation.payload.source.table = "relation".into();
    relation.payload.after["start_id"] = "one".into();
    relation.payload.after["endId"] = "two".into();
    relation.payload.after["endSourceId"] = "other".into();
    let SourceChange::Update {
        element: Element::Relation {
            in_node, out_node, ..
        },
        removed_properties,
    } = source_change(&relation, "input")?
    else {
        anyhow::bail!("expected a relation update");
    };
    assert!(removed_properties.is_empty());
    assert_eq!(in_node, ElementReference::new("input", "one"));
    assert_eq!(out_node, ElementReference::new("other", "two"));
    relation.op = "d".into();
    relation.payload.before = relation.payload.after.clone();
    relation.payload.after = Value::Null;
    assert!(matches!(source_change(&relation, "input")?,
        SourceChange::Delete { metadata } if metadata.reference == ElementReference::new("input", "2")));
    relation.payload.after = relation.payload.before.clone();
    relation.payload.before = json!({});
    assert!(matches!(
        source_change(&relation, "input")?,
        SourceChange::Delete { .. }
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_batches_are_rejected_before_acceptance() -> Result<()> {
    let mut dispatcher = dispatcher();
    let valid = event(0);
    for (field, value) in [
        ("id", json!("")),
        ("labels", json!(["Item", 1])),
        ("properties", json!([])),
        ("start_id", json!("missing-end")),
    ] {
        let mut invalid = event(1);
        invalid.payload.after[field] = value;
        assert!(dispatcher
            .dispatch_source_change_events(vec![&valid, &invalid])
            .await
            .is_err());
        assert!(dispatcher.queued_events.is_empty());
    }
    let mut invalid = event(1);
    invalid.op = "unsupported".into();
    assert!(dispatcher
        .dispatch_source_change_events(vec![&invalid])
        .await
        .is_err());
    dispatcher.close().await?;
    Ok(())
}

async fn cancelled_close() -> Result<()> {
    let (mut source, handle) = input()?;
    let mut dispatcher = dispatcher();
    let sender = dispatcher.start_receiver(handle).await?;
    let expected = (0..3)
        .map(|value| source_change(&event(value), "input"))
        .collect::<Result<Vec<_>>>()?;
    sender.send(expected.clone()).await?;
    drop(sender);
    let mut close = Box::pin(dispatcher.close());
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut close)
        .await
        .is_err());
    drop(close);
    assert!(dispatcher.receiver_task.get_mut().is_some());
    assert!(dispatcher.closing);
    assert!(dispatcher
        .dispatch_source_change_events(vec![&event(4)])
        .await
        .is_err());
    source.start().await?;
    let (closed, delivered) = tokio::join!(dispatcher.close(), async {
        let mut actual = Vec::new();
        for _ in 0..3 {
            let output = tokio::time::timeout(Duration::from_secs(2), source.next())
                .await??
                .context("native input closed")?;
            actual.extend(GraphChangeCodec::decode_changes(&output.envelope)?);
        }
        Ok::<_, anyhow::Error>(actual)
    });
    closed?;
    assert_eq!(delivered?, expected);
    assert!(dispatcher.receiver_task.get_mut().is_none());
    source.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_close_retains_the_worker_and_drains_current_thread() -> Result<()> {
    cancelled_close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_close_retains_the_worker_and_drains_multi_thread() -> Result<()> {
    cancelled_close().await
}

#[tokio::test]
async fn cancelled_queued_flush_preserves_accepted_records() -> Result<()> {
    let (mut source, handle) = input()?;
    let mut dispatcher = dispatcher();
    let sender = dispatcher.start_receiver(handle).await?;
    let expected = (0..4)
        .map(|value| source_change(&event(value), "input"))
        .collect::<Result<Vec<_>>>()?;
    sender.send(expected[..2].to_vec()).await?;
    sender.send(vec![expected[2].clone()]).await?;
    drop(sender);
    dispatcher.queued_events.push(expected[3].clone());
    let mut flush = Box::pin(dispatcher.flush_queued());
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut flush)
        .await
        .is_err());
    drop(flush);
    assert_eq!(dispatcher.queued_events, [expected[3].clone()]);
    source.start().await?;
    let (closed, delivered) = tokio::join!(dispatcher.close(), async {
        let mut actual = Vec::new();
        for _ in 0..4 {
            let output = tokio::time::timeout(Duration::from_secs(2), source.next())
                .await??
                .context("native input closed")?;
            actual.extend(GraphChangeCodec::decode_changes(&output.envelope)?);
        }
        Ok::<_, anyhow::Error>(actual)
    });
    closed?;
    assert_eq!(delivered?, expected);
    assert!(dispatcher.queued_events.is_empty());
    source.stop().await?;
    Ok(())
}

#[tokio::test]
async fn worker_failure_is_reported_and_remains_latched() -> Result<()> {
    let (source, handle) = input()?;
    drop(source);
    let mut dispatcher = dispatcher();
    let sender = dispatcher.start_receiver(handle).await?;
    sender
        .send(vec![source_change(&event(0), "input")?])
        .await?;
    drop(sender);
    let first = dispatcher
        .close()
        .await
        .expect_err("source closure must fail");
    assert!(first.to_string().contains("application input"), "{first}");
    assert_eq!(
        dispatcher.close().await.unwrap_err().to_string(),
        first.to_string()
    );
    assert_eq!(
        dispatcher
            .dispatch_source_change_events(Vec::new())
            .await
            .unwrap_err()
            .to_string(),
        first.to_string()
    );
    assert!(dispatcher.receiver_task.get_mut().is_none());
    Ok(())
}

#[tokio::test]
async fn forwarding_does_not_hold_the_test_run_registry_lock() -> Result<()> {
    let (instance, _directory) = crate::drasi_lib_instances::tests::native_instance(json!({
        "sources":[{"id":"input","kind":"application"}],
        "queries":[{"id":"values","query":"MATCH (n:Item) RETURN n.value AS value","inputs":["input"]}],
        "reactions":[{"id":"sink","kind":"application","queries":["values"]}]
    }))?;
    let run_id = instance.id.test_run_id.clone();
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
            drasi_lib_instances: HashMap::from([("instance1".into(), instance)]),
            queries: HashMap::new(),
            reactions: HashMap::new(),
            sources: HashMap::new(),
            status: crate::TestRunStatus::Initialized,
            lifecycle_tx: crate::test_run_completion::LifecycleTx::disabled(),
            completion_monitoring_task: None,
        },
    );
    let mut receiver = {
        let runs = host.test_runs.read().await;
        let instance = &runs[&run_id].drasi_lib_instances["instance1"];
        instance.start().await?;
        instance.take_reaction_receiver("sink").await?
    };
    let mut dispatcher = dispatcher();
    dispatcher
        .dispatch_source_change_events(vec![&event(1)])
        .await?;
    assert_eq!(dispatcher.queued_events.len(), 1);
    dispatcher.set_test_run_host(host.clone());
    dispatcher.dispatch_source_change_events(Vec::new()).await?;
    let delivered = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await?
        .context("native reaction closed")?;
    let result = QueryChangeCodec::to_legacy_result(&delivered.envelope)?;
    assert!(matches!(&result.results[..],
        [drasi_lib::channels::ResultDiff::Add { data, .. }] if data == &json!({"value":1})));
    let lock = tokio::time::timeout(Duration::from_secs(1), host.test_runs.write()).await?;
    dispatcher.close().await?;
    drop(lock);
    let runs = host.test_runs.read().await;
    runs[&run_id].drasi_lib_instances["instance1"]
        .stop()
        .await?;
    Ok(())
}
