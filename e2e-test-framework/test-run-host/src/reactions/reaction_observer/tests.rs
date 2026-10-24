// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use super::*;
use anyhow::Context;
use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex as StdMutex,
};
use test_data_store::{
    test_repo_storage::models::HttpReactionHandlerDefinition, test_run_storage::TestRunId,
    TestDataStore,
};
use tokio::sync::{mpsc, Notify, Semaphore};

struct Handler(Mutex<Option<mpsc::Receiver<ReactionHandlerMessage>>>);

#[async_trait]
impl ReactionOutputHandler for Handler {
    async fn init(&self) -> anyhow::Result<mpsc::Receiver<ReactionHandlerMessage>> {
        self.0
            .lock()
            .await
            .take()
            .context("fixture receiver already initialized")
    }
    async fn start(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn pause(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn stop(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn status(&self) -> ReactionHandlerStatus {
        ReactionHandlerStatus::Running
    }
}

struct Logger {
    entered: Arc<Notify>,
    release_record: Arc<Semaphore>,
    release_finish: Arc<Semaphore>,
    records: Arc<StdMutex<Vec<u64>>>,
    finished: Arc<AtomicUsize>,
    fail_record: bool,
}

#[async_trait]
impl OutputLogger for Logger {
    async fn log_handler_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()> {
        self.entered.notify_one();
        self.release_record.acquire().await?.forget();
        anyhow::ensure!(!self.fail_record, "intentional recording failure");
        self.records.lock().unwrap().push(record.sequence);
        Ok(())
    }

    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult> {
        self.release_finish.acquire().await?.forget();
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(OutputLoggerResult {
            has_output: true,
            logger_name: "fixture".into(),
            output_folder_path: None,
            summary: None,
        })
    }
}

async fn observer(
    logger: Logger,
) -> anyhow::Result<(
    ReactionObserver,
    mpsc::Sender<ReactionHandlerMessage>,
    TestDataStore,
)> {
    let store = TestDataStore::new_temp(None).await?;
    let id = TestRunReactionId::new(&TestRunId::new("repo", "test", "run"), "receiver");
    let settings = ReactionObserverSettings::new(
        id.clone(),
        ReactionHandlerDefinition::Http(HttpReactionHandlerDefinition {
            host: Some("127.0.0.1".into()),
            port: Some(0),
            path: Some("/unused".into()),
            correlation_header: None,
        }),
        store.get_test_run_reaction_storage(&id).await?,
        vec![],
        vec![],
        None,
    )
    .await?;
    let (tx, rx) = mpsc::channel(8);
    let observer = ReactionObserver {
        settings: Arc::new(settings),
        internal_state: Arc::new(Mutex::new(ReactionObserverInternalState::new(
            crate::test_run_completion::LifecycleTx::disabled(),
            id,
        ))),
        output_handler: Arc::new(Box::new(Handler(Mutex::new(Some(rx))))),
        observer_task_handle: Arc::new(Mutex::new(None)),
        observer_command_tx: Arc::new(Mutex::new(None)),
        lifecycle: Mutex::new(()),
    };
    observer.start().await?;
    observer
        .internal_state
        .lock()
        .await
        .loggers
        .push(Box::new(logger));
    Ok((observer, tx, store))
}

fn invocation() -> ReactionHandlerMessage {
    ReactionHandlerMessage::Invocation(ReactionInvocation {
        handler_type: ReactionHandlerType::Grpc,
        payload: crate::reactions::reaction_output_handler::ReactionHandlerPayload {
            value: serde_json::json!({"query_id":"items","result":{"value":1}}),
            timestamp: chrono::Utc::now(),
            invocation_id: None,
            metadata: None,
        },
    })
}

async fn queued_records_are_drained_before_stop_and_batch_confirmation() -> anyhow::Result<()> {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let records = Arc::new(StdMutex::new(Vec::new()));
    let finished = Arc::new(AtomicUsize::new(0));
    let (observer, tx, _store) = observer(Logger {
        entered: entered.clone(),
        release_record: release.clone(),
        release_finish: Arc::new(Semaphore::new(1)),
        records: records.clone(),
        finished: finished.clone(),
        fail_record: false,
    })
    .await?;
    tx.send(invocation()).await?;
    entered.notified().await;
    tx.send(invocation()).await?;
    tx.send(invocation()).await?;
    tx.send(ReactionHandlerMessage::BatchComplete {
        query_id: "items".into(),
        sequence: 55,
        item_count: 3,
    })
    .await?;
    let stopping = observer.stop();
    tokio::pin!(stopping);
    assert!(futures::poll!(&mut stopping).is_pending());
    assert!(records.lock().unwrap().is_empty());
    release.add_permits(3);
    let result = tokio::time::timeout(Duration::from_secs(2), stopping).await??;
    assert_eq!(result.state.status, ReactionObserverStatus::Stopped);
    assert_eq!(*records.lock().unwrap(), vec![1, 2, 3]);
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    let batch = &result.state.result_summary.recorded_batches["items"];
    assert_eq!(batch.sequence, 55);
    assert_eq!(batch.item_count, 3);
    assert!(batch.recorded_at_ns > 0);
    assert!(observer.observer_task_handle.lock().await.is_none());
    Ok(())
}

async fn failed_logging_cannot_confirm_a_batch() -> anyhow::Result<()> {
    let (observer, tx, _store) = observer(Logger {
        entered: Arc::new(Notify::new()),
        release_record: Arc::new(Semaphore::new(1)),
        release_finish: Arc::new(Semaphore::new(1)),
        records: Arc::new(StdMutex::new(Vec::new())),
        finished: Arc::new(AtomicUsize::new(0)),
        fail_record: true,
    })
    .await?;
    tx.send(invocation()).await?;
    tx.send(ReactionHandlerMessage::BatchComplete {
        query_id: "items".into(),
        sequence: 55,
        item_count: 1,
    })
    .await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), observer.stop())
            .await?
            .is_err()
    );
    let state = observer.get_state().await?.state;
    assert_eq!(state.status, ReactionObserverStatus::Error);
    assert!(state.result_summary.recorded_batches.is_empty());
    assert!(observer.observer_task_handle.lock().await.is_none());
    assert!(observer.start().await.is_err());
    Ok(())
}

async fn cancelled_finalization_retains_the_logger_and_rejects_restart() -> anyhow::Result<()> {
    let release = Arc::new(Semaphore::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let (observer, _, _store) = observer(Logger {
        entered: Arc::new(Notify::new()),
        release_record: Arc::new(Semaphore::new(0)),
        release_finish: release.clone(),
        records: Arc::new(StdMutex::new(Vec::new())),
        finished: finished.clone(),
        fail_record: false,
    })
    .await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), observer.stop())
            .await
            .is_err()
    );
    assert!(observer.start().await.is_err());
    assert_eq!(observer.internal_state.lock().await.loggers.len(), 1);
    release.add_permits(1);
    let state = tokio::time::timeout(Duration::from_secs(2), observer.stop())
        .await??
        .state;
    assert_eq!(state.logger_results.len(), 1);
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    Ok(())
}

async fn contracts() -> anyhow::Result<()> {
    queued_records_are_drained_before_stop_and_batch_confirmation().await?;
    failed_logging_cannot_confirm_a_batch().await?;
    cancelled_finalization_retains_the_logger_and_rejects_restart().await
}

#[tokio::test(flavor = "current_thread")]
async fn receiver_completion_on_current_thread() -> anyhow::Result<()> {
    contracts().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_completion_on_multiple_threads() -> anyhow::Result<()> {
    contracts().await
}
