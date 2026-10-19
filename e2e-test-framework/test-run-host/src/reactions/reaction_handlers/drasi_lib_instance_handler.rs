// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use drasi_lib::{
    channels::QueryResult,
    computation::v1::{InputEnvelope, QueryChangeCodec},
    context::workers::{join_owned_worker, spawn_owned_worker, WorkerCompletion},
};
use test_data_store::{
    test_repo_storage::models::{
        DrasiLibInstanceCallbackReactionHandlerDefinition,
        DrasiLibInstanceChannelReactionHandlerDefinition,
    },
    test_run_storage::{TestRunDrasiLibInstanceId, TestRunQueryId},
};
use tokio::{
    sync::{mpsc, watch, Mutex, RwLock},
    task::JoinHandle,
};

use crate::reactions::reaction_output_handler::{
    ReactionControlSignal, ReactionHandlerError, ReactionHandlerMessage, ReactionHandlerPayload,
    ReactionHandlerStatus, ReactionHandlerType, ReactionInvocation, ReactionOutputHandler,
};

#[derive(Clone)]
enum Projection {
    Changes { include_profiling: bool },
    Batch { callback_type: String },
}

#[derive(Clone)]
struct Settings {
    instance: TestRunDrasiLibInstanceId,
    reaction: String,
    capacity: usize,
    projection: Projection,
}

pub struct DrasiLibInstanceHandler {
    settings: Settings,
    status: watch::Sender<ReactionHandlerStatus>,
    host: watch::Sender<Option<Arc<crate::TestRunHost>>>,
    worker: RwLock<Option<JoinHandle<Result<()>>>>,
    lifecycle: Mutex<()>,
}

fn result_profiling(result: &QueryResult, enabled: bool) -> Result<Option<serde_json::Value>> {
    if !enabled {
        return Ok(None);
    }
    Ok(Some(serde_json::json!({
        "query_id": result.query_id,
        "query_sequence": result.sequence,
        "timestamps": result.profiling,
        "handler_received_ns": u64::try_from(
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
        )?,
    })))
}

async fn running(status: &mut watch::Receiver<ReactionHandlerStatus>) -> Result<bool> {
    loop {
        let current = *status.borrow_and_update();
        match current {
            ReactionHandlerStatus::Running => return Ok(true),
            ReactionHandlerStatus::Stopped | ReactionHandlerStatus::Error => return Ok(false),
            ReactionHandlerStatus::Uninitialized | ReactionHandlerStatus::Paused => {
                status.changed().await.context("handler owner closed")?;
            }
        }
    }
}

async fn send(
    sender: &mpsc::Sender<ReactionHandlerMessage>,
    status: &mut watch::Receiver<ReactionHandlerStatus>,
    message: ReactionHandlerMessage,
) -> Result<bool> {
    loop {
        if !running(status).await? {
            return Ok(false);
        }
        tokio::select! {
            biased;
            changed = status.changed() => {
                changed.context("handler owner closed")?;
            }
            permit = sender.reserve() => {
                let Ok(permit) = permit else {
                    log::debug!("Application reaction consumer closed");
                    return Ok(false);
                };
                if *status.borrow() == ReactionHandlerStatus::Running {
                    permit.send(message);
                    return Ok(true);
                }
            }
        }
    }
}

impl DrasiLibInstanceHandler {
    fn create(settings: Settings) -> Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
        anyhow::ensure!(
            settings.capacity > 0 && settings.capacity <= tokio::sync::Semaphore::MAX_PERMITS,
            "application handler capacity is outside the supported range"
        );
        Ok(Box::new(Self {
            settings,
            status: watch::channel(ReactionHandlerStatus::Uninitialized).0,
            host: watch::channel(None).0,
            worker: RwLock::new(None),
            lifecycle: Mutex::new(()),
        }))
    }

    pub async fn create_channel(
        id: TestRunQueryId,
        definition: DrasiLibInstanceChannelReactionHandlerDefinition,
    ) -> Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
        Self::create(Settings {
            instance: TestRunDrasiLibInstanceId::new(
                &id.test_run_id,
                &definition.drasi_lib_instance_id,
            ),
            reaction: definition.reaction_id,
            capacity: definition.buffer_size.unwrap_or(1024),
            projection: Projection::Changes {
                include_profiling: definition.include_profiling,
            },
        })
    }

    pub async fn create_callback(
        id: TestRunQueryId,
        definition: DrasiLibInstanceCallbackReactionHandlerDefinition,
    ) -> Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
        Self::create(Settings {
            instance: TestRunDrasiLibInstanceId::new(
                &id.test_run_id,
                &definition.drasi_lib_instance_id,
            ),
            reaction: definition.reaction_id,
            capacity: 1024,
            projection: Projection::Batch {
                callback_type: definition.callback_type.unwrap_or_else(|| "default".into()),
            },
        })
    }

    async fn receiver(
        settings: &Settings,
        host: &mut watch::Receiver<Option<Arc<crate::TestRunHost>>>,
        status: &mut watch::Receiver<ReactionHandlerStatus>,
    ) -> Result<Option<mpsc::Receiver<InputEnvelope>>> {
        loop {
            if !running(status).await? {
                return Ok(None);
            }
            let owner = host.borrow_and_update().clone();
            if let Some(owner) = owner {
                let bind = async {
                    let runs = owner.test_runs.read().await;
                    let run = runs
                        .get(&settings.instance.test_run_id)
                        .context("application handler test run is unavailable")?;
                    let instance = run
                        .drasi_lib_instances
                        .get(&settings.instance.test_drasi_lib_instance_id)
                        .context("application handler instance is unavailable")?;
                    instance.take_reaction_receiver(&settings.reaction).await
                };
                tokio::select! {
                    result = bind => return result.map(Some),
                    changed = status.changed() => {
                        changed.context("handler owner closed")?;
                    }
                }
            } else {
                tokio::select! {
                    changed = host.changed() => {
                        changed.context("test host binding closed")?;
                    }
                    changed = status.changed() => {
                        changed.context("handler owner closed")?;
                    }
                }
            }
        }
    }

    async fn forward(
        settings: &Settings,
        mut receiver: mpsc::Receiver<InputEnvelope>,
        sender: &mpsc::Sender<ReactionHandlerMessage>,
        status: &mut watch::Receiver<ReactionHandlerStatus>,
    ) -> Result<()> {
        loop {
            if !running(status).await? {
                return Ok(());
            }
            let input = tokio::select! {
                biased;
                changed = status.changed() => {
                    changed.context("handler owner closed")?;
                    continue;
                }
                input = receiver.recv() => match input {
                    Some(input) => input,
                    None => return Ok(()),
                }
            };
            let result = QueryChangeCodec::to_legacy_result(&input.envelope)?;
            match &settings.projection {
                Projection::Batch { .. } => {
                    let message = ReactionHandlerMessage::Invocation(ReactionInvocation {
                        handler_type: ReactionHandlerType::Grpc,
                        payload: ReactionHandlerPayload {
                            value: serde_json::json!({
                                "query_id": result.query_id, "results": result.results,
                            }),
                            timestamp: chrono::Utc::now(),
                            invocation_id: None,
                            metadata: None,
                        },
                    });
                    if !send(sender, status, message).await? {
                        return Ok(());
                    }
                }
                Projection::Changes { include_profiling } => {
                    let mut metadata = serde_json::json!({
                        "drasi_lib_instance_id": settings.instance.to_string(),
                        "reaction_id": settings.reaction,
                    });
                    if let Some(profiling) = result_profiling(&result, *include_profiling)? {
                        metadata["profiling"] = profiling;
                    }
                    let values = if result.results.is_empty() {
                        vec![serde_json::json!({"query_id": result.query_id, "results": []})]
                    } else {
                        result
                            .results
                            .iter()
                            .map(|change| {
                                serde_json::json!({
                                    "query_id": result.query_id, "result": change,
                                })
                            })
                            .collect()
                    };
                    for value in values {
                        let message = ReactionHandlerMessage::Invocation(ReactionInvocation {
                            handler_type: ReactionHandlerType::Http,
                            payload: ReactionHandlerPayload {
                                value,
                                timestamp: chrono::Utc::now(),
                                invocation_id: Some(uuid::Uuid::new_v4().to_string()),
                                metadata: Some(metadata.clone()),
                            },
                        });
                        if !send(sender, status, message).await? {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl ReactionOutputHandler for DrasiLibInstanceHandler {
    async fn init(&self) -> Result<mpsc::Receiver<ReactionHandlerMessage>> {
        let _lifecycle = self.lifecycle.lock().await;
        anyhow::ensure!(
            *self.status.borrow() == ReactionHandlerStatus::Uninitialized,
            "application handler has already been initialized"
        );
        let (sender, receiver) = mpsc::channel(self.settings.capacity);
        sender
            .send(ReactionHandlerMessage::Control(
                ReactionControlSignal::Start,
            ))
            .await?;
        self.status.send_replace(ReactionHandlerStatus::Running);
        let status = self.status.clone();
        let mut observed = status.subscribe();
        let mut host = self.host.subscribe();
        let settings = self.settings.clone();
        let registered = spawn_owned_worker(&self.worker, async move {
            let result = async {
                if let Some(receiver) = Self::receiver(&settings, &mut host, &mut observed).await? {
                    Self::forward(&settings, receiver, &sender, &mut observed).await?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(error) = &result {
                log::error!(
                    "Application reaction '{}' failed: {error:#}",
                    settings.reaction
                );
                send(
                    &sender,
                    &mut observed,
                    ReactionHandlerMessage::Error(ReactionHandlerError::new(
                        format!("{error:#}"),
                        false,
                    )),
                )
                .await?;
            }
            send(
                &sender,
                &mut observed,
                ReactionHandlerMessage::Control(ReactionControlSignal::Stop),
            )
            .await?;
            if *status.borrow() != ReactionHandlerStatus::Stopped {
                status.send_replace(if result.is_ok() {
                    ReactionHandlerStatus::Stopped
                } else {
                    ReactionHandlerStatus::Error
                });
            }
            result
        })
        .await;
        if let Err(error) = registered {
            self.status.send_replace(ReactionHandlerStatus::Error);
            return Err(error.into());
        }
        Ok(receiver)
    }

    async fn start(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let status = *self.status.borrow();
        anyhow::ensure!(
            matches!(
                status,
                ReactionHandlerStatus::Paused | ReactionHandlerStatus::Running
            ),
            "cannot resume application handler from {status:?}"
        );
        self.status.send_replace(ReactionHandlerStatus::Running);
        Ok(())
    }

    async fn pause(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let status = *self.status.borrow();
        anyhow::ensure!(
            matches!(
                status,
                ReactionHandlerStatus::Running | ReactionHandlerStatus::Paused
            ),
            "cannot pause application handler from {status:?}"
        );
        self.status.send_replace(ReactionHandlerStatus::Paused);
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        self.status.send_replace(ReactionHandlerStatus::Stopped);
        let completion =
            join_owned_worker(&mut *self.worker.write().await, Duration::from_secs(5)).await?;
        self.host.send_replace(None);
        if let WorkerCompletion::Completed(result) = completion {
            result?;
        }
        Ok(())
    }

    async fn status(&self) -> ReactionHandlerStatus {
        *self.status.borrow()
    }

    async fn set_test_run_host(&self, host: Arc<crate::TestRunHost>) {
        self.host.send_replace(Some(host));
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        let mut metrics = serde_json::json!({
            "drasi_lib_instance_id": self.settings.instance.to_string(),
            "reaction_id": self.settings.reaction,
        });
        match &self.settings.projection {
            Projection::Changes { .. } => {
                metrics["handler_type"] = "drasi_lib_instance_channel".into();
                metrics["buffer_size"] = self.settings.capacity.into();
            }
            Projection::Batch { callback_type } => {
                metrics["handler_type"] = "drasi_lib_instance_callback".into();
                metrics["callback_type"] = callback_type.clone().into();
            }
        }
        Some(metrics)
    }
}

impl Drop for DrasiLibInstanceHandler {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().as_ref() {
            log::warn!("Application handler dropped before awaited worker cleanup");
            worker.abort();
        }
    }
}

#[cfg(test)]
mod tests;
