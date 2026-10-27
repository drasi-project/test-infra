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

use anyhow::{Context, Result};
use async_trait::async_trait;
use drasi_core::models::{
    Element, ElementMetadata, ElementPropertyMap, ElementReference, SourceChange,
};
use drasi_lib::context::workers::{
    join_owned_worker_gracefully, spawn_owned_worker, WorkerCleanupError, WorkerCompletion,
};
use std::{
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use drasi_source_application::ApplicationSourceHandle;

use test_data_store::{
    scripts::SourceChangeEvent,
    test_repo_storage::models::DrasiLibInstanceChannelSourceChangeDispatcherDefinition,
    test_run_storage::{TestRunDrasiLibInstanceId, TestRunSourceStorage},
};

use super::SourceChangeDispatcher;

#[derive(Debug)]
pub struct DrasiLibInstanceChannelSourceChangeDispatcherSettings {
    pub drasi_lib_instance_id: TestRunDrasiLibInstanceId,
    pub source_id: String,
    pub buffer_size: usize,
}

impl DrasiLibInstanceChannelSourceChangeDispatcherSettings {
    pub fn new(
        definition: &DrasiLibInstanceChannelSourceChangeDispatcherDefinition,
        test_run_source_storage: &TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        // Parse the drasi_lib_instance_id from the definition
        let drasi_lib_instance_id = TestRunDrasiLibInstanceId::new(
            &test_run_source_storage.id.test_run_id,
            &definition.drasi_lib_instance_id,
        );
        let buffer_size = definition.buffer_size.unwrap_or(1024);
        anyhow::ensure!(
            buffer_size > 0 && buffer_size <= tokio::sync::Semaphore::MAX_PERMITS,
            "application input dispatcher capacity is outside the supported range"
        );
        drasi_lib::computation::v1::ComponentId::try_new(definition.source_id.as_str())?;

        Ok(Self {
            drasi_lib_instance_id,
            source_id: definition.source_id.clone(),
            buffer_size,
        })
    }
}

#[derive(Clone, Debug)]
struct DispatchFailure(Arc<anyhow::Error>);

impl fmt::Display for DispatchFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:#}", self.0)
    }
}

impl std::error::Error for DispatchFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

pub struct DrasiLibInstanceChannelSourceChangeDispatcher {
    settings: DrasiLibInstanceChannelSourceChangeDispatcherSettings,
    test_run_host: Option<Weak<crate::TestRunHost>>,
    sender: Option<mpsc::Sender<Vec<SourceChange>>>,
    queued_events: Vec<SourceChange>,
    receiver_task: RwLock<Option<JoinHandle<Result<()>>>>,
    closing: bool,
    failure: Option<DispatchFailure>,
}

impl DrasiLibInstanceChannelSourceChangeDispatcher {
    pub fn new(
        definition: &DrasiLibInstanceChannelSourceChangeDispatcherDefinition,
        storage: &TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        log::info!(
            "Creating DrasiLibInstanceChannelSourceChangeDispatcher for source '{}' on drasi-lib instance '{}'",
            definition.source_id,
            definition.drasi_lib_instance_id
        );

        let settings =
            DrasiLibInstanceChannelSourceChangeDispatcherSettings::new(definition, storage)?;
        log::debug!(
            "DrasiLibInstanceChannelSourceChangeDispatcher created with settings: drasi_lib_instance_id={:?}, source_id={}, buffer_size={}",
            settings.drasi_lib_instance_id,
            settings.source_id,
            settings.buffer_size
        );

        Ok(Self {
            settings,
            test_run_host: None,
            sender: None,
            queued_events: Vec::new(),
            receiver_task: RwLock::new(None),
            closing: false,
            failure: None,
        })
    }

    async fn get_or_create_channel(&mut self) -> Result<mpsc::Sender<Vec<SourceChange>>> {
        anyhow::ensure!(
            !self.closing,
            "application dispatcher close must finish before reuse"
        );
        if let Some(failure) = &self.failure {
            return Err(failure.clone().into());
        }
        if let Some(sender) = &self.sender {
            if !sender.is_closed() {
                return Ok(sender.clone());
            }
        }
        self.finish_receiver().await?;
        let test_run_host = self
            .test_run_host
            .as_ref()
            .context("application dispatcher TestRunHost is not set")?
            .upgrade()
            .context("application dispatcher TestRunHost has closed")?;
        let source_handle = {
            let test_runs = test_run_host.test_runs.read().await;
            let run = test_runs
                .get(&self.settings.drasi_lib_instance_id.test_run_id)
                .context("application dispatcher test run is unavailable")?;
            let instance = run
                .drasi_lib_instances
                .get(
                    &self
                        .settings
                        .drasi_lib_instance_id
                        .test_drasi_lib_instance_id,
                )
                .context("application dispatcher instance is unavailable")?;
            instance.get_source_handle(&self.settings.source_id).await?
        };
        self.start_receiver(source_handle).await
    }

    async fn start_receiver(
        &mut self,
        source: ApplicationSourceHandle,
    ) -> Result<mpsc::Sender<Vec<SourceChange>>> {
        let (sender, mut receiver) = mpsc::channel::<Vec<SourceChange>>(self.settings.buffer_size);
        let id = self.settings.source_id.clone();
        spawn_owned_worker(&self.receiver_task, async move {
            while let Some(changes) = receiver.recv().await {
                for change in changes {
                    source
                        .send(change)
                        .await
                        .with_context(|| format!("application input '{id}' failed"))?;
                }
            }
            Ok(())
        })
        .await?;
        self.sender = Some(sender.clone());
        Ok(sender)
    }

    async fn finish_receiver(&mut self) -> Result<()> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone().into());
        }
        let result = match join_owned_worker_gracefully(
            self.receiver_task.get_mut(),
            Duration::from_secs(5),
        )
        .await
        {
            Ok(WorkerCompletion::Absent | WorkerCompletion::Completed(Ok(()))) => Ok(()),
            Ok(WorkerCompletion::Completed(Err(error))) => Err(error),
            Ok(WorkerCompletion::Cancelled) => Err(anyhow::anyhow!(
                "application input worker was interrupted before draining"
            )),
            Err(error @ WorkerCleanupError::TimedOut { .. }) => return Err(error.into()),
            Err(error) => Err(error.into()),
        };
        if let Err(error) = result {
            let failure = DispatchFailure(Arc::new(error));
            log::error!("Application input dispatcher failed: {failure}");
            self.failure = Some(failure.clone());
            return Err(failure.into());
        }
        Ok(())
    }

    async fn reserve(&mut self) -> Result<mpsc::OwnedPermit<Vec<SourceChange>>> {
        let sender = self.get_or_create_channel().await?;
        match sender.reserve_owned().await {
            Ok(permit) => Ok(permit),
            Err(_) => {
                self.finish_receiver().await?;
                anyhow::bail!("application input channel closed before accepting the batch")
            }
        }
    }

    async fn flush_queued(&mut self) -> Result<()> {
        if !self.queued_events.is_empty() {
            let permit = self.reserve().await?;
            permit.send(std::mem::take(&mut self.queued_events));
        }
        Ok(())
    }
}

#[async_trait]
impl SourceChangeDispatcher for DrasiLibInstanceChannelSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        self.flush_queued().await?;
        self.closing = true;
        self.sender = None;
        let result = self.finish_receiver().await;
        if self.receiver_task.get_mut().is_none() {
            self.closing = false;
        }
        result
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.closing,
            "application dispatcher close must finish before dispatch"
        );
        if let Some(failure) = &self.failure {
            return Err(failure.clone().into());
        }
        let changes = events
            .into_iter()
            .map(|event| source_change(event, &self.settings.source_id))
            .collect::<Result<Vec<_>>>()?;
        if self.test_run_host.is_none() {
            self.queued_events.extend(changes);
            return Ok(());
        }
        self.flush_queued().await?;
        if !changes.is_empty() {
            self.reserve().await?.send(changes);
        }
        Ok(())
    }

    fn set_test_run_host(&mut self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        self.test_run_host = Some(Arc::downgrade(&test_run_host));
    }
}

fn source_change(event: &SourceChangeEvent, source_id: &str) -> Result<SourceChange> {
    let delete = matches!(event.op.as_str(), "d" | "delete");
    anyhow::ensure!(
        delete || matches!(event.op.as_str(), "i" | "insert" | "u" | "update"),
        "unknown application input operation '{}'",
        event.op
    );
    let data = if delete
        && event
            .payload
            .before
            .as_object()
            .is_some_and(|data| !data.is_empty())
    {
        &event.payload.before
    } else {
        &event.payload.after
    };
    let data = data
        .as_object()
        .context("application input must contain an element object")?;
    let id = data
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .context("application input needs a nonempty string id")?;
    let labels: Vec<String> = match data.get("labels") {
        Some(labels) => serde_json::from_value(labels.clone()).context("labels must be strings")?,
        None => Vec::new(),
    };
    let metadata = ElementMetadata {
        reference: ElementReference::new(source_id, id),
        labels: labels.into_iter().map(Arc::from).collect::<Vec<_>>().into(),
        effective_from: event.payload.source.ts_ns / 1_000_000,
    };
    if delete {
        return Ok(SourceChange::Delete { metadata });
    }
    let properties = match data.get("properties") {
        Some(properties) => ElementPropertyMap::from(
            properties
                .as_object()
                .context("application properties must be an object")?,
        ),
        None => ElementPropertyMap::default(),
    };
    let start = data.get("start_id").or_else(|| data.get("startId"));
    let end = data.get("end_id").or_else(|| data.get("endId"));
    let element = if event.payload.source.table == "relation" || start.is_some() || end.is_some() {
        let reference = |value: Option<&serde_json::Value>, source: Option<&serde_json::Value>| {
            let id = value
                .and_then(serde_json::Value::as_str)
                .filter(|id| !id.is_empty())
                .context("relation endpoint must be a nonempty string")?;
            let source = match source {
                Some(source) => source
                    .as_str()
                    .filter(|source| !source.is_empty())
                    .context("relation source id must be a nonempty string")?,
                None => source_id,
            };
            Ok::<_, anyhow::Error>(ElementReference::new(source, id))
        };
        Element::Relation {
            metadata,
            properties,
            in_node: reference(
                start,
                data.get("start_source_id")
                    .or_else(|| data.get("startSourceId")),
            )?,
            out_node: reference(
                end,
                data.get("end_source_id")
                    .or_else(|| data.get("endSourceId")),
            )?,
        }
    } else {
        Element::Node {
            metadata,
            properties,
        }
    };
    Ok(if matches!(event.op.as_str(), "i" | "insert") {
        SourceChange::Insert { element }
    } else {
        SourceChange::update(element)
    })
}

impl Drop for DrasiLibInstanceChannelSourceChangeDispatcher {
    fn drop(&mut self) {
        if let Some(worker) = self.receiver_task.get_mut().as_ref() {
            log::warn!("Application input dispatcher dropped before awaited close");
            worker.abort();
        }
    }
}

#[cfg(test)]
mod tests;
