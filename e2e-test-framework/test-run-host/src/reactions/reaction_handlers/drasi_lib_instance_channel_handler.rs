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

use async_trait::async_trait;
use test_data_store::{
    test_repo_storage::models::DrasiLibInstanceChannelReactionHandlerDefinition,
    test_run_storage::{TestRunDrasiLibInstanceId, TestRunQueryId},
};
use tokio::sync::{
    mpsc::{channel, Receiver},
    Mutex, Notify, RwLock,
};

use crate::reactions::reaction_output_handler::{
    ReactionControlSignal, ReactionHandlerMessage, ReactionHandlerPayload, ReactionHandlerStatus,
    ReactionHandlerType, ReactionInvocation, ReactionOutputHandler,
};

#[derive(Clone, Debug)]
pub struct DrasiLibInstanceChannelHandlerSettings {
    pub drasi_lib_instance_id: TestRunDrasiLibInstanceId,
    pub reaction_id: String,
    pub buffer_size: usize,
    pub test_run_query_id: TestRunQueryId,
}

impl DrasiLibInstanceChannelHandlerSettings {
    pub fn new(
        id: TestRunQueryId,
        definition: DrasiLibInstanceChannelReactionHandlerDefinition,
    ) -> anyhow::Result<Self> {
        // Parse the drasi_lib_instance_id from the definition
        let drasi_lib_instance_id =
            TestRunDrasiLibInstanceId::new(&id.test_run_id, &definition.drasi_lib_instance_id);

        Ok(Self {
            drasi_lib_instance_id,
            reaction_id: definition.reaction_id.clone(),
            buffer_size: definition.buffer_size.unwrap_or(1024),
            test_run_query_id: id,
        })
    }
}

pub struct DrasiLibInstanceChannelHandler {
    settings: DrasiLibInstanceChannelHandlerSettings,
    status: Arc<RwLock<ReactionHandlerStatus>>,
    notifier: Arc<Notify>,
    shutdown_notify: Arc<Notify>,
    test_run_host: Arc<Mutex<Option<Arc<crate::TestRunHost>>>>,
}

impl DrasiLibInstanceChannelHandler {
    pub async fn create(
        id: TestRunQueryId,
        definition: DrasiLibInstanceChannelReactionHandlerDefinition,
    ) -> anyhow::Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
        let settings = DrasiLibInstanceChannelHandlerSettings::new(id, definition)?;
        log::trace!("Creating DrasiLibInstanceChannelHandler with settings {settings:?}");

        let status = Arc::new(RwLock::new(ReactionHandlerStatus::Uninitialized));
        let notifier = Arc::new(Notify::new());
        let shutdown_notify = Arc::new(Notify::new());

        Ok(Box::new(Self {
            settings,
            status,
            notifier,
            shutdown_notify,
            test_run_host: Arc::new(Mutex::new(None)),
        }))
    }

    pub async fn set_test_run_host(&self, test_run_host: Arc<crate::TestRunHost>) {
        let mut host_lock = self.test_run_host.lock().await;
        *host_lock = Some(test_run_host);
    }

    async fn create_channel_connection_static(
        test_run_host: &Arc<Mutex<Option<Arc<crate::TestRunHost>>>>,
        settings: &DrasiLibInstanceChannelHandlerSettings,
    ) -> anyhow::Result<Receiver<serde_json::Value>> {
        // Get the test run host
        let test_run_host_lock = test_run_host.lock().await;
        let test_run_host = test_run_host_lock
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("TestRunHost not set"))?
            .clone();
        drop(test_run_host_lock);

        // Create a channel for receiving reactions
        let (tx, rx) = channel(settings.buffer_size);

        log::info!(
            "Created channel connection for reaction '{}' on drasi-lib instance {}",
            settings.reaction_id,
            settings.drasi_lib_instance_id
        );

        // Start a task to monitor for the reaction handle
        let settings_clone = settings.clone();
        let test_run_host = test_run_host.clone();

        tokio::spawn(async move {
            log::debug!(
                "Channel handler starting for reaction {} on instance {}",
                settings_clone.reaction_id,
                settings_clone.drasi_lib_instance_id
            );

            // Get the drasi-lib instance and application handle
            let test_runs = test_run_host.test_runs.read().await;
            if let Some(test_run) = test_runs.get(&settings_clone.drasi_lib_instance_id.test_run_id)
            {
                if let Some(drasi_lib_instance) = test_run.drasi_lib_instances.get(
                    &settings_clone
                        .drasi_lib_instance_id
                        .test_drasi_lib_instance_id,
                ) {
                    match drasi_lib_instance
                        .get_reaction_handle(&settings_clone.reaction_id)
                        .await
                    {
                        Ok(reaction_handle) => {
                            log::info!(
                                "Successfully obtained ApplicationReactionHandle for reaction '{}' on drasi-lib instance {}",
                                settings_clone.reaction_id, settings_clone.drasi_lib_instance_id
                            );

                            match reaction_handle
                                .subscribe_with_options(Default::default())
                                .await
                            {
                                Ok(mut subscription) => {
                                    while let Some(query_result) = subscription.recv().await {
                                        if query_result.results.is_empty() {
                                            let result_json = serde_json::json!({
                                                "query_id": query_result.query_id,
                                                "results": [],
                                            });
                                            if tx.send(result_json).await.is_err() {
                                                break;
                                            }
                                        } else {
                                            for result_item in &query_result.results {
                                                let result_json = serde_json::json!({
                                                    "query_id": query_result.query_id.clone(),
                                                    "result": result_item,
                                                });
                                                if tx.send(result_json).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => log::error!(
                                    "Failed to subscribe to reaction '{}': {}",
                                    settings_clone.reaction_id,
                                    e
                                ),
                            }
                        }
                        Err(e) => {
                            log::error!(
                                "No reaction handle found for reaction '{}': {}",
                                settings_clone.reaction_id,
                                e
                            );
                        }
                    }
                } else {
                    log::error!(
                        "drasi-lib instance {} not found in test run",
                        settings_clone
                            .drasi_lib_instance_id
                            .test_drasi_lib_instance_id
                    );
                }
            } else {
                log::error!(
                    "Test run {} not found",
                    settings_clone.drasi_lib_instance_id.test_run_id
                );
            }

            log::debug!(
                "Channel handler stopped for reaction {} on instance {}",
                settings_clone.reaction_id,
                settings_clone.drasi_lib_instance_id
            );
        });

        Ok(rx)
    }
}

#[async_trait]
impl ReactionOutputHandler for DrasiLibInstanceChannelHandler {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
        log::debug!("Initializing DrasiLibInstanceChannelHandler");

        // Create the output channel for ReactionHandlerMessages
        let (tx, rx) = channel(self.settings.buffer_size);

        // Update status
        *self.status.write().await = ReactionHandlerStatus::Running;

        // Start the bridge task that converts reactions to handler messages
        let tx_clone = tx.clone();
        let settings = self.settings.clone();
        let status = self.status.clone();
        let notifier = self.notifier.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let test_run_host = self.test_run_host.clone();

        tokio::spawn(async move {
            // Wait for TestRunHost to be set and create the channel connection
            let mut reaction_rx = loop {
                // Check if TestRunHost is set
                let host_lock = test_run_host.lock().await;
                if host_lock.is_some() {
                    drop(host_lock);

                    // Try to create the channel connection
                    match DrasiLibInstanceChannelHandler::create_channel_connection_static(
                        &test_run_host,
                        &settings,
                    )
                    .await
                    {
                        Ok(rx) => break rx,
                        Err(e) => {
                            log::error!("Failed to create channel connection: {e}");
                            // Wait a bit before retrying
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                    }
                } else {
                    drop(host_lock);
                    // Wait a bit before checking again
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            };

            loop {
                tokio::select! {
                    // Receive reaction from drasi-lib instance channel
                    Some(reaction_data) = reaction_rx.recv() => {
                        // Check if we should process (not paused)
                        let current_status = *status.read().await;
                        if current_status == ReactionHandlerStatus::Paused {
                            // Wait for unpause notification
                            notifier.notified().await;
                            continue;
                        }

                        // Convert to ReactionHandlerMessage
                        let message = ReactionHandlerMessage::Invocation(ReactionInvocation {
                            handler_type: ReactionHandlerType::Http, // TODO: Add DrasiLibInstanceChannel type
                            payload: ReactionHandlerPayload {
                                value: reaction_data,
                                timestamp: chrono::Utc::now(),
                                invocation_id: Some(uuid::Uuid::new_v4().to_string()),
                                metadata: Some(serde_json::json!({
                                    "drasi_lib_instance_id": settings.drasi_lib_instance_id.to_string(),
                                    "reaction_id": settings.reaction_id,
                                })),
                            },
                        });

                        // Send the message
                        if let Err(e) = tx_clone.send(message).await {
                            log::debug!("Channel closed (likely due to stop trigger): {e}");
                            break;
                        }
                    }

                    // Shutdown signal
                    _ = shutdown_notify.notified() => {
                        log::info!("Received shutdown signal for channel handler");
                        break;
                    }
                }
            }

            // Send stop signal before exiting
            let _ = tx_clone
                .send(ReactionHandlerMessage::Control(ReactionControlSignal::Stop))
                .await;
        });

        // Send start signal
        tx.send(ReactionHandlerMessage::Control(
            ReactionControlSignal::Start,
        ))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send start signal: {e}"))?;

        Ok(rx)
    }

    async fn start(&self) -> anyhow::Result<()> {
        log::debug!("Starting DrasiLibInstanceChannelHandler");

        let mut status = self.status.write().await;
        match *status {
            ReactionHandlerStatus::Paused => {
                *status = ReactionHandlerStatus::Running;
                self.notifier.notify_one();
                Ok(())
            }
            ReactionHandlerStatus::Running => Ok(()),
            _ => Err(anyhow::anyhow!(
                "Cannot start handler from {:?} state",
                *status
            )),
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {
        log::debug!("Pausing DrasiLibInstanceChannelHandler");

        let mut status = self.status.write().await;
        match *status {
            ReactionHandlerStatus::Running => {
                *status = ReactionHandlerStatus::Paused;
                Ok(())
            }
            ReactionHandlerStatus::Paused => Ok(()),
            _ => Err(anyhow::anyhow!(
                "Cannot pause handler from {:?} state",
                *status
            )),
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {
        log::debug!("Stopping DrasiLibInstanceChannelHandler");

        // Update status
        *self.status.write().await = ReactionHandlerStatus::Stopped;

        // Signal shutdown to the bridge task
        self.shutdown_notify.notify_one();

        Ok(())
    }

    async fn status(&self) -> ReactionHandlerStatus {
        *self.status.read().await
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "handler_type": "drasi_lib_instance_channel",
            "drasi_lib_instance_id": self.settings.drasi_lib_instance_id.to_string(),
            "reaction_id": self.settings.reaction_id,
            "buffer_size": self.settings.buffer_size,
        }))
    }

    async fn set_test_run_host(&self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        self.set_test_run_host(test_run_host).await;
    }
}
