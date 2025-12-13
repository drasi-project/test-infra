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
    test_repo_storage::models::DrasiServerChannelReactionHandlerDefinition,
    test_run_storage::{TestRunDrasiServerId, TestRunQueryId},
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
pub struct DrasiServerChannelHandlerSettings {
    pub drasi_server_id: TestRunDrasiServerId,
    pub reaction_id: String,
    pub buffer_size: usize,
    pub test_run_query_id: TestRunQueryId,
}

impl DrasiServerChannelHandlerSettings {
    pub fn new(
        id: TestRunQueryId,
        definition: DrasiServerChannelReactionHandlerDefinition,
    ) -> anyhow::Result<Self> {
        // Parse the drasi_server_id from the definition
        let drasi_server_id =
            TestRunDrasiServerId::new(&id.test_run_id, &definition.drasi_server_id);

        Ok(Self {
            drasi_server_id,
            reaction_id: definition.reaction_id.clone(),
            buffer_size: definition.buffer_size.unwrap_or(1024),
            test_run_query_id: id,
        })
    }
}

pub struct DrasiServerChannelHandler {
    settings: DrasiServerChannelHandlerSettings,
    status: Arc<RwLock<ReactionHandlerStatus>>,
    notifier: Arc<Notify>,
    shutdown_notify: Arc<Notify>,
    test_run_host: Arc<Mutex<Option<Arc<crate::TestRunHost>>>>,
}

impl DrasiServerChannelHandler {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        id: TestRunQueryId,
        definition: DrasiServerChannelReactionHandlerDefinition,
    ) -> anyhow::Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
        let settings = DrasiServerChannelHandlerSettings::new(id, definition)?;
        log::trace!("Creating DrasiServerChannelHandler with settings {settings:?}");

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
        settings: &DrasiServerChannelHandlerSettings,
    ) -> anyhow::Result<Receiver<serde_json::Value>> {
        // Get the test run host
        let test_run_host_lock = test_run_host.lock().await;
        let _test_run_host = test_run_host_lock
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("TestRunHost not set"))?
            .clone();
        drop(test_run_host_lock);

        // Create a channel for receiving reactions
        let (_tx, rx) = channel(settings.buffer_size);

        // NOTE: ApplicationReactionHandle is no longer available in drasi_lib.
        // The new plugin architecture requires reactions to be created as instances.
        // This handler is currently non-functional and will log warnings.
        log::warn!(
            "Created channel connection for reaction '{}' on Drasi Server {}, but ApplicationReactionHandle is no longer available in drasi_lib. No data will be received.",
            settings.reaction_id,
            settings.drasi_server_id
        );

        // Start a task that just logs the situation
        let settings_clone = settings.clone();

        tokio::spawn(async move {
            log::warn!(
                "Channel handler starting for reaction {} on server {}, but ApplicationReactionHandle is no longer available in drasi_lib. \
                 Reactions must be added as instances implementing the Reaction trait.",
                settings_clone.reaction_id,
                settings_clone.drasi_server_id
            );

            // The channel will remain open but never receive any data
            // since we can't get reaction handles anymore
        });

        Ok(rx)
    }
}

#[async_trait]
impl ReactionOutputHandler for DrasiServerChannelHandler {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
        log::debug!("Initializing DrasiServerChannelHandler");

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
                    match DrasiServerChannelHandler::create_channel_connection_static(
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
                    // Receive reaction from Drasi Server channel
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
                            handler_type: ReactionHandlerType::Http, // TODO: Add DrasiServerChannel type
                            payload: ReactionHandlerPayload {
                                value: reaction_data,
                                timestamp: chrono::Utc::now(),
                                invocation_id: Some(uuid::Uuid::new_v4().to_string()),
                                metadata: Some(serde_json::json!({
                                    "drasi_server_id": settings.drasi_server_id.to_string(),
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
        log::debug!("Starting DrasiServerChannelHandler");

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
        log::debug!("Pausing DrasiServerChannelHandler");

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
        log::debug!("Stopping DrasiServerChannelHandler");

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
            "handler_type": "drasi_server_channel",
            "drasi_server_id": self.settings.drasi_server_id.to_string(),
            "reaction_id": self.settings.reaction_id,
            "buffer_size": self.settings.buffer_size,
        }))
    }

    async fn set_test_run_host(&self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        self.set_test_run_host(test_run_host).await;
    }
}
