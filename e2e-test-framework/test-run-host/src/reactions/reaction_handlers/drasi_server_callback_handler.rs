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
    test_repo_storage::models::DrasiServerCallbackReactionHandlerDefinition,
    test_run_storage::{TestRunDrasiServerId, TestRunQueryId},
};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Notify, RwLock,
};

use crate::reactions::reaction_output_handler::{
    ReactionControlSignal, ReactionHandlerMessage, ReactionHandlerStatus, ReactionOutputHandler,
};

#[derive(Clone, Debug)]
pub struct DrasiServerCallbackHandlerSettings {
    pub drasi_server_id: TestRunDrasiServerId,
    pub reaction_id: String,
    pub callback_type: String,
    pub test_run_query_id: TestRunQueryId,
}

impl DrasiServerCallbackHandlerSettings {
    pub fn new(
        id: TestRunQueryId,
        definition: DrasiServerCallbackReactionHandlerDefinition,
    ) -> anyhow::Result<Self> {
        // Parse the drasi_server_id from the definition
        let drasi_server_id =
            TestRunDrasiServerId::new(&id.test_run_id, &definition.drasi_server_id);

        Ok(Self {
            drasi_server_id,
            reaction_id: definition.reaction_id.clone(),
            callback_type: definition
                .callback_type
                .unwrap_or_else(|| "default".to_string()),
            test_run_query_id: id,
        })
    }
}

pub struct DrasiServerCallbackHandler {
    settings: DrasiServerCallbackHandlerSettings,
    status: Arc<RwLock<ReactionHandlerStatus>>,
    notifier: Arc<Notify>,
    shutdown_notify: Arc<Notify>,
    test_run_host: Option<Arc<crate::TestRunHost>>,
}

impl DrasiServerCallbackHandler {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        id: TestRunQueryId,
        definition: DrasiServerCallbackReactionHandlerDefinition,
    ) -> anyhow::Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
        let settings = DrasiServerCallbackHandlerSettings::new(id, definition)?;
        log::trace!("Creating DrasiServerCallbackHandler with settings {settings:?}");

        let status = Arc::new(RwLock::new(ReactionHandlerStatus::Uninitialized));
        let notifier = Arc::new(Notify::new());
        let shutdown_notify = Arc::new(Notify::new());

        Ok(Box::new(Self {
            settings,
            status,
            notifier,
            shutdown_notify,
            test_run_host: None,
        }))
    }

    pub fn set_test_run_host(&mut self, test_run_host: Arc<crate::TestRunHost>) {
        self.test_run_host = Some(test_run_host);
    }

    async fn register_callback(&self, tx: Sender<ReactionHandlerMessage>) -> anyhow::Result<()> {
        // TODO: In a real implementation, we would register this callback
        // with the Drasi Server instance through its API or internal mechanism.
        // For now, we'll just log the registration.
        log::info!(
            "Registered callback for reaction {} on Drasi Server {}",
            self.settings.reaction_id,
            self.settings.drasi_server_id
        );

        // Simulate callback registration
        let settings = self.settings.clone();
        let tx = tx.clone();

        tokio::spawn(async move {
            // This would be replaced with actual callback registration
            // that receives invocations from the Drasi Server
            log::debug!(
                "Callback handler ready for reaction {} on server {}",
                settings.reaction_id,
                settings.drasi_server_id
            );

            // Keep the task alive to simulate an active callback
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

                // Check if channel is still open
                if tx.is_closed() {
                    break;
                }
            }
        });

        Ok(())
    }

    async fn unregister_callback(&self) -> anyhow::Result<()> {
        // TODO: Unregister the callback from the Drasi Server
        log::info!(
            "Unregistered callback for reaction {} on Drasi Server {}",
            self.settings.reaction_id,
            self.settings.drasi_server_id
        );
        Ok(())
    }
}

#[async_trait]
impl ReactionOutputHandler for DrasiServerCallbackHandler {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
        log::debug!("Initializing DrasiServerCallbackHandler");

        // Create the channel
        let (tx, rx) = channel(1024);

        // Register the callback with the Drasi Server
        self.register_callback(tx.clone()).await?;

        // Store the sender for later use
        // Note: This is a simplified approach - in reality we'd need proper
        // synchronization for mutable access
        // self.tx = Some(tx);

        // Update status
        *self.status.write().await = ReactionHandlerStatus::Running;

        // Send start signal
        tx.send(ReactionHandlerMessage::Control(
            ReactionControlSignal::Start,
        ))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send start signal: {e}"))?;

        Ok(rx)
    }

    async fn start(&self) -> anyhow::Result<()> {
        log::debug!("Starting DrasiServerCallbackHandler");

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
        log::debug!("Pausing DrasiServerCallbackHandler");

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
        log::debug!("Stopping DrasiServerCallbackHandler");

        // Unregister the callback
        self.unregister_callback().await?;

        // Update status
        *self.status.write().await = ReactionHandlerStatus::Stopped;

        // Signal shutdown
        self.shutdown_notify.notify_one();

        Ok(())
    }

    async fn status(&self) -> ReactionHandlerStatus {
        *self.status.read().await
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "handler_type": "drasi_server_callback",
            "drasi_server_id": self.settings.drasi_server_id.to_string(),
            "reaction_id": self.settings.reaction_id,
            "callback_type": self.settings.callback_type,
        }))
    }
}
