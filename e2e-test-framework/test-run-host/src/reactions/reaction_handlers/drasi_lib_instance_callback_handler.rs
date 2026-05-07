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
    test_repo_storage::models::DrasiLibInstanceCallbackReactionHandlerDefinition,
    test_run_storage::{TestRunDrasiLibInstanceId, TestRunQueryId},
};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex, Notify, RwLock,
};

use crate::reactions::reaction_output_handler::{
    ReactionControlSignal, ReactionHandlerMessage, ReactionHandlerPayload, ReactionHandlerStatus,
    ReactionHandlerType, ReactionInvocation, ReactionOutputHandler,
};

#[derive(Clone, Debug)]
pub struct DrasiLibInstanceCallbackHandlerSettings {
    pub drasi_lib_instance_id: TestRunDrasiLibInstanceId,
    pub reaction_id: String,
    pub callback_type: String,
    pub test_run_query_id: TestRunQueryId,
}

impl DrasiLibInstanceCallbackHandlerSettings {
    pub fn new(
        id: TestRunQueryId,
        definition: DrasiLibInstanceCallbackReactionHandlerDefinition,
    ) -> anyhow::Result<Self> {
        // Parse the drasi_lib_instance_id from the definition
        let drasi_lib_instance_id =
            TestRunDrasiLibInstanceId::new(&id.test_run_id, &definition.drasi_lib_instance_id);

        Ok(Self {
            drasi_lib_instance_id,
            reaction_id: definition.reaction_id.clone(),
            callback_type: definition
                .callback_type
                .unwrap_or_else(|| "default".to_string()),
            test_run_query_id: id,
        })
    }
}

pub struct DrasiLibInstanceCallbackHandler {
    settings: DrasiLibInstanceCallbackHandlerSettings,
    status: Arc<RwLock<ReactionHandlerStatus>>,
    notifier: Arc<Notify>,
    shutdown_notify: Arc<Notify>,
    test_run_host: Arc<Mutex<Option<Arc<crate::TestRunHost>>>>,
}

impl DrasiLibInstanceCallbackHandler {
    pub async fn create(
        id: TestRunQueryId,
        definition: DrasiLibInstanceCallbackReactionHandlerDefinition,
    ) -> anyhow::Result<Box<dyn ReactionOutputHandler + Send + Sync>> {
        let settings = DrasiLibInstanceCallbackHandlerSettings::new(id, definition)?;
        log::trace!("Creating DrasiLibInstanceCallbackHandler with settings {settings:?}");

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

    async fn register_callback(&self, tx: Sender<ReactionHandlerMessage>) -> anyhow::Result<()> {
        let host = self.test_run_host.lock().await.clone().ok_or_else(|| {
            anyhow::anyhow!("TestRunHost not set for drasi-lib instance callback handler")
        })?;
        let settings = self.settings.clone();

        tokio::spawn(async move {
            let test_runs = host.test_runs.read().await;
            let Some(test_run) = test_runs.get(&settings.drasi_lib_instance_id.test_run_id) else {
                log::error!(
                    "Test run {} not found",
                    settings.drasi_lib_instance_id.test_run_id
                );
                return;
            };
            let Some(instance) = test_run
                .drasi_lib_instances
                .get(&settings.drasi_lib_instance_id.test_drasi_lib_instance_id)
            else {
                log::error!(
                    "drasi-lib instance {} not found in test run",
                    settings.drasi_lib_instance_id.test_drasi_lib_instance_id
                );
                return;
            };
            let handle = match instance.get_reaction_handle(&settings.reaction_id).await {
                Ok(handle) => handle,
                Err(e) => {
                    log::error!(
                        "Reaction handle '{}' not found: {}",
                        settings.reaction_id,
                        e
                    );
                    return;
                }
            };
            drop(test_runs);

            let callback_tx = tx.clone();
            if let Err(e) = handle
                .subscribe(move |query_result| {
                    let payload = ReactionHandlerPayload {
                        value: serde_json::json!({
                            "query_id": query_result.query_id,
                            "results": query_result.results,
                        }),
                        timestamp: chrono::Utc::now(),
                        invocation_id: None,
                        metadata: None,
                    };
                    let message = ReactionHandlerMessage::Invocation(ReactionInvocation {
                        handler_type: ReactionHandlerType::Grpc,
                        payload,
                    });
                    if let Err(e) = callback_tx.try_send(message) {
                        log::debug!("Failed to forward drasi-lib instance callback result: {e}");
                    }
                })
                .await
            {
                log::error!(
                    "Failed to subscribe callback for reaction '{}': {}",
                    settings.reaction_id,
                    e
                );
            }
        });

        Ok(())
    }

    async fn unregister_callback(&self) -> anyhow::Result<()> {
        // TODO: Unregister the callback from the drasi-lib instance
        log::info!(
            "Unregistered callback for reaction {} on drasi-lib instance {}",
            self.settings.reaction_id,
            self.settings.drasi_lib_instance_id
        );
        Ok(())
    }
}

#[async_trait]
impl ReactionOutputHandler for DrasiLibInstanceCallbackHandler {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
        log::debug!("Initializing DrasiLibInstanceCallbackHandler");

        // Create the channel
        let (tx, rx) = channel(1024);

        // Register the callback with the drasi-lib instance
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
        log::debug!("Starting DrasiLibInstanceCallbackHandler");

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
        log::debug!("Pausing DrasiLibInstanceCallbackHandler");

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
        log::debug!("Stopping DrasiLibInstanceCallbackHandler");

        // Unregister the callback
        self.unregister_callback().await?;

        // Update status
        *self.status.write().await = ReactionHandlerStatus::Stopped;

        // Signal shutdown
        self.shutdown_notify.notify_one();

        Ok(())
    }

    async fn set_test_run_host(&self, test_run_host: Arc<crate::TestRunHost>) {
        let mut host = self.test_run_host.lock().await;
        *host = Some(test_run_host);
    }

    async fn status(&self) -> ReactionHandlerStatus {
        *self.status.read().await
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "handler_type": "drasi_lib_instance_callback",
            "drasi_lib_instance_id": self.settings.drasi_lib_instance_id.to_string(),
            "reaction_id": self.settings.reaction_id,
            "callback_type": self.settings.callback_type,
        }))
    }
}
