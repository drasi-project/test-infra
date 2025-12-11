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

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

// NOTE: ApplicationSourceHandle and PropertyMapBuilder are no longer available in drasi_lib.
// The new plugin architecture requires sources to be created as instances.
// This dispatcher is currently non-functional and will log warnings when used.

use test_data_store::{
    scripts::SourceChangeEvent,
    test_repo_storage::models::DrasiServerChannelSourceChangeDispatcherDefinition,
    test_run_storage::{TestRunDrasiServerId, TestRunSourceStorage},
};

use super::SourceChangeDispatcher;

#[derive(Debug)]
pub struct DrasiServerChannelSourceChangeDispatcherSettings {
    pub drasi_server_id: TestRunDrasiServerId,
    pub source_id: String,
    pub buffer_size: usize,
}

impl DrasiServerChannelSourceChangeDispatcherSettings {
    pub fn new(
        definition: &DrasiServerChannelSourceChangeDispatcherDefinition,
        test_run_source_storage: &TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        // Parse the drasi_server_id from the definition
        let drasi_server_id = TestRunDrasiServerId::new(
            &test_run_source_storage.id.test_run_id,
            &definition.drasi_server_id,
        );

        Ok(Self {
            drasi_server_id,
            source_id: definition.source_id.clone(),
            buffer_size: definition.buffer_size.unwrap_or(1024),
        })
    }
}

pub struct DrasiServerChannelSourceChangeDispatcher {
    settings: DrasiServerChannelSourceChangeDispatcherSettings,
    test_run_host: Option<std::sync::Arc<crate::TestRunHost>>,
    sender: Option<mpsc::Sender<Vec<SourceChangeEvent>>>,
    queued_events: Vec<SourceChangeEvent>,
    receiver_task: Option<JoinHandle<()>>,
}

impl DrasiServerChannelSourceChangeDispatcher {
    pub fn new(
        definition: &DrasiServerChannelSourceChangeDispatcherDefinition,
        storage: &TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        log::info!(
            "Creating DrasiServerChannelSourceChangeDispatcher for source '{}' on Drasi Server '{}'",
            definition.source_id,
            definition.drasi_server_id
        );

        let settings = DrasiServerChannelSourceChangeDispatcherSettings::new(definition, storage)?;
        log::debug!(
            "DrasiServerChannelSourceChangeDispatcher created with settings: drasi_server_id={:?}, source_id={}, buffer_size={}",
            settings.drasi_server_id,
            settings.source_id,
            settings.buffer_size
        );

        Ok(Self {
            settings,
            test_run_host: None,
            sender: None,
            queued_events: Vec::new(),
            receiver_task: None,
        })
    }

    async fn get_or_create_channel(
        &mut self,
    ) -> anyhow::Result<mpsc::Sender<Vec<SourceChangeEvent>>> {
        if let Some(sender) = &self.sender {
            // Check if the channel is still open
            if !sender.is_closed() {
                return Ok(sender.clone());
            }
        }

        // We need the test run host to get access to the Drasi Server
        // NOTE: test_run_host is no longer used since ApplicationSourceHandle is not available.
        // We keep the check for API consistency but the actual dispatch is no longer functional.
        let _test_run_host = match self.test_run_host.as_ref() {
            Some(host) => host.clone(),
            None => {
                // Can't create channel without TestRunHost
                return Err(anyhow::anyhow!(
                    "Cannot create channel for source '{}' on server '{}': TestRunHost not set yet",
                    self.settings.source_id,
                    self.settings.drasi_server_id
                ));
            }
        };

        // Create a new channel
        let (sender, mut receiver) =
            mpsc::channel::<Vec<SourceChangeEvent>>(self.settings.buffer_size);

        let source_id = self.settings.source_id.clone();
        let drasi_server_id = self.settings.drasi_server_id.clone();

        // Start a task to process events from the channel
        // NOTE: ApplicationSourceHandle is no longer available in drasi_lib.
        // This receiver task now just logs warnings since the functionality is not available.
        let receiver_task = tokio::spawn(async move {
            log::warn!(
                "Started channel receiver for source {source_id} on Drasi Server {drasi_server_id}, but ApplicationSourceHandle is no longer available in drasi_lib. Events will be logged but not dispatched."
            );

            // Process events from the channel - just log them since we can't dispatch
            let mut event_count = 0;
            while let Some(events) = receiver.recv().await {
                event_count += events.len();
                log::warn!(
                    "Channel receiver for source '{}' received {} events but cannot dispatch - \
                     ApplicationSourceHandle is not available in drasi_lib. \
                     Total events received: {}",
                    source_id,
                    events.len(),
                    event_count
                );
            }

            log::info!(
                "Channel receiver for source {source_id} on Drasi Server {drasi_server_id} stopped. Total events received (not dispatched): {event_count}"
            );
        });

        self.sender = Some(sender.clone());
        self.receiver_task = Some(receiver_task);
        Ok(sender)
    }
}

#[async_trait]
impl SourceChangeDispatcher for DrasiServerChannelSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        log::debug!("Closing Drasi Server Channel source change dispatcher");

        // Drop the sender to close the channel
        self.sender = None;

        // Wait for the receiver task to complete processing all events
        if let Some(receiver_task) = self.receiver_task.take() {
            log::info!("Waiting for channel receiver to finish processing events...");
            match receiver_task.await {
                Ok(()) => {
                    log::info!("Channel receiver finished successfully");
                }
                Err(e) => {
                    log::error!("Channel receiver task failed: {e}");
                }
            }
        }

        Ok(())
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        // First, check if we have queued events to dispatch
        if self.test_run_host.is_some() && !self.queued_events.is_empty() {
            log::debug!(
                "DrasiServerChannelDispatcher: Dispatching {} previously queued events to source '{}' on Drasi Server {}",
                self.queued_events.len(),
                self.settings.source_id,
                self.settings.drasi_server_id
            );

            // Take the queued events and dispatch them
            let queued_events = std::mem::take(&mut self.queued_events);

            // Get or create the channel
            let sender = self.get_or_create_channel().await?;

            // Send the queued events through the channel
            match sender.send(queued_events).await {
                Ok(()) => {
                    log::trace!("Successfully dispatched queued events");
                }
                Err(e) => {
                    log::error!("Failed to dispatch queued events: {e}");
                    // Clear the sender so we'll create a new channel next time
                    self.sender = None;
                    return Err(anyhow::anyhow!("Channel send failed: {e}"));
                }
            }
        }

        if events.is_empty() {
            return Ok(());
        }

        // Check if TestRunHost is available
        if self.test_run_host.is_none() {
            log::debug!(
                "DrasiServerChannelDispatcher: Queueing {} events to source '{}' on Drasi Server {} - TestRunHost not yet available",
                events.len(),
                self.settings.source_id,
                self.settings.drasi_server_id
            );
            // Queue the events for later dispatch
            self.queued_events.extend(events.into_iter().cloned());
            return Ok(());
        }

        log::trace!(
            "DrasiServerChannelDispatcher: Dispatching {} events to source '{}' on Drasi Server {}",
            events.len(),
            self.settings.source_id,
            self.settings.drasi_server_id
        );

        // Get or create the channel
        let sender = self.get_or_create_channel().await?;

        // Clone the events to send them through the channel
        let owned_events: Vec<SourceChangeEvent> = events.into_iter().cloned().collect();

        // Send the events through the channel
        let num_events = owned_events.len();
        match sender.send(owned_events).await {
            Ok(()) => {
                log::trace!(
                    "Successfully dispatched {} events to Drasi Server Channel for source {}",
                    num_events,
                    self.settings.source_id
                );
                Ok(())
            }
            Err(e) => {
                log::error!(
                    "Failed to dispatch events to Drasi Server Channel for source {}: {}",
                    self.settings.source_id,
                    e
                );
                // Clear the sender so we'll create a new channel next time
                self.sender = None;
                anyhow::bail!("Channel send failed: {e}")
            }
        }
    }

    fn set_test_run_host(&mut self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        self.test_run_host = Some(test_run_host);

        // If we have queued events, log that they will be dispatched on next call
        if !self.queued_events.is_empty() {
            log::info!(
                "DrasiServerChannelDispatcher: TestRunHost is now available, {} queued events for source '{}' on Drasi Server {} will be dispatched on next call",
                self.queued_events.len(),
                self.settings.source_id,
                self.settings.drasi_server_id
            );
        }
    }
}

// NOTE: dispatch_event_to_drasi function has been removed.
// ApplicationSourceHandle and PropertyMapBuilder are no longer available in drasi_lib.
// The new plugin architecture requires sources to be created as instances implementing
// the Source trait, with their own mechanisms for receiving data.
