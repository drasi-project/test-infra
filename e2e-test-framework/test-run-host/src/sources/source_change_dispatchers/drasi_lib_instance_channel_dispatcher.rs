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

use drasi_source_application::{ApplicationSourceHandle, PropertyMapBuilder};

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

        Ok(Self {
            drasi_lib_instance_id,
            source_id: definition.source_id.clone(),
            buffer_size: definition.buffer_size.unwrap_or(1024),
        })
    }
}

pub struct DrasiLibInstanceChannelSourceChangeDispatcher {
    settings: DrasiLibInstanceChannelSourceChangeDispatcherSettings,
    test_run_host: Option<std::sync::Arc<crate::TestRunHost>>,
    sender: Option<mpsc::Sender<Vec<SourceChangeEvent>>>,
    queued_events: Vec<SourceChangeEvent>,
    receiver_task: Option<JoinHandle<()>>,
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

        // We need the test run host to get access to the drasi-lib instance
        let test_run_host = match self.test_run_host.as_ref() {
            Some(host) => host.clone(),
            None => {
                // Can't create channel without TestRunHost
                return Err(anyhow::anyhow!(
                    "Cannot create channel for source '{}' on instance '{}': TestRunHost not set yet",
                    self.settings.source_id,
                    self.settings.drasi_lib_instance_id
                ));
            }
        };

        // Create a new channel
        let (sender, mut receiver) =
            mpsc::channel::<Vec<SourceChangeEvent>>(self.settings.buffer_size);

        let source_id = self.settings.source_id.clone();
        let drasi_lib_instance_id = self.settings.drasi_lib_instance_id.clone();

        // Start a task to process events from the channel
        let receiver_task = tokio::spawn(async move {
            log::info!(
                "Started channel receiver for source {source_id} on drasi-lib instance {drasi_lib_instance_id}"
            );

            // Get the drasi-lib instance and application handle
            let test_runs = test_run_host.test_runs.read().await;
            if let Some(test_run) = test_runs.get(&drasi_lib_instance_id.test_run_id) {
                if let Some(drasi_lib_instance) = test_run
                    .drasi_lib_instances
                    .get(&drasi_lib_instance_id.test_drasi_lib_instance_id)
                {
                    match drasi_lib_instance.get_source_handle(&source_id).await {
                        Ok(source_handle) => {
                            log::info!(
                                "Successfully obtained ApplicationSourceHandle for source '{source_id}' on drasi-lib instance {drasi_lib_instance_id}"
                            );

                            while let Some(events) = receiver.recv().await {
                                log::trace!(
                                    "Channel receiver for source {} received {} events",
                                    source_id,
                                    events.len()
                                );

                                for event in &events {
                                    if let Err(e) =
                                        dispatch_event_to_drasi(&source_handle, event).await
                                    {
                                        log::error!(
                                            "Failed to dispatch event to source '{source_id}': {e}"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("No source handle found for source '{source_id}': {e}");
                        }
                    }
                } else {
                    log::error!(
                        "drasi-lib instance {} not found in test run",
                        drasi_lib_instance_id.test_drasi_lib_instance_id
                    );
                }
            } else {
                log::error!("Test run {} not found", drasi_lib_instance_id.test_run_id);
            }

            log::info!(
                "Channel receiver for source {source_id} on drasi-lib instance {drasi_lib_instance_id} stopped"
            );
        });

        self.sender = Some(sender.clone());
        self.receiver_task = Some(receiver_task);
        Ok(sender)
    }
}

#[async_trait]
impl SourceChangeDispatcher for DrasiLibInstanceChannelSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        log::debug!("Closing drasi-lib instance Channel source change dispatcher");

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
                "DrasiLibInstanceChannelDispatcher: Dispatching {} previously queued events to source '{}' on drasi-lib instance {}",
                self.queued_events.len(),
                self.settings.source_id,
                self.settings.drasi_lib_instance_id
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
                "DrasiLibInstanceChannelDispatcher: Queueing {} events to source '{}' on drasi-lib instance {} - TestRunHost not yet available",
                events.len(),
                self.settings.source_id,
                self.settings.drasi_lib_instance_id
            );
            // Queue the events for later dispatch
            self.queued_events.extend(events.into_iter().cloned());
            return Ok(());
        }

        log::trace!(
            "DrasiLibInstanceChannelDispatcher: Dispatching {} events to source '{}' on drasi-lib instance {}",
            events.len(),
            self.settings.source_id,
            self.settings.drasi_lib_instance_id
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
                    "Successfully dispatched {} events to drasi-lib instance Channel for source {}",
                    num_events,
                    self.settings.source_id
                );
                Ok(())
            }
            Err(e) => {
                log::error!(
                    "Failed to dispatch events to drasi-lib instance Channel for source {}: {}",
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
                "DrasiLibInstanceChannelDispatcher: TestRunHost is now available, {} queued events for source '{}' on drasi-lib instance {} will be dispatched on next call",
                self.queued_events.len(),
                self.settings.source_id,
                self.settings.drasi_lib_instance_id
            );
        }
    }
}

/// Dispatch a test framework SourceChangeEvent to Drasi using ApplicationSourceHandle helper methods
async fn dispatch_event_to_drasi(
    source_handle: &ApplicationSourceHandle,
    event: &SourceChangeEvent,
) -> anyhow::Result<()> {
    // Log the event structure for debugging
    log::trace!(
        "Event structure: op={}, payload.after={:?}",
        event.op,
        event.payload.after
    );

    // Extract data from the payload.after field
    let after_data = &event.payload.after;

    // Extract id and labels from the after data
    let id = after_data
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing 'id' field in event payload"))?;

    let labels: Vec<String> = after_data
        .get("labels")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    // Determine type based on table field from source info or labels
    let table = event.payload.source.table.as_str();
    let typ = match table {
        "node" => "n",
        "relation" => "r",
        _ => {
            // Fallback: check ID patterns or labels
            if id.starts_with("B_")
                || id.starts_with("F_")
                || id.starts_with("R_")
                || labels.contains(&"Building".to_string())
                || labels.contains(&"Floor".to_string())
                || labels.contains(&"Room".to_string())
                || labels.contains(&"Stock".to_string())
            {
                "n" // Node
            } else if id.contains("HAS_FLOOR") || id.contains("HAS_ROOM") {
                "r" // Relation
            } else {
                // Default to node if we can't determine
                log::debug!("Using default type 'node' for element {id} with labels {labels:?}");
                "n"
            }
        }
    };

    // Convert the properties field to properties using PropertyMapBuilder
    let mut property_builder = PropertyMapBuilder::new();
    if let Some(data_obj) = after_data.get("properties").and_then(|v| v.as_object()) {
        for (key, value) in data_obj {
            // Add properties based on value type
            property_builder = match value {
                serde_json::Value::String(s) => property_builder.with_string(key, s),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        property_builder.with_integer(key, i)
                    } else if let Some(f) = n.as_f64() {
                        property_builder.with_float(key, f)
                    } else {
                        property_builder
                    }
                }
                serde_json::Value::Bool(b) => property_builder.with_bool(key, *b),
                serde_json::Value::Null => property_builder.with_null(key),
                _ => {
                    log::warn!(
                        "Skipping complex property '{key}' - only primitive types supported"
                    );
                    property_builder
                }
            };
        }
    }
    let properties = property_builder.build();

    // Dispatch based on operation and type
    match (event.op.as_str(), typ) {
        ("i", "n") => {
            // Insert node
            source_handle
                .send_node_insert(id, labels, properties)
                .await?;
        }
        ("u", "n") => {
            // Update node
            source_handle
                .send_node_update(id, labels, properties)
                .await?;
        }
        ("d", _) => {
            // Delete (node or relation)
            source_handle.send_delete(id, labels).await?;
        }
        ("i", "r") => {
            // Insert relation
            let start_id = after_data
                .get("start_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing 'start_id' field for relation"))?;

            let end_id = after_data
                .get("end_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing 'end_id' field for relation"))?;

            source_handle
                .send_relation_insert(id, labels, properties, start_id, end_id)
                .await?;
        }
        ("u", "r") => {
            // Update relation - ApplicationSourceHandle doesn't have a specific method for this
            // We would need to use the generic send() method with a constructed SourceChange
            // For now, log a warning
            log::warn!("Relation updates are not yet supported through ApplicationSourceHandle helper methods");
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown operation/type combination: {}/{}",
                event.op,
                typ
            ));
        }
    }

    Ok(())
}
