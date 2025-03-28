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

use std::{collections::HashSet, fmt::{self, Debug, Formatter}, num::NonZeroU32, sync::Arc, time::{Duration, SystemTime}, u32};

use async_trait::async_trait;
use building_graph::{BuildingGraph, GraphElementType, ModelChange};
use futures::future::join_all;
use governor::{Quota, RateLimiter};
use rand::Rng;
use serde::Serialize;
use time::{format_description, OffsetDateTime};
use tokio::{sync::{mpsc::{Receiver, Sender}, oneshot, Mutex}, task::JoinHandle, time::sleep};

use test_data_store::{
    scripts::{
        NodeRecord, RelationRecord, SourceChangeEvent, SourceChangeEventPayload, SourceChangeEventSourceInfo
    }, 
    test_repo_storage::{
        models::{BuildingHierarchyDataGeneratorDefinition, SensorDefinition, SourceChangeDispatcherDefinition, SpacingMode, TimeMode}, 
        TestSourceStorage
    }, 
    test_run_storage::{
        TestRunSourceId, TestRunSourceStorage
    }
};

use crate::sources::{bootstrap_data_generators::{BootstrapData, BootstrapDataGenerator}, source_change_dispatchers::{create_source_change_dispatcher, SourceChangeDispatcher}, source_change_generators::{SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorState, SourceChangeGeneratorStatus}};

use super::ModelDataGenerator;

mod building_graph;

#[derive(Debug, thiserror::Error)]
pub enum BuildingHierarchyDataGeneratorError {
    #[error("BuildingHierarchyDataGenerator is already finished. Reset to start over.")]
    AlreadyFinished,
    #[error("BuildingHierarchyDataGenerator is already stopped. Reset to start over.")]
    AlreadyStopped,
    #[error("BuildingHierarchyDataGenerator is currently Skipping. {0} skips remaining. Pause before Skip, Step, or Reset.")]
    CurrentlySkipping(u64),
    #[error("BuildingHierarchyDataGenerator is currently Stepping. {0} steps remaining. Pause before Skip, Step, or Reset.")]
    CurrentlyStepping(u64),
    #[error("BuildingHierarchyDataGenerator is currently in an Error state - {0:?}")]
    Error(SourceChangeGeneratorStatus),
    #[error("BuildingHierarchyDataGenerator is currently Running. Pause before trying to Skip.")]
    PauseToSkip,
    #[error("BuildingHierarchyDataGenerator is currently Running. Pause before trying to Step.")]
    PauseToStep,
    #[error("BuildingHierarchyDataGenerator is currently Running. Pause before trying to Reset.")]
    PauseToReset,
}

#[derive(Clone, Debug, Serialize)]
pub struct BuildingHierarchyDataGeneratorSettings {
    pub building_count: (u32, f64),
    pub floor_count: (u32, f64),
    pub room_count: (u32, f64),
    pub change_count: u64,
    pub dispatchers: Vec<SourceChangeDispatcherDefinition>,
    pub id: TestRunSourceId,
    pub input_storage: TestSourceStorage,
    pub output_storage: TestRunSourceStorage,
    pub room_sensors: Vec<SensorDefinition>,
    pub seed: u64,
    pub spacing_mode: SpacingMode,
    pub time_mode: TimeMode,
}

impl BuildingHierarchyDataGeneratorSettings {
    pub async fn new(
        test_run_source_id: TestRunSourceId, 
        definition: BuildingHierarchyDataGeneratorDefinition, 
        input_storage: TestSourceStorage, 
        output_storage: TestRunSourceStorage,
        dispatchers: Vec<SourceChangeDispatcherDefinition>,
    ) -> anyhow::Result<Self> {

        Ok(BuildingHierarchyDataGeneratorSettings {
            building_count: definition.building_count.unwrap_or((1, 0.0)),
            floor_count: definition.floor_count.unwrap_or((5, 0.0)),
            room_count: definition.room_count.unwrap_or((10, 0.0)),
            change_count: definition.common.change_count.unwrap_or(100000),
            dispatchers,
            id: test_run_source_id,
            input_storage,
            output_storage,
            room_sensors: definition.room_sensors,
            seed: definition.common.seed.unwrap_or(rand::rng().random()),
            spacing_mode: definition.common.spacing_mode,
            time_mode: definition.common.time_mode,
        })
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.id.clone()
    }
}

// Enum of BuildingHierarchyDataGenerator commands sent from Web API handler functions.
#[derive(Debug)]
pub enum BuildingHierarchyDataGeneratorCommand {
    // Command to get the current state of the BuildingHierarchyDataGenerator.
    GetState,
    // Command to pause the BuildingHierarchyDataGenerator.
    Pause,
    // Command to reset the BuildingHierarchyDataGenerator.
    Reset,
    // Command to skip the BuildingHierarchyDataGenerator forward a specified number of ChangeScriptRecords.
    Skip{skips: u64, spacing_mode: Option<SpacingMode>},
    // Command to start the BuildingHierarchyDataGenerator.
    Start,
    // Command to step the BuildingHierarchyDataGenerator forward a specified number of ChangeScriptRecords.
    Step{steps: u64, spacing_mode: Option<SpacingMode>},
    // Command to stop the BuildingHierarchyDataGenerator.
    Stop,
}

// Struct for messages sent to the BuildingHierarchyDataGenerator from the functions in the Web API.
#[derive(Debug,)]
pub struct BuildingHierarchyDataGeneratorMessage {
    // Command sent to the BuildingHierarchyDataGenerator.
    pub command: BuildingHierarchyDataGeneratorCommand,
    // One-shot channel for BuildingHierarchyDataGenerator to send a response back to the caller.
    pub response_tx: Option<oneshot::Sender<BuildingHierarchyDataGeneratorMessageResponse>>,
}

// A struct for the Response sent back from the BuildingHierarchyDataGenerator to the calling Web API handler.
#[derive(Debug)]
pub struct BuildingHierarchyDataGeneratorMessageResponse {
    // Result of the command.
    pub result: anyhow::Result<()>,
    // State of the BuildingHierarchyDataGenerator after the command.
    pub state: BuildingHierarchyDataGeneratorExternalState,
}

#[derive(Clone, Debug)]
pub struct ScheduledChangeEventMessage {
    pub delay_ns: u64,
    pub seq_num: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessedChangeEvent {
    pub dispatch_status: SourceChangeGeneratorStatus,
    pub event: SourceChangeEvent,
    pub seq: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct BuildingHierarchyDataGenerator {
    #[serde(skip_serializing)]
    building_graph: Arc<Mutex<BuildingGraph>>,
    settings: BuildingHierarchyDataGeneratorSettings,
    #[serde(skip_serializing)]
    model_host_tx_channel: Sender<BuildingHierarchyDataGeneratorMessage>,
    #[serde(skip_serializing)]
    _model_host_thread_handle: Arc<Mutex<JoinHandle<anyhow::Result<()>>>>,
}

impl BuildingHierarchyDataGenerator {
    pub async fn new(
        test_run_source_id: TestRunSourceId, 
        definition: BuildingHierarchyDataGeneratorDefinition, 
        input_storage: TestSourceStorage, 
        output_storage: TestRunSourceStorage,
        dispatchers: Vec<SourceChangeDispatcherDefinition>,
    ) -> anyhow::Result<Self> {
        let settings = BuildingHierarchyDataGeneratorSettings::new(
            test_run_source_id, definition, input_storage, output_storage.clone(), dispatchers).await?;
        log::debug!("Creating BuildingHierarchyDataGenerator from {:?}", &settings);

        let building_graph = Arc::new(Mutex::new(BuildingGraph::new(&settings)?));

        let (model_host_tx_channel, model_host_rx_channel) = tokio::sync::mpsc::channel(500);
        let model_host_thread_handle = tokio::spawn(model_host_thread(model_host_rx_channel, settings.clone(), building_graph.clone()));

        Ok(Self {
            building_graph,
            settings,
            model_host_tx_channel,
            _model_host_thread_handle: Arc::new(Mutex::new(model_host_thread_handle)),
        })
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> BuildingHierarchyDataGeneratorSettings {
        self.settings.clone()
    }

    async fn send_command(&self, command: BuildingHierarchyDataGeneratorCommand) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self.model_host_tx_channel.send(BuildingHierarchyDataGeneratorMessage {
            command,
            response_tx: Some(response_tx),
        }).await;

        match r {
            Ok(_) => {
                let player_response = response_rx.await?;

                Ok(SourceChangeGeneratorCommandResponse {
                    result: player_response.result,
                    state: SourceChangeGeneratorState {
                        status: player_response.state.status,
                        state: serde_json::to_value(player_response.state).unwrap(),
                    },
                })
            },
            Err(e) => anyhow::bail!("Error sending command to BuildingHierarchyDataGenerator: {:?}", e),
        }
    }
}

#[async_trait]
impl BootstrapDataGenerator for BuildingHierarchyDataGenerator {
    async fn get_data(&self, node_labels: &HashSet<String>, rel_labels: &HashSet<String>) -> anyhow::Result<BootstrapData> {
        log::debug!("Node labels: [{:?}], Rel labels: [{:?}]", node_labels, rel_labels);
        
        let mut building_nodes = Vec::new();
        let mut floor_nodes = Vec::new();
        let mut room_nodes = Vec::new();
        let mut building_floor_rels = Vec::new();
        let mut floor_room_rels = Vec::new();

        let building_graph = self.building_graph.lock().await;
        for change in building_graph.get_current_state(node_labels) {
            match change {
                ModelChange::BuildingAdded(building) => {
                    let node_record = NodeRecord {
                        id: building.id.into(),
                        labels: building.labels.clone(),                        
                        properties: serde_json::json!({}),
                    };
                    building_nodes.push(node_record);
                },
                ModelChange::FloorAdded(floor) => {
                    let node_record = NodeRecord {
                        id: floor.id.into(),
                        labels: floor.labels.clone(),                        
                        properties: serde_json::json!({}),
                    };
                    floor_nodes.push(node_record);
                },
                ModelChange::RoomAdded(room) => {
                    let node_record = NodeRecord {
                        id: room.id.into(),
                        labels: room.labels.clone(),                        
                        properties: serde_json::json!(room.properties),
                    };
                    room_nodes.push(node_record);
                },
                _ => {
                    log::debug!("Other change: {:?}", change);
                }
            }
        }

        for change in building_graph.get_current_state(rel_labels) {
            match change {
                ModelChange::BuildingFloorRelationAdded(relation) => {
                    let rel_record = RelationRecord {
                        id: relation.id.into(),
                        labels: relation.labels.clone(),
                        properties: serde_json::json!({}),
                        start_id: relation.building_id.into(),
                        start_label: Some(GraphElementType::BUILDING.to_string()),
                        end_id: relation.floor_id.into(),
                        end_label: Some(GraphElementType::FLOOR.to_string()),
                    };
                    building_floor_rels.push(rel_record);
                },
                ModelChange::FloorRoomRelationAdded(relation) => {
                    let rel_record = RelationRecord {
                        id: relation.id.into(),
                        labels: relation.labels.clone(),
                        properties: serde_json::json!({}),
                        start_id: relation.floor_id.into(),
                        start_label: Some(GraphElementType::FLOOR.to_string()),
                        end_id: relation.room_id.into(),
                        end_label: Some(GraphElementType::ROOM.to_string()),
                    };
                    floor_room_rels.push(rel_record);                },
                _ => {
                    log::debug!("Other change: {:?}", change);
                }
            }
        }

        let mut bootstrap_data = BootstrapData::new();

        if building_nodes.len() > 0 {
            bootstrap_data.nodes.insert("Building".to_string(), building_nodes);
        }   
        if floor_nodes.len() > 0 {
            bootstrap_data.nodes.insert("Floor".to_string(), floor_nodes);
        }
        if room_nodes.len() > 0 {
            bootstrap_data.nodes.insert("Room".to_string(), room_nodes);
        }
        if building_floor_rels.len() > 0 {
            bootstrap_data.rels.insert("BuildingFloor".to_string(), building_floor_rels);
        }
        if floor_room_rels.len() > 0 {
            bootstrap_data.rels.insert("FloorRoom".to_string(), floor_room_rels);
        }

        Ok(bootstrap_data)
    }
}

#[async_trait]
impl SourceChangeGenerator for BuildingHierarchyDataGenerator {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::GetState).await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Pause).await
    }

    async fn reset(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Reset).await
    }

    async fn skip(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Skip{skips, spacing_mode}).await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Start).await
    }

    async fn step(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Step{steps, spacing_mode}).await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(BuildingHierarchyDataGeneratorCommand::Stop).await
    }
}

#[async_trait]
impl ModelDataGenerator for BuildingHierarchyDataGenerator {}

#[derive(Debug, Serialize)]
pub struct BuildingHierarchyDataGeneratorExternalState {
    pub error_messages: Vec<String>,
    pub previous_event: Option<ProcessedChangeEvent>,
    pub skips_remaining: u64,
    pub skips_spacing_mode: Option<SpacingMode>,
    pub spacing_mode: SpacingMode,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub steps_spacing_mode: Option<SpacingMode>,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_next: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl From<&mut BuildingHierarchyDataGeneratorInternalState> for BuildingHierarchyDataGeneratorExternalState {
    fn from(state: &mut BuildingHierarchyDataGeneratorInternalState) -> Self {
        Self {
            error_messages: state.error_messages.clone(),
            previous_event: state.previous_event.clone(),
            skips_remaining: state.skips_remaining,
            skips_spacing_mode: state.skips_spacing_mode.clone(),
            spacing_mode: state.settings.spacing_mode.clone(),
            status: state.status,
            steps_remaining: state.steps_remaining,
            steps_spacing_mode: state.steps_spacing_mode.clone(),
            test_run_source_id: state.settings.id.clone(),
            time_mode: state.settings.time_mode.clone(),
            virtual_time_ns_current: state.virtual_time_ns_current,
            virtual_time_ns_next: state.virtual_time_ns_next,
            virtual_time_ns_offset: state.virtual_time_ns_offset,
            virtual_time_ns_start: state.virtual_time_ns_start,
        }
    }
}

pub struct BuildingHierarchyDataGeneratorInternalState {
    pub building_graph: Arc<Mutex<BuildingGraph>>,
    pub change_tx_channel: Sender<ScheduledChangeEventMessage>,
    pub delayer_tx_channel: Sender<ScheduledChangeEventMessage>,
    pub dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>>,
    pub error_messages: Vec<String>,
    pub message_seq_num: u64,
    pub previous_event: Option<ProcessedChangeEvent>,
    pub rate_limiter_tx_channel: Sender<ScheduledChangeEventMessage>,
    pub settings: BuildingHierarchyDataGeneratorSettings,
    pub skips_remaining: u64,
    pub skips_spacing_mode: Option<SpacingMode>,
    pub status: SourceChangeGeneratorStatus,
    pub stats: BuildingHierarchyDataGeneratorStats,
    pub steps_remaining: u64,
    pub steps_spacing_mode: Option<SpacingMode>,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_next: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl BuildingHierarchyDataGeneratorInternalState {

    async fn initialize(settings: BuildingHierarchyDataGeneratorSettings, building_graph: Arc<Mutex<BuildingGraph>>) -> anyhow::Result<(Self, Receiver<ScheduledChangeEventMessage>)> {
        log::debug!("Initializing BuildingHierarchyDataGenerator using {:?}", settings);

        // Create the dispatchers
        let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();
        for def in settings.dispatchers.iter() {
            match create_source_change_dispatcher(def, &settings.output_storage).await {
                Ok(dispatcher) => dispatchers.push(dispatcher),
                Err(e) => {
                    anyhow::bail!("Error creating SourceChangeDispatcher: {:?}; Error: {:?}", def, e);
                }
            }
        }

        // Create the channels and threads used for message passing.
        let (change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(1000);

        let (delayer_tx_channel, delayer_rx_channel) = tokio::sync::mpsc::channel(1000);
        let _ = tokio::spawn(delayer_thread(settings.id.clone(), delayer_rx_channel, change_tx_channel.clone()));

        let (rate_limiter_tx_channel, rate_limiter_rx_channel) = tokio::sync::mpsc::channel(1000);
        let _ = tokio::spawn(rate_limiter_thread(settings.id.clone(), settings.spacing_mode.clone(), rate_limiter_rx_channel, change_tx_channel.clone()));

        let state = Self {
            building_graph,
            change_tx_channel,
            delayer_tx_channel,
            dispatchers,
            error_messages: Vec::new(),
            message_seq_num: 0,
            previous_event: None,
            rate_limiter_tx_channel,
            settings,
            skips_remaining: 0,
            skips_spacing_mode: None,
            status: SourceChangeGeneratorStatus::Paused,
            stats: BuildingHierarchyDataGeneratorStats::default(),
            steps_remaining: 0,
            steps_spacing_mode: None,
            virtual_time_ns_current: 0,
            virtual_time_ns_next: 0,
            virtual_time_ns_offset: 0,
            virtual_time_ns_start: 0,
        };
    
        Ok((state, change_rx_channel))
    }

    async fn close_dispatchers(&mut self) {
        let dispatchers = &mut self.dispatchers;
    
        log::debug!("Closing dispatchers - #dispatchers:{}", dispatchers.len());
    
        let futures: Vec<_> = dispatchers.iter_mut()
            .map(|dispatcher| {
                async move {
                    let _ = dispatcher.close().await;
                }
            })
            .collect();
    
        // Wait for all of them to complete
        // TODO - Handle errors properly.
        let _ = join_all(futures).await;
    }
        
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) {
        let dispatchers = &mut self.dispatchers;

        log::debug!("Dispatching SourceChangeEvents - #dispatchers:{}, #events:{}", dispatchers.len(), events.len());

        let futures: Vec<_> = dispatchers.iter_mut()
            .map(|dispatcher| {
                let events = events.clone();
                async move {
                    let _ = dispatcher.dispatch_source_change_events(events).await;
                }
            })
            .collect();

        // Wait for all of them to complete
        // TODO - Handle errors properly.
        let _ = join_all(futures).await;
    }

    // Function to log the Player State at varying levels of detail.
    fn log_state(&self, msg: &str) {
        match log::max_level() {
            log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, self),
            log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, self),
            _ => {}
        }
    }

    async fn process_next_source_change_event(&mut self, message: ScheduledChangeEventMessage) -> anyhow::Result<()> {
        log::trace!("Processing next source change event: {:?}", message);
    
        let update = {
            let building_graph = &mut self.building_graph.lock().await;
            building_graph.update_random_room(self.virtual_time_ns_next)?
        };

        // Update the virtual time.
        self.virtual_time_ns_current = self.virtual_time_ns_next;

        let source_change_event = match update {
            Some(model_change) => {
                match model_change {
                    ModelChange::RoomUpdated(room_before, room_after) => {
                        SourceChangeEvent {
                            op: "u".to_string(),
                            ts_ms: self.virtual_time_ns_current / 1_000_000,
                            schema: "".to_string(),
                            payload: SourceChangeEventPayload {
                                source: SourceChangeEventSourceInfo {
                                    db: self.settings.id.test_source_id.to_string(),
                                    table: "node".to_string(),
                                    ts_ms: self.virtual_time_ns_current / 1_000_000,
                                    ts_sec: self.virtual_time_ns_current / 1_000_000_000,
                                    lsn: message.seq_num,
                                },
                                before: serde_json::json!(room_before),
                                after: serde_json::json!(room_after),
                            }
                        }
                    },
                    _ => {
                        log::debug!("Unexpected model change: {:?}", model_change);
                        return Ok(());
                    }
                }
            },
            None => {
                self.stats.num_source_change_events += 1;

                if self.stats.num_source_change_events >= self.settings.change_count {
                    self.transition_to_finished_state().await;
                } else {
                    self.schedule_next_change_event().await?;
                }

                return Ok(());
                }
        };

        match &self.status {
            SourceChangeGeneratorStatus::Running => {
                // Dispatch the SourceChangeEvent.
                self.dispatch_source_change_events(vec!(&source_change_event)).await;
                self.schedule_next_change_event().await?;
            },
            SourceChangeGeneratorStatus::Stepping => {
                if self.steps_remaining > 0 {
                    // Dispatch the SourceChangeEvent.
                    self.dispatch_source_change_events(vec!(&source_change_event)).await;

                    self.steps_remaining -= 1;
                    if self.steps_remaining == 0 {
                        self.status = SourceChangeGeneratorStatus::Paused;
                        self.steps_spacing_mode = None;
                    } else {
                        self.schedule_next_change_event().await?;
                    }
                } else {
                    // Transition to an error state.
                    self.transition_to_error_state("Stepping with no steps remaining", None);
                }
            },
            SourceChangeGeneratorStatus::Skipping => {
                if self.skips_remaining > 0 {
                    // DON'T dispatch the SourceChangeEvent.
                    log::trace!("Skipping ChangeScriptRecord: {:?}", source_change_event);
                    self.stats.num_skipped_source_change_events += 1;

                    self.skips_remaining -= 1;
                    if self.skips_remaining == 0 {
                        self.status = SourceChangeGeneratorStatus::Paused;
                        self.skips_spacing_mode = None;
                    } else {
                        self.schedule_next_change_event().await?;
                    }
                } else {
                    // Transition to an error state.
                    self.transition_to_error_state("Skipping with no skips remaining", None);
                }
            },
            _ => {
                // Transition to an error state.
                self.transition_to_error_state("Unexpected status for SourceChange processing", None);
            },
        }

        // Process the event.
        self.stats.num_source_change_events += 1;

        if self.stats.num_source_change_events >= self.settings.change_count {
            self.transition_to_finished_state().await;
        }

        Ok(())
    }    

    async fn process_command_message(&mut self, message: BuildingHierarchyDataGeneratorMessage) -> anyhow::Result<()> {
        log::debug!("Received command message: {:?}", message.command);
    
        if let BuildingHierarchyDataGeneratorCommand::GetState = message.command {
            let message_response = BuildingHierarchyDataGeneratorMessageResponse {
                result: Ok(()),
                state: self.into(),
            };
    
            let r = message.response_tx.unwrap().send(message_response);
            if let Err(e) = r {
                anyhow::bail!("Error sending message response back to caller: {:?}", e);
            }
        } else {
            let transition_response = match self.status {
                SourceChangeGeneratorStatus::Running => self.transition_from_running_state(&message.command).await,
                SourceChangeGeneratorStatus::Stepping => self.transition_from_stepping_state(&message.command).await,
                SourceChangeGeneratorStatus::Skipping => self.transition_from_skipping_state(&message.command).await,
                SourceChangeGeneratorStatus::Paused => self.transition_from_paused_state(&message.command).await,
                SourceChangeGeneratorStatus::Stopped => self.transition_from_stopped_state(&message.command).await,
                SourceChangeGeneratorStatus::Finished => self.transition_from_finished_state(&message.command).await,
                SourceChangeGeneratorStatus::Error => self.transition_from_error_state(&message.command).await,
            };
    
            if message.response_tx.is_some() {
                let message_response = BuildingHierarchyDataGeneratorMessageResponse {
                    result: transition_response,
                    state: self.into(),
                };
    
                let r = message.response_tx.unwrap().send(message_response);
                if let Err(e) = r {
                    anyhow::bail!("Error sending message response back to caller: {:?}", e);
                }
            }    
        }
    
        Ok(())
    }
    
    async fn reset(&mut self) -> anyhow::Result<()> {

        // Create the new dispatchers
        self.close_dispatchers().await;    
        let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();
        for def in self.settings.dispatchers.iter() {
            match create_source_change_dispatcher(def, &self.settings.output_storage).await {
                Ok(dispatcher) => dispatchers.push(dispatcher),
                Err(e) => {
                    anyhow::bail!("Error creating SourceChangeDispatcher: {:?}; Error: {:?}", def, e);
                }
            }
        }    
        // These fields do not get reset:
        //   state.change_tx_channel
        //   state.delayer_tx_channel
        //   state.settings
    
        self.dispatchers = dispatchers;
        self.error_messages = Vec::new();
        self.message_seq_num = 0;
        self.previous_event = None;
        self.skips_remaining = 0;
        self.skips_spacing_mode = None;
        self.status = SourceChangeGeneratorStatus::Paused;
        self.stats = BuildingHierarchyDataGeneratorStats::default();
        self.steps_remaining = 0;
        self.steps_spacing_mode = None;
        self.virtual_time_ns_current = 0;
        self.virtual_time_ns_offset = 0;
        self.virtual_time_ns_start = 0;
    
        Ok(())
    }

    async fn schedule_next_change_event(&mut self) -> anyhow::Result<()> {

        self.virtual_time_ns_next = if self.message_seq_num == 0 {
            // First event after start, fire immediately.
            self.virtual_time_ns_current
        } else {
            // Calculate the next event time based on the current time and the delay.
            self.virtual_time_ns_current + 1_000_000
        };

        self.message_seq_num += 1;

        let sch_msg = ScheduledChangeEventMessage {
            delay_ns: self.virtual_time_ns_next - self.virtual_time_ns_current,
            seq_num: self.message_seq_num,
        };   

        match self.status {
            SourceChangeGeneratorStatus::Skipping => {
                if let Err(e) = self.change_tx_channel.send(sch_msg).await {
                    anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                }    
            },
            SourceChangeGeneratorStatus::Stepping => {
                match self.steps_spacing_mode {
                    Some(SpacingMode::None) => {
                        if let Err(e) = self.change_tx_channel.send(sch_msg).await {
                            anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                        }    
                    },
                    Some(SpacingMode::Rate(_)) => {
                        if let Err(e) = self.rate_limiter_tx_channel.send(sch_msg).await {
                            anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                        }   
                    },
                    Some(SpacingMode::Recorded) => {
                        if let Err(e) = self.delayer_tx_channel.send(sch_msg).await {
                            anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                        };
                    },
                    None => match self.settings.spacing_mode {
                        SpacingMode::None => {
                            if let Err(e) = self.change_tx_channel.send(sch_msg).await {
                                anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                            }    
                        },
                        SpacingMode::Rate(_) => {
                            if let Err(e) = self.rate_limiter_tx_channel.send(sch_msg).await {
                                anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                            }   
                        },
                        SpacingMode::Recorded => {
                            if let Err(e) = self.delayer_tx_channel.send(sch_msg).await {
                                anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                            };
                        }
                    },
                }
            },
            SourceChangeGeneratorStatus::Running => match self.settings.spacing_mode {
                SpacingMode::None => {
                    if let Err(e) = self.change_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                    }    
                },
                SpacingMode::Rate(_) => {
                    if let Err(e) = self.rate_limiter_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                    }   
                },
                SpacingMode::Recorded => {
                    if let Err(e) = self.delayer_tx_channel.send(sch_msg).await {
                        anyhow::bail!("Error sending ScheduledChangeEventMessage: {:?}", e);
                    };
                }
            },
            _ => anyhow::bail!("Calculating record delay for unexpected status: {:?}", self.status),
        };
    
        Ok(())
    }
            
    async fn transition_from_error_state(&mut self, command: &BuildingHierarchyDataGeneratorCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let BuildingHierarchyDataGeneratorCommand::Reset = command {
            self.reset().await
        } else {
            Err(BuildingHierarchyDataGeneratorError::Error(self.status).into())
        }
    }
    
    async fn transition_from_finished_state(&mut self, command: &BuildingHierarchyDataGeneratorCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let BuildingHierarchyDataGeneratorCommand::Reset = command {
            self.reset().await
        } else {
            Err(BuildingHierarchyDataGeneratorError::AlreadyFinished.into())
        }
    }
    
    async fn transition_from_paused_state(&mut self, command: &BuildingHierarchyDataGeneratorCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        // If we are unpausing for the first time, we need to initialize the start times based on time_mode config.
        if self.previous_event.is_none() && 
            matches!(command, BuildingHierarchyDataGeneratorCommand::Start 
                | BuildingHierarchyDataGeneratorCommand::Step { .. }
                | BuildingHierarchyDataGeneratorCommand::Skip { .. }
            ) {
            self.stats.actual_start_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
    
            self.virtual_time_ns_start = match self.settings.time_mode {
                TimeMode::Live => self.stats.actual_start_time_ns,
                TimeMode::Recorded => self.stats.actual_start_time_ns,
                TimeMode::Rebased(ns) => ns,
            };
    
            self.virtual_time_ns_current = self.virtual_time_ns_start;
            self.virtual_time_ns_next = self.virtual_time_ns_start;
            self.virtual_time_ns_offset = 0;
        }
    
        match command {
            BuildingHierarchyDataGeneratorCommand::GetState => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Pause => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Reset => self.reset().await,
            BuildingHierarchyDataGeneratorCommand::Skip{skips, spacing_mode} => {
                log::info!("Script Skipping {} skips for TestRunSource {}", skips, self.settings.id);
    
                self.status = SourceChangeGeneratorStatus::Skipping;
                self.skips_remaining = *skips;
                self.skips_spacing_mode = spacing_mode.clone();
                self.schedule_next_change_event().await
            },
            BuildingHierarchyDataGeneratorCommand::Start => {
                log::info!("Script Started for TestRunSource {}", self.settings.id);
                
                self.status = SourceChangeGeneratorStatus::Running;
                self.schedule_next_change_event().await
            },
            BuildingHierarchyDataGeneratorCommand::Step{steps, spacing_mode} => {
                log::info!("Script Stepping {} steps for TestRunSource {}", steps, self.settings.id);
    
                self.status = SourceChangeGeneratorStatus::Stepping;
                self.steps_remaining = *steps;
                self.steps_spacing_mode = spacing_mode.clone();
                self.schedule_next_change_event().await
            },
            BuildingHierarchyDataGeneratorCommand::Stop => Ok(self.transition_to_stopped_state().await),
        }
    }
    
    async fn transition_from_running_state(&mut self, command: &BuildingHierarchyDataGeneratorCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        match command {
            BuildingHierarchyDataGeneratorCommand::GetState => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                Ok(())
            },
            BuildingHierarchyDataGeneratorCommand::Reset => {
                Err(BuildingHierarchyDataGeneratorError::PauseToReset.into())
            },
            BuildingHierarchyDataGeneratorCommand::Skip{..} => {
                Err(BuildingHierarchyDataGeneratorError::PauseToSkip.into())
            },
            BuildingHierarchyDataGeneratorCommand::Start => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Step{..} => {
                Err(BuildingHierarchyDataGeneratorError::PauseToStep.into())
            },
            BuildingHierarchyDataGeneratorCommand::Stop => {
                Ok(self.transition_to_stopped_state().await)
            },
        }
    }
    
    async fn transition_from_skipping_state(&mut self, command: &BuildingHierarchyDataGeneratorCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        match command {
            BuildingHierarchyDataGeneratorCommand::GetState => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                self.skips_remaining = 0;
                self.skips_spacing_mode = None;
                Ok(())
            },
            BuildingHierarchyDataGeneratorCommand::Stop => Ok(self.transition_to_stopped_state().await),
            BuildingHierarchyDataGeneratorCommand::Reset
            | BuildingHierarchyDataGeneratorCommand::Skip {..}
            | BuildingHierarchyDataGeneratorCommand::Start
            | BuildingHierarchyDataGeneratorCommand::Step {..}
                => Err(BuildingHierarchyDataGeneratorError::CurrentlySkipping(self.skips_remaining).into())
        }
    }
    
    async fn transition_from_stepping_state(&mut self, command: &BuildingHierarchyDataGeneratorCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        match command {
            BuildingHierarchyDataGeneratorCommand::GetState => Ok(()),
            BuildingHierarchyDataGeneratorCommand::Pause => {
                self.status = SourceChangeGeneratorStatus::Paused;
                self.steps_remaining = 0;
                self.steps_spacing_mode = None;
                Ok(())
            },
            BuildingHierarchyDataGeneratorCommand::Stop => Ok(self.transition_to_stopped_state().await),
            BuildingHierarchyDataGeneratorCommand::Reset
            | BuildingHierarchyDataGeneratorCommand::Skip {..}
            | BuildingHierarchyDataGeneratorCommand::Start
            | BuildingHierarchyDataGeneratorCommand::Step {..}
                => Err(BuildingHierarchyDataGeneratorError::CurrentlyStepping(self.steps_remaining).into())
        }
    }
    
    async fn transition_from_stopped_state(&mut self, command: &BuildingHierarchyDataGeneratorCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let BuildingHierarchyDataGeneratorCommand::Reset = command {
            self.reset().await
        } else {
            Err(BuildingHierarchyDataGeneratorError::AlreadyStopped.into())
        }
    }    

    async fn transition_to_finished_state(&mut self) {
        log::info!("Script Finished for TestRunSource {}", self.settings.id);
    
        self.status = SourceChangeGeneratorStatus::Finished;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.skips_remaining = 0;
        self.skips_spacing_mode = None;
        self.steps_remaining = 0;
        self.steps_spacing_mode = None;
        
        self.close_dispatchers().await;
        self.write_result_summary().await.ok();
    }
    
    async fn transition_to_stopped_state(&mut self) {
        log::info!("Script Stopped for TestRunSource {}", self.settings.id);
    
        self.status = SourceChangeGeneratorStatus::Stopped;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.skips_remaining = 0;
        self.skips_spacing_mode = None;
        self.steps_remaining = 0;
        self.steps_spacing_mode = None;

        self.close_dispatchers().await;
        self.write_result_summary().await.ok();
    }
    
    fn transition_to_error_state(&mut self, error_message: &str, error: Option<&anyhow::Error>) {    
        self.status = SourceChangeGeneratorStatus::Error;
    
        let msg = match error {
            Some(e) => format!("{}: {:?}", error_message, e),
            None => error_message.to_string(),
        };
    
        self.log_state(&msg);
    
        self.error_messages.push(msg);
    }    

    pub async fn write_result_summary(&mut self) -> anyhow::Result<()> {

        let result_summary: BuildingHierarchyDataGeneratorResultSummary = self.into();
        log::info!("Stats for TestRunSource:\n{:#?}", &result_summary);
    
        let result_summary_value = serde_json::to_value(result_summary).unwrap();
        match self.settings.output_storage.write_test_run_summary(&result_summary_value).await {
            Ok(_) => Ok(()),
            Err(e) => {
                log::error!("Error writing result summary to output storage: {:?}", e);
                Err(e)
            }
        }
    }    
}


impl Debug for BuildingHierarchyDataGeneratorInternalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuildingHierarchyDataGeneratorInternalState")
            .field("error_messages", &self.error_messages)
            .field("previous_record", &self.previous_event)
            .field("skips_remaining", &self.skips_remaining)
            .field("skips_spacing_mode", &self.skips_spacing_mode)
            .field("spacing_mode", &self.settings.spacing_mode)
            .field("status", &self.status)
            .field("stats", &self.stats)
            .field("steps_remaining", &self.steps_remaining)
            .field("steps_spacing_mode", &self.steps_spacing_mode)
            .field("time_mode", &self.settings.time_mode)
            .field("virtual_time_ns_current", &self.virtual_time_ns_current)
            .field("virtual_time_ns_next", &self.virtual_time_ns_next)
            .field("virtual_time_ns_offset", &self.virtual_time_ns_offset)
            .field("virtual_time_ns_start", &self.virtual_time_ns_start)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct BuildingHierarchyDataGeneratorStats {
    pub actual_start_time_ns: u64,
    pub actual_end_time_ns: u64,
    pub num_source_change_events: u64,
    pub num_skipped_source_change_events: u64,
}

impl Default for BuildingHierarchyDataGeneratorStats {
    fn default() -> Self {
        Self {
            actual_start_time_ns: 0,
            actual_end_time_ns: 0,
            num_source_change_events: 0,
            num_skipped_source_change_events: 0,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct BuildingHierarchyDataGeneratorResultSummary {
    pub actual_start_time: String,
    pub actual_start_time_ns: u64,
    pub actual_end_time: String,
    pub actual_end_time_ns: u64,
    pub run_duration_ns: u64,
    pub run_duration_sec: f64,
    pub num_source_change_events: u64,
    pub num_skipped_source_events: u64,
    pub processing_rate: f64,
    pub test_run_source_id: String,
}

impl From<&mut BuildingHierarchyDataGeneratorInternalState> for BuildingHierarchyDataGeneratorResultSummary {
    fn from(state: &mut BuildingHierarchyDataGeneratorInternalState) -> Self {
        let run_duration_ns = state.stats.actual_end_time_ns - state.stats.actual_start_time_ns;
        let run_duration_sec = run_duration_ns as f64 / 1_000_000_000.0;

        Self {
            actual_start_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.actual_start_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            actual_start_time_ns: state.stats.actual_start_time_ns,
            actual_end_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.actual_end_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            actual_end_time_ns: state.stats.actual_end_time_ns,
            run_duration_ns,
            run_duration_sec,
            num_source_change_events: state.stats.num_source_change_events,
            num_skipped_source_events: state.stats.num_skipped_source_change_events,
            processing_rate: state.stats.num_source_change_events as f64 / run_duration_sec,
            test_run_source_id: state.settings.id.to_string(),
        }
    }
}

impl Debug for BuildingHierarchyDataGeneratorResultSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let start_time = format!("{} ({} ns)", self.actual_start_time, self.actual_start_time_ns);
        let end_time = format!("{} ({} ns)", self.actual_end_time, self.actual_end_time_ns);
        let run_duration = format!("{} sec ({} ns)", self.run_duration_sec, self.run_duration_ns, );
        let source_change_events = format!("{} (skipped:{})", self.num_source_change_events, self.num_skipped_source_events);
        let processing_rate = format!("{:.2} changes / sec", self.processing_rate);

        f.debug_struct("BuildingHierarchyDataGeneratorResultSummary")
            .field("test_run_source_id", &self.test_run_source_id)
            .field("start_time", &start_time)
            .field("end_time", &end_time)
            .field("run_duration", &run_duration)
            .field("source_change_events", &source_change_events)
            .field("processing_rate", &processing_rate)
            .finish()
    }
}

// Function that defines the operation of the BuildingHierarchyDataGenerator thread.
// The BuildingHierarchyDataGenerator thread processes ChangeScriptPlayerCommands sent to it from the Web API handler functions.
// The Web API function communicate via a channel and provide oneshot channels for the BuildingHierarchyDataGenerator to send responses back.
pub async fn model_host_thread(mut command_rx_channel: Receiver<BuildingHierarchyDataGeneratorMessage>, settings: BuildingHierarchyDataGeneratorSettings, building_graph: Arc<Mutex<BuildingGraph>>) -> anyhow::Result<()>{
    log::info!("Script processor thread started for TestRunSource {} ...", settings.id);

    // The BuildingHierarchyDataGenerator always starts with the first script record loaded and Paused.
    let (mut state, mut change_rx_channel) = match BuildingHierarchyDataGeneratorInternalState::initialize(settings, building_graph).await {
        Ok((state, change_rx_channel)) => (state, change_rx_channel),
        Err(e) => {
            // If initialization fails, don't dont transition to an error state, just log an error and exit the thread.
            let msg = format!("Error initializing BuildingHierarchyDataGenerator: {:?}", e);
            log::error!("{}", msg);
            anyhow::bail!(msg);
        }
    };

    // Loop to process commands sent to the BuildingHierarchyDataGenerator or read from the Change Stream.
    loop {
        state.log_state("Top of script processor loop");

        tokio::select! {
            // Always process all messages in the command channel and act on them first.
            biased;

            // Process messages from the command channel.
            command_message = command_rx_channel.recv() => {
                match command_message {
                    Some(command_message) => {
                        state.process_command_message(command_message).await
                            .inspect_err(|e| state.transition_to_error_state("Error calling process_command_message.", Some(e))).ok();
                    }
                    None => {
                        state.transition_to_error_state("Command channel closed.", None);
                        break;
                    }
                }
            },

            // Process messages from the Change Stream.
            change_stream_message = change_rx_channel.recv() => {
                match change_stream_message {
                    Some(change_stream_message) => {
                        // Only process the message if the seq_num matches the expected one.
                        // This avoids dealing with delayed messages from the delayer thread that are no longer relevant.
                        if change_stream_message.seq_num == state.message_seq_num && state.status.is_processing() {
                            state.process_next_source_change_event(change_stream_message).await
                                .inspect_err(|e| state.transition_to_error_state("Error calling process_change_stream_message", Some(e))).ok();
                        }
                    }
                    None => {
                        state.transition_to_error_state("Change stream channel closed.", None);
                        break;
                    }
                }
            },

            else => {
                log::error!("Script processor loop activated for {} but no command or change to process.", state.settings.id);
            }
        }
    }

    log::info!("Script processor thread exiting for TestRunSource {} ...", state.settings.id);    
    Ok(())
}

pub async fn delayer_thread(id: TestRunSourceId, mut delayer_rx_channel: Receiver<ScheduledChangeEventMessage>, change_tx_channel: Sender<ScheduledChangeEventMessage>) {
    log::info!("Delayer thread started for TestRunSource {} ...", id);

    loop {
        match delayer_rx_channel.recv().await {
            Some(message) => {
                // Sleep for the specified time before sending the message to the change_tx_channel.
                sleep(Duration::new(0, message.delay_ns as u32)).await;
                if let Err(e) = change_tx_channel.send(message).await {
                    log::error!("Error sending ScheduledChangeEventMessage to change_tx_channel: {:?}", e);
                }
            },
            None => {
                log::error!("ChangeScriptRecord delayer channel closed.");
                break;
            }
        }
    }
}

pub async fn rate_limiter_thread(id: TestRunSourceId, spacing_mode: SpacingMode, mut delayer_rx_channel: Receiver<ScheduledChangeEventMessage>, change_tx_channel: Sender<ScheduledChangeEventMessage>) {
    log::info!("Rate limiter thread started for TestRunSource {} ...", id);

    let limiter = match spacing_mode {
        SpacingMode::Rate(rate) => RateLimiter::direct(Quota::per_second(rate)),
        _ => RateLimiter::direct(Quota::per_second(NonZeroU32::new(u32::MAX).unwrap())),
    };

    loop {
        match delayer_rx_channel.recv().await {
            Some(message) => {
                limiter.until_ready().await;
                if let Err(e) = change_tx_channel.send(message).await {
                    log::error!("Error sending ScheduledChangeEventMessage to change_tx_channel: {:?}", e);
                }
            },
            None => {
                log::error!("ChangeScriptRecord delayer channel closed.");
                break;
            }
        }
    }
}