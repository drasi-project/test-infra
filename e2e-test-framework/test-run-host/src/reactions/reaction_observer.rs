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

//! ReactionObserver implementation for handling reaction invocations
//!
//! This module provides an observer for reactions that handles
//! HTTP callbacks and other reaction types using reaction-specific handlers.

use std::{fmt, sync::Arc, time::SystemTime};

use derive_more::Debug;

use serde::Serialize;
use test_data_store::{
    test_repo_storage::models::{ReactionHandlerDefinition, StopTriggerDefinition},
    test_run_storage::{TestRunReactionId, TestRunReactionStorage},
};
use tokio::{
    sync::{mpsc::Sender, oneshot, Mutex},
    task::JoinHandle,
};

use crate::{
    common::{HandlerPayload, HandlerRecord},
    reactions::{
        output_loggers::{OutputLogger, OutputLoggerConfig, OutputLoggerResult},
        reaction_output_handler::{
            create_reaction_handler as create_handler, ReactionControlSignal,
            ReactionHandlerMessage, ReactionHandlerStatus, ReactionHandlerType, ReactionInvocation,
            ReactionOutputHandler,
        },
        stop_triggers::{create_stop_trigger, StopTrigger},
    },
};

use super::TestRunReactionOverrides;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReactionObserverStatus {
    Running,
    Paused,
    Stopped,
    Error,
}

impl Serialize for ReactionObserverStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ReactionObserverStatus::Running => serializer.serialize_str("Running"),
            ReactionObserverStatus::Paused => serializer.serialize_str("Paused"),
            ReactionObserverStatus::Stopped => serializer.serialize_str("Stopped"),
            ReactionObserverStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReactionObserverError {
    #[error("ReactionObserver is already stopped. Reset to start over.")]
    AlreadyStopped,
    #[error("ReactionObserver is currently in an Error state - {0:?}")]
    Error(ReactionObserverStatus),
    #[error("ReactionObserver is currently Running. Pause before trying to Reset.")]
    PauseToReset,
}

#[derive(Debug)]
pub struct ReactionObserverCommandResponse {
    pub result: anyhow::Result<()>,
    pub state: ReactionObserverExternalState,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReactionObserverSettings {
    pub definition: ReactionHandlerDefinition,
    pub id: TestRunReactionId,
    pub output_storage: TestRunReactionStorage,
    pub loggers: Vec<OutputLoggerConfig>,
    pub stop_triggers: Vec<StopTriggerDefinition>,
}

impl ReactionObserverSettings {
    pub async fn new(
        test_run_reaction_id: TestRunReactionId,
        definition: ReactionHandlerDefinition,
        output_storage: TestRunReactionStorage,
        loggers: Vec<OutputLoggerConfig>,
        stop_triggers: Vec<StopTriggerDefinition>,
        _test_run_overrides: Option<TestRunReactionOverrides>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            definition,
            id: test_run_reaction_id,
            output_storage,
            loggers,
            stop_triggers,
        })
    }

    pub fn get_id(&self) -> TestRunReactionId {
        self.id.clone()
    }
}

#[derive(Debug)]
pub enum ReactionObserverCommand {
    GetState,
    Pause,
    Reset,
    Start,
    Stop,
}

pub struct ReactionObserverMessage {
    pub command: ReactionObserverCommand,
    pub response_tx: Option<oneshot::Sender<ReactionObserverMessageResponse>>,
}

#[derive(Debug)]
pub struct ReactionObserverMessageResponse {
    pub result: anyhow::Result<()>,
    pub state: ReactionObserverExternalState,
}

#[derive(Debug, Serialize)]
pub struct ReactionObserverExternalState {
    pub status: ReactionObserverStatus,
    pub handler_status: ReactionHandlerStatus,
    pub error_message: Option<String>,
    pub result_summary: ReactionObserverSummary,
    pub settings: ReactionObserverSettings,
    pub logger_results: Vec<OutputLoggerResult>,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct ReactionObserverMetrics {
    pub observer_create_time_ns: u64,
    pub observer_start_time_ns: u64,
    pub observer_stop_time_ns: u64,
    pub reaction_invocation_count: u64,
    pub reaction_invocation_first_ns: u64,
    pub reaction_invocation_last_ns: u64,
}

impl ReactionObserverMetrics {
    pub fn get_observer_run_duration_ns(&self, now_ns: Option<u64>) -> u64 {
        match (
            self.observer_start_time_ns > 0,
            self.observer_stop_time_ns > 0,
        ) {
            (true, true) => self.observer_stop_time_ns - self.observer_start_time_ns,
            (true, false) => {
                let now = now_ns.unwrap_or(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64,
                );
                now - self.observer_start_time_ns
            }
            _ => 0,
        }
    }

    pub fn get_observer_run_duration_s(&self, now_ns: Option<u64>) -> f64 {
        self.get_observer_run_duration_ns(now_ns) as f64 / 1_000_000_000.0
    }

    pub fn get_observer_run_duration_s_string(&self, now_ns: Option<u64>) -> String {
        let runtime_s = self.get_observer_run_duration_s(now_ns);
        if runtime_s > 86400.0 {
            format!("{:.1} days", runtime_s / 86400.0)
        } else if runtime_s > 3600.0 {
            format!("{:.1} hours", runtime_s / 3600.0)
        } else if runtime_s > 60.0 {
            format!("{:.1} minutes", runtime_s / 60.0)
        } else if runtime_s > 0.0 {
            format!("{:.1} seconds", runtime_s)
        } else {
            "0 seconds".to_string()
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ReactionObserverSummary {
    pub observer_runtime_s: String,
    pub reaction_invocation_count: u64,
}

impl fmt::Display for ReactionObserverSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Observer Runtime: {}, Reaction Invocations: {}",
            self.observer_runtime_s, self.reaction_invocation_count
        )
    }
}

impl From<&ReactionObserverMetrics> for ReactionObserverSummary {
    fn from(metrics: &ReactionObserverMetrics) -> Self {
        let now_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        Self {
            observer_runtime_s: metrics.get_observer_run_duration_s_string(Some(now_ns)),
            reaction_invocation_count: metrics.reaction_invocation_count,
        }
    }
}

#[derive(Debug)]
struct ReactionObserverInternalState {
    status: ReactionObserverStatus,
    handler_status: ReactionHandlerStatus,
    error_message: Option<String>,
    metrics: ReactionObserverMetrics,
    #[debug(skip)]
    loggers: Vec<Box<dyn OutputLogger + Send + Sync>>,
    logger_results: Vec<OutputLoggerResult>,
    #[debug(skip)]
    stop_triggers: Vec<Box<dyn StopTrigger + Send + Sync>>,
}

impl ReactionObserverInternalState {
    fn new() -> Self {
        let now_ns = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        Self {
            status: ReactionObserverStatus::Stopped,
            handler_status: ReactionHandlerStatus::Uninitialized,
            error_message: None,
            metrics: ReactionObserverMetrics {
                observer_create_time_ns: now_ns,
                ..Default::default()
            },
            loggers: vec![],
            logger_results: vec![],
            stop_triggers: vec![],
        }
    }
}

#[derive(Debug)]
pub struct ReactionObserver {
    settings: Arc<ReactionObserverSettings>,
    internal_state: Arc<Mutex<ReactionObserverInternalState>>,
    #[debug(skip)]
    output_handler: Arc<Box<dyn ReactionOutputHandler + Send + Sync>>,
    observer_task_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    observer_command_tx: Arc<Mutex<Option<Sender<ReactionObserverMessage>>>>,
}

impl ReactionObserver {
    pub async fn new(
        id: TestRunReactionId,
        definition: ReactionHandlerDefinition,
        output_storage: TestRunReactionStorage,
        loggers: Vec<OutputLoggerConfig>,
        stop_triggers: Vec<StopTriggerDefinition>,
        test_run_overrides: Option<TestRunReactionOverrides>,
    ) -> anyhow::Result<Self> {
        log::info!(
            "ReactionObserver::new() for {} with {} loggers: {:?}",
            id,
            loggers.len(),
            loggers
        );

        let settings = Arc::new(
            ReactionObserverSettings::new(
                id.clone(),
                definition.clone(),
                output_storage,
                loggers,
                stop_triggers,
                test_run_overrides,
            )
            .await?,
        );

        let internal_state = Arc::new(Mutex::new(ReactionObserverInternalState::new()));

        // Create output handler
        // Note: We convert the reaction ID to a query ID for compatibility with the handler
        let handler_query_id = test_data_store::test_run_storage::TestRunQueryId::new(
            &id.test_run_id,
            &id.test_reaction_id,
        );
        let output_handler = Arc::new(create_handler(handler_query_id, definition).await?);

        Ok(Self {
            settings,
            internal_state,
            output_handler,
            observer_task_handle: Arc::new(Mutex::new(None)),
            observer_command_tx: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn get_state(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        let internal_state = self.internal_state.lock().await;
        let external_state = ReactionObserverExternalState {
            status: internal_state.status,
            handler_status: internal_state.handler_status,
            error_message: internal_state.error_message.clone(),
            result_summary: ReactionObserverSummary::from(&internal_state.metrics),
            settings: (*self.settings).clone(),
            logger_results: internal_state.logger_results.clone(),
        };

        Ok(ReactionObserverCommandResponse {
            result: Ok(()),
            state: external_state,
        })
    }

    pub async fn pause(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        let mut internal_state = self.internal_state.lock().await;

        match internal_state.status {
            ReactionObserverStatus::Running => {
                self.output_handler.pause().await?;
                internal_state.status = ReactionObserverStatus::Paused;
                internal_state.handler_status = self.output_handler.status().await;
            }
            ReactionObserverStatus::Paused => {
                // Already paused
            }
            ReactionObserverStatus::Stopped => {
                return Err(ReactionObserverError::AlreadyStopped.into());
            }
            ReactionObserverStatus::Error => {
                return Err(ReactionObserverError::Error(internal_state.status).into());
            }
        }

        let external_state = ReactionObserverExternalState {
            status: internal_state.status,
            handler_status: internal_state.handler_status,
            error_message: internal_state.error_message.clone(),
            result_summary: ReactionObserverSummary::from(&internal_state.metrics),
            settings: (*self.settings).clone(),
            logger_results: internal_state.logger_results.clone(),
        };

        Ok(ReactionObserverCommandResponse {
            result: Ok(()),
            state: external_state,
        })
    }

    pub async fn reset(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        let mut internal_state = self.internal_state.lock().await;

        match internal_state.status {
            ReactionObserverStatus::Running => {
                return Err(ReactionObserverError::PauseToReset.into());
            }
            ReactionObserverStatus::Paused => {
                // Stop and reinitialize the handler
                self.output_handler.stop().await?;

                // Close current loggers
                let mut results = Vec::new();
                for logger in &mut internal_state.loggers {
                    if let Ok(result) = logger.end_test_run().await {
                        results.push(result);
                    }
                }
                internal_state.logger_results.extend(results);

                // Create new loggers
                internal_state.loggers = create_reaction_loggers(
                    self.settings.id.clone(),
                    &self.settings.loggers,
                    &self.settings.output_storage,
                )
                .await?;
                internal_state.logger_results = vec![];

                // Create new stop triggers
                internal_state.stop_triggers =
                    create_reaction_stop_triggers(&self.settings.stop_triggers).await?;

                internal_state.handler_status = self.output_handler.status().await;
                internal_state.error_message = None;
                internal_state.metrics = ReactionObserverMetrics {
                    observer_create_time_ns: internal_state.metrics.observer_create_time_ns,
                    ..Default::default()
                };
            }
            ReactionObserverStatus::Stopped => {
                return Err(ReactionObserverError::AlreadyStopped.into());
            }
            ReactionObserverStatus::Error => {
                return Err(ReactionObserverError::Error(internal_state.status).into());
            }
        }

        let external_state = ReactionObserverExternalState {
            status: internal_state.status,
            handler_status: internal_state.handler_status,
            error_message: internal_state.error_message.clone(),
            result_summary: ReactionObserverSummary::from(&internal_state.metrics),
            settings: (*self.settings).clone(),
            logger_results: internal_state.logger_results.clone(),
        };

        Ok(ReactionObserverCommandResponse {
            result: Ok(()),
            state: external_state,
        })
    }

    pub async fn start(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        let mut internal_state = self.internal_state.lock().await;

        match internal_state.status {
            ReactionObserverStatus::Running => {
                // Already running
            }
            ReactionObserverStatus::Paused => {
                self.output_handler.start().await?;
                internal_state.status = ReactionObserverStatus::Running;
                internal_state.handler_status = self.output_handler.status().await;
            }
            ReactionObserverStatus::Stopped => {
                // Initialize loggers
                internal_state.loggers = create_reaction_loggers(
                    self.settings.id.clone(),
                    &self.settings.loggers,
                    &self.settings.output_storage,
                )
                .await?;

                // Initialize stop triggers
                internal_state.stop_triggers =
                    create_reaction_stop_triggers(&self.settings.stop_triggers).await?;

                // Initialize and start the handler
                log::info!(
                    "[ReactionObserver] Initializing output handler for reaction: {}",
                    self.settings.id
                );
                let handler_rx_channel = self.output_handler.init().await?;
                log::info!(
                    "[ReactionObserver] Starting output handler for reaction: {}",
                    self.settings.id
                );
                self.output_handler.start().await?;
                log::info!(
                    "[ReactionObserver] Output handler started successfully for reaction: {}",
                    self.settings.id
                );

                // Start observer task
                let (command_tx, command_rx) = tokio::sync::mpsc::channel(100);
                *self.observer_command_tx.lock().await = Some(command_tx);

                let internal_state_clone = self.internal_state.clone();
                let output_handler_clone = self.output_handler.clone();
                let observer_task = tokio::spawn(async move {
                    observe_reaction_handler(
                        handler_rx_channel,
                        command_rx,
                        internal_state_clone,
                        output_handler_clone,
                    )
                    .await;
                });

                *self.observer_task_handle.lock().await = Some(observer_task);

                internal_state.status = ReactionObserverStatus::Running;
                internal_state.handler_status = self.output_handler.status().await;
                internal_state.metrics.observer_start_time_ns = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
                    as u64;
            }
            ReactionObserverStatus::Error => {
                return Err(ReactionObserverError::Error(internal_state.status).into());
            }
        }

        let external_state = ReactionObserverExternalState {
            status: internal_state.status,
            handler_status: internal_state.handler_status,
            error_message: internal_state.error_message.clone(),
            result_summary: ReactionObserverSummary::from(&internal_state.metrics),
            settings: (*self.settings).clone(),
            logger_results: internal_state.logger_results.clone(),
        };

        Ok(ReactionObserverCommandResponse {
            result: Ok(()),
            state: external_state,
        })
    }

    pub async fn stop(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        let mut internal_state = self.internal_state.lock().await;

        match internal_state.status {
            ReactionObserverStatus::Running | ReactionObserverStatus::Paused => {
                // Stop the output handler
                self.output_handler.stop().await?;

                // Stop the observer task
                if let Some(tx) = self.observer_command_tx.lock().await.take() {
                    let (response_tx, _) = oneshot::channel();
                    let _ = tx
                        .send(ReactionObserverMessage {
                            command: ReactionObserverCommand::Stop,
                            response_tx: Some(response_tx),
                        })
                        .await;
                }

                if let Some(handle) = self.observer_task_handle.lock().await.take() {
                    let _ = handle.await;
                }

                // Close loggers and collect results
                log::info!(
                    "Closing {} loggers in stop() method",
                    internal_state.loggers.len()
                );
                let mut results = Vec::new();
                for (idx, logger) in internal_state.loggers.iter_mut().enumerate() {
                    log::debug!("Calling end_test_run on logger {} in stop()", idx);
                    match logger.end_test_run().await {
                        Ok(result) => {
                            log::info!("Logger {} completed in stop(): {:?}", idx, result);
                            results.push(result);
                        }
                        Err(e) => {
                            log::error!("Logger {} failed to end test run in stop(): {}", idx, e);
                        }
                    }
                }
                internal_state.logger_results.extend(results);
                internal_state.loggers.clear();

                internal_state.status = ReactionObserverStatus::Stopped;
                internal_state.handler_status = self.output_handler.status().await;
                internal_state.metrics.observer_stop_time_ns = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
                    as u64;
            }
            ReactionObserverStatus::Stopped => {
                return Err(ReactionObserverError::AlreadyStopped.into());
            }
            ReactionObserverStatus::Error => {
                return Err(ReactionObserverError::Error(internal_state.status).into());
            }
        }

        let external_state = ReactionObserverExternalState {
            status: internal_state.status,
            handler_status: internal_state.handler_status,
            error_message: internal_state.error_message.clone(),
            result_summary: ReactionObserverSummary::from(&internal_state.metrics),
            settings: (*self.settings).clone(),
            logger_results: internal_state.logger_results.clone(),
        };

        Ok(ReactionObserverCommandResponse {
            result: Ok(()),
            state: external_state,
        })
    }

    pub async fn send_command(
        &self,
        command: ReactionObserverCommand,
    ) -> anyhow::Result<ReactionObserverCommandResponse> {
        match command {
            ReactionObserverCommand::GetState => self.get_state().await,
            ReactionObserverCommand::Pause => self.pause().await,
            ReactionObserverCommand::Reset => self.reset().await,
            ReactionObserverCommand::Start => self.start().await,
            ReactionObserverCommand::Stop => self.stop().await,
        }
    }

    /// Sets the TestRunHost for handlers that need it (e.g., DrasiServerChannelHandler)
    pub fn set_test_run_host(&self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        // Clone the handler reference to move into the async block
        let handler = self.output_handler.clone();
        tokio::spawn(async move {
            handler.set_test_run_host(test_run_host).await;
        });
    }
}

async fn observe_reaction_handler(
    mut handler_rx: tokio::sync::mpsc::Receiver<ReactionHandlerMessage>,
    mut command_rx: tokio::sync::mpsc::Receiver<ReactionObserverMessage>,
    internal_state: Arc<Mutex<ReactionObserverInternalState>>,
    output_handler: Arc<Box<dyn ReactionOutputHandler + Send + Sync>>,
) {
    log::debug!("Starting reaction observer task");

    loop {
        tokio::select! {
            Some(handler_msg) = handler_rx.recv() => {
                match handler_msg {
                    ReactionHandlerMessage::Control(ReactionControlSignal::Stop) => {
                        log::debug!("Reaction handler stopping");
                        break;
                    }
                    ReactionHandlerMessage::Invocation(invocation) => {
                        let mut state = internal_state.lock().await;
                        handle_reaction_invocation(&mut state, invocation).await;

                        // Check stop triggers
                        let handler_status = output_handler.status().await;
                        log::trace!(
                            "Checking {} stop triggers after {} invocations",
                            state.stop_triggers.len(),
                            state.metrics.reaction_invocation_count
                        );

                        for (idx, trigger) in state.stop_triggers.iter().enumerate() {
                            match trigger.is_true(&handler_status, &state.metrics).await {
                                Ok(true) => {
                                    log::error!(
                                        "Stop trigger {} fired after {} invocations, stopping reaction observer",
                                        idx,
                                        state.metrics.reaction_invocation_count
                                    );
                                    state.status = ReactionObserverStatus::Stopped;

                                // Close loggers and collect results before stopping
                                log::info!("Closing {} loggers after stop trigger fired", state.loggers.len());
                                let mut results = Vec::new();
                                for (idx, logger) in state.loggers.iter_mut().enumerate() {
                                    log::debug!("Calling end_test_run on logger {}", idx);
                                    match logger.end_test_run().await {
                                        Ok(result) => {
                                            log::info!("Logger {} completed: {:?}", idx, result);
                                            results.push(result);
                                        }
                                        Err(e) => {
                                            log::error!("Logger {} failed to end test run: {}", idx, e);
                                        }
                                    }
                                }
                                state.logger_results.extend(results);
                                state.loggers.clear();

                                // Record stop time
                                state.metrics.observer_stop_time_ns = SystemTime::now()
                                    .duration_since(SystemTime::UNIX_EPOCH)
                                    .unwrap()
                                    .as_nanos() as u64;

                                output_handler.stop().await.ok();
                                return;
                                }
                                Ok(false) => {
                                    log::trace!("Stop trigger {} not fired yet", idx);
                                }
                                Err(e) => {
                                    log::error!("Error checking stop trigger {}: {}", idx, e);
                                }
                            }
                        }
                    }
                    ReactionHandlerMessage::Error(error) => {
                        log::error!("Reaction handler error: {}", error);
                        let mut state = internal_state.lock().await;
                        state.status = ReactionObserverStatus::Error;
                        state.error_message = Some(format!("Handler error: {}", error));
                    }
                    _ => {
                        // Ignore other control signals
                    }
                }
            }
            Some(command_msg) = command_rx.recv() => {
                match command_msg.command {
                    ReactionObserverCommand::Stop => {
                        log::debug!("Reaction observer received stop command");
                        if let Some(_tx) = command_msg.response_tx {
                            // We don't need to send the external state here as it's only used internally
                        }
                        break;
                    }
                    _ => {
                        log::warn!("Unexpected command in observer task: {:?}", command_msg.command);
                    }
                }
            }
        }
    }

    log::debug!("Reaction observer task ending");
}

async fn handle_reaction_invocation(
    state: &mut ReactionObserverInternalState,
    invocation: ReactionInvocation,
) {
    // Update metrics
    let timestamp_ns = invocation
        .payload
        .timestamp
        .timestamp_nanos_opt()
        .unwrap_or(0) as u64;
    // Always increment by 1 since each invocation is a single item
    state.metrics.reaction_invocation_count += 1;
    if state.metrics.reaction_invocation_first_ns == 0 {
        state.metrics.reaction_invocation_first_ns = timestamp_ns;
    }
    state.metrics.reaction_invocation_last_ns = timestamp_ns;

    // Log the reaction
    log::debug!(
        "Reaction invoked: type={:?}, invocation_id={:?}, timestamp={}, total_count={}",
        invocation.handler_type,
        invocation.payload.invocation_id,
        invocation.payload.timestamp,
        state.metrics.reaction_invocation_count
    );

    // Convert reaction invocation to HandlerRecord and log it
    let now_ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let handler_record = HandlerRecord {
        id: invocation
            .payload
            .invocation_id
            .clone()
            .unwrap_or_else(|| format!("reaction-{}", state.metrics.reaction_invocation_count)),
        sequence: state.metrics.reaction_invocation_count,
        created_time_ns: timestamp_ns,
        processed_time_ns: now_ns,
        traceparent: None, // TODO: Extract from headers if available
        tracestate: None,
        payload: HandlerPayload::ReactionInvocation {
            reaction_type: match invocation.handler_type {
                ReactionHandlerType::Http => "Http".to_string(),
                ReactionHandlerType::EventGrid => "EventGrid".to_string(),
                ReactionHandlerType::Grpc => "Grpc".to_string(),
            },
            query_id: "unknown".to_string(), // TODO: Extract from payload if available
            request_method: invocation
                .payload
                .metadata
                .as_ref()
                .and_then(|m| m.get("method"))
                .and_then(|v| v.as_str())
                .unwrap_or("POST")
                .to_string(),
            request_path: invocation
                .payload
                .metadata
                .as_ref()
                .and_then(|m| m.get("path"))
                .and_then(|v| v.as_str())
                .unwrap_or("/")
                .to_string(),
            request_body: invocation.payload.value.clone(),
            headers: invocation
                .payload
                .metadata
                .as_ref()
                .and_then(|m| m.get("headers"))
                .and_then(|v| v.as_object())
                .map(|h| {
                    h.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                        .collect()
                })
                .unwrap_or_default(),
        },
    };

    // Log to all configured loggers
    log::debug!(
        "Logging handler record (seq: {}) to {} loggers",
        handler_record.sequence,
        state.loggers.len()
    );

    for (idx, logger) in state.loggers.iter_mut().enumerate() {
        log::trace!("Sending record to logger {}", idx);
        if let Err(e) = logger.log_handler_record(&handler_record).await {
            log::error!("Failed to log reaction invocation to logger {}: {}", idx, e);
        }
    }
}

// Helper function to create reaction loggers
async fn create_reaction_loggers(
    reaction_id: TestRunReactionId,
    configs: &Vec<OutputLoggerConfig>,
    output_storage: &TestRunReactionStorage,
) -> anyhow::Result<Vec<Box<dyn OutputLogger + Send + Sync>>> {
    use crate::reactions::output_loggers::create_output_logger;

    log::info!(
        "create_reaction_loggers() for {} with {} configs, storage path: {:?}",
        reaction_id,
        configs.len(),
        output_storage.reaction_output_path
    );

    let mut result = Vec::new();
    for config in configs {
        log::info!("Creating logger with config: {:?}", config);
        result.push(create_output_logger(reaction_id.clone(), config, output_storage).await?);
    }

    log::info!("Successfully created {} loggers", result.len());
    Ok(result)
}

// Helper function to create reaction stop triggers
async fn create_reaction_stop_triggers(
    definitions: &Vec<StopTriggerDefinition>,
) -> anyhow::Result<Vec<Box<dyn StopTrigger + Send + Sync>>> {
    let mut result = Vec::new();
    for def in definitions {
        result.push(create_stop_trigger(def).await?);
    }
    Ok(result)
}
