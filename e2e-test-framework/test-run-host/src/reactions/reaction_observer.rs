use std::{fmt::{self, Debug, Formatter}, sync::Arc, time::SystemTime};

use futures::future::join_all;
use serde::Serialize;
use time::{OffsetDateTime, format_description};
use tokio::{sync::{mpsc::{Receiver, Sender}, oneshot, Mutex}, task::JoinHandle};

use test_data_store::{
    test_repo_storage::models::TestReactionDefinition, 
    test_run_storage::{TestRunReactionId, TestRunReactionStorage}
};

use crate::reactions::reaction_handlers::create_reaction_handler;

use super::{reaction_handlers::{ReactionHandler, ReactionHandlerMessage, ReactionOutputRecord}, reaction_loggers::{create_reaction_logger, ReactionLogger}, TestRunReactionLoggerConfig};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReactionObserverStatus {
    Running,
    Paused,
    Stopped,
    Finished,
    Error
}

impl Serialize for ReactionObserverStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            ReactionObserverStatus::Running => serializer.serialize_str("Running"),
            ReactionObserverStatus::Paused => serializer.serialize_str("Paused"),
            ReactionObserverStatus::Stopped => serializer.serialize_str("Stopped"),
            ReactionObserverStatus::Finished => serializer.serialize_str("Finished"),
            ReactionObserverStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReactionObserverError {
    #[error("ReactionObserver is already finished. Reset to start over.")]
    AlreadyFinished,
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
    pub state: ReactionObserverState,
}

#[derive(Debug, Serialize)]
pub struct ReactionObserverState {    
    state: serde_json::Value,
    status: ReactionObserverStatus,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReactionObserverSettings {
    pub definition: TestReactionDefinition,
    pub id: TestRunReactionId,
    pub loggers: Vec<TestRunReactionLoggerConfig>,
    pub output_storage: TestRunReactionStorage,
}

impl ReactionObserverSettings {
    pub async fn new(
        test_run_reaction_id: TestRunReactionId, 
        definition: TestReactionDefinition, 
        output_storage: TestRunReactionStorage,
        loggers: Vec<TestRunReactionLoggerConfig>,
    ) -> anyhow::Result<Self> {

        Ok(ReactionObserverSettings {
            definition,
            id: test_run_reaction_id,
            loggers,
            output_storage,
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

pub struct ReactionObserver {
    settings: ReactionObserverSettings,
    observer_tx_channel: Sender<ReactionObserverMessage>,
    _observer_thread_handle: Arc<Mutex<JoinHandle<anyhow::Result<()>>>>,
}

impl ReactionObserver {
    pub async fn new(
        test_run_reaction_id: TestRunReactionId, 
        definition: TestReactionDefinition, 
        output_storage: TestRunReactionStorage,
        loggers: Vec<TestRunReactionLoggerConfig>,
    ) -> anyhow::Result<Self> {
        let settings = ReactionObserverSettings::new(test_run_reaction_id, definition, output_storage.clone(), loggers).await?;
        log::debug!("Creating ReactionObserver from {:?}", &settings);

        let (observer_tx_channel, observer_rx_channel) = tokio::sync::mpsc::channel(100);
        let observer_thread_handle = tokio::spawn(observer_thread(observer_rx_channel, settings.clone()));

        Ok(Self {
            settings,
            observer_tx_channel,
            _observer_thread_handle: Arc::new(Mutex::new(observer_thread_handle)),
        })
    }

    pub fn get_id(&self) -> TestRunReactionId {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> ReactionObserverSettings {
        self.settings.clone()
    }

    async fn send_command(&self, command: ReactionObserverCommand) -> anyhow::Result<ReactionObserverCommandResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self.observer_tx_channel.send(ReactionObserverMessage {
            command,
            response_tx: Some(response_tx),
        }).await;

        match r {
            Ok(_) => {
                let observer_response = response_rx.await?;

                Ok(ReactionObserverCommandResponse {
                    result: observer_response.result,
                    state: super::ReactionObserverState {
                        status: observer_response.state.status,
                        state: serde_json::to_value(observer_response.state).unwrap(),
                    },
                })
            },
            Err(e) => anyhow::bail!("Error sending command to ReactionObserver: {:?}", e),
        }
    }

    pub async fn get_state(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        self.send_command(ReactionObserverCommand::GetState).await
    }

    pub async fn pause(&self) -> anyhow::Result<ReactionObserverCommandResponse>  {
        self.send_command(ReactionObserverCommand::Pause).await
    }

    pub async fn reset(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        self.send_command(ReactionObserverCommand::Reset).await
    }

    pub async fn start(&self) -> anyhow::Result<ReactionObserverCommandResponse> {
        self.send_command(ReactionObserverCommand::Start).await
    }

    pub async fn stop(&self) -> anyhow::Result<ReactionObserverCommandResponse>  {
        self.send_command(ReactionObserverCommand::Stop).await
    }
}

#[derive(Debug, Serialize)]
pub struct ReactionObserverExternalState {
    pub error_messages: Vec<String>,
    pub result_summary: ReactionObserverResultSummary,
    pub stats: ReactionObserverStats,
    pub status: ReactionObserverStatus,
    pub test_run_reaction_id: TestRunReactionId,
}

impl From<&ReactionObserverInternalState> for ReactionObserverExternalState {
    fn from(state: &ReactionObserverInternalState) -> Self {
        Self {
            error_messages: state.error_messages.clone(),
            result_summary: ReactionObserverResultSummary::from(state),
            stats: state.stats.clone(),
            status: state.status,
            test_run_reaction_id: state.settings.id.clone(),
        }
    }
}

pub struct ReactionObserverInternalState {
    pub reaction_handler: Box<dyn ReactionHandler + Send + Sync>,
    pub reaction_handler_rx_channel: Receiver<ReactionHandlerMessage>,
    pub loggers: Vec<Box<dyn ReactionLogger + Send>>,
    pub error_messages: Vec<String>,
    pub record_seq_num: u64,
    pub next_event: Option<ReactionOutputRecord>,
    pub settings: ReactionObserverSettings,
    pub status: ReactionObserverStatus,
    pub stats: ReactionObserverStats,
}

impl ReactionObserverInternalState {

    async fn initialize(settings: ReactionObserverSettings) -> anyhow::Result<Self> {
        log::debug!("Initializing ReactionObserver using {:?}", settings);
    
        let reaction_handler = create_reaction_handler(settings.id.clone(), settings.definition.clone()).await?;
        let reaction_handler_rx_channel = reaction_handler.init().await?;
        
        // Create the loggers
        let mut loggers: Vec<Box<dyn ReactionLogger + Send>> = Vec::new();
        for logger_cfg in settings.loggers.iter() {
            match create_reaction_logger(logger_cfg, &settings.output_storage).await {
                Ok(logger) => loggers.push(logger),
                Err(e) => {
                    anyhow::bail!("Error creating ReactionLogger: {:?}; Error: {:?}", logger_cfg, e);
                }
            }
        }
    
        Ok(Self {
            reaction_handler,
            reaction_handler_rx_channel,
            loggers,
            error_messages: Vec::new(),
            record_seq_num: 0,
            next_event: None,
            settings,
            status: ReactionObserverStatus::Paused,
            stats: ReactionObserverStats::default(),
        })
    }

    async fn close_loggers(&mut self) {
        let loggers = &mut self.loggers;
    
        log::debug!("Closing loggers - #loggers:{}", loggers.len());
    
        let futures: Vec<_> = loggers.iter_mut()
            .map(|logger| {
                async move {
                    let _ = logger.close().await;
                }
            })
            .collect();
    
        // Wait for all of them to complete
        // TODO - Handle errors properly.
        let _ = join_all(futures).await;
    }

    fn log_observer_state(&self, msg: &str) {
        match log::max_level() {
            log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, self),
            log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, self),
            _ => {}
        }
    }

    async fn log_reaction_record(&mut self, record: &ReactionOutputRecord) {
        let loggers = &mut self.loggers;

        // log::debug!("Logging ReactionOutputRecord - #loggers:{}", loggers.len());
        // log::trace!("Logging ReactionOutputRecord: {:?}", record);

        let futures: Vec<_> = loggers.iter_mut()
            .map(|logger| {
                async move {
                    let _ = logger.log_reaction_record(record).await;
                }
            })
            .collect();

        // Wait for all of them to complete
        // TODO - Handle errors properly.
        let _ = join_all(futures).await;
    }

    async fn process_reaction_handler_message(&mut self, message: ReactionHandlerMessage) -> anyhow::Result<()> {
        log::trace!("Received Reaction Handler message: {:?}", message);
    
        match message {
            ReactionHandlerMessage::Record(record) => {
                // Update the stats
                if self.stats.num_reaction_records == 0 {
                    self.stats.first_result_time_ns = record.receive_time_ns;
                }
                self.stats.num_reaction_records += 1;
                self.stats.last_result_time_ns = record.receive_time_ns;
    
                self.log_reaction_record(&record).await;
    
                // Increment the record sequence number.
                self.record_seq_num += 1;
            },
            ReactionHandlerMessage::Error(error) => {
                self.transition_to_error_state(&format!("Error in Reaction Handler stream: {:?}", error), None);
            },
            ReactionHandlerMessage::TestCompleted => {
                self.transition_to_finished_state().await;
            },

        };
    
        Ok(())
    }    

    async fn process_command_message(&mut self, message: ReactionObserverMessage) -> anyhow::Result<()> {
        log::debug!("Received ReactionObserver command message: {:?}", message.command);
    
        if let ReactionObserverCommand::GetState = message.command {
            let message_response = ReactionObserverMessageResponse {
                result: Ok(()),
                state: (&*self).into(),
            };
    
            let r = message.response_tx.unwrap().send(message_response);
            if let Err(e) = r {
                anyhow::bail!("Error sending message response back to caller: {:?}", e);
            }
        } else {
            let transition_response = match self.status {
                ReactionObserverStatus::Running => self.transition_from_running_state(&message.command).await,
                ReactionObserverStatus::Paused => self.transition_from_paused_state(&message.command).await,
                ReactionObserverStatus::Stopped => self.transition_from_stopped_state(&message.command).await,
                ReactionObserverStatus::Finished => self.transition_from_finished_state(&message.command).await,
                ReactionObserverStatus::Error => self.transition_from_error_state(&message.command).await,
            };
    
            if message.response_tx.is_some() {
                let message_response = ReactionObserverMessageResponse {
                    result: transition_response,
                    state: (&*self).into(),
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

        // Create the new loggers
        self.close_loggers().await;    
        let mut loggers: Vec<Box<dyn ReactionLogger + Send>> = Vec::new();
        for logger_cfg in self.settings.loggers.iter() {
            match create_reaction_logger(logger_cfg, &self.settings.output_storage).await {
                Ok(logger) => loggers.push(logger),
                Err(e) => {
                    anyhow::bail!("Error creating ReactionLogger: {:?}; Error: {:?}", logger_cfg, e);
                }
            }
        }    

        self.loggers = loggers;
        self.error_messages = Vec::new();
        self.record_seq_num = 0;
        self.next_event = None;
        self.status = ReactionObserverStatus::Paused;
        self.stats = ReactionObserverStats::default();
    
        Ok(())
    }

    async fn transition_from_error_state(&mut self, command: &ReactionObserverCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let ReactionObserverCommand::Reset = command {
            self.reset().await
        } else {
            Err(ReactionObserverError::Error(self.status).into())
        }
    }
    
    async fn transition_from_finished_state(&mut self, command: &ReactionObserverCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let ReactionObserverCommand::Reset = command {
            self.reset().await
        } else {
            Err(ReactionObserverError::AlreadyFinished.into())
        }
    }
    
    async fn transition_from_paused_state(&mut self, command: &ReactionObserverCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        match command {
            ReactionObserverCommand::GetState => Ok(()),
            ReactionObserverCommand::Pause => Ok(()),
            ReactionObserverCommand::Reset => self.reset().await,
            ReactionObserverCommand::Start => {
                log::info!("ReactionObserver Started for TestRunReaction {}", self.settings.id);
                
                self.status = ReactionObserverStatus::Running;
                if self.stats.actual_start_time_ns == 0 {
                    self.stats.actual_start_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
                }
                self.reaction_handler.start().await
            },
            ReactionObserverCommand::Stop => Ok(self.transition_to_stopped_state().await),
        }
    }
    
    async fn transition_from_running_state(&mut self, command: &ReactionObserverCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        match command {
            ReactionObserverCommand::GetState => Ok(()),
            ReactionObserverCommand::Pause => {
                self.status = ReactionObserverStatus::Paused;
                self.reaction_handler.pause().await?;
                Ok(())
            },
            ReactionObserverCommand::Reset => {
                Err(ReactionObserverError::PauseToReset.into())
            },
            ReactionObserverCommand::Start => Ok(()),
            ReactionObserverCommand::Stop => {
                Ok(self.transition_to_stopped_state().await)
            },
        }
    }
    
    async fn transition_from_stopped_state(&mut self, command: &ReactionObserverCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let ReactionObserverCommand::Reset = command {
            self.reset().await
        } else {
            Err(ReactionObserverError::AlreadyStopped.into())
        }
    }    

    async fn transition_to_finished_state(&mut self) {
        log::info!("ReactionObserver Finished for TestRunReaction {}", self.settings.id);
    
        self.status = ReactionObserverStatus::Finished;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.reaction_handler.stop().await.ok();
        
        self.close_loggers().await;
        self.write_result_summary().await.ok();
    }
    
    async fn transition_to_stopped_state(&mut self) {
        log::info!("ReactionObserver Stopped for TestRunReaction {}", self.settings.id);
    
        self.status = ReactionObserverStatus::Stopped;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.reaction_handler.stop().await.ok();

        self.close_loggers().await;
        self.write_result_summary().await.ok();
    }
    
    fn transition_to_error_state(&mut self, error_message: &str, error: Option<&anyhow::Error>) {    
        self.status = ReactionObserverStatus::Error;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
    
        let msg = match error {
            Some(e) => format!("{}: {:?}", error_message, e),
            None => error_message.to_string(),
        };
        self.error_messages.push(msg.clone());
    
        self.log_observer_state(&msg);
    }    

    pub async fn write_result_summary(&self) -> anyhow::Result<()> {

        let result_summary: ReactionObserverResultSummary = self.into();
        log::info!("Stats for TestRunReaction:\n{:#?}", &result_summary);
    
        let result_summary_value = serde_json::to_value(result_summary).unwrap();
        match self.settings.output_storage.write_result_summary(&result_summary_value).await {
            Ok(_) => Ok(()),
            Err(e) => {
                log::error!("Error writing result summary to output storage: {:?}", e);
                Err(e)
            }
        }
    }    
}


impl Debug for ReactionObserverInternalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReactionObserverInternalState")
            .field("error_messages", &self.error_messages)
            .field("next_record", &self.next_event)
            .field("status", &self.status)
            .field("stats", &self.stats)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ReactionObserverStats {
    pub actual_start_time_ns: u64,
    pub actual_end_time_ns: u64,
    pub first_result_time_ns: u64,
    pub last_result_time_ns: u64,
    pub num_reaction_records: u64,
}

impl Default for ReactionObserverStats {
    fn default() -> Self {
        Self {
            actual_start_time_ns: 0,
            actual_end_time_ns: 0,
            first_result_time_ns: 0,
            last_result_time_ns: 0,
            num_reaction_records: 0,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct ReactionObserverResultSummary {
    pub actual_start_time: String,
    pub actual_start_time_ns: u64,
    pub actual_end_time: String,
    pub actual_end_time_ns: u64,
    pub first_result_time: String,
    pub first_result_time_ns: u64,
    pub last_result_time: String,
    pub last_result_time_ns: u64,
    pub num_reaction_records: u64,
    pub run_duration_ns: u64,
    pub run_duration_sec: f64,
    pub processing_rate: f64,
    pub test_run_reaction_id: String,
    pub time_since_last_result_ns: u64,
}

impl From<&ReactionObserverInternalState> for ReactionObserverResultSummary {
    fn from(state: &ReactionObserverInternalState) -> Self {
        let now_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

        let run_duration_ns = state.stats.last_result_time_ns - state.stats.first_result_time_ns;
        let run_duration_sec = run_duration_ns as f64 / 1_000_000_000.0;

        Self {
            actual_start_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.actual_start_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            actual_start_time_ns: state.stats.actual_start_time_ns,
            actual_end_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.actual_end_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            actual_end_time_ns: state.stats.actual_end_time_ns,
            first_result_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.first_result_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            first_result_time_ns: state.stats.first_result_time_ns,
            last_result_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.last_result_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            last_result_time_ns: state.stats.last_result_time_ns,
            num_reaction_records: state.stats.num_reaction_records,
            run_duration_ns,
            run_duration_sec,
            processing_rate: state.stats.num_reaction_records as f64 / run_duration_sec,
            test_run_reaction_id: state.settings.id.to_string(),
            time_since_last_result_ns: now_ns - state.stats.last_result_time_ns,
        }
    }
}

impl Debug for ReactionObserverResultSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let start_time = format!("{} ({} ns)", self.actual_start_time, self.actual_start_time_ns);
        let end_time = format!("{} ({} ns)", self.actual_end_time, self.actual_end_time_ns);
        let first_time = format!("{} ({} ns)", self.first_result_time, self.first_result_time_ns);
        let last_time = format!("{} ({} ns)", self.last_result_time, self.last_result_time_ns);
        let run_duration = format!("{} sec ({} ns)", self.run_duration_sec, self.run_duration_ns, );
        let processing_rate = format!("{:.2} changes / sec", self.processing_rate);

        f.debug_struct("ReactionObserverResultSummary")
            .field("test_run_source_id", &self.test_run_reaction_id)
            .field("start_time", &start_time)
            .field("end_time", &end_time)
            .field("first_result_time", &first_time)
            .field("last_result_time", &last_time)
            .field("time_since_last_result_ns", &self.time_since_last_result_ns)
            .field("run_duration", &run_duration)
            .field("num_reaction_change_events", &self.num_reaction_records)
            .field("processing_rate", &processing_rate)            
            .finish()
    }
}

pub async fn observer_thread(mut command_rx_channel: Receiver<ReactionObserverMessage>, settings: ReactionObserverSettings) -> anyhow::Result<()>{
    log::info!("ReactionObserver thread started for TestRunReaction {} ...", settings.id);

    let mut state = ReactionObserverInternalState::initialize(settings).await?;

    // Loop to process commands sent to the ReactionObserver or read from the Change Stream.
    loop {
        state.log_observer_state("Top of ReactionObserver processor loop");

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

            // Process messages from the Reaction Handler.
            reaction_handler_message = state.reaction_handler_rx_channel.recv() => {
                match reaction_handler_message {
                    Some(reaction_handler_message) => {
                        // log::trace!("Received Reaction Handler message: {:?}", reaction_handler_message);
                        state.process_reaction_handler_message(reaction_handler_message).await
                            .inspect_err(|e| state.transition_to_error_state("Error calling process_reaction_handler_message", Some(e))).ok();
                    }
                    None => {
                        state.transition_to_error_state("Reaction handler channel closed.", None);
                        break;
                    }
                }
            },

            else => {
                log::error!("ReactionObserver loop activated for {} but no command or reaction output to process.", state.settings.id);
            }
        }
    }

    log::info!("ReactionObserver thread exiting for TestRunReaction {} ...", state.settings.id);    
    Ok(())
}