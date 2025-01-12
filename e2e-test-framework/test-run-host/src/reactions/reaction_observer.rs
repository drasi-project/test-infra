use std::{fmt::{self, Debug, Formatter}, sync::Arc, time::SystemTime};

use futures::future::join_all;
use serde::Serialize;
use time::{OffsetDateTime, format_description};
use tokio::{sync::{mpsc::{Receiver, Sender}, oneshot, Mutex}, task::JoinHandle};

use test_data_store::{
    test_repo_storage::models::TestReactionDefinition, 
    test_run_storage::{TestRunReactionId, TestRunReactionStorage}
};

use crate::reactions::reaction_collector::create_reaction_collector;

use super::{reaction_collector::{ReactionCollector, ReactionCollectorMessage, ReactionOutputRecord}, reaction_loggers::{create_reaction_logger, ReactionLogger}, TestRunReactionLoggerConfig};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReactionObserverStatus {
    Running,
    Paused,
    Stopped,
    Finished,
    Error
}

impl ReactionObserverStatus {
    pub fn is_active(&self) -> bool {
        match self {
            ReactionObserverStatus::Running => true,
            ReactionObserverStatus::Paused => true,
            _ => false,
        }
    }

    pub fn is_processing(&self) -> bool {
        match self {
            ReactionObserverStatus::Running => true,
            _ => false,
        }
    }
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
    #[error("ScriptSourceChangeGenerator is already finished. Reset to start over.")]
    AlreadyFinished,
    #[error("ScriptSourceChangeGenerator is already stopped. Reset to start over.")]
    AlreadyStopped,
    #[error("ScriptSourceChangeGenerator is currently in an Error state - {0:?}")]
    Error(ReactionObserverStatus),
    #[error("ScriptSourceChangeGenerator is currently Running. Pause before trying to Reset.")]
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

#[derive(Debug,)]
pub struct ReactionObserverMessage {
    pub command: ReactionObserverCommand,
    pub response_tx: Option<oneshot::Sender<ReactionObserverMessageResponse>>,
}

#[derive(Debug)]
pub struct ReactionObserverMessageResponse {
    pub result: anyhow::Result<()>,
    pub state: ReactionObserverExternalState,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReactionObserver {
    settings: ReactionObserverSettings,
    #[serde(skip_serializing)]
    script_processor_tx_channel: Sender<ReactionObserverMessage>,
    #[serde(skip_serializing)]
    _script_processor_thread_handle: Arc<Mutex<JoinHandle<anyhow::Result<()>>>>,
}

impl ReactionObserver {
    pub async fn new(
        test_run_reaction_id: TestRunReactionId, 
        definition: TestReactionDefinition, 
        output_storage: TestRunReactionStorage,
        loggers: Vec<TestRunReactionLoggerConfig>,
    ) -> anyhow::Result<Self> {
        let settings = ReactionObserverSettings::new(test_run_reaction_id, definition, output_storage.clone(), loggers).await?;
        log::debug!("Creating ScriptSourceChangeGenerator from {:?}", &settings);

        let (script_processor_tx_channel, script_processor_rx_channel) = tokio::sync::mpsc::channel(100);
        let script_processor_thread_handle = tokio::spawn(observer_thread(script_processor_rx_channel, settings.clone()));

        Ok(Self {
            settings,
            script_processor_tx_channel,
            _script_processor_thread_handle: Arc::new(Mutex::new(script_processor_thread_handle)),
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

        let r = self.script_processor_tx_channel.send(ReactionObserverMessage {
            command,
            response_tx: Some(response_tx),
        }).await;

        match r {
            Ok(_) => {
                let player_response = response_rx.await?;

                Ok(ReactionObserverCommandResponse {
                    result: player_response.result,
                    state: super::ReactionObserverState {
                        status: player_response.state.status,
                        state: serde_json::to_value(player_response.state).unwrap(),
                    },
                })
            },
            Err(e) => anyhow::bail!("Error sending command to ScriptSourceChangeGenerator: {:?}", e),
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
    pub status: ReactionObserverStatus,
    pub test_run_reaction_id: TestRunReactionId,
}

impl From<&mut ReactionObserverInternalState> for ReactionObserverExternalState {
    fn from(state: &mut ReactionObserverInternalState) -> Self {
        Self {
            error_messages: state.error_messages.clone(),
            status: state.status,
            test_run_reaction_id: state.settings.id.clone(),
        }
    }
}

pub struct ReactionObserverInternalState {
    pub collector: Box<dyn ReactionCollector + Send + Sync>,
    pub collector_tx_channel: Sender<ReactionCollectorMessage>,
    pub dispatchers: Vec<Box<dyn ReactionLogger + Send>>,
    pub error_messages: Vec<String>,
    pub message_seq_num: u64,
    pub next_event: Option<ReactionOutputRecord>,
    pub settings: ReactionObserverSettings,
    pub status: ReactionObserverStatus,
    pub stats: ReactionObserverStats,
}

impl ReactionObserverInternalState {

    async fn initialize(settings: ReactionObserverSettings) -> anyhow::Result<(Self, Receiver<ReactionCollectorMessage>)> {
        log::info!("Initializing ScriptSourceChangeGenerator using {:?}", settings);
    
        // Create the change stream.
        let collector = create_reaction_collector(settings.id.clone(), settings.definition.clone()).await?;
        
        // Create the dispatchers
        let mut dispatchers: Vec<Box<dyn ReactionLogger + Send>> = Vec::new();
        for def in settings.loggers.iter() {
            match create_reaction_logger(def, &settings.output_storage).await {
                Ok(dispatcher) => dispatchers.push(dispatcher),
                Err(e) => {
                    anyhow::bail!("Error creating ReactionDataDispatcher: {:?}; Error: {:?}", def, e);
                }
            }
        }

        // Create the channels used for message passing.
        let (collector_tx_channel, collector_rx_channel) = tokio::sync::mpsc::channel(100);
    
        let state = Self {
            collector,
            collector_tx_channel,
            dispatchers,
            error_messages: Vec::new(),
            message_seq_num: 0,
            next_event: None,
            settings,
            status: ReactionObserverStatus::Paused,
            stats: ReactionObserverStats::default(),
        };
    
        Ok((state, collector_rx_channel))
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
        
    async fn dispatch_reaction_event(&mut self, _event: &ReactionOutputRecord) {
        // let dispatchers = &mut self.dispatchers;

        // log::debug!("Dispatching SourceChangeEvents - #dispatchers:{}, #events:{}", dispatchers.len(), events.len());

        // let futures: Vec<_> = dispatchers.iter_mut()
        //     .map(|dispatcher| {
        //         let events = events.clone();
        //         async move {
        //             let _ = dispatcher.dispatch_reaction_data(events).await;
        //         }
        //     })
        //     .collect();

        // // Wait for all of them to complete
        // // TODO - Handle errors properly.
        // let _ = join_all(futures).await;
    }

    // Function to log the Player State at varying levels of detail.
    fn log_state(&self, msg: &str) {
        match log::max_level() {
            log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, self),
            log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, self),
            _ => {}
        }
    }

    async fn process_collector_message(&mut self, message: ReactionCollectorMessage) -> anyhow::Result<()> {
        log::trace!("Received reaction collector message: {:?}", message);
    
        // Process the evmessageent.
        match message {
            ReactionCollectorMessage::Event(event) => {
                self.stats.num_reaction_change_events += 1;
    
                // Dispatch the event to the dispatchers.
                self.dispatch_reaction_event(&event).await;
    
                // Increment the message sequence number.
                self.message_seq_num += 1;
            },
            ReactionCollectorMessage::Error(error) => {
                self.transition_to_error_state(&format!("Error in change stream: {:?}", error), None);
            },

        };
    
        Ok(())
    }    

    async fn process_command_message(&mut self, message: ReactionObserverMessage) -> anyhow::Result<()> {
        log::debug!("Received command message: {:?}", message.command);
    
        if let ReactionObserverCommand::GetState = message.command {
            let message_response = ReactionObserverMessageResponse {
                result: Ok(()),
                state: self.into(),
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
        let mut dispatchers: Vec<Box<dyn ReactionLogger + Send>> = Vec::new();
        for def in self.settings.loggers.iter() {
            match create_reaction_logger(def, &self.settings.output_storage).await {
                Ok(dispatcher) => dispatchers.push(dispatcher),
                Err(e) => {
                    anyhow::bail!("Error creating ReactionDataDispatcher: {:?}; Error: {:?}", def, e);
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
    
        // If we are unpausing for the first time, we need to initialize the start times based on time_mode config.
        // if self.previous_record.is_none() && 
        //     matches!(command, ReactionObserverCommand::Start) {
        //     self.stats.actual_start_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        // }
    
        match command {
            ReactionObserverCommand::GetState => Ok(()),
            ReactionObserverCommand::Pause => Ok(()),
            ReactionObserverCommand::Reset => self.reset().await,
            ReactionObserverCommand::Start => {
                log::info!("Script Started for TestRunSource {}", self.settings.id);
                
                self.status = ReactionObserverStatus::Running;
                self.collector.start().await
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
                self.collector.pause().await?;
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

    // async fn transition_to_finished_state(&mut self) {
    //     log::info!("Script Finished for TestRunSource {}", self.settings.id);
    
    //     self.status = ReactionObserverStatus::Finished;
    //     self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        
    //     self.close_dispatchers().await;
    //     self.write_result_summary().await.ok();
    // }
    
    async fn transition_to_stopped_state(&mut self) {
        log::info!("Script Stopped for TestRunSource {}", self.settings.id);
    
        self.status = ReactionObserverStatus::Stopped;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

        self.close_dispatchers().await;
        self.write_result_summary().await.ok();
    }
    
    fn transition_to_error_state(&mut self, error_message: &str, error: Option<&anyhow::Error>) {    
        self.status = ReactionObserverStatus::Error;
    
        let msg = match error {
            Some(e) => format!("{}: {:?}", error_message, e),
            None => error_message.to_string(),
        };
    
        self.log_state(&msg);
    
        self.error_messages.push(msg);
    }    

    pub async fn write_result_summary(&mut self) -> anyhow::Result<()> {

        let result_summary: ReactionObserverResultSummary = self.into();
        log::info!("Stats for TestRunSource:\n{:#?}", &result_summary);
    
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
        f.debug_struct("ScriptSourceChangeGeneratorInternalState")
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
    pub num_reaction_change_events: u64,
}

impl Default for ReactionObserverStats {
    fn default() -> Self {
        Self {
            actual_start_time_ns: 0,
            actual_end_time_ns: 0,
            num_reaction_change_events: 0,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct ReactionObserverResultSummary {
    pub actual_start_time: String,
    pub actual_start_time_ns: u64,
    pub actual_end_time: String,
    pub actual_end_time_ns: u64,
    pub num_reaction_change_events: u64,
    pub run_duration_ns: u64,
    pub run_duration_sec: f64,
    pub processing_rate: f64,
    pub test_run_reaction_id: String,
}

impl From<&mut ReactionObserverInternalState> for ReactionObserverResultSummary {
    fn from(state: &mut ReactionObserverInternalState) -> Self {
        let run_duration_ns = state.stats.actual_end_time_ns - state.stats.actual_start_time_ns;
        let run_duration_sec = run_duration_ns as f64 / 1_000_000_000.0;

        Self {
            actual_start_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.actual_start_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            actual_start_time_ns: state.stats.actual_start_time_ns,
            actual_end_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.actual_end_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            actual_end_time_ns: state.stats.actual_end_time_ns,
            num_reaction_change_events: state.stats.num_reaction_change_events,
            run_duration_ns,
            run_duration_sec,
            processing_rate: state.stats.num_reaction_change_events as f64 / run_duration_sec,
            test_run_reaction_id: state.settings.id.to_string(),
        }
    }
}

impl Debug for ReactionObserverResultSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let start_time = format!("{} ({} ns)", self.actual_start_time, self.actual_start_time_ns);
        let end_time = format!("{} ({} ns)", self.actual_end_time, self.actual_end_time_ns);
        let run_duration = format!("{} sec ({} ns)", self.run_duration_sec, self.run_duration_ns, );
        let processing_rate = format!("{:.2} changes / sec", self.processing_rate);

        f.debug_struct("ScriptSourceChangeGeneratorResultSummary")
            .field("test_run_source_id", &self.test_run_reaction_id)
            .field("start_time", &start_time)
            .field("end_time", &end_time)
            .field("run_duration", &run_duration)
            .field("num_reaction_change_events", &self.num_reaction_change_events)
            .field("processing_rate", &processing_rate)
            .finish()
    }
}


pub async fn observer_thread(mut command_rx_channel: Receiver<ReactionObserverMessage>, settings: ReactionObserverSettings) -> anyhow::Result<()>{
    log::info!("Reaction observer thread started for TestRunSource {} ...", settings.id);

    // The ScriptSourceChangeGenerator always starts with the first script record loaded and Paused.
    let (mut state, mut collector_rx_channel) = match ReactionObserverInternalState::initialize(settings).await {
        Ok((state, change_rx_channel)) => (state, change_rx_channel),
        Err(e) => {
            // If initialization fails, don't dont transition to an error state, just log an error and exit the thread.
            let msg = format!("Error initializing ScriptSourceChangeGenerator: {:?}", e);
            log::error!("{}", msg);
            anyhow::bail!(msg);
        }
    };

    // Loop to process commands sent to the ScriptSourceChangeGenerator or read from the Change Stream.
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

            // Process messages from the Reaction Stream.
            collector_message = collector_rx_channel.recv() => {
                match collector_message {
                    Some(collector_message) => {
                        state.process_collector_message(collector_message).await
                            .inspect_err(|e| state.transition_to_error_state("Error calling process_collector_message", Some(e))).ok();
                    }
                    None => {
                        state.transition_to_error_state("Collector channel closed.", None);
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