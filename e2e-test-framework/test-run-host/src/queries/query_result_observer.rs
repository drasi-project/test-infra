use std::{fmt::{self, Debug, Formatter}, sync::Arc, time::SystemTime};

use futures::future::join_all;
use serde::Serialize;
use time::{OffsetDateTime, format_description};
use tokio::{sync::{mpsc::{Receiver, Sender}, oneshot, Mutex}, task::JoinHandle};

use test_data_store::{
    test_repo_storage::models::TestQueryDefinition, 
    test_run_storage::{TestRunQueryId, TestRunQueryStorage}
};

use crate::queries::result_stream_handlers::create_result_stream_handler;

use super::{result_stream_handlers::{ResultStreamHandler, ResultStreamHandlerMessage, ResultStreamRecord}, result_stream_loggers::{create_result_stream_logger, ResultStreamLogger}, ResultStreamLoggerConfig};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueryResultObserverStatus {
    Running,
    Paused,
    Stopped,
    Finished,
    Error
}

impl Serialize for QueryResultObserverStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            QueryResultObserverStatus::Running => serializer.serialize_str("Running"),
            QueryResultObserverStatus::Paused => serializer.serialize_str("Paused"),
            QueryResultObserverStatus::Stopped => serializer.serialize_str("Stopped"),
            QueryResultObserverStatus::Finished => serializer.serialize_str("Finished"),
            QueryResultObserverStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryResultObserverError {
    #[error("QueryResultObserver is already finished. Reset to start over.")]
    AlreadyFinished,
    #[error("QueryResultObserver is already stopped. Reset to start over.")]
    AlreadyStopped,
    #[error("QueryResultObserver is currently in an Error state - {0:?}")]
    Error(QueryResultObserverStatus),
    #[error("QueryResultObserver is currently Running. Pause before trying to Reset.")]
    PauseToReset,
}

#[derive(Debug)]
pub struct QueryResultObserverCommandResponse {
    pub result: anyhow::Result<()>,
    pub state: QueryResultObserverState,
}

#[derive(Debug, Serialize)]
pub struct QueryResultObserverState {    
    state: serde_json::Value,
    status: QueryResultObserverStatus,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryResultObserverSettings {
    pub definition: TestQueryDefinition,
    pub id: TestRunQueryId,
    pub loggers: Vec<ResultStreamLoggerConfig>,
    pub output_storage: TestRunQueryStorage,
}

impl QueryResultObserverSettings {
    pub async fn new(
        test_run_query_id: TestRunQueryId, 
        definition: TestQueryDefinition, 
        output_storage: TestRunQueryStorage,
        loggers: Vec<ResultStreamLoggerConfig>,
    ) -> anyhow::Result<Self> {

        Ok(QueryResultObserverSettings {
            definition,
            id: test_run_query_id,
            loggers,
            output_storage,
        })
    }

    pub fn get_id(&self) -> TestRunQueryId {
        self.id.clone()
    }
}

#[derive(Debug)]
pub enum QueryResultObserverCommand {
    GetState,
    Pause,
    Reset,
    Start,
    Stop,
}

pub struct QueryResultObserverMessage {
    pub command: QueryResultObserverCommand,
    pub response_tx: Option<oneshot::Sender<QueryResultObserverMessageResponse>>,
}

#[derive(Debug)]
pub struct QueryResultObserverMessageResponse {
    pub result: anyhow::Result<()>,
    pub state: QueryResultObserverExternalState,
}

pub struct QueryResultObserver {
    settings: QueryResultObserverSettings,
    observer_tx_channel: Sender<QueryResultObserverMessage>,
    _observer_thread_handle: Arc<Mutex<JoinHandle<anyhow::Result<()>>>>,
}

impl QueryResultObserver {
    pub async fn new(
        test_run_query_id: TestRunQueryId, 
        definition: TestQueryDefinition, 
        output_storage: TestRunQueryStorage,
        loggers: Vec<ResultStreamLoggerConfig>,
    ) -> anyhow::Result<Self> {
        let settings = QueryResultObserverSettings::new(test_run_query_id, definition, output_storage.clone(), loggers).await?;
        log::debug!("Creating QueryResultObserver from {:?}", &settings);

        let (observer_tx_channel, observer_rx_channel) = tokio::sync::mpsc::channel(100);
        let observer_thread_handle = tokio::spawn(observer_thread(observer_rx_channel, settings.clone()));

        Ok(Self {
            settings,
            observer_tx_channel,
            _observer_thread_handle: Arc::new(Mutex::new(observer_thread_handle)),
        })
    }

    pub fn get_id(&self) -> TestRunQueryId {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> QueryResultObserverSettings {
        self.settings.clone()
    }

    async fn send_command(&self, command: QueryResultObserverCommand) -> anyhow::Result<QueryResultObserverCommandResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self.observer_tx_channel.send(QueryResultObserverMessage {
            command,
            response_tx: Some(response_tx),
        }).await;

        match r {
            Ok(_) => {
                let observer_response = response_rx.await?;

                Ok(QueryResultObserverCommandResponse {
                    result: observer_response.result,
                    state: super::QueryResultObserverState {
                        status: observer_response.state.status,
                        state: serde_json::to_value(observer_response.state).unwrap(),
                    },
                })
            },
            Err(e) => anyhow::bail!("Error sending command to QueryResultObserver: {:?}", e),
        }
    }

    pub async fn get_state(&self) -> anyhow::Result<QueryResultObserverCommandResponse> {
        self.send_command(QueryResultObserverCommand::GetState).await
    }

    pub async fn pause(&self) -> anyhow::Result<QueryResultObserverCommandResponse>  {
        self.send_command(QueryResultObserverCommand::Pause).await
    }

    pub async fn reset(&self) -> anyhow::Result<QueryResultObserverCommandResponse> {
        self.send_command(QueryResultObserverCommand::Reset).await
    }

    pub async fn start(&self) -> anyhow::Result<QueryResultObserverCommandResponse> {
        self.send_command(QueryResultObserverCommand::Start).await
    }

    pub async fn stop(&self) -> anyhow::Result<QueryResultObserverCommandResponse>  {
        self.send_command(QueryResultObserverCommand::Stop).await
    }
}

#[derive(Debug, Serialize)]
pub struct QueryResultObserverExternalState {
    pub error_messages: Vec<String>,
    pub result_summary: QueryResultObserverResultSummary,
    pub stats: QueryResultObserverStats,
    pub status: QueryResultObserverStatus,
    pub test_run_query_id: TestRunQueryId,
}

impl From<&QueryResultObserverInternalState> for QueryResultObserverExternalState {
    fn from(state: &QueryResultObserverInternalState) -> Self {
        Self {
            error_messages: state.error_messages.clone(),
            result_summary: QueryResultObserverResultSummary::from(state),
            stats: state.stats.clone(),
            status: state.status,
            test_run_query_id: state.settings.id.clone(),
        }
    }
}

pub struct QueryResultObserverInternalState {
    pub result_stream_handler: Box<dyn ResultStreamHandler + Send + Sync>,
    pub result_stream_handler_rx_channel: Receiver<ResultStreamHandlerMessage>,
    pub loggers: Vec<Box<dyn ResultStreamLogger + Send>>,
    pub error_messages: Vec<String>,
    pub record_seq_num: u64,
    pub next_event: Option<ResultStreamRecord>,
    pub settings: QueryResultObserverSettings,
    pub status: QueryResultObserverStatus,
    pub stats: QueryResultObserverStats,
}

impl QueryResultObserverInternalState {

    async fn initialize(settings: QueryResultObserverSettings) -> anyhow::Result<Self> {
        log::debug!("Initializing QueryResultObserver using {:?}", settings);
    
        let result_stream_handler = create_result_stream_handler(settings.id.clone(), settings.definition.result_stream_handler.clone()).await?;
        let result_stream_handler_rx_channel = result_stream_handler.init().await?;
        
        // Create the loggers
        let mut loggers: Vec<Box<dyn ResultStreamLogger + Send>> = Vec::new();
        for logger_cfg in settings.loggers.iter() {
            match create_result_stream_logger(logger_cfg, &settings.output_storage).await {
                Ok(logger) => loggers.push(logger),
                Err(e) => {
                    anyhow::bail!("Error creating ResultStreamLogger: {:?}; Error: {:?}", logger_cfg, e);
                }
            }
        }
    
        Ok(Self {
            result_stream_handler,
            result_stream_handler_rx_channel,
            loggers,
            error_messages: Vec::new(),
            record_seq_num: 0,
            next_event: None,
            settings,
            status: QueryResultObserverStatus::Paused,
            stats: QueryResultObserverStats::default(),
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

    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) {
        let loggers = &mut self.loggers;

        // log::debug!("Logging ResultStreamRecord - #loggers:{}", loggers.len());
        // log::trace!("Logging ResultStreamRecord: {:?}", record);

        let futures: Vec<_> = loggers.iter_mut()
            .map(|logger| {
                async move {
                    let _ = logger.log_result_stream_record(record).await;
                }
            })
            .collect();

        // Wait for all of them to complete
        // TODO - Handle errors properly.
        let _ = join_all(futures).await;
    }

    async fn process_result_stream_handler_message(&mut self, message: ResultStreamHandlerMessage) -> anyhow::Result<()> {
        log::trace!("Received ResultStreamHandlerMessage: {:?}", message);
    
        match message {
            ResultStreamHandlerMessage::Record(record) => {
                // Update the stats
                if self.stats.num_result_stream_records == 0 {
                    self.stats.first_result_time_ns = record.dequeue_time_ns;
                }
                self.stats.num_result_stream_records += 1;
                self.stats.last_result_time_ns = record.dequeue_time_ns;
    
                self.log_result_stream_record(&record).await;
    
                // Increment the record sequence number.
                self.record_seq_num += 1;
            },
            ResultStreamHandlerMessage::Error(error) => {
                self.transition_to_error_state(&format!("Error in ResultStreamHandler stream: {:?}", error), None);
            },
            ResultStreamHandlerMessage::TestCompleted => {
                self.transition_to_finished_state().await;
            },

        };
    
        Ok(())
    }    

    async fn process_command_message(&mut self, message: QueryResultObserverMessage) -> anyhow::Result<()> {
        log::debug!("Received QueryResultObserver command message: {:?}", message.command);
    
        if let QueryResultObserverCommand::GetState = message.command {
            let message_response = QueryResultObserverMessageResponse {
                result: Ok(()),
                state: (&*self).into(),
            };
    
            let r = message.response_tx.unwrap().send(message_response);
            if let Err(e) = r {
                anyhow::bail!("Error sending message response back to caller: {:?}", e);
            }
        } else {
            let transition_response = match self.status {
                QueryResultObserverStatus::Running => self.transition_from_running_state(&message.command).await,
                QueryResultObserverStatus::Paused => self.transition_from_paused_state(&message.command).await,
                QueryResultObserverStatus::Stopped => self.transition_from_stopped_state(&message.command).await,
                QueryResultObserverStatus::Finished => self.transition_from_finished_state(&message.command).await,
                QueryResultObserverStatus::Error => self.transition_from_error_state(&message.command).await,
            };
    
            if message.response_tx.is_some() {
                let message_response = QueryResultObserverMessageResponse {
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
        let mut loggers: Vec<Box<dyn ResultStreamLogger + Send>> = Vec::new();
        for logger_cfg in self.settings.loggers.iter() {
            match create_result_stream_logger(logger_cfg, &self.settings.output_storage).await {
                Ok(logger) => loggers.push(logger),
                Err(e) => {
                    anyhow::bail!("Error creating ResultStreamLogger: {:?}; Error: {:?}", logger_cfg, e);
                }
            }
        }    

        self.loggers = loggers;
        self.error_messages = Vec::new();
        self.record_seq_num = 0;
        self.next_event = None;
        self.status = QueryResultObserverStatus::Paused;
        self.stats = QueryResultObserverStats::default();
    
        Ok(())
    }

    async fn transition_from_error_state(&mut self, command: &QueryResultObserverCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let QueryResultObserverCommand::Reset = command {
            self.reset().await
        } else {
            Err(QueryResultObserverError::Error(self.status).into())
        }
    }
    
    async fn transition_from_finished_state(&mut self, command: &QueryResultObserverCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let QueryResultObserverCommand::Reset = command {
            self.reset().await
        } else {
            Err(QueryResultObserverError::AlreadyFinished.into())
        }
    }
    
    async fn transition_from_paused_state(&mut self, command: &QueryResultObserverCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        match command {
            QueryResultObserverCommand::GetState => Ok(()),
            QueryResultObserverCommand::Pause => Ok(()),
            QueryResultObserverCommand::Reset => self.reset().await,
            QueryResultObserverCommand::Start => {
                log::info!("QueryResultObserver Started for TestRunQuery {}", self.settings.id);
                
                self.status = QueryResultObserverStatus::Running;
                if self.stats.actual_start_time_ns == 0 {
                    self.stats.actual_start_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
                }
                self.result_stream_handler.start().await
            },
            QueryResultObserverCommand::Stop => Ok(self.transition_to_stopped_state().await),
        }
    }
    
    async fn transition_from_running_state(&mut self, command: &QueryResultObserverCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        match command {
            QueryResultObserverCommand::GetState => Ok(()),
            QueryResultObserverCommand::Pause => {
                self.status = QueryResultObserverStatus::Paused;
                self.result_stream_handler.pause().await?;
                Ok(())
            },
            QueryResultObserverCommand::Reset => {
                Err(QueryResultObserverError::PauseToReset.into())
            },
            QueryResultObserverCommand::Start => Ok(()),
            QueryResultObserverCommand::Stop => {
                Ok(self.transition_to_stopped_state().await)
            },
        }
    }
    
    async fn transition_from_stopped_state(&mut self, command: &QueryResultObserverCommand) -> anyhow::Result<()> {
        log::debug!("Attempting to transition from {:?} state via command: {:?}", self.status, command);
    
        if let QueryResultObserverCommand::Reset = command {
            self.reset().await
        } else {
            Err(QueryResultObserverError::AlreadyStopped.into())
        }
    }    

    async fn transition_to_finished_state(&mut self) {
        log::info!("QueryResultObserver Finished for TestRunQuery {}", self.settings.id);
    
        self.status = QueryResultObserverStatus::Finished;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.result_stream_handler.stop().await.ok();
        
        self.close_loggers().await;
        self.write_result_summary().await.ok();
    }
    
    async fn transition_to_stopped_state(&mut self) {
        log::info!("QueryResultObserver Stopped for TestRunQuery {}", self.settings.id);
    
        self.status = QueryResultObserverStatus::Stopped;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.result_stream_handler.stop().await.ok();

        self.close_loggers().await;
        self.write_result_summary().await.ok();
    }
    
    fn transition_to_error_state(&mut self, error_message: &str, error: Option<&anyhow::Error>) {    
        self.status = QueryResultObserverStatus::Error;
        self.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
    
        let msg = match error {
            Some(e) => format!("{}: {:?}", error_message, e),
            None => error_message.to_string(),
        };
        self.error_messages.push(msg.clone());
    
        self.log_observer_state(&msg);
    }    

    pub async fn write_result_summary(&self) -> anyhow::Result<()> {

        let result_summary: QueryResultObserverResultSummary = self.into();
        log::info!("Stats for TestRunQuery:\n{:#?}", &result_summary);
    
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


impl Debug for QueryResultObserverInternalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryResultObserverInternalState")
            .field("error_messages", &self.error_messages)
            .field("next_record", &self.next_event)
            .field("status", &self.status)
            .field("stats", &self.stats)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryResultObserverStats {
    pub actual_start_time_ns: u64,
    pub actual_end_time_ns: u64,
    pub first_result_time_ns: u64,
    pub last_result_time_ns: u64,
    pub num_result_stream_records: u64,
}

impl Default for QueryResultObserverStats {
    fn default() -> Self {
        Self {
            actual_start_time_ns: 0,
            actual_end_time_ns: 0,
            first_result_time_ns: 0,
            last_result_time_ns: 0,
            num_result_stream_records: 0,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct QueryResultObserverResultSummary {
    pub actual_start_time: String,
    pub actual_start_time_ns: u64,
    pub actual_end_time: String,
    pub actual_end_time_ns: u64,
    pub first_result_time: String,
    pub first_result_time_ns: u64,
    pub last_result_time: String,
    pub last_result_time_ns: u64,
    pub num_result_stream_records: u64,
    pub run_duration_ns: u64,
    pub run_duration_sec: f64,
    pub processing_rate: f64,
    pub test_run_query_id: String,
    pub time_since_last_result_ns: u64,
}

impl From<&QueryResultObserverInternalState> for QueryResultObserverResultSummary {
    fn from(state: &QueryResultObserverInternalState) -> Self {
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
            num_result_stream_records: state.stats.num_result_stream_records,
            run_duration_ns,
            run_duration_sec,
            processing_rate: state.stats.num_result_stream_records as f64 / run_duration_sec,
            test_run_query_id: state.settings.id.to_string(),
            time_since_last_result_ns: now_ns - state.stats.last_result_time_ns,
        }
    }
}

impl Debug for QueryResultObserverResultSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let start_time = format!("{} ({} ns)", self.actual_start_time, self.actual_start_time_ns);
        let end_time = format!("{} ({} ns)", self.actual_end_time, self.actual_end_time_ns);
        let first_time = format!("{} ({} ns)", self.first_result_time, self.first_result_time_ns);
        let last_time = format!("{} ({} ns)", self.last_result_time, self.last_result_time_ns);
        let run_duration = format!("{} sec ({} ns)", self.run_duration_sec, self.run_duration_ns, );
        let processing_rate = format!("{:.2} changes / sec", self.processing_rate);

        f.debug_struct("QueryResultObserverResultSummary")
            .field("test_run_source_id", &self.test_run_query_id)
            .field("start_time", &start_time)
            .field("end_time", &end_time)
            .field("first_result_time", &first_time)
            .field("last_result_time", &last_time)
            .field("time_since_last_result_ns", &self.time_since_last_result_ns)
            .field("run_duration", &run_duration)
            .field("num_result_stream_records", &self.num_result_stream_records)
            .field("processing_rate", &processing_rate)            
            .finish()
    }
}

pub async fn observer_thread(mut command_rx_channel: Receiver<QueryResultObserverMessage>, settings: QueryResultObserverSettings) -> anyhow::Result<()>{
    log::info!("QueryResultObserver thread started for TestRunQuery {} ...", settings.id);

    let mut state = QueryResultObserverInternalState::initialize(settings).await?;

    // Loop to process commands sent to the QueryResultObserver or read from the Change Stream.
    loop {
        state.log_observer_state("Top of QueryResultObserver processor loop");

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

            // Process messages from the Result Stream Handler.
            result_stream_handler_message = state.result_stream_handler_rx_channel.recv() => {
                match result_stream_handler_message {
                    Some(msg) => {
                        // log::trace!("Received ResultStreamHandler message: {:?}", msg);
                        state.process_result_stream_handler_message(msg).await
                            .inspect_err(|e| state.transition_to_error_state("Error calling process_result_stream_handler_message", Some(e))).ok();
                    }
                    None => {
                        state.transition_to_error_state("ResultStreamHandler channel closed.", None);
                        break;
                    }
                }
            },

            else => {
                log::error!("QueryResultObserver loop activated for {} but no command or result stream output to process.", state.settings.id);
            }
        }
    }

    log::info!("QueryResultObserver thread exiting for TestRunQuery {} ...", state.settings.id);    
    Ok(())
}