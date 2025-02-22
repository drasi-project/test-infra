use std::{cmp::max, fmt::{self, Debug, Display, Formatter}, sync::Arc, time::SystemTime};

use futures::future::join_all;
use serde::Serialize;
use time::{OffsetDateTime, format_description};
use tokio::{sync::{mpsc::{Receiver, Sender}, oneshot, Mutex}, task::JoinHandle};

use test_data_store::{
    test_repo_storage::models::TestQueryDefinition, 
    test_run_storage::{TestRunQueryId, TestRunQueryStorage}
};

use crate::queries::{result_stream_handlers::create_result_stream_handler, result_stream_record::{ControlSignal, QueryResultRecord}, stop_triggers::create_stop_trigger};

use super::{result_stream_handlers::{ResultStreamHandler, ResultStreamHandlerMessage, ResultStreamRecord, ResultStreamStatus}, result_stream_loggers::{create_result_stream_loggers, ResultStreamLogger}, stop_triggers::StopTrigger, ResultStreamLoggerConfig};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum QueryResultObserverStatus {
    Running,
    Paused,
    Stopped,
    Error
}

impl Serialize for QueryResultObserverStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            QueryResultObserverStatus::Running => serializer.serialize_str("Running"),
            QueryResultObserverStatus::Paused => serializer.serialize_str("Paused"),
            QueryResultObserverStatus::Stopped => serializer.serialize_str("Stopped"),
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
    pub state: QueryResultObserverExternalState,
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
                    state: observer_response.state,
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
    pub status: QueryResultObserverStatus,
    pub stream_status: ResultStreamStatus,
    pub error_messages: Vec<String>,
    pub result_summary: QueryResultObserverSummary,
    pub settings: QueryResultObserverSettings,
}

impl From<&QueryResultObserverInternalState> for QueryResultObserverExternalState {
    fn from(state: &QueryResultObserverInternalState) -> Self {
        Self {
            status: state.status,
            stream_status: state.stream_status,
            error_messages: state.error_messages.clone(),
            result_summary: QueryResultObserverSummary::from(state),
            settings: state.settings.clone(),
        }
    }
}

pub struct QueryResultObserverInternalState {
    result_stream_handler: Box<dyn ResultStreamHandler + Send + Sync>,
    result_stream_handler_rx_channel: Receiver<ResultStreamHandlerMessage>,
    loggers: Vec<Box<dyn ResultStreamLogger + Send + Sync>>,
    error_messages: Vec<String>,
    settings: QueryResultObserverSettings,
    status: QueryResultObserverStatus,
    metrics: QueryResultObserverMetrics,
    stop_trigger: Box<dyn StopTrigger + Send + Sync>,
    stream_status: ResultStreamStatus,
}
    

impl QueryResultObserverInternalState {

    async fn initialize(settings: QueryResultObserverSettings) -> anyhow::Result<Self> {
        log::debug!("Initializing QueryResultObserver using {:?}", settings);
    
        let metrics = QueryResultObserverMetrics {
            observer_create_time_ns: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64,
            ..Default::default()
        };

        let result_stream_handler = create_result_stream_handler(settings.id.clone(), settings.definition.result_stream_handler.clone()).await?;
        let result_stream_handler_rx_channel = result_stream_handler.init().await?;
        let loggers = create_result_stream_loggers(settings.id.clone(), &settings.loggers, &settings.output_storage).await?;        
        let stop_trigger = create_stop_trigger(&settings.definition.stop_trigger).await?;

        Ok(Self {
            result_stream_handler,
            result_stream_handler_rx_channel,
            loggers,
            error_messages: Vec::new(),
            settings,
            status: QueryResultObserverStatus::Paused,
            metrics,
            stop_trigger,
            stream_status: ResultStreamStatus::Unknown,
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
    
                self.log_result_stream_record(&record).await;

                let record_time_ns = record.dequeue_time_ns as u64;
                self.metrics.result_stream_record_seq = record.record_data.get_source_seq();

                match record.record_data {
                    QueryResultRecord::Change(change) => {
                        if change.base.metadata.is_some() {
                            self.metrics.try_set_first_change_record_time(record_time_ns);
                            self.metrics.try_set_last_change_record_time(record_time_ns);
                            self.metrics.result_stream_change_record_count += 1;
                        } else {
                            self.metrics.try_set_first_bootstrap_record_time(record_time_ns);
                            self.metrics.try_set_last_bootstrap_record_time(record_time_ns);
                            self.metrics.result_stream_bootstrap_record_count += 1;
                        }
                    },
                    QueryResultRecord::Control(control) => {
                        self.metrics.control_stream_record_count += 1;
                        match control.control_signal {
                            ControlSignal::BootstrapStarted(_) => {
                                self.stream_status = ResultStreamStatus::BootstrapStarted;
                                self.metrics.try_set_control_stream_bootstrap_start_time(record_time_ns);
                            },
                            ControlSignal::BootstrapCompleted(_) => {
                                self.stream_status = ResultStreamStatus::BootstrapComplete;
                                self.metrics.try_set_control_stream_bootstrap_complete_time(record_time_ns);
                            },
                            ControlSignal::Running(_) => {
                                self.stream_status = ResultStreamStatus::Running;
                                self.metrics.try_set_control_stream_running_time(record_time_ns);
                            },
                            ControlSignal::Stopped(_) => {
                                self.stream_status = ResultStreamStatus::Stopped;
                                self.metrics.try_set_control_stream_stop_time(record_time_ns);
                            },
                            ControlSignal::Deleted(_) => {
                                self.stream_status = ResultStreamStatus::Deleted;
                                self.metrics.try_set_control_stream_delete_time(record_time_ns);
                            }
                        }
                    }
                }

                // Check if we should stop
                if self.stop_trigger.is_true(&self.stream_status, &self.metrics).await? {
                    log::info!("Stopping QueryResultObserver for TestRunQuery {} because stop trigger is true.", self.settings.id);
                    self.transition_to_stopped_state().await;
                }
            },
            ResultStreamHandlerMessage::Error(error) => {
                self.transition_to_error_state(&format!("Error in ResultStreamHandler stream: {:?}", error), None);
            }
        }
    
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

        self.close_loggers().await;    

        self.loggers = create_result_stream_loggers(self.settings.id.clone(), &self.settings.loggers, &self.settings.output_storage).await?;
        self.error_messages = Vec::new();
        self.status = QueryResultObserverStatus::Paused;
        self.stream_status = ResultStreamStatus::Unknown;
        self.metrics = QueryResultObserverMetrics {
            observer_create_time_ns: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64,
            ..Default::default()
        };
    
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
    
    async fn transition_from_paused_state(&mut self, command: &QueryResultObserverCommand) -> anyhow::Result<()> {
        log::debug!("Transitioning from {:?} state via command: {:?}", self.status, command);
    
        match command {
            QueryResultObserverCommand::GetState => Ok(()),
            QueryResultObserverCommand::Pause => Ok(()),
            QueryResultObserverCommand::Reset => self.reset().await,
            QueryResultObserverCommand::Start => {
                log::info!("QueryResultObserver Started for TestRunQuery {}", self.settings.id);
                
                self.status = QueryResultObserverStatus::Running;
                self.metrics.try_set_observer_start_time(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64);
                self.metrics.observer_stop_time_ns = 0;
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
                self.metrics.observer_stop_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
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

    async fn transition_to_stopped_state(&mut self) {
        log::info!("QueryResultObserver Stopped for TestRunQuery {}", self.settings.id);
    
        self.status = QueryResultObserverStatus::Stopped;
        self.metrics.observer_stop_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
        self.result_stream_handler.stop().await.ok();

        self.close_loggers().await;
        self.write_result_summary().await.ok();
    }
    
    fn transition_to_error_state(&mut self, error_message: &str, error: Option<&anyhow::Error>) {    
        self.status = QueryResultObserverStatus::Error;
        self.metrics.observer_stop_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
    
        let msg = match error {
            Some(e) => format!("{}: {:?}", error_message, e),
            None => error_message.to_string(),
        };
        self.error_messages.push(msg.clone());
    
        self.log_observer_state(&msg);
    }    

    pub async fn write_result_summary(&self) -> anyhow::Result<()> {

        let result_summary: QueryResultObserverSummary = self.into();
        log::info!("Summary for TestRunQuery:\n{:#?}", &result_summary);
    
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
            .field("status", &self.status)
            .field("metrics", &self.metrics)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryResultObserverMetrics {
    pub observer_create_time_ns: u64,
    pub observer_start_time_ns: u64,
    pub observer_stop_time_ns: u64,
    pub result_stream_bootstrap_record_count: u64,
    pub result_stream_bootstrap_record_first_ns: u64,
    pub result_stream_bootstrap_record_last_ns: u64,
    pub result_stream_change_record_count: u64,
    pub result_stream_change_record_first_ns: u64,
    pub result_stream_change_record_last_ns: u64,
    pub result_stream_record_seq: i64,
    pub control_stream_record_count: u64,
    pub control_stream_bootstrap_start_time_ns: u64,
    pub control_stream_bootstrap_complete_time_ns: u64,
    pub control_stream_running_time_ns: u64,
    pub control_stream_stop_time_ns: u64,
    pub control_stream_delete_time_ns: u64,
}

impl QueryResultObserverMetrics {
    pub fn get_observer_run_duration_ns(&self, now_ns: Option<u64>) -> u64 {

        match (self.observer_start_time_ns > 0, self.observer_stop_time_ns > 0) {
            (true, true) => self.observer_stop_time_ns - self.observer_start_time_ns,
            (true, false) => {
                let now = now_ns.unwrap_or(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64);
                now - self.observer_start_time_ns
            }
            _ => 0
        }
    }

    pub fn get_result_stream_bootstrap_duration_ns(&self, now_ns: Option<u64>) -> u64 {
        match (self.result_stream_bootstrap_record_first_ns > 0, self.result_stream_bootstrap_record_last_ns > 0) {
            (true, true) => self.result_stream_bootstrap_record_last_ns - self.result_stream_bootstrap_record_first_ns,
            (true, false) => {
                let now = now_ns.unwrap_or(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64);
                now - self.result_stream_bootstrap_record_first_ns
            }            
            _ => 0
        }
    }

    pub fn get_result_stream_change_duration_ns(&self, now_ns: Option<u64>) -> u64 {
        match (self.result_stream_change_record_first_ns > 0, self.result_stream_change_record_last_ns > 0) {
            (true, true) => self.result_stream_change_record_last_ns - self.result_stream_change_record_first_ns,
            (true, false) => {
                let now = now_ns.unwrap_or(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64);
                now - self.result_stream_change_record_first_ns
            }            
            _ => 0
        }
    }

    pub fn try_set_observer_start_time(&mut self, time_ns: u64) {
        if self.observer_start_time_ns == 0 {
            self.observer_start_time_ns = time_ns;
        }
    }

    pub fn try_set_observer_stop_time(&mut self, time_ns: u64) {
        if self.observer_stop_time_ns == 0 {
            self.observer_stop_time_ns = time_ns;
        }
    }

    pub fn try_set_first_bootstrap_record_time(&mut self, time_ns: u64) {
        if self.result_stream_bootstrap_record_first_ns == 0 {
            self.result_stream_bootstrap_record_first_ns = time_ns;
        }
    }

    pub fn try_set_last_bootstrap_record_time(&mut self, time_ns: u64) {
        if self.result_stream_bootstrap_record_last_ns == 0 {
            self.result_stream_bootstrap_record_last_ns = time_ns;
        }
    }

    pub fn try_set_first_change_record_time(&mut self, time_ns: u64) {
        if self.result_stream_change_record_first_ns == 0 {
            self.result_stream_change_record_first_ns = time_ns;
        }
    }

    pub fn try_set_last_change_record_time(&mut self, time_ns: u64) {
        if self.result_stream_change_record_last_ns == 0 {
            self.result_stream_change_record_last_ns = time_ns;
        }
    }

    pub fn try_set_control_stream_bootstrap_start_time(&mut self, time_ns: u64) {
        if self.control_stream_bootstrap_start_time_ns == 0 {
            self.control_stream_bootstrap_start_time_ns = time_ns;
        }
    }

    pub fn try_set_control_stream_bootstrap_complete_time(&mut self, time_ns: u64) {
        if self.control_stream_bootstrap_complete_time_ns == 0 {
            self.control_stream_bootstrap_complete_time_ns = time_ns;
        }
    }

    pub fn try_set_control_stream_running_time(&mut self, time_ns: u64) {
        if self.control_stream_running_time_ns == 0 {
            self.control_stream_running_time_ns = time_ns;
        }
    }

    pub fn try_set_control_stream_stop_time(&mut self, time_ns: u64) {
        if self.control_stream_stop_time_ns == 0 {
            self.control_stream_stop_time_ns = time_ns;
        }
    }

    pub fn try_set_control_stream_delete_time(&mut self, time_ns: u64) {
        if self.control_stream_delete_time_ns == 0 {
            self.control_stream_delete_time_ns = time_ns;
        }
    }
}

impl Default for QueryResultObserverMetrics {
    fn default() -> Self {
        Self {
            observer_create_time_ns: 0,
            observer_start_time_ns: 0,
            observer_stop_time_ns: 0,
            result_stream_bootstrap_record_count: 0,
            result_stream_bootstrap_record_first_ns: 0,
            result_stream_bootstrap_record_last_ns: 0,
            result_stream_change_record_count: 0,
            result_stream_change_record_first_ns: 0,
            result_stream_change_record_last_ns: 0,
            result_stream_record_seq: 0,
            control_stream_record_count: 0,
            control_stream_bootstrap_start_time_ns: 0,
            control_stream_bootstrap_complete_time_ns: 0,
            control_stream_running_time_ns: 0,
            control_stream_stop_time_ns: 0,
            control_stream_delete_time_ns: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryResultObserverSummary {
    pub test_run_query_id: TestRunQueryId,
    pub observer_create_time: String,
    pub observer_start_time: String,
    pub observer_stop_time: String,
    pub observer_run_duration_ns: u64,
    pub observer_run_duration_sec: f64, 
    pub bootstrap_start_time: String,
    pub bootstrap_stop_time: String,
    pub bootstrap_duration_ns: u64,
    pub bootstrap_duration_sec: f64,
    pub bootstrap_processing_rate: f64,
    pub change_stream_start_time: String,
    pub change_stream_stop_time: String,
    pub change_stream_duration_ns: u64,
    pub change_stream_duration_sec: f64,
    pub change_stream_processing_rate: f64,
    pub time_since_last_result_ns: u64,
    pub time_since_last_result_sec: f64,
    pub observer_metrics: QueryResultObserverMetrics,
}

impl From<&QueryResultObserverInternalState> for QueryResultObserverSummary {
    fn from(state: &QueryResultObserverInternalState) -> Self {
        let now_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

        let metrics = state.metrics.clone();

        let observer_create_time = if metrics.observer_create_time_ns > 0 {
            OffsetDateTime::from_unix_timestamp_nanos(metrics.observer_create_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap()
        } else {
            "N/A".to_string()
        };

        let observer_start_time = if metrics.observer_start_time_ns > 0 {
            OffsetDateTime::from_unix_timestamp_nanos(metrics.observer_start_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap()
        } else {
            "N/A".to_string()
        };

        let observer_stop_time = if metrics.observer_stop_time_ns > 0 {
            OffsetDateTime::from_unix_timestamp_nanos(metrics.observer_stop_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap()
        } else {
            "N/A".to_string()
        };

        let observer_run_duration_ns = metrics.get_observer_run_duration_ns(Some(now_ns));

        let bootstrap_start_time = if metrics.result_stream_bootstrap_record_first_ns > 0 {
            OffsetDateTime::from_unix_timestamp_nanos(metrics.result_stream_bootstrap_record_first_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap()
        } else {
            "N/A".to_string()
        };

        let bootstrap_stop_time = if metrics.result_stream_bootstrap_record_last_ns > 0 {
            OffsetDateTime::from_unix_timestamp_nanos(metrics.result_stream_bootstrap_record_last_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap()
        } else {
            "N/A".to_string()
        };

        let bootstrap_duration_ns = metrics.get_result_stream_bootstrap_duration_ns(Some(now_ns));

        let change_stream_start_time = if metrics.result_stream_change_record_first_ns > 0 {
            OffsetDateTime::from_unix_timestamp_nanos(metrics.result_stream_change_record_first_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap()
        } else {
            "N/A".to_string()
        };

        let change_stream_stop_time = if metrics.result_stream_change_record_last_ns > 0 {
            OffsetDateTime::from_unix_timestamp_nanos(metrics.result_stream_change_record_last_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap()
        } else {
            "N/A".to_string()
        };

        let change_stream_duration_ns = metrics.get_result_stream_change_duration_ns(Some(now_ns));

        let time_since_last_result_ns = now_ns - max(metrics.result_stream_bootstrap_record_last_ns, metrics.result_stream_change_record_last_ns);

        Self {
            test_run_query_id: state.settings.id.clone(),
            observer_create_time,
            observer_start_time,
            observer_stop_time,
            observer_run_duration_ns,
            observer_run_duration_sec: observer_run_duration_ns as f64 / 1_000_000_000.0,
            bootstrap_start_time,
            bootstrap_stop_time,
            bootstrap_duration_ns,
            bootstrap_duration_sec: bootstrap_duration_ns as f64 / 1_000_000_000.0,
            bootstrap_processing_rate: metrics.result_stream_bootstrap_record_count as f64 / (bootstrap_duration_ns as f64 / 1_000_000_000.0),
            change_stream_start_time,
            change_stream_stop_time,
            change_stream_duration_ns,
            change_stream_duration_sec: change_stream_duration_ns as f64 / 1_000_000_000.0,
            change_stream_processing_rate: metrics.result_stream_change_record_count as f64 / (change_stream_duration_ns as f64 / 1_000_000_000.0),
            time_since_last_result_ns,
            time_since_last_result_sec: time_since_last_result_ns as f64 / 1_000_000_000.0,
            observer_metrics: metrics,
        }
    }
}

impl Display for QueryResultObserverSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Query Result Observer Summary:")?;
        writeln!(f, "  Test Run Query ID: {}", self.test_run_query_id.to_string())?;
        
        // Observer Timing Section
        writeln!(f, "\n  Observer Timing:")?;
        writeln!(f, "    Created: {}", self.observer_create_time)?;
        writeln!(f, "    Started: {}", self.observer_start_time)?;
        writeln!(f, "    Stopped: {}", self.observer_stop_time)?;
        writeln!(f, "    Duration: {:.3} sec ({} ns)", 
            self.observer_run_duration_sec, 
            self.observer_run_duration_ns
        )?;
        
        // Bootstrap Timing Section
        writeln!(f, "\n  Bootstrap Timing:")?;
        writeln!(f, "    Started: {}", self.bootstrap_start_time)?;
        writeln!(f, "    Stopped: {}", self.bootstrap_stop_time)?;
        writeln!(f, "    Duration: {:.3} sec ({} ns)", 
            self.bootstrap_duration_sec, 
            self.bootstrap_duration_ns
        )?;
        writeln!(f, "    Processing Rate: {:.2}", self.bootstrap_processing_rate)?;
        
        // Change Stream Timing Section
        writeln!(f, "\n  Change Stream Timing:")?;
        writeln!(f, "    Started: {}", self.change_stream_start_time)?;
        writeln!(f, "    Stopped: {}", self.change_stream_stop_time)?;
        writeln!(f, "    Duration: {:.3} sec ({} ns)", 
            self.change_stream_duration_sec, 
            self.change_stream_duration_ns
        )?;
        writeln!(f, "    Processing Rate: {:.2}", self.change_stream_processing_rate)?;
        
        // Time Since Last Result
        writeln!(f, "\n  Time Since Last Result: {:.3} sec ({} ns)", 
            self.time_since_last_result_sec, 
            self.time_since_last_result_ns
        )?;
        
        // Observer Metrics
        writeln!(f, "\n  Observer Metrics: {:?}", self.observer_metrics)?;
        
        Ok(())
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