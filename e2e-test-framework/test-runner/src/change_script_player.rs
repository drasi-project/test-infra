use std::{path::PathBuf, sync::Arc, time::SystemTime};

use futures::future::join_all;
use serde::Serialize;
use tokio::sync::{mpsc::{Receiver, Sender}, oneshot, Mutex};
use tokio::sync::mpsc::error::TryRecvError::{Empty, Disconnected};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use test_data_store::scripts::{change_script_file_reader::ChangeScriptReader, ChangeScriptRecord, SequencedChangeScriptRecord, SourceChangeEvent};

use crate::{
    config::SourceChangeDispatcherConfig, SpacingMode, TestRunReactivator, TestRunSource, TimeMode, 
    source_change_dispatchers::{
        console_dispatcher::{ConsoleSourceChangeDispatcher, ConsoleSourceChangeDispatcherSettings}, 
        dapr_dispatcher::{DaprSourceChangeDispatcher, DaprSourceChangeDispatcherSettings}, 
        jsonl_file_dispatcher::{JsonlFileSourceChangeDispatcher, JsonlFileSourceChangeDispatcherSettings}, 
    }
};
use crate::source_change_dispatchers::SourceChangeDispatcher;

#[derive(Debug, thiserror::Error)]
pub enum ChangeScriptPlayerError {
    #[error("ChangeScriptPlayer is already finished.")]
    AlreadyFinished,
    #[error("ChangeScriptPlayer is already paused.")]
    AlreadyPaused,
    #[error("ChangeScriptPlayer is already running.")]
    AlreadyRunning,
    #[error("ChangeScriptPlayer is already stopped.")]
    AlreadyStopped,
    #[error("ChangeScriptPlayer is currently Skipping. {0} skips remaining.")]
    CurrentlySkipping(u64),
    #[error("ChangeScriptPlayer is currently Stepping. {0} steps remaining.")]
    CurrentlyStepping(u64),
    #[error("ChangeScriptPlayer is currently in an Error state - {0:?}")]
    Error(ChangeScriptPlayerStatus),
    #[error("ChangeScriptPlayer is currently Running. Pause before trying to Skip.")]
    PauseToSkip,
    #[error("ChangeScriptPlayer is currently Running. Pause before trying to Step.")]
    PauseToStep,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChangeScriptPlayerSettings {
    pub data_store_path: PathBuf,
    pub reactivator: TestRunReactivator,
    pub script_files: Vec<PathBuf>,
    pub test_run_source: TestRunSource,
}

impl ChangeScriptPlayerSettings {
    pub fn try_from_test_run_source(test_run_source: TestRunSource, script_files: Vec<PathBuf>, data_store_path: PathBuf) -> anyhow::Result<Self> {

        // If the SourceConfig doesn't contain a ReactivatorConfig, log and return an error.
        // Otherwise, clone the TestRunSource and extract the TestRunReactivator.
        let (test_run_source, reactivator) = match test_run_source.reactivator {
            Some(reactivator) => {
                (TestRunSource { reactivator: None, ..test_run_source }, reactivator)
            },
            None => {
                anyhow::bail!("No ReactivatorConfig provided in TestRunSource: {:?}", test_run_source);
            }
        };

        Ok(ChangeScriptPlayerSettings {
            data_store_path,
            reactivator,
            script_files,
            test_run_source,
        })
    }

    pub fn get_id(&self) -> String {
        self.test_run_source.id.clone()
    }
}

// Enum of ChangeScriptPlayer status.
// Running --start--> <ignore>
// Running --step--> <ignore>
// Running --pause--> Paused
// Running --stop--> Stopped
// Running --finish_files--> Finished

// Stepping --start--> <ignore>
// Stepping --step--> <ignore>
// Stepping --pause--> Paused
// Stepping --stop--> Stopped
// Stepping --finish_files--> Finished

// Paused --start--> Started
// Paused --step--> Stepping
// Paused --pause--> <ignore>
// Paused --stop--> Stopped

// Stopped --*--> <ignore>
// Finished --*--> <ignore>
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ChangeScriptPlayerStatus {
    Running,
    Stepping,
    Skipping,
    Paused,
    Stopped,
    Finished,
    Error
}

impl Serialize for ChangeScriptPlayerStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            ChangeScriptPlayerStatus::Running => serializer.serialize_str("Running"),
            ChangeScriptPlayerStatus::Stepping => serializer.serialize_str("Stepping"),
            ChangeScriptPlayerStatus::Skipping => serializer.serialize_str("Skipping"),
            ChangeScriptPlayerStatus::Paused => serializer.serialize_str("Paused"),
            ChangeScriptPlayerStatus::Stopped => serializer.serialize_str("Stopped"),
            ChangeScriptPlayerStatus::Finished => serializer.serialize_str("Finished"),
            ChangeScriptPlayerStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ChangeScriptPlayerState {
    pub current_replay_time: u64,
    pub delayed_record: Option<ScheduledChangeScriptRecord>,    
    pub error_message: Option<String>,
    pub ignore_scripted_pause_commands: bool,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub skips_remaining: u64,
    pub spacing_mode: SpacingMode,
    pub start_replay_time: u64,
    pub status: ChangeScriptPlayerStatus,
    pub steps_remaining: u64,
    pub time_mode: TimeMode,
}

impl Default for ChangeScriptPlayerState {
    fn default() -> Self {
        Self {
            current_replay_time: 0,
            delayed_record: None,
            error_message: None,
            ignore_scripted_pause_commands: false,
            next_record: None,
            skips_remaining: 0,
            spacing_mode: SpacingMode::None,
            start_replay_time: 0,
            status: ChangeScriptPlayerStatus::Paused,
            steps_remaining: 0,
            time_mode: TimeMode::Live,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScheduledChangeScriptRecord {
    pub replay_time_ns: u64,
    pub seq_record: SequencedChangeScriptRecord,
}

impl ScheduledChangeScriptRecord {
    pub fn new(record: SequencedChangeScriptRecord, replay_time_ns: u64, ) -> Self {
        Self {
            replay_time_ns,
            seq_record: record,
        }
    }
}

// Enum of ChangeScriptPlayer commands sent from Web API handler functions.
#[derive(Debug)]
pub enum ChangeScriptPlayerCommand {
    // Command to start the ChangeScriptPlayer.
    Start,
    // Command to process the delayed Change Script record. The param is the sequence number of the record to process.
    // It is used as a guard to ensure the timer is only processed if the sequence number matches the expected sequence number.
    ProcessDelayedRecord(u64),
    // Command to step the ChangeScriptPlayer forward a specified number of ChangeScriptRecords.
    Step(u64),
    // Command to skip the ChangeScriptPlayer forward a specified number of ChangeScriptRecords.
    Skip(u64),
    // Command to pause the ChangeScriptPlayer.
    Pause,
    // Command to stop the ChangeScriptPlayer.
    Stop,
    // Command to get the current state of the ChangeScriptPlayer.
    GetState,
}

// Struct for messages sent to the ChangeScriptPlayer from the functions in the Web API.
#[derive(Debug)]
pub struct ChangeScriptPlayerMessage {
    // Command sent to the ChangeScriptPlayer.
    pub command: ChangeScriptPlayerCommand,
    // One-shot channel for ChangeScriptPlayer to send a response back to the caller.
    pub response_tx: Option<oneshot::Sender<ChangeScriptPlayerMessageResponse>>,
}

// A struct for the Response sent back from the ChangeScriptPlayer to the calling Web API handler.
#[derive(Debug)]
pub struct ChangeScriptPlayerMessageResponse {
    // Result of the command.
    pub result: anyhow::Result<()>,
    // State of the ChangeScriptPlayer after the command.
    pub state: ChangeScriptPlayerState,
}

#[derive(Clone, Debug)]
pub struct DelayChangeScriptRecordMessage {
    pub delay_ns: u64,
    pub delay_sequence: u64,
}

#[derive(Clone, Debug)]
pub struct ChangeScriptPlayer {
    // Channel used to send messages to the ChangeScriptPlayer thread.
    settings: ChangeScriptPlayerSettings,
    player_tx_channel: Sender<ChangeScriptPlayerMessage>,
    _delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>,
    _player_thread_handle: Arc<Mutex<JoinHandle<()>>>,
    _delayer_thread_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl ChangeScriptPlayer {
    pub async fn new(settings: ChangeScriptPlayerSettings) -> Self {
        let (player_tx_channel, player_rx_channel) = tokio::sync::mpsc::channel(100);
        let (delayer_tx_channel, delayer_rx_channel) = tokio::sync::mpsc::channel(100);

        let player_thread_handle = tokio::spawn(player_thread(player_rx_channel, delayer_tx_channel.clone(), settings.clone()));
        let delayer_thread_handle = tokio::spawn(delayer_thread(delayer_rx_channel, player_tx_channel.clone()));

        Self {
            settings,
            player_tx_channel,
            _delayer_tx_channel: delayer_tx_channel,
            _player_thread_handle: Arc::new(Mutex::new(player_thread_handle)),
            _delayer_thread_handle: Arc::new(Mutex::new(delayer_thread_handle)),
        }
    }

    pub fn get_id(&self) -> String {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> ChangeScriptPlayerSettings {
        self.settings.clone()
    }

    pub async fn get_state(&self) -> anyhow::Result<ChangeScriptPlayerMessageResponse> {
        self.send_command(ChangeScriptPlayerCommand::GetState).await
    }

    pub async fn start(&self) -> anyhow::Result<ChangeScriptPlayerMessageResponse> {
        self.send_command(ChangeScriptPlayerCommand::Start).await
    }

    pub async fn step(&self, steps: u64) -> anyhow::Result<ChangeScriptPlayerMessageResponse>  {
        self.send_command(ChangeScriptPlayerCommand::Step(steps)).await
    }

    pub async fn skip(&self, skips: u64) -> anyhow::Result<ChangeScriptPlayerMessageResponse>  {
        self.send_command(ChangeScriptPlayerCommand::Skip(skips)).await
    }

    pub async fn pause(&self) -> anyhow::Result<ChangeScriptPlayerMessageResponse>  {
        self.send_command(ChangeScriptPlayerCommand::Pause).await
    }

    pub async fn stop(&self) -> anyhow::Result<ChangeScriptPlayerMessageResponse>  {
        self.send_command(ChangeScriptPlayerCommand::Stop).await
    }

    async fn send_command(&self, command: ChangeScriptPlayerCommand) -> anyhow::Result<ChangeScriptPlayerMessageResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self.player_tx_channel.send(ChangeScriptPlayerMessage {
            command,
            response_tx: Some(response_tx),
        }).await;

        match r {
            Ok(_) => Ok(response_rx.await.unwrap()),
            Err(e) => anyhow::bail!("Error sending command to ChangeScriptPlayer: {:?}", e),
        }
    }
}

// Function that defines the operation of the ChangeScriptPlayer thread.
// The ChangeScriptPlayer thread processes ChangeScriptPlayerCommands sent to it from the Web API handler functions.
// The Web API function communicate via a channel and provide oneshot channels for the ChangeScriptPlayer to send responses back.
pub async fn player_thread(mut player_rx_channel: Receiver<ChangeScriptPlayerMessage>, delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>, player_settings: ChangeScriptPlayerSettings) {

    log::info!("ChangeScriptPlayer thread started...");

    // Initialize the ChangeScriptPlayer infrastructure.
    // Create a ChangeScriptReader to read the ChangeScript files.
    // Create a ChangeScriptPlayerState to hold the state of the ChangeScriptPlayer. The ChangeScriptPlayer always starts in a paused state.
    // Create a SourceChangeDispatcher to send SourceChangeEvents.
    let (mut player_state, mut test_script_reader, mut dispatchers) = match ChangeScriptReader::new(player_settings.script_files.clone()) {
        Ok(mut reader) => {

            let header = reader.get_header();
            log::debug!("Loaded ChangeScript. {:?}", header);

            let mut player_state = ChangeScriptPlayerState::default();
            player_state.ignore_scripted_pause_commands = player_settings.reactivator.ignore_scripted_pause_commands;
            player_state.time_mode = player_settings.reactivator.time_mode;
            player_state.spacing_mode = player_settings.reactivator.spacing_mode;
        
            // Set the start_replay_time based on the time mode and the script start time from the header.
            player_state.start_replay_time = match player_state.time_mode {
                TimeMode::Live => {
                    // Use the current system time.
                    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
                },
                TimeMode::Recorded => {
                    // Use the start time as provided in the Header.
                    header.start_time.timestamp_nanos_opt().unwrap() as u64
                },
                TimeMode::Rebased(nanos) => {
                    // Use the rebased time as provided in the AppConfig.
                    nanos
                },
            };
            player_state.current_replay_time = player_state.start_replay_time;

            let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();

            for dispatcher_config in player_settings.reactivator.dispatchers.iter() {
                match dispatcher_config {
                    SourceChangeDispatcherConfig::Dapr(dapr_config) => {
                        match DaprSourceChangeDispatcherSettings::try_from_config(dapr_config, player_settings.test_run_source.source_id.clone()) {
                            Ok(settings) => {
                                match DaprSourceChangeDispatcher::new(settings) {
                                    Ok(d) => dispatchers.push(d),
                                    Err(e) => {
                                        transition_to_error_state(format!("Error creating DaprSourceChangeDispatcher: {:?}", e).as_str(), &mut player_state);
                                    }
                                }
                            },
                            Err(e) => {
                                transition_to_error_state(format!("Error creating DaprSourceChangeDispatcherSettings: {:?}", e).as_str(), &mut player_state);
                            }
                        }
                    },
                    SourceChangeDispatcherConfig::Console(console_config ) => {
                        match ConsoleSourceChangeDispatcherSettings::try_from_config(console_config) {
                            Ok(settings) => {
                                match ConsoleSourceChangeDispatcher::new(settings) {
                                    Ok(d) => dispatchers.push(d),
                                    Err(e) => {
                                        transition_to_error_state(format!("Error creating ConsoleSourceChangeDispatcher: {:?}", e).as_str(), &mut player_state);
                                    }
                                }
                            },
                            Err(e) => {
                                transition_to_error_state(format!("Error creating ConsoleSourceChangeDispatcherSettings: {:?}", e).as_str(), &mut player_state);
                            }
                        }
                    },
                    SourceChangeDispatcherConfig::JsonlFile(jsonl_file_config) => {
                        // Construct the path to the local file used to store the generated SourceChangeEvents.
                        // let folder_path = format!("./{}/test_output/{}/{}/change_logs/{}", 
                        //     jsonl_file_config.folder_path.clone().unwrap_or(player_settings.data_store_path.to_owned()),
                        //     player_settings.test_run_source.test_id, 
                        //     player_settings.test_run_source.test_run_id, 
                        //     player_settings.test_run_source.source_id);

                        let folder_path = match &jsonl_file_config.folder_path {
                            Some(config_path) => {
                                if config_path.starts_with("/") {
                                    PathBuf::from(format!("{}/test_output/{}/{}/change_logs/{}", 
                                        config_path,
                                        &player_settings.test_run_source.test_id, 
                                        &player_settings.test_run_source.test_run_id, 
                                        &player_settings.test_run_source.source_id))
                                } else {
                                    PathBuf::from(format!("./{}/test_output/{}/{}/change_logs/{}", 
                                        config_path,
                                        &player_settings.test_run_source.test_id, 
                                        &player_settings.test_run_source.test_run_id, 
                                        &player_settings.test_run_source.source_id))
                                }
                            },
                            None => player_settings.data_store_path.clone()
                                        .join(&player_settings.test_run_source.test_id)
                                        .join(&player_settings.test_run_source.test_run_id)
                                        .join(&player_settings.test_run_source.source_id)
                        };

                        match JsonlFileSourceChangeDispatcherSettings::try_from_config(jsonl_file_config, folder_path) {
                            Ok(settings) => {
                                match JsonlFileSourceChangeDispatcher::new(settings) {
                                    Ok(d) => dispatchers.push(d),
                                    Err(e) => {
                                        transition_to_error_state(format!("Error creating JsonlFileSourceChangeDispatcher: {:?}", e).as_str(), &mut player_state);
                                    }
                                }
                            },
                            Err(e) => {
                                transition_to_error_state(format!("Error creating JsonlFileSourceChangeDispatcherSettings: {:?}", e).as_str(), &mut player_state);
                            }
                        }                        
                    },
                }
            }

            // Read the first ChangeScriptRecord.
            read_next_sequenced_test_script_record(&mut player_state, &mut reader);

            (player_state, Some(reader), dispatchers)    
        },
        Err(e) => {
            let mut player_state = ChangeScriptPlayerState::default();
            transition_to_error_state(format!("Error creating ChangeScriptReader: {:?}", e).as_str(), &mut player_state);
            (player_state, None, vec![])
        }
    };

    log::trace!("Starting ChangeScriptPlayer loop...");

    // Loop to process messages sent to the ChangeScriptPlayer and read data from Change Script.
    // Always process all messages in the command channel and act on them first.
    // If there are no messages and the ChangeScriptPlayer is running, stepping, or skipping, continue processing the Change Script.
    loop {
        log_test_script_player_state("Top of ChangeScriptPlayer loop", &player_state);

        // If the ChangeScriptPlayer needs a command to continue, block until a command is received.
        // Otherwise, try to receive a command message without blocking and then process the next Change Script record.
        let block_on_message = 
            (player_state.delayed_record.is_some() && (
                player_state.status == ChangeScriptPlayerStatus::Running 
                || player_state.status == ChangeScriptPlayerStatus::Stepping 
                || player_state.status == ChangeScriptPlayerStatus::Skipping
            ))
            || player_state.status == ChangeScriptPlayerStatus::Paused 
            || player_state.status == ChangeScriptPlayerStatus::Stopped 
            || player_state.status == ChangeScriptPlayerStatus::Finished 
            || player_state.status == ChangeScriptPlayerStatus::Error;
        
        let message: Option<ChangeScriptPlayerMessage> = if block_on_message {
            log::trace!("ChangeScriptPlayer Thread Blocked; waiting for a command message...");

            match player_rx_channel.recv().await {
                Some(message) => {
                    log::trace!("Received {:?} command message.", message.command);
                    Some(message)
                },
                None => {
                    // Cannot continue if the channel is closed.
                    transition_to_error_state("Message channel closed", &mut player_state);
                    None
                },
            }
        } else {
            log::trace!("ChangeScriptPlayer Thread Unblocked; TRYing for a command message before processing next script record");

            match player_rx_channel.try_recv() {
                Ok(message) => {
                    log::trace!("Received {:?} command message.", message.command);
                    Some(message)
                },
                Err(Empty) => None,
                Err(Disconnected) => {
                    // Cannot continue if the channel is closed.
                    transition_to_error_state("Message channel closed", &mut player_state);
                    None
                }
            }
        };

        // If a message was received, process it. 
        // Otherwise, continue processing the log files.
        match message {
            Some(message) => {

                log::debug!("Processing command: {:?}", message.command);                

                if let ChangeScriptPlayerCommand::ProcessDelayedRecord(seq) = message.command {
                    process_delayed_test_script_record(seq, &mut player_state, &mut dispatchers).await;
                } else {
                    let transition_response = match player_state.status {
                        ChangeScriptPlayerStatus::Running => transition_from_running_state(&message.command, &mut player_state),
                        ChangeScriptPlayerStatus::Stepping => transition_from_stepping_state(&message.command, &mut player_state),
                        ChangeScriptPlayerStatus::Skipping => transition_from_skipping_state(&message.command, &mut player_state),
                        ChangeScriptPlayerStatus::Paused => transition_from_paused_state(&message.command, &mut player_state),
                        ChangeScriptPlayerStatus::Stopped => transition_from_stopped_state(&message.command, &mut player_state),
                        ChangeScriptPlayerStatus::Finished => transition_from_finished_state(&message.command, &mut player_state),
                        ChangeScriptPlayerStatus::Error => transition_from_error_state(&message.command, &mut player_state),
                    };
    
                    if message.response_tx.is_some() {
                        let message_response = ChangeScriptPlayerMessageResponse {
                            result: transition_response,
                            state: player_state.clone(),
                        };
        
                        let r = message.response_tx.unwrap().send(message_response);
                        if let Err(e) = r {
                            log::error!("Error in ChangeScriptPlayer sending message response back to caller: {:?}", e);
                        }
                    }
                }

                log_test_script_player_state(format!("Post {:?} command", message.command).as_str(), &player_state);
            },
            None => {
                // Only process the next ChangeScriptRecord if the player is not in an error state.
                if let ChangeScriptPlayerStatus::Error = player_state.status {
                    log_test_script_player_state("Trying to process Next ChangeScriptRecord, but Player in error state", &player_state);
                } else {
                    process_next_test_script_record(&mut player_state, &mut dispatchers, delayer_tx_channel.clone()).await;
                    read_next_sequenced_test_script_record(&mut player_state, &mut test_script_reader.as_mut().unwrap());
                }
            }
        }
    }
}

fn read_next_sequenced_test_script_record(player_state: &mut ChangeScriptPlayerState, test_script_reader: &mut ChangeScriptReader) {

    // Do nothing if the player is already in an error state.
    if let ChangeScriptPlayerStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring read_next_sequenced_test_script_record call due to error state", &player_state);
        return;
    }

    // Do nothing if the ChangeScriptPlayer has finished processing all records.
    if let ChangeScriptPlayerStatus::Finished = player_state.status {
        log_test_script_player_state("Ignoring read_next_sequenced_test_script_record call because script is already Finished", &player_state);
        return;
    }
    
    match test_script_reader.next() {
        Some(Ok(seq_record)) => {
            // Assign the next record to the player state.
            player_state.next_record = Some(seq_record);
        },
        Some(Err(e)) => {
            transition_to_error_state(format!("Error reading ChangeScriptRecord: {:?}", e).as_str(), player_state);
        },
        None => {
            log_test_script_player_state("ChangeScriptReader.next() returned None, shouldn't be seeing this.", &player_state);
        }
    };
}

async fn process_next_test_script_record(mut player_state: &mut ChangeScriptPlayerState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>, delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>) {

    // Do nothing if the player is already in an error state.
    if let ChangeScriptPlayerStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring process_next_test_script_record call due to error state", &player_state);
        return;
    }

    // next_record should never be None.
    if player_state.next_record.is_none() {
        transition_to_error_state(format!("ChangeScriptReader should never return None for next_record").as_str(), player_state);
        return;
    }

    let seq_record = player_state.next_record.as_ref().unwrap().clone();
    log_sequenced_test_script_record("Next ChangeScriptRecord Pre-TimeShift", &seq_record);

    // Check if the ChangeScriptPlayer has finished processing all records.
    if let ChangeScriptRecord::Finish(_) = seq_record.record {
        transition_to_finished_state(&mut player_state);
        return;
    }

    // Time shift the ChangeScriptRecord based on the time mode settings.
    let next_record = time_shift_test_script_record(player_state, seq_record);
    log_scheduled_test_script_record("Next ChangeScriptRecord Post-TimeShift", &next_record);

    // Processing of ChangeScriptRecord depends on the spacing mode settings.
    let delay = match player_state.spacing_mode {
        SpacingMode::None => {
            // Process the record immediately.
            0
        },
        SpacingMode::Fixed(nanos) => {
            nanos
        },
        SpacingMode::Recorded => {
            // Delay the record based on the difference between the record's replay time and the current replay time.
            // Ensure the delay is not negative.
            std::cmp::max(0, next_record.replay_time_ns - player_state.current_replay_time) as u64
        },
    };

    // Take action based on size of the delay.
    // It doesn't make sense to delay for trivially small amounts of time, nor does it make sense to send 
    // a message to the delayer thread for relatively small delays. 
    // TODO: This figures might need to be adjusted, or made configurable.
    if delay < 1_000 { 
        // Process the record immediately.
        log_scheduled_test_script_record(
            format!("Minimal delay of {}ns; processing immediately", delay).as_str(), &next_record);
        resolve_test_script_record_effect(next_record, player_state, dispatchers).await;
    } else if delay < 10_000_000 {
        // Sleep inproc for efficiency, then process the record.
        log_scheduled_test_script_record(
            format!("Short delay of {}ns; processing after in-proc sleep", delay).as_str(), &next_record);
        std::thread::sleep(Duration::from_nanos(delay));
        resolve_test_script_record_effect(next_record, player_state, dispatchers).await;
    } else {
        log_scheduled_test_script_record(
            format!("Long delay of {}ns; delaying record", delay).as_str(), &next_record);

        // Delay the record.
        let delay_msg = DelayChangeScriptRecordMessage {
            delay_ns: delay,
            delay_sequence: next_record.seq_record.seq,
        };

        // Setting the delayed record in the player state will cause the player to not process any more records until
        // the delayed record is processed.
        player_state.delayed_record = Some(next_record);

        // Send a message to the ChangeDispatchTimer to process the delayed record.
        let delay_send_res = delayer_tx_channel.send(delay_msg).await;
        if let Err(e) = delay_send_res {
            transition_to_error_state(format!("Error sending DelayChangeScriptRecordMessage: {:?}", e).as_str(), player_state);
        }
    };
}

async fn process_delayed_test_script_record(delayed_record_seq: u64, player_state: &mut ChangeScriptPlayerState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>) {

    // Do nothing if the player is already in an error state.
    if let ChangeScriptPlayerStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring process_delayed_test_script_record call due to error state", &player_state);
        return;
    }

    if let Some(mut delayed_record) = player_state.delayed_record.as_mut().cloned() {

        // Ensure the ProcessDelayedRecord command is only processed if the sequence number matches the expected sequence number.
        if delayed_record_seq != delayed_record.seq_record.seq {
            log_scheduled_test_script_record(
                format!("Received command to process incorrect ChangeScript Record with seq:{}", delayed_record_seq).as_str()
                , &delayed_record);
            return;
        }        

        log_scheduled_test_script_record("Processing Delayed ChangeScriptRecord", &delayed_record);

        // Adjust the time in the ChangeScriptRecord if the player is in Live Time Mode.
        // This will make sure the times are as accurate as possible.
        if player_state.time_mode == TimeMode::Live {
            // Live Time Mode means we just use the current time as the record's Replay Time regardless of offset.
            let replay_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

            delayed_record.replay_time_ns = replay_time_ns;
            player_state.current_replay_time = replay_time_ns;
            log_test_script_player_state(
                format!("Shifted Player Current Replay Time to delayed record scheduled time: {}", replay_time_ns).as_str(),
                &player_state);
        };
    
        // Process the delayed record.
        resolve_test_script_record_effect(delayed_record, player_state, dispatchers).await;
    
        // Clear the delayed record.
        player_state.delayed_record = None;
    } else {
        log::error!("Should not be processing ProcessDelayedRecord when player_state.delayed_record is None.");
    };
}

async fn resolve_test_script_record_effect(record: ScheduledChangeScriptRecord, player_state: &mut ChangeScriptPlayerState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>) {

    // Do nothing if the player is already in an error state.
    if let ChangeScriptPlayerStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring resolve_test_script_record_effect call due to error state", &player_state);
        return;
    }
        
    match &record.seq_record.record {
        ChangeScriptRecord::SourceChange(change_record) => {
            match player_state.status {
                ChangeScriptPlayerStatus::Running => {
                    // Dispatch the SourceChangeEvent.
                    dispatch_source_change_events(dispatchers, vec!(&change_record.source_change_event)).await;
                    // let _ = dispatcher.dispatch_source_change_events(vec!(&change_record.source_change_event)).await;
                },
                ChangeScriptPlayerStatus::Stepping => {
                    // Dispatch the SourceChangeEvent.
                    if player_state.steps_remaining > 0 {
                        dispatch_source_change_events(dispatchers, vec!(&change_record.source_change_event)).await;
                        // let _ = dispatcher.dispatch_source_change_events(vec!(&change_record.source_change_event)).await;

                        player_state.steps_remaining -= 1;
                        if player_state.steps_remaining == 0 {
                            let _ = transition_from_stepping_state(&ChangeScriptPlayerCommand::Pause, player_state);
                        }
                    }
                },
                ChangeScriptPlayerStatus::Skipping => {
                    // Skip the SourceChangeEvent.
                    if player_state.skips_remaining > 0 {
                        log::debug!("Skipping ChangeScriptRecord: {:?}", change_record);

                        player_state.skips_remaining -= 1;
                        if player_state.skips_remaining == 0 {
                            let _ = transition_from_skipping_state(&ChangeScriptPlayerCommand::Pause, player_state);
                        }
                    }
                },
                _ => {
                    log::warn!("Should not be processing ChangeScriptRecord when in state {:?}", player_state.status);
                },
            }
        },
        ChangeScriptRecord::PauseCommand(_) => {
            // Process the PauseCommand only if the Player is not configured to ignore them.
            if player_state.ignore_scripted_pause_commands {
                log::info!("Ignoring Change Script Pause Command: {:?}", record);
            } else {
                let _ = match player_state.status {
                    ChangeScriptPlayerStatus::Running => transition_from_running_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    ChangeScriptPlayerStatus::Stepping => transition_from_stepping_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    ChangeScriptPlayerStatus::Skipping => transition_from_skipping_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    ChangeScriptPlayerStatus::Paused => transition_from_paused_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    ChangeScriptPlayerStatus::Stopped => transition_from_stopped_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    ChangeScriptPlayerStatus::Finished => transition_from_finished_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    ChangeScriptPlayerStatus::Error => transition_from_error_state(&ChangeScriptPlayerCommand::Pause, player_state),
                };
            }
        },
        ChangeScriptRecord::Label(label_record) => {
            log::info!("Reached Change Script Label: {:?}", label_record);
        },
        ChangeScriptRecord::Finish(_) => {
            transition_to_finished_state(player_state);
        },
        ChangeScriptRecord::Header(header_record) => {
            // Only the first record should be a Header record, and this is handled in the ChangeScriptReader.
            log::warn!("Ignoring unexpected Change Script Header: {:?}", header_record);
        },
        ChangeScriptRecord::Comment(comment_record) => {
            // The ChangeScriptReader should not return Comment records.
            log::warn!("Ignoring unexpected Change Script Comment: {:?}", comment_record);
        },
    };
}

async fn dispatch_source_change_events(dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>, events: Vec<&SourceChangeEvent>) {
    
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

fn time_shift_test_script_record(player_state: &ChangeScriptPlayerState, seq_record: SequencedChangeScriptRecord) -> ScheduledChangeScriptRecord {

    let replay_time_ns = match player_state.time_mode {
        TimeMode::Live => {
            // Live Time Mode means we just use the current time as the record's Replay Time regardless of offset.
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
        },
        TimeMode::Recorded | TimeMode::Rebased(_) => {
            // Recorded or Rebased Time Mode means we have to adjust the record's Replay Time based on the offset from
            // the start time. The player_state.start_replay_time has the base time regardless of the time mode.
            player_state.start_replay_time + seq_record.offset_ns
        },
    };

    // TODO: Also need to adjust the time values in the data based on the time mode settings.

    ScheduledChangeScriptRecord::new(seq_record, replay_time_ns)
}

fn transition_from_paused_state(command: &ChangeScriptPlayerCommand, player_state: &mut ChangeScriptPlayerState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ChangeScriptPlayerCommand::Start => {
            player_state.status = ChangeScriptPlayerStatus::Running;
            Ok(())
        },
        ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // The ChangeScriptPlayer is Paused, so we ignore the DispatchChangeEvent command.
            Ok(())
        },
        ChangeScriptPlayerCommand::Step(steps) => {
            player_state.status = ChangeScriptPlayerStatus::Stepping;
            player_state.steps_remaining = *steps;
            Ok(())
        },
        ChangeScriptPlayerCommand::Skip(skips) => {
            player_state.status = ChangeScriptPlayerStatus::Skipping;
            player_state.skips_remaining = *skips;
            Ok(())
        },
        ChangeScriptPlayerCommand::Pause => {
            Err(ChangeScriptPlayerError::AlreadyPaused.into())
        },
        ChangeScriptPlayerCommand::Stop => {
            player_state.status = ChangeScriptPlayerStatus::Stopped;
            Ok(())
        },
        ChangeScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_running_state(command: &ChangeScriptPlayerCommand, player_state: &mut ChangeScriptPlayerState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ChangeScriptPlayerCommand::Start => {
            Err(ChangeScriptPlayerError::AlreadyRunning.into())
        },
        ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while ChangeScriptPlayer is Running.");
            Ok(())
        },
        ChangeScriptPlayerCommand::Step(_) => {
            Err(ChangeScriptPlayerError::PauseToStep.into())
        },
        ChangeScriptPlayerCommand::Skip(_) => {
            Err(ChangeScriptPlayerError::PauseToSkip.into())
        },
        ChangeScriptPlayerCommand::Pause => {
            player_state.status = ChangeScriptPlayerStatus::Paused;
            Ok(())
        },
        ChangeScriptPlayerCommand::Stop => {
            player_state.status = ChangeScriptPlayerStatus::Stopped;
            Ok(())
        },
        ChangeScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stepping_state(command: &ChangeScriptPlayerCommand, player_state: &mut ChangeScriptPlayerState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ChangeScriptPlayerCommand::Start => {
            Err(ChangeScriptPlayerError::CurrentlyStepping(player_state.steps_remaining).into())
        },
        ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while ChangeScriptPlayer is Stepping.");
            Ok(())
        },
        ChangeScriptPlayerCommand::Step(_) => {
            Err(ChangeScriptPlayerError::CurrentlyStepping(player_state.steps_remaining).into())
        },
        ChangeScriptPlayerCommand::Skip(_) => {
            Err(ChangeScriptPlayerError::CurrentlyStepping(player_state.steps_remaining).into())
        },
        ChangeScriptPlayerCommand::Pause => {
            player_state.status = ChangeScriptPlayerStatus::Paused;
            player_state.steps_remaining = 0;
            Ok(())
        },
        ChangeScriptPlayerCommand::Stop => {
            player_state.status = ChangeScriptPlayerStatus::Stopped;
            player_state.steps_remaining = 0;
            Ok(())
        },
        ChangeScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_skipping_state(command: &ChangeScriptPlayerCommand, player_state: &mut ChangeScriptPlayerState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ChangeScriptPlayerCommand::Start => {
            Err(ChangeScriptPlayerError::CurrentlySkipping(player_state.skips_remaining).into())
        },
        ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while ChangeScriptPlayer is Skipping.");
            Ok(())
        },
        ChangeScriptPlayerCommand::Step(_) => {
            Err(ChangeScriptPlayerError::CurrentlySkipping(player_state.skips_remaining).into())
        },
        ChangeScriptPlayerCommand::Skip(_) => {
            Err(ChangeScriptPlayerError::CurrentlySkipping(player_state.skips_remaining).into())
        },
        ChangeScriptPlayerCommand::Pause => {
            player_state.status = ChangeScriptPlayerStatus::Paused;
            player_state.skips_remaining = 0;
            Ok(())
        },
        ChangeScriptPlayerCommand::Stop => {
            player_state.status = ChangeScriptPlayerStatus::Stopped;
            player_state.skips_remaining = 0;
            Ok(())
        },
        ChangeScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stopped_state(command: &ChangeScriptPlayerCommand, player_state: &mut ChangeScriptPlayerState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ChangeScriptPlayerError::AlreadyStopped.into())
}

fn transition_from_finished_state(command: &ChangeScriptPlayerCommand, player_state: &mut ChangeScriptPlayerState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ChangeScriptPlayerError::AlreadyFinished.into())
}

fn transition_from_error_state(command: &ChangeScriptPlayerCommand, player_state: &mut ChangeScriptPlayerState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ChangeScriptPlayerError::Error(player_state.status).into())
}

fn transition_to_finished_state(player_state: &mut ChangeScriptPlayerState) {
    player_state.status = ChangeScriptPlayerStatus::Finished;
    player_state.skips_remaining = 0;
    player_state.steps_remaining = 0;
}

fn transition_to_error_state(error_message: &str, player_state: &mut ChangeScriptPlayerState) {
    log::error!("{}", error_message);
    player_state.status = ChangeScriptPlayerStatus::Error;
    player_state.error_message = Some(error_message.to_string());
}

pub async fn delayer_thread(mut delayer_rx_channel: Receiver<DelayChangeScriptRecordMessage>, player_tx_channel: Sender<ChangeScriptPlayerMessage>) {
    log::info!("ChangeScriptRecord Delayer thread started...");
    log::trace!("Starting ChangeScriptRecord Delayer loop...");

    loop {
        log::trace!("Top of ChangeScriptRecord Delayer loop.");

        match delayer_rx_channel.recv().await {
            Some(message) => {
                log::debug!("Processing DelayChangeScriptRecordMessage: {:?}", message);

                // Sleep for the specified time.
                sleep(Duration::from_nanos(message.delay_ns)).await;

                // Send a DispatchChangeEvent command to the ChangeScriptPlayer.
                let msg = ChangeScriptPlayerMessage {
                    command: ChangeScriptPlayerCommand::ProcessDelayedRecord(message.delay_sequence),
                    response_tx: None,
                };
                let response = player_tx_channel.send(msg).await;

                match response {
                    Ok(_) => {
                        log::debug!("Sent ProcessDelayedRecord command to ChangeScriptPlayer.");
                    },
                    Err(e) => {
                        log::error!("Error sending ProcessDelayedRecord command to ChangeScriptPlayer: {:?}", e);
                    }
                }
            },
            None => {
                log::error!("ChangeScriptRecord delayer channel closed.");
                break;
            }
        }
    }
}

// Function to log the current Delayed ChangeScriptRecord at varying levels of detail.
fn log_scheduled_test_script_record(msg: &str, record: &ScheduledChangeScriptRecord) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, record),
        log::LevelFilter::Debug => log::debug!("{} - seq:{:?}, offset_ns:{:?}, replay_time_ns:{:?}", 
            msg, record.seq_record.seq, record.seq_record.offset_ns, record.replay_time_ns),
        _ => {}
    }
}

// Function to log the current Next ChangeScriptRecord at varying levels of detail.
fn log_sequenced_test_script_record(msg: &str, record: &SequencedChangeScriptRecord) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, record),
        log::LevelFilter::Debug => log::debug!("{} - seq:{:?}, offset_ns:{:?}", msg, record.seq, record.offset_ns),
        _ => {}
    }
}

// Function to log the Player State at varying levels of detail.
fn log_test_script_player_state(msg: &str, state: &ChangeScriptPlayerState) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, state),
        log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, state),
        log::LevelFilter::Info => log::info!("{} - status:{:?}, error_message:{:?}, start_replay_time:{:?}, current_replay_time:{:?}, skips_remaining:{:?}, steps_remaining:{:?}",
            msg, state.status, state.error_message, state.start_replay_time, state.current_replay_time, state.skips_remaining, state.steps_remaining),
        _ => {}
    }
}
