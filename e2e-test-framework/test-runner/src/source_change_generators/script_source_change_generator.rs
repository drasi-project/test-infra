use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use futures::future::join_all;
use serde::Serialize;
use tokio::sync::{mpsc::{Receiver, Sender}, oneshot, Mutex};
use tokio::sync::mpsc::error::TryRecvError::{Empty, Disconnected};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use test_data_store::{test_repo_storage::{models::SpacingMode, scripts::{change_script_file_reader::ChangeScriptReader, ChangeScriptRecord, SequencedChangeScriptRecord, SourceChangeEvent}, TestSourceStorage}, test_run_storage::{TestRunSourceId, TestRunSourceStorage}};

use crate::{
    config::{CommonSourceChangeGeneratorConfig, ScriptSourceChangeGeneratorConfig, SourceChangeDispatcherConfig}, source_change_dispatchers::{
        console_dispatcher::{ConsoleSourceChangeDispatcher, ConsoleSourceChangeDispatcherSettings}, 
        dapr_dispatcher::{DaprSourceChangeDispatcher, DaprSourceChangeDispatcherSettings}, 
        jsonl_file_dispatcher::{JsonlFileSourceChangeDispatcher, JsonlFileSourceChangeDispatcherSettings}, 
    }, TimeMode
};
use crate::source_change_dispatchers::SourceChangeDispatcher;

use super::{SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorStatus};

#[derive(Debug, thiserror::Error)]
pub enum ScriptSourceGeneratorError {
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
    Error(SourceChangeGeneratorStatus),
    #[error("ChangeScriptPlayer is currently Running. Pause before trying to Skip.")]
    PauseToSkip,
    #[error("ChangeScriptPlayer is currently Running. Pause before trying to Step.")]
    PauseToStep,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptSourceGeneratorSettings {
    pub id: TestRunSourceId,
    pub ignore_scripted_pause_commands: bool,
    pub input_storage: TestSourceStorage,
    pub dispatchers: Vec<SourceChangeDispatcherConfig>,
    pub output_storage: TestRunSourceStorage,
    pub spacing_mode: SpacingMode,
    pub time_mode: TimeMode,
}

impl ScriptSourceGeneratorSettings {
    pub async fn new(
        test_run_source_id: TestRunSourceId, 
        common_config: CommonSourceChangeGeneratorConfig, 
        unique_config: ScriptSourceChangeGeneratorConfig, 
        input_storage: TestSourceStorage, 
        output_storage: TestRunSourceStorage
    ) -> anyhow::Result<Self> {

        Ok(ScriptSourceGeneratorSettings {
            id: test_run_source_id,
            ignore_scripted_pause_commands: unique_config.ignore_scripted_pause_commands,
            input_storage,
            dispatchers: common_config.dispatchers,
            output_storage,
            spacing_mode: common_config.spacing_mode,
            time_mode: common_config.time_mode,
        })
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptSourceGeneratorState {
    pub current_replay_time: u64,
    pub delayed_record: Option<ScheduledChangeScriptRecord>,    
    pub error_message: Option<String>,
    pub ignore_scripted_pause_commands: bool,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub skips_remaining: u64,
    pub spacing_mode: SpacingMode,
    pub start_replay_time: u64,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub time_mode: TimeMode,
}

impl Default for ScriptSourceGeneratorState {
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
            status: SourceChangeGeneratorStatus::Paused,
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
#[derive(Debug,)]
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
    pub state: ScriptSourceGeneratorState,
}

#[derive(Clone, Debug)]
pub struct DelayChangeScriptRecordMessage {
    pub delay_ns: u64,
    pub delay_sequence: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChangeScriptPlayer {
    settings: ScriptSourceGeneratorSettings,
    #[serde(skip_serializing)]
    player_tx_channel: Sender<ChangeScriptPlayerMessage>,
    #[serde(skip_serializing)]
    _delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>,
    #[serde(skip_serializing)]
    _player_thread_handle: Arc<Mutex<JoinHandle<()>>>,
    #[serde(skip_serializing)]
    _delayer_thread_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl ChangeScriptPlayer {
    pub async fn new(test_run_source_id: TestRunSourceId, common_config: CommonSourceChangeGeneratorConfig, unique_config: ScriptSourceChangeGeneratorConfig, input_storage: TestSourceStorage, output_storage: TestRunSourceStorage) -> anyhow::Result<Box<dyn SourceChangeGenerator + Send + Sync>> {
        let settings = ScriptSourceGeneratorSettings::new(test_run_source_id, common_config, unique_config, input_storage, output_storage).await?;
        log::debug!("Creating ChangeScriptPlayer from {:#?}", &settings);

        let (player_tx_channel, player_rx_channel) = tokio::sync::mpsc::channel(100);
        let (delayer_tx_channel, delayer_rx_channel) = tokio::sync::mpsc::channel(100);

        let player_thread_handle = tokio::spawn(player_thread(player_rx_channel, delayer_tx_channel.clone(), settings.clone()));
        let delayer_thread_handle = tokio::spawn(delayer_thread(delayer_rx_channel, player_tx_channel.clone()));

        Ok(Box::new(Self {
            settings,
            player_tx_channel,
            _delayer_tx_channel: delayer_tx_channel,
            _player_thread_handle: Arc::new(Mutex::new(player_thread_handle)),
            _delayer_thread_handle: Arc::new(Mutex::new(delayer_thread_handle)),
        }))
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> ScriptSourceGeneratorSettings {
        self.settings.clone()
    }

    async fn send_command(&self, command: ChangeScriptPlayerCommand) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self.player_tx_channel.send(ChangeScriptPlayerMessage {
            command,
            response_tx: Some(response_tx),
        }).await;

        match r {
            Ok(_) => {
                let player_response = response_rx.await?;

                Ok(SourceChangeGeneratorCommandResponse {
                    result: player_response.result,
                    state: super::SourceChangeGeneratorState {
                        status: player_response.state.status,
                    },
                })
            },
            Err(e) => anyhow::bail!("Error sending command to ChangeScriptPlayer: {:?}", e),
        }
    }
}

#[async_trait]
impl SourceChangeGenerator for ChangeScriptPlayer {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ChangeScriptPlayerCommand::GetState).await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ChangeScriptPlayerCommand::Start).await
    }

    async fn step(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ChangeScriptPlayerCommand::Step(steps)).await
    }

    async fn skip(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ChangeScriptPlayerCommand::Skip(skips)).await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ChangeScriptPlayerCommand::Pause).await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ChangeScriptPlayerCommand::Stop).await
    }
}

// Function that defines the operation of the ChangeScriptPlayer thread.
// The ChangeScriptPlayer thread processes ChangeScriptPlayerCommands sent to it from the Web API handler functions.
// The Web API function communicate via a channel and provide oneshot channels for the ChangeScriptPlayer to send responses back.
pub async fn player_thread(mut player_rx_channel: Receiver<ChangeScriptPlayerMessage>, delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>, player_settings: ScriptSourceGeneratorSettings) {

    log::info!("Player thread started...");

    // Initialize the ChangeScriptPlayer infrastructure.
    // Create a ChangeScriptReader to read the ChangeScript files.
    // Create a ChangeScriptPlayerState to hold the state of the ChangeScriptPlayer. The ChangeScriptPlayer always starts in a paused state.
    // Create a SourceChangeDispatcher to send SourceChangeEvents.
    let script_files = match player_settings.input_storage.get_dataset().await {
        Ok(ds) => ds.source_change_script_files,
        Err(e) => {
            log::error!("Error getting dataset: {:?}", e);
            Vec::new()
        }
    };

    let (mut player_state, mut test_script_reader, mut dispatchers) = match ChangeScriptReader::new(script_files) {
        Ok(mut reader) => {

            let header = reader.get_header();
            log::debug!("Loaded ChangeScript. {:?}", header);

            let mut player_state = ScriptSourceGeneratorState::default();
            player_state.ignore_scripted_pause_commands = player_settings.ignore_scripted_pause_commands;
            player_state.time_mode = player_settings.time_mode;
            player_state.spacing_mode = player_settings.spacing_mode;
        
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

            for dispatcher_config in player_settings.dispatchers.iter() {
                match dispatcher_config {
                    SourceChangeDispatcherConfig::Dapr(dapr_config) => {
                        match DaprSourceChangeDispatcherSettings::new(dapr_config, player_settings.id.test_source_id.clone()) {
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
                        match ConsoleSourceChangeDispatcherSettings::new(console_config) {
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
                        let folder_path = match &jsonl_file_config.folder_path {
                            Some(config_path) => 
                                player_settings.output_storage.source_change_path.join(config_path),
                            None => 
                                player_settings.output_storage.source_change_path.join("jsonl_file_dispatcher"),
                        };

                        match JsonlFileSourceChangeDispatcherSettings::new(jsonl_file_config, folder_path) {
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
            let mut player_state = ScriptSourceGeneratorState::default();
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
                player_state.status == SourceChangeGeneratorStatus::Running 
                || player_state.status == SourceChangeGeneratorStatus::Stepping 
                || player_state.status == SourceChangeGeneratorStatus::Skipping
            ))
            || player_state.status == SourceChangeGeneratorStatus::Paused 
            || player_state.status == SourceChangeGeneratorStatus::Stopped 
            || player_state.status == SourceChangeGeneratorStatus::Finished 
            || player_state.status == SourceChangeGeneratorStatus::Error;
        
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
                        SourceChangeGeneratorStatus::Running => transition_from_running_state(&message.command, &mut player_state),
                        SourceChangeGeneratorStatus::Stepping => transition_from_stepping_state(&message.command, &mut player_state),
                        SourceChangeGeneratorStatus::Skipping => transition_from_skipping_state(&message.command, &mut player_state),
                        SourceChangeGeneratorStatus::Paused => transition_from_paused_state(&message.command, &mut player_state),
                        SourceChangeGeneratorStatus::Stopped => transition_from_stopped_state(&message.command, &mut player_state),
                        SourceChangeGeneratorStatus::Finished => transition_from_finished_state(&message.command, &mut player_state),
                        SourceChangeGeneratorStatus::Error => transition_from_error_state(&message.command, &mut player_state),
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
                if let SourceChangeGeneratorStatus::Error = player_state.status {
                    log_test_script_player_state("Trying to process Next ChangeScriptRecord, but Player in error state", &player_state);
                } else {
                    process_next_test_script_record(&mut player_state, &mut dispatchers, delayer_tx_channel.clone()).await;
                    read_next_sequenced_test_script_record(&mut player_state, &mut test_script_reader.as_mut().unwrap());
                }
            }
        }
    }
}

fn read_next_sequenced_test_script_record(player_state: &mut ScriptSourceGeneratorState, test_script_reader: &mut ChangeScriptReader) {

    // Do nothing if the player is already in an error state.
    if let SourceChangeGeneratorStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring read_next_sequenced_test_script_record call due to error state", &player_state);
        return;
    }

    // Do nothing if the ChangeScriptPlayer has finished processing all records.
    if let SourceChangeGeneratorStatus::Finished = player_state.status {
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

async fn process_next_test_script_record(mut player_state: &mut ScriptSourceGeneratorState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>, delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>) {

    // Do nothing if the player is already in an error state.
    if let SourceChangeGeneratorStatus::Error = player_state.status {
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

async fn process_delayed_test_script_record(delayed_record_seq: u64, player_state: &mut ScriptSourceGeneratorState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>) {

    // Do nothing if the player is already in an error state.
    if let SourceChangeGeneratorStatus::Error = player_state.status {
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

async fn resolve_test_script_record_effect(record: ScheduledChangeScriptRecord, player_state: &mut ScriptSourceGeneratorState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>) {

    // Do nothing if the player is already in an error state.
    if let SourceChangeGeneratorStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring resolve_test_script_record_effect call due to error state", &player_state);
        return;
    }
        
    match &record.seq_record.record {
        ChangeScriptRecord::SourceChange(change_record) => {
            match player_state.status {
                SourceChangeGeneratorStatus::Running => {
                    // Dispatch the SourceChangeEvent.
                    dispatch_source_change_events(dispatchers, vec!(&change_record.source_change_event)).await;
                    // let _ = dispatcher.dispatch_source_change_events(vec!(&change_record.source_change_event)).await;
                },
                SourceChangeGeneratorStatus::Stepping => {
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
                SourceChangeGeneratorStatus::Skipping => {
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
                    SourceChangeGeneratorStatus::Running => transition_from_running_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    SourceChangeGeneratorStatus::Stepping => transition_from_stepping_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    SourceChangeGeneratorStatus::Skipping => transition_from_skipping_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    SourceChangeGeneratorStatus::Paused => transition_from_paused_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    SourceChangeGeneratorStatus::Stopped => transition_from_stopped_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    SourceChangeGeneratorStatus::Finished => transition_from_finished_state(&ChangeScriptPlayerCommand::Pause, player_state),
                    SourceChangeGeneratorStatus::Error => transition_from_error_state(&ChangeScriptPlayerCommand::Pause, player_state),
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

fn time_shift_test_script_record(player_state: &ScriptSourceGeneratorState, seq_record: SequencedChangeScriptRecord) -> ScheduledChangeScriptRecord {

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

fn transition_from_paused_state(command: &ChangeScriptPlayerCommand, player_state: &mut ScriptSourceGeneratorState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ChangeScriptPlayerCommand::Start => {
            player_state.status = SourceChangeGeneratorStatus::Running;
            Ok(())
        },
        ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // The ChangeScriptPlayer is Paused, so we ignore the DispatchChangeEvent command.
            Ok(())
        },
        ChangeScriptPlayerCommand::Step(steps) => {
            player_state.status = SourceChangeGeneratorStatus::Stepping;
            player_state.steps_remaining = *steps;
            Ok(())
        },
        ChangeScriptPlayerCommand::Skip(skips) => {
            player_state.status = SourceChangeGeneratorStatus::Skipping;
            player_state.skips_remaining = *skips;
            Ok(())
        },
        ChangeScriptPlayerCommand::Pause => {
            Err(ScriptSourceGeneratorError::AlreadyPaused.into())
        },
        ChangeScriptPlayerCommand::Stop => {
            player_state.status = SourceChangeGeneratorStatus::Stopped;
            Ok(())
        },
        ChangeScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_running_state(command: &ChangeScriptPlayerCommand, player_state: &mut ScriptSourceGeneratorState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ChangeScriptPlayerCommand::Start => {
            Err(ScriptSourceGeneratorError::AlreadyRunning.into())
        },
        ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while ChangeScriptPlayer is Running.");
            Ok(())
        },
        ChangeScriptPlayerCommand::Step(_) => {
            Err(ScriptSourceGeneratorError::PauseToStep.into())
        },
        ChangeScriptPlayerCommand::Skip(_) => {
            Err(ScriptSourceGeneratorError::PauseToSkip.into())
        },
        ChangeScriptPlayerCommand::Pause => {
            player_state.status = SourceChangeGeneratorStatus::Paused;
            Ok(())
        },
        ChangeScriptPlayerCommand::Stop => {
            player_state.status = SourceChangeGeneratorStatus::Stopped;
            Ok(())
        },
        ChangeScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stepping_state(command: &ChangeScriptPlayerCommand, player_state: &mut ScriptSourceGeneratorState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ChangeScriptPlayerCommand::Start => {
            Err(ScriptSourceGeneratorError::CurrentlyStepping(player_state.steps_remaining).into())
        },
        ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while ChangeScriptPlayer is Stepping.");
            Ok(())
        },
        ChangeScriptPlayerCommand::Step(_) => {
            Err(ScriptSourceGeneratorError::CurrentlyStepping(player_state.steps_remaining).into())
        },
        ChangeScriptPlayerCommand::Skip(_) => {
            Err(ScriptSourceGeneratorError::CurrentlyStepping(player_state.steps_remaining).into())
        },
        ChangeScriptPlayerCommand::Pause => {
            player_state.status = SourceChangeGeneratorStatus::Paused;
            player_state.steps_remaining = 0;
            Ok(())
        },
        ChangeScriptPlayerCommand::Stop => {
            player_state.status = SourceChangeGeneratorStatus::Stopped;
            player_state.steps_remaining = 0;
            Ok(())
        },
        ChangeScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_skipping_state(command: &ChangeScriptPlayerCommand, player_state: &mut ScriptSourceGeneratorState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ChangeScriptPlayerCommand::Start => {
            Err(ScriptSourceGeneratorError::CurrentlySkipping(player_state.skips_remaining).into())
        },
        ChangeScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while ChangeScriptPlayer is Skipping.");
            Ok(())
        },
        ChangeScriptPlayerCommand::Step(_) => {
            Err(ScriptSourceGeneratorError::CurrentlySkipping(player_state.skips_remaining).into())
        },
        ChangeScriptPlayerCommand::Skip(_) => {
            Err(ScriptSourceGeneratorError::CurrentlySkipping(player_state.skips_remaining).into())
        },
        ChangeScriptPlayerCommand::Pause => {
            player_state.status = SourceChangeGeneratorStatus::Paused;
            player_state.skips_remaining = 0;
            Ok(())
        },
        ChangeScriptPlayerCommand::Stop => {
            player_state.status = SourceChangeGeneratorStatus::Stopped;
            player_state.skips_remaining = 0;
            Ok(())
        },
        ChangeScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stopped_state(command: &ChangeScriptPlayerCommand, player_state: &mut ScriptSourceGeneratorState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ScriptSourceGeneratorError::AlreadyStopped.into())
}

fn transition_from_finished_state(command: &ChangeScriptPlayerCommand, player_state: &mut ScriptSourceGeneratorState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ScriptSourceGeneratorError::AlreadyFinished.into())
}

fn transition_from_error_state(command: &ChangeScriptPlayerCommand, player_state: &mut ScriptSourceGeneratorState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ScriptSourceGeneratorError::Error(player_state.status).into())
}

fn transition_to_finished_state(player_state: &mut ScriptSourceGeneratorState) {
    player_state.status = SourceChangeGeneratorStatus::Finished;
    player_state.skips_remaining = 0;
    player_state.steps_remaining = 0;
}

fn transition_to_error_state(error_message: &str, player_state: &mut ScriptSourceGeneratorState) {
    log::error!("{}", error_message);
    player_state.status = SourceChangeGeneratorStatus::Error;
    player_state.error_message = Some(error_message.to_string());
}

pub async fn delayer_thread(mut delayer_rx_channel: Receiver<DelayChangeScriptRecordMessage>, player_tx_channel: Sender<ChangeScriptPlayerMessage>) {
    log::info!("Record delayer thread started...");
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
fn log_test_script_player_state(msg: &str, state: &ScriptSourceGeneratorState) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, state),
        log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, state),
        log::LevelFilter::Info => log::info!("{} - status:{:?}, error_message:{:?}, start_replay_time:{:?}, current_replay_time:{:?}, skips_remaining:{:?}, steps_remaining:{:?}",
            msg, state.status, state.error_message, state.start_replay_time, state.current_replay_time, state.skips_remaining, state.steps_remaining),
        _ => {}
    }
}
