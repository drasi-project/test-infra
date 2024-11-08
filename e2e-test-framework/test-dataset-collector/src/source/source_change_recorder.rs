use std::{path::PathBuf, sync::Arc, time::SystemTime};

use futures::future::join_all;
use serde::Serialize;
use tokio::sync::{mpsc::{Receiver, Sender}, oneshot, Mutex};
use tokio::sync::mpsc::error::TryRecvError::{Empty, Disconnected};
use tokio::task::JoinHandle;
use tokio::time::Duration;

use test_runner::script_source::{change_script_file_writer::ChangeScriptWriter, ChangeScriptRecord, SourceChangeEvent};

use crate::{ config::{SourceChangeQueueReaderConfig, SourceChangeRecorderConfig}, DatasetSource, TestRunReactivator };

use super::change_queue_readers::SourceChangeQueueReader;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeRecorderError {
    #[error("SourceChangeRecorder is already paused.")]
    AlreadyPaused,
    #[error("SourceChangeRecorder is already running.")]
    AlreadyRunning,
    #[error("SourceChangeRecorder is already stopped.")]
    AlreadyStopped,
    #[error("SourceChangeRecorder is currently in an Error state - {0:?}")]
    Error(SourceChangeRecorderStatus),
    #[error("SourceChangeRecorder is currently Running. Pause or Stop before trying to Publish.")]
    PauseToPublish,
}

#[derive(Clone, Debug, Serialize)]
pub struct SourceChangeRecorderSettings {
    pub change_queue_reader: SourceChangeQueueReader,
    pub drain_queue_on_stop: bool,
    pub change_event_loggers: Vec<ChangeEventLogger>,
}   

impl SourceChangeRecorderSettings {
    pub fn try_from_config(dataset_source: DatasetSource, data_store_path: String) -> anyhow::Result<Self> {

        // If the SourceConfig doesn't contain a ReactivatorConfig, log and return an error.
        // Otherwise, clone the TestRunSource and extract the TestRunReactivator.
        let (test_run_source, reactivator) = match dataset_source.reactivator {
            Some(reactivator) => {
                (DatasetSource { reactivator: None, ..dataset_source }, reactivator)
            },
            None => {
                anyhow::bail!("No ReactivatorConfig provided in TestRunSource: {:?}", dataset_source);
            }
        };

        Ok(SourceChangeRecorderSettings {
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SourceChangeRecorderStatus {
    Running,
    Paused,
    Stopped,
    Error
}

impl Serialize for SourceChangeRecorderStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            SourceChangeRecorderStatus::Running => serializer.serialize_str("Running"),
            SourceChangeRecorderStatus::Paused => serializer.serialize_str("Paused"),
            SourceChangeRecorderStatus::Stopped => serializer.serialize_str("Stopped"),
            SourceChangeRecorderStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SourceChangeRecorderState {
    pub current_time_offset: u64,
    pub error_message: Option<String>,
    pub last_record: Option<ChangeScriptRecord>,
    pub start_record_time: u64,
    pub status: SourceChangeRecorderStatus,
}

impl Default for SourceChangeRecorderState {
    fn default() -> Self {
        Self {
            current_time_offset: 0,
            error_message: None,
            last_record: None,
            start_record_time: 0,
            status: SourceChangeRecorderStatus::Paused,
        }
    }
}

// Enum of SourceChangeRecorder commands sent from Web API handler functions.
#[derive(Debug)]
pub enum SourceChangeRecorderCommand {
    // Command to start the SourceChangeRecorder.
    Start,
    // Command to pause the SourceChangeRecorder.
    Pause,
    // Command to stop the SourceChangeRecorder.
    Stop,
    // Command to get the current state of the SourceChangeRecorder.
    GetState,
}

// Struct for messages sent to the SourceChangeRecorder from the functions in the Web API.
#[derive(Debug)]
pub struct SourceChangeRecorderMessage {
    // Command sent to the SourceChangeRecorder.
    pub command: SourceChangeRecorderCommand,
    // One-shot channel for SourceChangeRecorder to send a response back to the caller.
    pub response_tx: Option<oneshot::Sender<SourceChangeRecorderMessageResponse>>,
}

// A struct for the Response sent back from the SourceChangeRecorder to the calling Web API handler.
#[derive(Debug)]
pub struct SourceChangeRecorderMessageResponse {
    // Result of the command.
    pub result: anyhow::Result<()>,
    // State of the SourceChangeRecorder after the command.
    pub state: SourceChangeRecorderState,
}

#[derive(Clone, Debug)]
pub struct SourceChangeRecorder {
    // Channel used to send messages to the SourceChangeRecorder thread.
    settings: SourceChangeRecorderSettings,
    recorder_tx_channel: Sender<SourceChangeRecorderMessage>,
    _recorder_thread_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl SourceChangeRecorder {
    pub async fn try_from_config(config: &SourceChangeRecorderConfig, data_store_path: PathBuf) -> anyhow::Result<Self> {

        let settings = SourceChangeRecorderSettings::try_from_config(test_run_source, script_files, data_store_path)
        let (recorder_tx_channel, player_rx_channel) = tokio::sync::mpsc::channel(100);
        let recorder_thread_handle = tokio::spawn(recorder_thread(player_rx_channel, settings.clone()));

        Self {
            settings,
            recorder_tx_channel,
            _recorder_thread_handle: Arc::new(Mutex::new(recorder_thread_handle)),
        }
    }

    pub fn get_id(&self) -> String {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> SourceChangeRecorderSettings {
        self.settings.clone()
    }

    pub async fn get_state(&self) -> anyhow::Result<SourceChangeRecorderMessageResponse> {
        self.send_command(SourceChangeRecorderCommand::GetState).await
    }

    pub async fn start(&self) -> anyhow::Result<SourceChangeRecorderMessageResponse> {
        self.send_command(SourceChangeRecorderCommand::Start).await
    }

    pub async fn pause(&self) -> anyhow::Result<SourceChangeRecorderMessageResponse>  {
        self.send_command(SourceChangeRecorderCommand::Pause).await
    }

    pub async fn stop(&self) -> anyhow::Result<SourceChangeRecorderMessageResponse>  {
        self.send_command(SourceChangeRecorderCommand::Stop).await
    }

    async fn send_command(&self, command: SourceChangeRecorderCommand) -> anyhow::Result<SourceChangeRecorderMessageResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self.recorder_tx_channel.send(SourceChangeRecorderMessage {
            command,
            response_tx: Some(response_tx),
        }).await;

        match r {
            Ok(_) => Ok(response_rx.await.unwrap()),
            Err(e) => anyhow::bail!("Error sending command to SourceChangeRecorder: {:?}", e),
        }
    }
}

// Function that defines the operation of the SourceChangeRecorder thread.
// The SourceChangeRecorder thread processes ChangeScriptRecorderCommands sent to it from the Web API handler functions.
// The Web API function communicate via a channel and provide oneshot channels for the SourceChangeRecorder to send responses back.
pub async fn recorder_thread(mut recorder_rx_channel: Receiver<SourceChangeRecorderMessage>, recorder_settings: SourceChangeRecorderSettings) {

    log::info!("SourceChangeRecorder thread started...");

    // Initialize the SourceChangeRecorder infrastructure.
    // Create a ChangeScriptReader to read the ChangeScript files.
    // Create a ChangeScriptRecorderState to hold the state of the SourceChangeRecorder. The SourceChangeRecorder always starts in a paused state.
    // Create a SourceChangeDispatcher to send SourceChangeEvents.
    let (mut recorder_state, mut test_script_reader, mut dispatchers) = match ChangeScriptReader::new(recorder_settings.script_files.clone()) {
        Ok(mut reader) => {

            let header = reader.get_header();
            log::debug!("Loaded ChangeScript. {:?}", header);

            let mut player_state = SourceChangeRecorderState::default();
            player_state.ignore_scripted_pause_commands = recorder_settings.reactivator.ignore_scripted_pause_commands;
            player_state.time_mode = recorder_settings.reactivator.time_mode;
            player_state.spacing_mode = recorder_settings.reactivator.spacing_mode;
        
            // Set the start_replay_time based on the time mode and the script start time from the header.
            player_state.start_record_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
            player_state.current_time_offset = 0;

            let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();

            for dispatcher_config in recorder_settings.reactivator.dispatchers.iter() {
                match dispatcher_config {
                    SourceChangeQueueReaderConfig::Dapr(dapr_config) => {
                        match DaprSourceChangeDispatcherSettings::try_from_config(dapr_config, recorder_settings.test_run_source.source_id.clone()) {
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
                    SourceChangeQueueReaderConfig::Console(console_config ) => {
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
                    SourceChangeQueueReaderConfig::JsonlFile(jsonl_file_config) => {
                        // Construct the path to the local file used to store the generated SourceChangeEvents.
                        let folder_path = format!("./{}/test_output/{}/{}/change_logs/{}", 
                            jsonl_file_config.folder_path.clone().unwrap_or(recorder_settings.data_store_path.clone()),
                            recorder_settings.test_run_source.test_id, 
                            recorder_settings.test_run_source.test_run_id, 
                            recorder_settings.test_run_source.source_id);

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
            let mut player_state = SourceChangeRecorderState::default();
            transition_to_error_state(format!("Error creating ChangeScriptWriter: {:?}", e).as_str(), &mut player_state);
            (player_state, None, vec![])
        }
    };

    log::trace!("Starting SourceChangeRecorder loop...");

    // Set up Redis client and connection
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut con = client.get_async_connection().await?;

    // Loop to process messages sent to the SourceChangeRecorder and read data from Change Script.
    // Always process all messages in the command channel and act on them first.
    // If there are no messages and the SourceChangeRecorder is running, stepping, or skipping, continue processing the Change Script.
    loop {
        log_change_script_recorder_state("Top of SourceChangeRecorder loop", &recorder_state);

        tokio::select! {

            // Process the branch in sequence to prioritize processing of command messages.
            biased;

            // Process all messages in the command channel.
            Some(message) = recorder_rx_channel.recv() => {
                log::debug!("Received {:?} command message.", message.command);

                let transition_response = match recorder_state.status {
                    SourceChangeRecorderStatus::Running => transition_from_running_state(&message.command, &mut recorder_state),
                    SourceChangeRecorderStatus::Paused => transition_from_paused_state(&message.command, &mut recorder_state),
                    SourceChangeRecorderStatus::Stopped => transition_from_stopped_state(&message.command, &mut recorder_state),
                    SourceChangeRecorderStatus::Error => transition_from_error_state(&message.command, &mut recorder_state),
                };

                if message.response_tx.is_some() {
                    let message_response = SourceChangeRecorderMessageResponse {
                        result: transition_response,
                        state: recorder_state.clone(),
                    };
        
                    let r = message.response_tx.unwrap().send(message_response);
                    if let Err(e) = r {
                        log::error!("Error in SourceChangeRecorder sending message response back to caller: {:?}", e);
                    }
                }

                log_change_script_recorder_state(format!("Post {:?} command", message.command).as_str(), &recorder_state);
            }

            // Check for a message from the Redis queue
            redis_msg = con.blpop::<_, (String, String)>("my_queue", 0) => {
                match redis_msg {
                    Ok((_, msg)) => println!("Received from Redis queue: {}", msg),
                    Err(e) => {
                        eprintln!("Failed to read from Redis: {}", e);
                        break;
                    }
                }
            }

        }

        // // If the SourceChangeRecorder needs a command to continue, block until a command is received.
        // // Otherwise, try to receive a command message without blocking and then process the next Source Change Event.
        // let block_on_message = 
        //     recorder_state.status == SourceChangeRecorderStatus::Paused 
        //     || recorder_state.status == SourceChangeRecorderStatus::Stopped 
        //     || recorder_state.status == SourceChangeRecorderStatus::Error;
        
        // let message: Option<SourceChangeRecorderMessage> = if block_on_message {
        //     log::trace!("SourceChangeRecorder Thread Blocked; waiting for a command message...");

        //     match recorder_rx_channel.recv().await {
        //         Some(message) => {
        //             log::trace!("Received {:?} command message.", message.command);
        //             Some(message)
        //         },
        //         None => {
        //             // Cannot continue if the channel is closed.
        //             transition_to_error_state("Message channel closed", &mut recorder_state);
        //             None
        //         },
        //     }
        // } else {
        //     log::trace!("SourceChangeRecorder Thread Unblocked; TRYing for a command message before processing next script record");

        //     match recorder_rx_channel.try_recv() {
        //         Ok(message) => {
        //             log::trace!("Received {:?} command message.", message.command);
        //             Some(message)
        //         },
        //         Err(Empty) => None,
        //         Err(Disconnected) => {
        //             // Cannot continue if the channel is closed.
        //             transition_to_error_state("Message channel closed", &mut recorder_state);
        //             None
        //         }
        //     }
        // };

        // // If a message was received, process it. 
        // // Otherwise, continue processing the Source Change Queue.
        // match message {
        //     Some(message) => {

        //         log::debug!("Processing command: {:?}", message.command);                

        //         let transition_response = match recorder_state.status {
        //             SourceChangeRecorderStatus::Running => transition_from_running_state(&message.command, &mut recorder_state),
        //             SourceChangeRecorderStatus::Paused => transition_from_paused_state(&message.command, &mut recorder_state),
        //             SourceChangeRecorderStatus::Stopped => transition_from_stopped_state(&message.command, &mut recorder_state),
        //             SourceChangeRecorderStatus::Error => transition_from_error_state(&message.command, &mut recorder_state),
        //         };

        //         if message.response_tx.is_some() {
        //             let message_response = SourceChangeRecorderMessageResponse {
        //                 result: transition_response,
        //                 state: recorder_state.clone(),
        //             };
    
        //             let r = message.response_tx.unwrap().send(message_response);
        //             if let Err(e) = r {
        //                 log::error!("Error in SourceChangeRecorder sending message response back to caller: {:?}", e);
        //             }
        //         }

        //         log_change_script_recorder_state(format!("Post {:?} command", message.command).as_str(), &recorder_state);
        //     },
        //     None => {
        //         // Only process the next ChangeScriptRecord if the player is not in an error state.
        //         if let SourceChangeRecorderStatus::Error = recorder_state.status {
        //             log_change_script_recorder_state("Trying to process Next ChangeScriptRecord, but Player in error state", &recorder_state);
        //         } else {
        //             process_next_test_script_record(&mut recorder_state, &mut dispatchers, delayer_tx_channel.clone()).await;
        //             read_next_sequenced_test_script_record(&mut recorder_state, &mut test_script_reader.as_mut().unwrap());
        //         }
        //     }
        // }
    }
}

fn read_next_sequenced_test_script_record(player_state: &mut SourceChangeRecorderState, test_script_reader: &mut ChangeScriptReader) {

    // Do nothing if the player is already in an error state.
    if let SourceChangeRecorderStatus::Error = player_state.status {
        log_change_script_recorder_state("Ignoring read_next_sequenced_test_script_record call due to error state", &player_state);
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
            log_change_script_recorder_state("ChangeScriptReader.next() returned None, shouldn't be seeing this.", &player_state);
        }
    };
}

async fn process_next_test_script_record(mut player_state: &mut SourceChangeRecorderState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>, delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>) {

    // Do nothing if the player is already in an error state.
    if let SourceChangeRecorderStatus::Error = player_state.status {
        log_change_script_recorder_state("Ignoring process_next_test_script_record call due to error state", &player_state);
        return;
    }

    // next_record should never be None.
    if player_state.next_record.is_none() {
        transition_to_error_state(format!("ChangeScriptReader should never return None for next_record").as_str(), player_state);
        return;
    }

    let seq_record = player_state.next_record.as_ref().unwrap().clone();
    log_sequenced_test_script_record("Next ChangeScriptRecord Pre-TimeShift", &seq_record);

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

async fn process_delayed_test_script_record(delayed_record_seq: u64, player_state: &mut SourceChangeRecorderState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>) {

    // Do nothing if the player is already in an error state.
    if let SourceChangeRecorderStatus::Error = player_state.status {
        log_change_script_recorder_state("Ignoring process_delayed_test_script_record call due to error state", &player_state);
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
            log_change_script_recorder_state(
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

async fn resolve_test_script_record_effect(record: ScheduledChangeScriptRecord, player_state: &mut SourceChangeRecorderState, dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>) {

    // Do nothing if the player is already in an error state.
    if let SourceChangeRecorderStatus::Error = player_state.status {
        log_change_script_recorder_state("Ignoring resolve_test_script_record_effect call due to error state", &player_state);
        return;
    }
        
    match &record.seq_record.record {
        ChangeScriptRecord::SourceChange(change_record) => {
            match player_state.status {
                SourceChangeRecorderStatus::Running => {
                    // Dispatch the SourceChangeEvent.
                    dispatch_source_change_events(dispatchers, vec!(&change_record.source_change_event)).await;
                    // let _ = dispatcher.dispatch_source_change_events(vec!(&change_record.source_change_event)).await;
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
                    SourceChangeRecorderStatus::Running => transition_from_running_state(&SourceChangeRecorderCommand::Pause, player_state),
                    SourceChangeRecorderStatus::Paused => transition_from_paused_state(&SourceChangeRecorderCommand::Pause, player_state),
                    SourceChangeRecorderStatus::Stopped => transition_from_stopped_state(&SourceChangeRecorderCommand::Pause, player_state),
                    SourceChangeRecorderStatus::Error => transition_from_error_state(&SourceChangeRecorderCommand::Pause, player_state),
                };
            }
        },
        ChangeScriptRecord::Label(label_record) => {
            log::info!("Reached Change Script Label: {:?}", label_record);
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

fn transition_from_paused_state(command: &SourceChangeRecorderCommand, player_state: &mut SourceChangeRecorderState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        SourceChangeRecorderCommand::Start => {
            player_state.status = SourceChangeRecorderStatus::Running;
            Ok(())
        },
        SourceChangeRecorderCommand::ProcessDelayedRecord(_) => {
            // The SourceChangeRecorder is Paused, so we ignore the DispatchChangeEvent command.
            Ok(())
        },
        SourceChangeRecorderCommand::Step(steps) => {
            player_state.status = SourceChangeRecorderStatus::Stepping;
            player_state.steps_remaining = *steps;
            Ok(())
        },
        SourceChangeRecorderCommand::Skip(skips) => {
            player_state.status = SourceChangeRecorderStatus::Skipping;
            player_state.skips_remaining = *skips;
            Ok(())
        },
        SourceChangeRecorderCommand::Pause => {
            Err(SourceChangeRecorderError::AlreadyPaused.into())
        },
        SourceChangeRecorderCommand::Stop => {
            player_state.status = SourceChangeRecorderStatus::Stopped;
            Ok(())
        },
        SourceChangeRecorderCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_running_state(command: &SourceChangeRecorderCommand, player_state: &mut SourceChangeRecorderState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        SourceChangeRecorderCommand::Start => {
            Err(SourceChangeRecorderError::AlreadyRunning.into())
        },
        SourceChangeRecorderCommand::Pause => {
            player_state.status = SourceChangeRecorderStatus::Paused;
            Ok(())
        },
        SourceChangeRecorderCommand::Stop => {
            player_state.status = SourceChangeRecorderStatus::Stopped;
            Ok(())
        },
        SourceChangeRecorderCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stopped_state(command: &SourceChangeRecorderCommand, player_state: &mut SourceChangeRecorderState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(SourceChangeRecorderError::AlreadyStopped.into())
}

fn transition_from_error_state(command: &SourceChangeRecorderCommand, player_state: &mut SourceChangeRecorderState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(SourceChangeRecorderError::Error(player_state.status).into())
}

fn transition_to_error_state(error_message: &str, player_state: &mut SourceChangeRecorderState) {
    log::error!("{}", error_message);
    player_state.status = SourceChangeRecorderStatus::Error;
    player_state.error_message = Some(error_message.to_string());
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
fn log_change_script_recorder_state(msg: &str, state: &SourceChangeRecorderState) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, state),
        log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, state),
        log::LevelFilter::Info => log::info!("{} - status:{:?}, error_message:{:?}, start_replay_time:{:?}, current_replay_time:{:?}, skips_remaining:{:?}, steps_remaining:{:?}",
            msg, state.status, state.error_message, state.start_record_time, state.current_replay_time, state.skips_remaining, state.steps_remaining),
        _ => {}
    }
}