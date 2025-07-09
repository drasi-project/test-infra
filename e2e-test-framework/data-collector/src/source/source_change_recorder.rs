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

use std::{sync::Arc, time::SystemTime};

use serde::Serialize;
use tokio::{sync::{mpsc::{Receiver, Sender}, oneshot, Mutex}, task::JoinHandle };

use test_data_store::{data_collection_storage::DataCollectionSourceStorage, scripts::ChangeScriptRecord};

use crate::{config::{SourceChangeQueueReaderConfig, SourceChangeRecorderConfig}, source::change_queue_readers::{get_source_change_queue_reader, none_change_queue_reader::NoneSourceChangeQueueReader, SourceChangeQueueReader, SourceChangeQueueReaderMessage}};

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

#[derive(Clone, Debug)]
pub struct SourceChangeRecorderSettings {
    pub drain_queue_on_stop: bool,
    pub source_id: String,
    pub source_change_queue_reader_config: Option<SourceChangeQueueReaderConfig>,
    pub storage: DataCollectionSourceStorage,
}   

impl SourceChangeRecorderSettings {
    pub fn new(config: &SourceChangeRecorderConfig, source_id: String, storage: DataCollectionSourceStorage) -> anyhow::Result<Self> {
        Ok( SourceChangeRecorderSettings {
            drain_queue_on_stop: config.drain_queue_on_stop.unwrap_or(false),
            source_id,
            source_change_queue_reader_config: config.change_queue_reader.clone(),
            storage
        })
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
    settings: SourceChangeRecorderSettings,
    recorder_tx_channel: Sender<SourceChangeRecorderMessage>,
    _recorder_thread_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl SourceChangeRecorder {
    pub async fn new(config: &SourceChangeRecorderConfig, source_id: String, storage: DataCollectionSourceStorage) -> anyhow::Result<Self> {
        log::debug!("Creating SourceChangeRecorder from config {:?}", config);

        let settings = SourceChangeRecorderSettings::new(config, source_id, storage)?;

        let (recorder_tx_channel, player_rx_channel) = tokio::sync::mpsc::channel(100);
        let recorder_thread_handle = tokio::spawn(recorder_thread(settings.clone(), player_rx_channel));

        Ok(Self {
            settings,
            recorder_tx_channel,
            _recorder_thread_handle: Arc::new(Mutex::new(recorder_thread_handle)),
        })
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
pub async fn recorder_thread(settings: SourceChangeRecorderSettings, mut recorder_rx_channel: Receiver<SourceChangeRecorderMessage>) {
    log::info!("SourceChangeRecorder thread started...");
    log::debug!("SourceChangeRecorder thread started using settings: {:?}", settings);

    // Initialize the SourceChangeRecorderState.
    let mut recorder_state = SourceChangeRecorderState {
        current_time_offset: 0,
        error_message: None,
        last_record: None,
        start_record_time: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64,
        status: SourceChangeRecorderStatus::Paused,
    };

    // Create the ChangeScriptWriter based on the provided configuration.
    // let writer_settings = change_script_file_writer::ChangeScriptWriterSettings {
    //     folder_path: settings.data_store_path.clone(),
    //     script_name: "source_change_recorder".to_string(),
    //     max_size: None,
    // };

    // let mut change_script_writer = match ChangeScriptWriter::new(writer_settings) {
    //     Ok(mut writer) => {
    //         Some(writer)
    //     },
    //     Err(e) => {
    //         transition_to_error_state(format!("Error creating ChangeScriptWriter: {:?}", e).as_str(), &mut recorder_state);
    //         None
    //     }
    // };

    // Create the SourceChangeQueueReader based on the provided configuration.
    let change_queue_reader: Box<dyn SourceChangeQueueReader + Send + Sync> = match get_source_change_queue_reader( settings.source_change_queue_reader_config, &settings.source_id).await {
        Ok(reader) => {
            reader
        },
        Err(e) => {
            transition_to_error_state(format!("Error creating SourceChangeQueueReader: {:?}", e).as_str(), &mut recorder_state);
            NoneSourceChangeQueueReader::new()
        }
    };

    let init_result_future = change_queue_reader.init();

    let init_result = init_result_future.await;

    let mut change_queue_reader_channel = match init_result {
        Ok(channel) => {
            channel
        },
        Err(e) => {
            transition_to_error_state(format!("Error initializing SourceChangeQueueReader: {:?}", e).as_str(), &mut recorder_state);
            NoneSourceChangeQueueReader::new().init().await.unwrap()
        }
    };

    change_queue_reader.start().await.unwrap();

    // TODO: Create the BootstrapDataRecorder based on the provided configuration.

    log::trace!("Starting SourceChangeRecorder loop...");

    // Loop to process messages sent to the SourceChangeRecorder or received from the Source Change Queue.
    // Always process all messages in the command channel and act on them first.
    loop {
        log_change_script_recorder_state("Top of SourceChangeRecorder loop", &recorder_state);

        tokio::select! {
            // Process the branch in sequence to prioritize processing of control messages.
            // biased;

            // Process messages from the command channel.
            cmd = recorder_rx_channel.recv() => {
                match cmd {
                    Some(message) => {
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
                    },
                    None => {
                        log::error!("SourceChangeRecorder command channel closed.");
                        break;
                    }
                }
            },
            change_queue_message = change_queue_reader_channel.recv() => {
                match change_queue_message {
                    Some(SourceChangeQueueReaderMessage::QueueRecord(queue_record)) => {
                        log::trace!("Received SOurce Change Queue Record: {:?}", queue_record);

                        // let record = SourceChangeRecord {
                        //     offset_ns: recorder_state.current_time_offset,
                        //     queue_record,
                        // };

                        // Display the record to the console
                        log::debug!("Record In Recorder: {:?}", queue_record);
                    },
                    Some(SourceChangeQueueReaderMessage::Error(e)) => {
                        log::error!("Error getting next change event: {:?}", e);
                    },
                    None => {
                        log::info!("SourceChangeQueueReader channel closed.");
                    }
                }
            },
            else => {
                log::debug!("SourceChangeRecorder loop - no messages to process.");
            }
        }
    }

    log::debug!("SourceChangeRecorder thread exiting...");

}

fn transition_from_paused_state(command: &SourceChangeRecorderCommand, recorder_state: &mut SourceChangeRecorderState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", recorder_state.status, command);

    match command {
        SourceChangeRecorderCommand::Start => {
            recorder_state.status = SourceChangeRecorderStatus::Running;
            Ok(())
        },
        SourceChangeRecorderCommand::Pause => {
            Err(SourceChangeRecorderError::AlreadyPaused.into())
        },
        SourceChangeRecorderCommand::Stop => {
            recorder_state.status = SourceChangeRecorderStatus::Stopped;
            Ok(())
        },
        SourceChangeRecorderCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_running_state(command: &SourceChangeRecorderCommand, recorder_state: &mut SourceChangeRecorderState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", recorder_state.status, command);

    match command {
        SourceChangeRecorderCommand::Start => {
            Err(SourceChangeRecorderError::AlreadyRunning.into())
        },
        SourceChangeRecorderCommand::Pause => {
            recorder_state.status = SourceChangeRecorderStatus::Paused;
            Ok(())
        },
        SourceChangeRecorderCommand::Stop => {
            recorder_state.status = SourceChangeRecorderStatus::Stopped;
            Ok(())
        },
        SourceChangeRecorderCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stopped_state(command: &SourceChangeRecorderCommand, recorder_state: &mut SourceChangeRecorderState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", recorder_state.status, command);

    Err(SourceChangeRecorderError::AlreadyStopped.into())
}

fn transition_from_error_state(command: &SourceChangeRecorderCommand, recorder_state: &mut SourceChangeRecorderState) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", recorder_state.status, command);

    Err(SourceChangeRecorderError::Error(recorder_state.status).into())
}

fn transition_to_error_state(error_message: &str, recorder_state: &mut SourceChangeRecorderState) {
    log::error!("{}", error_message);
    recorder_state.status = SourceChangeRecorderStatus::Error;
    recorder_state.error_message = Some(error_message.to_string());
}

// Function to log the Player State at varying levels of detail.
fn log_change_script_recorder_state(msg: &str, state: &SourceChangeRecorderState) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, state),
        log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, state),
        log::LevelFilter::Info => log::info!("{} - status:{:?}, error_message:{:?}, start_replay_time:{:?}, current_time_offset:{:?}",
            msg, state.status, state.error_message, state.start_record_time, state.current_time_offset),
        _ => {}
    }
}