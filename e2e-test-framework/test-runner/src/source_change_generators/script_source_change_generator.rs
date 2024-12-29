use std::{pin::Pin, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use futures::{future::join_all, Stream};
use serde::Serialize;
use tokio::{sync::{mpsc::{Receiver, Sender}, oneshot, Mutex}, task::JoinHandle, time::{sleep, Duration}};
use tokio_stream::StreamExt;

use test_data_store::{
    scripts::{
        change_script_file_reader::ChangeScriptReader, ChangeScriptRecord, SequencedChangeScriptRecord, SourceChangeEvent
    }, 
    test_repo_storage::{
        models::{CommonSourceChangeGeneratorDefinition, ScriptSourceChangeGeneratorDefinition, SourceChangeDispatcherDefinition, SpacingMode}, 
        TestSourceStorage
    }, 
    test_run_storage::{
        TestRunSourceId, TestRunSourceStorage
    }
};

use crate::{source_change_dispatchers::create_source_change_dispatcher, TimeMode};
use crate::source_change_dispatchers::SourceChangeDispatcher;

use super::{SourceChangeGenerator, SourceChangeGeneratorCommandResponse, SourceChangeGeneratorStatus};

type ChangeStream = Pin<Box<dyn Stream<Item = anyhow::Result<SequencedChangeScriptRecord>> + Send>>;

#[derive(Debug, thiserror::Error)]
pub enum ScriptSourceChangeGeneratorError {
    #[error("ScriptSourceChangeGenerator is already finished.")]
    AlreadyFinished,
    #[error("ScriptSourceChangeGenerator is already paused.")]
    AlreadyPaused,
    #[error("ScriptSourceChangeGenerator is already running.")]
    AlreadyRunning,
    #[error("ScriptSourceChangeGenerator is already stopped.")]
    AlreadyStopped,
    #[error("ScriptSourceChangeGenerator is currently Skipping. {0} skips remaining.")]
    CurrentlySkipping(u64),
    #[error("ScriptSourceChangeGenerator is currently Stepping. {0} steps remaining.")]
    CurrentlyStepping(u64),
    #[error("ScriptSourceChangeGenerator is currently in an Error state - {0:?}")]
    Error(SourceChangeGeneratorStatus),
    #[error("ScriptSourceChangeGenerator is currently Running. Pause before trying to Skip.")]
    PauseToSkip,
    #[error("ScriptSourceChangeGenerator is currently Running. Pause before trying to Step.")]
    PauseToStep,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptSourceChangeGeneratorSettings {
    pub id: TestRunSourceId,
    pub ignore_scripted_pause_commands: bool,
    pub input_storage: TestSourceStorage,
    pub dispatchers: Vec<SourceChangeDispatcherDefinition>,
    pub output_storage: TestRunSourceStorage,
    pub spacing_mode: SpacingMode,
    pub time_mode: TimeMode,
}

impl ScriptSourceChangeGeneratorSettings {
    pub async fn new(
        test_run_source_id: TestRunSourceId, 
        common_config: CommonSourceChangeGeneratorDefinition, 
        unique_config: ScriptSourceChangeGeneratorDefinition, 
        input_storage: TestSourceStorage, 
        output_storage: TestRunSourceStorage
    ) -> anyhow::Result<Self> {

        Ok(ScriptSourceChangeGeneratorSettings {
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
pub struct ScriptSourceChangeGeneratorState {
    pub actual_time_ns_start: u64,
    pub error_messages: Vec<String>,
    pub ignore_scripted_pause_commands: bool,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub previous_record: Option<ProcessedChangeScriptRecord>,
    pub skips_remaining: u64,
    pub spacing_mode: SpacingMode,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl Default for ScriptSourceChangeGeneratorState {
    fn default() -> Self {
        Self {
            actual_time_ns_start: 0,
            error_messages: Vec::new(),
            ignore_scripted_pause_commands: false,
            next_record: None,
            previous_record: None,
            skips_remaining: 0,
            spacing_mode: SpacingMode::None,
            status: SourceChangeGeneratorStatus::Paused,
            steps_remaining: 0,
            time_mode: TimeMode::Live,
            virtual_time_ns_current: 0,
            virtual_time_ns_offset: 0,
            virtual_time_ns_start: 0,
        }
    }
}

// Enum of ScriptSourceChangeGenerator commands sent from Web API handler functions.
#[derive(Debug)]
pub enum ScriptSourceChangeGeneratorCommand {
    // Command to get the current state of the ScriptSourceChangeGenerator.
    GetState,
    // Command to pause the ScriptSourceChangeGenerator.
    Pause,
    // Command to skip the ScriptSourceChangeGenerator forward a specified number of ChangeScriptRecords.
    Skip(u64),
    // Command to start the ScriptSourceChangeGenerator.
    Start,
    // Command to step the ScriptSourceChangeGenerator forward a specified number of ChangeScriptRecords.
    Step(u64),
    // Command to stop the ScriptSourceChangeGenerator.
    Stop,
}

// Struct for messages sent to the ScriptSourceChangeGenerator from the functions in the Web API.
#[derive(Debug,)]
pub struct ScriptSourceChangeGeneratorMessage {
    // Command sent to the ScriptSourceChangeGenerator.
    pub command: ScriptSourceChangeGeneratorCommand,
    // One-shot channel for ScriptSourceChangeGenerator to send a response back to the caller.
    pub response_tx: Option<oneshot::Sender<ScriptSourceChangeGeneratorMessageResponse>>,
}

// A struct for the Response sent back from the ScriptSourceChangeGenerator to the calling Web API handler.
#[derive(Debug)]
pub struct ScriptSourceChangeGeneratorMessageResponse {
    // Result of the command.
    pub result: anyhow::Result<()>,
    // State of the ScriptSourceChangeGenerator after the command.
    pub state: ScriptSourceChangeGeneratorState,
}

#[derive(Clone, Debug)]
pub struct DelayedChangeScriptRecordMessage {
    pub delay_ns: u64,
    pub record_seq_num: u64,
    pub virtual_time_ns_replay: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessedChangeScriptRecord {
    pub scripted: SequencedChangeScriptRecord,
    // pub dispatched: ChangeScriptRecord,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScriptSourceChangeGenerator {
    settings: ScriptSourceChangeGeneratorSettings,
    #[serde(skip_serializing)]
    player_tx_channel: Sender<ScriptSourceChangeGeneratorMessage>,
    #[serde(skip_serializing)]
    _player_thread_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl ScriptSourceChangeGenerator {
    pub async fn new(test_run_source_id: TestRunSourceId, common_config: CommonSourceChangeGeneratorDefinition, unique_config: ScriptSourceChangeGeneratorDefinition, input_storage: TestSourceStorage, output_storage: TestRunSourceStorage) -> anyhow::Result<Box<dyn SourceChangeGenerator + Send + Sync>> {
        let settings = ScriptSourceChangeGeneratorSettings::new(test_run_source_id, common_config, unique_config, input_storage, output_storage.clone()).await?;
        log::debug!("Creating ScriptSourceChangeGenerator from {:?}", &settings);

        let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();

        for def in settings.dispatchers.iter() {
            match create_source_change_dispatcher(def, &output_storage).await {
                Ok(dispatcher) => dispatchers.push(dispatcher),
                Err(e) => {
                    anyhow::bail!("Error creating SourceChangeDispatcher: {:?}; Error: {:?}", def, e);
                }
            }
        }

        let (player_tx_channel, player_rx_channel) = tokio::sync::mpsc::channel(100);
        let player_thread_handle = tokio::spawn(player_thread(player_rx_channel, settings.clone(), dispatchers));

        Ok(Box::new(Self {
            settings,
            player_tx_channel,
            _player_thread_handle: Arc::new(Mutex::new(player_thread_handle)),
        }))
    }

    pub fn get_id(&self) -> TestRunSourceId {
        self.settings.get_id()
    }

    pub fn get_settings(&self) -> ScriptSourceChangeGeneratorSettings {
        self.settings.clone()
    }

    async fn send_command(&self, command: ScriptSourceChangeGeneratorCommand) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self.player_tx_channel.send(ScriptSourceChangeGeneratorMessage {
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
            Err(e) => anyhow::bail!("Error sending command to ScriptSourceChangeGenerator: {:?}", e),
        }
    }
}

#[async_trait]
impl SourceChangeGenerator for ScriptSourceChangeGenerator {
    async fn get_state(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::GetState).await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::Start).await
    }

    async fn step(&self, steps: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ScriptSourceChangeGeneratorCommand::Step(steps)).await
    }

    async fn skip(&self, skips: u64) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ScriptSourceChangeGeneratorCommand::Skip(skips)).await
    }

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ScriptSourceChangeGeneratorCommand::Pause).await
    }

    async fn stop(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ScriptSourceChangeGeneratorCommand::Stop).await
    }
}

// Function that defines the operation of the ScriptSourceChangeGenerator thread.
// The ScriptSourceChangeGenerator thread processes ChangeScriptPlayerCommands sent to it from the Web API handler functions.
// The Web API function communicate via a channel and provide oneshot channels for the ScriptSourceChangeGenerator to send responses back.
pub async fn player_thread(mut player_rx_channel: Receiver<ScriptSourceChangeGeneratorMessage>, player_settings: ScriptSourceChangeGeneratorSettings, mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>>) {
    log::info!("ScriptSourceChangeGenerator {} player thread started...", player_settings.id);

    let script_files = match player_settings.input_storage.get_script_files().await {
        Ok(ds) => ds.source_change_script_files,
        Err(e) => {
            log::error!("Error getting dataset: {:?}", e);
            Vec::new()
        }
    };

    let (_delayer_tx_channel, delayer_rx_channel) = tokio::sync::mpsc::channel(100);
    let (change_tx_channel, mut change_rx_channel) = tokio::sync::mpsc::channel(100);
    let _ = tokio::spawn(delayer_thread(player_settings.id.clone(), delayer_rx_channel, change_tx_channel.clone()));

    let mut player_state = ScriptSourceChangeGeneratorState {
        ignore_scripted_pause_commands: player_settings.ignore_scripted_pause_commands,
        spacing_mode: player_settings.spacing_mode,
        time_mode: player_settings.time_mode,
        ..ScriptSourceChangeGeneratorState::default()
    };

    let mut change_stream = match ChangeScriptReader::new(script_files) {
        Ok(reader) => {

            let header = reader.get_header();
            log::debug!("Loaded ChangeScript - Header: {:?}", header);

            // Set the start_replay_time based on the time mode and the script start time from the header.
            player_state.virtual_time_ns_start = match player_state.time_mode {
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
            player_state.virtual_time_ns_current = player_state.virtual_time_ns_start;
            
            Some(Box::pin(reader) as ChangeStream)
        }
        Err(e) => {
            log::error!("Error creating ChangeScriptReader: {:?}", e);
            None
        }
    };

    get_next_change_stream_record(&mut player_state, change_stream.as_mut()).await
        .inspect_err(|e| transition_to_error_state(&mut player_state, "Error calling get_next_change_stream_record: {:?}", Some(e))).ok();

    // The ScriptSourceChangeGenerator always starts Paused.

    // Loop to process messages sent to the ScriptSourceChangeGenerator or read from the Change Stream.
    loop {
        log_test_script_player_state(&player_state, "Top of ScriptSourceChangeGenerator loop");

        tokio::select! {
            // Always process all messages in the command channel and act on them first.
            biased;

            // Process messages from the command channel.
            cmd = player_rx_channel.recv() => {
                match cmd {
                    Some(message) => {
                        log::debug!("Received {:?} command message.", message.command);

                        let transition_response = match player_state.status {
                            SourceChangeGeneratorStatus::Running => transition_from_running_state(&mut player_state, &message.command),
                            SourceChangeGeneratorStatus::Stepping => transition_from_stepping_state(&mut player_state, &message.command),
                            SourceChangeGeneratorStatus::Skipping => transition_from_skipping_state(&mut player_state, &message.command),
                            SourceChangeGeneratorStatus::Paused => transition_from_paused_state(&mut player_state, &message.command),
                            SourceChangeGeneratorStatus::Stopped => transition_from_stopped_state(&mut player_state, &message.command),
                            SourceChangeGeneratorStatus::Finished => transition_from_finished_state(&mut player_state, &message.command),
                            SourceChangeGeneratorStatus::Error => transition_from_error_state(&mut player_state, &message.command),
                        };

                        if message.response_tx.is_some() {
                            let message_response = ScriptSourceChangeGeneratorMessageResponse {
                                result: transition_response,
                                state: player_state.clone(),
                            };
                
                            let r = message.response_tx.unwrap().send(message_response);
                            if let Err(e) = r {
                                log::error!("Error in ScriptSourceChangeGenerator sending message response back to caller: {:?}", e);
                            }
                        }
                
                        log_test_script_player_state(&player_state, format!("Post {:?} command", message.command).as_str());
                    },
                    None => {
                        log::error!("ScriptSourceChangeGenerator command channel closed.");
                        break;
                    }
                }
            },
            channel_message = change_rx_channel.recv(), if player_state.status.is_active() => {
                match channel_message {
                    Some(change_stream_message) => {
                        log::trace!("Received DelayedChangeScriptRecordMessage: {:?}", change_stream_message);
                        process_next_change_stream_record(&mut player_state, change_stream_message, &mut dispatchers).await
                            .inspect_err(|e| transition_to_error_state(&mut player_state, "Error calling process_next_change_stream_record.", Some(e))).ok();
                    },
                    None => {
                        log::info!("SourceChangeQueueReader channel closed.");
                    }
                }
            },
            else => {
                log::debug!("ScriptSourceChangeGenerator loop - no messages to process.");
            }
        }
    }

    log::debug!("ScriptSourceChangeGenerator thread exiting...");    
}
async fn get_next_change_stream_record(
    player_state: &mut ScriptSourceChangeGeneratorState, 
    change_stream: Option<&mut ChangeStream>,
) -> anyhow::Result<()> {
    // Do nothing if the SourceChangeGenerator is already in an error state.
    if let SourceChangeGeneratorStatus::Error = player_state.status {
        anyhow::bail!("Ignoring get_next_change_stream_record call due to error state");
    }

    match change_stream {
        Some(stream) => {
            match stream.next().await {
                Some(Ok(seq_record)) => {
                    // Assign the next record to the player state.
                    player_state.previous_record = Some(ProcessedChangeScriptRecord {
                        scripted: player_state.next_record.clone().unwrap(),
                    });
                    player_state.next_record = Some(seq_record);
                },
                Some(Err(e)) => {
                    anyhow::bail!(format!("Error reading ChangeScriptRecord: {:?}", e));
                },
                None => {
                    anyhow::bail!("ChangeScriptReader.next() returned None, shouldn't be seeing this.");
                }
            };
        },
        None => {
            anyhow::bail!("ChangeScriptReader is None, can't read script.");
        }
    };

    Ok(())
}

async fn _push_next_change_stream_record(
    player_state: &mut ScriptSourceChangeGeneratorState,
    change_tx_channel: Sender<DelayedChangeScriptRecordMessage>,
    delayer_tx_channel: Sender<DelayedChangeScriptRecordMessage>,
) -> anyhow::Result<()> {
    // Do nothing if the SourceChangeGenerator is already in an error state.
    if let SourceChangeGeneratorStatus::Error = player_state.status {
        anyhow::bail!("Ignoring get_next_change_stream_record call due to error state");
    }

    // Get the next record from the player state. Error if it is None.
    let next_record = match player_state.next_record.as_ref() {
        Some(record) => record.clone(),
        None => {
            anyhow::bail!("Received DelayedChangeScriptRecordMessage when player_state.next_record is None");
        }
    };
    
    // Processing of ChangeScriptRecord depends on the spacing mode settings.
    let delay_ns = match player_state.spacing_mode {
        SpacingMode::None => {
            // Process the record without introducing a delay.
            0
        },
        SpacingMode::Fixed(nanos) => {
            // Use the specified delay.
            nanos
        },
        SpacingMode::Recorded => {
            // Delay the record based on the difference between the record's offset and the generators current virtual time offset.
            // Ensure the delay is not negative.
            std::cmp::max(0, next_record.offset_ns - player_state.virtual_time_ns_offset) as u64
        },
    };

    let delay_msg = DelayedChangeScriptRecordMessage {
        delay_ns,
        record_seq_num: next_record.seq,
        virtual_time_ns_replay: player_state.virtual_time_ns_current + delay_ns,
    };    

    // Take action based on size of the delay.
    // It doesn't make sense to delay for trivially small amounts of time, nor does it make sense to send 
    // a message to the delayer thread for relatively small delays. 
    // TODO: This figures might need to be adjusted, or made configurable.
    if delay_ns < 10_000 {
        // Process the record immediately.
        if let Err(e) = change_tx_channel.send(delay_msg).await {
            anyhow::bail!("Error sending DelayedChangeScriptRecordMessage: {:?}", e);
        }
    } else if delay_ns < 10_000_000 {
        // Sleep inproc, then process the record.
        std::thread::sleep(Duration::from_nanos(delay_ns));
        if let Err(e) = change_tx_channel.send(delay_msg).await {
            anyhow::bail!("Error sending DelayedChangeScriptRecordMessage: {:?}", e);
        }
    } else {
        if let Err(e) = delayer_tx_channel.send(delay_msg).await {
            anyhow::bail!("Error sending DelayedChangeScriptRecordMessage: {:?}", e);
        }
    };

    Ok(())
}

async fn process_next_change_stream_record(
    player_state: &mut ScriptSourceChangeGeneratorState, 
    delayed_message: DelayedChangeScriptRecordMessage, 
    dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>
) -> anyhow::Result<()> {
    // Do nothing if the SourceChangeGenerator is already in an error state.
    if let SourceChangeGeneratorStatus::Error = player_state.status {
        anyhow::bail!("Ignoring get_next_change_stream_record call due to error state");
    }
    
    // Get the next record from the player state. Error if it is None.
    let next_record = match player_state.next_record.as_ref() {
        Some(record) => record.clone(),
        None => anyhow::bail!("Received DelayedChangeScriptRecordMessage when player_state.next_record is None"),
    };

    // Transition to an error state if the delay message does not match the next record.
    if delayed_message.record_seq_num != next_record.seq {
        anyhow::bail!("Received DelayedChangeScriptRecordMessage with incorrect seq");
    } 
    
    // Process the delayed record.
    match &next_record.record {
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
                            let _ = transition_from_stepping_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause);
                        }
                    }
                },
                SourceChangeGeneratorStatus::Skipping => {
                    // Skip the SourceChangeEvent.
                    if player_state.skips_remaining > 0 {
                        log::debug!("Skipping ChangeScriptRecord: {:?}", change_record);

                        player_state.skips_remaining -= 1;
                        if player_state.skips_remaining == 0 {
                            let _ = transition_from_skipping_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause);
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
                log::debug!("Ignoring Change Script Pause Command: {:?}", next_record);
            } else {
                transition_to_paused_state(player_state).ok();
            }
        },
        ChangeScriptRecord::Label(label_record) => {
            log::debug!("Reached Change Script Label: {:?}", label_record);
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

    Ok(())
}

async fn dispatch_source_change_events(
    dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>, 
    events: Vec<&SourceChangeEvent>
) {
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

fn transition_from_paused_state(player_state: &mut ScriptSourceChangeGeneratorState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Start => {
            player_state.status = SourceChangeGeneratorStatus::Running;
            // process_next_test_script_record(player_state, dispatchers, delayer_tx_channel);
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Step(steps) => {
            player_state.status = SourceChangeGeneratorStatus::Stepping;
            player_state.steps_remaining = *steps;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Skip(skips) => {
            player_state.status = SourceChangeGeneratorStatus::Skipping;
            player_state.skips_remaining = *skips;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Pause => {
            Err(ScriptSourceChangeGeneratorError::AlreadyPaused.into())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            player_state.status = SourceChangeGeneratorStatus::Stopped;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_running_state(player_state: &mut ScriptSourceChangeGeneratorState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Start => {
            Err(ScriptSourceChangeGeneratorError::AlreadyRunning.into())
        },
        ScriptSourceChangeGeneratorCommand::Step(_) => {
            Err(ScriptSourceChangeGeneratorError::PauseToStep.into())
        },
        ScriptSourceChangeGeneratorCommand::Skip(_) => {
            Err(ScriptSourceChangeGeneratorError::PauseToSkip.into())
        },
        ScriptSourceChangeGeneratorCommand::Pause => {
            player_state.status = SourceChangeGeneratorStatus::Paused;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            player_state.status = SourceChangeGeneratorStatus::Stopped;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stepping_state(player_state: &mut ScriptSourceChangeGeneratorState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Pause => {
            player_state.status = SourceChangeGeneratorStatus::Paused;
            player_state.steps_remaining = 0;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            player_state.status = SourceChangeGeneratorStatus::Stopped;
            player_state.steps_remaining = 0;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        },
        _ => Err(ScriptSourceChangeGeneratorError::CurrentlyStepping(player_state.steps_remaining).into())
    }
}

fn transition_from_skipping_state(player_state: &mut ScriptSourceChangeGeneratorState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Pause => {
            player_state.status = SourceChangeGeneratorStatus::Paused;
            player_state.skips_remaining = 0;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            player_state.status = SourceChangeGeneratorStatus::Stopped;
            player_state.skips_remaining = 0;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        },
        _ => Err(ScriptSourceChangeGeneratorError::CurrentlySkipping(player_state.skips_remaining).into())
    }
}

fn transition_from_stopped_state(player_state: &mut ScriptSourceChangeGeneratorState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ScriptSourceChangeGeneratorError::AlreadyStopped.into())
}

fn transition_from_finished_state(player_state: &mut ScriptSourceChangeGeneratorState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ScriptSourceChangeGeneratorError::AlreadyFinished.into())
}

fn transition_from_error_state(player_state: &mut ScriptSourceChangeGeneratorState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(ScriptSourceChangeGeneratorError::Error(player_state.status).into())
}

fn transition_to_finished_state(player_state: &mut ScriptSourceChangeGeneratorState) {
    log::debug!("Transitioning to Finished state from: {:?}", player_state.status);

    player_state.status = SourceChangeGeneratorStatus::Finished;
    player_state.skips_remaining = 0;
    player_state.steps_remaining = 0;
}

fn transition_to_error_state(player_state: &mut ScriptSourceChangeGeneratorState, error_message: &str, error: Option<&anyhow::Error>) {    
    player_state.status = SourceChangeGeneratorStatus::Error;

    let msg = match error {
        Some(e) => format!("{}: {:?}", error_message, e),
        None => error_message.to_string(),
    };

    log_test_script_player_state(&player_state, &msg);

    player_state.error_messages.push(msg);
}

fn transition_to_paused_state(player_state: &mut ScriptSourceChangeGeneratorState) -> anyhow::Result<()>{
    match player_state.status {
        SourceChangeGeneratorStatus::Running => transition_from_running_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Stepping => transition_from_stepping_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Skipping => transition_from_skipping_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Paused => transition_from_paused_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Stopped => transition_from_stopped_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Finished => transition_from_finished_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Error => transition_from_error_state(player_state,&ScriptSourceChangeGeneratorCommand::Pause),
    }
}

pub async fn delayer_thread(id: TestRunSourceId, mut delayer_rx_channel: Receiver<DelayedChangeScriptRecordMessage>, change_tx_channel: Sender<DelayedChangeScriptRecordMessage>) {
    log::info!("ScriptSourceChangeGenerator {} delayer thread started...", id);

    loop {
        match delayer_rx_channel.recv().await {
            Some(message) => {
                // Sleep for the specified time before sending the message to the change_tx_channel.
                sleep(Duration::from_nanos(message.delay_ns)).await;

                match change_tx_channel.send(message).await {
                    Ok(_) => {},
                    Err(e) => {
                        log::error!("Error sending DelayedChangeScriptRecordMessage to change_tx_channel: {:?}", e);
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

// Function to log the Player State at varying levels of detail.
fn log_test_script_player_state(player_state: &ScriptSourceChangeGeneratorState, msg: &str) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, player_state),
        log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, player_state),
        log::LevelFilter::Info => log::info!("{} - status:{:?}, error_message:{:?}, virtual_time_ns_start:{:?}, virtual_time_ns_current:{:?}, skips_remaining:{:?}, steps_remaining:{:?}",
            msg, player_state.status, player_state.error_messages, player_state.virtual_time_ns_start, player_state.virtual_time_ns_current, player_state.skips_remaining, player_state.steps_remaining),
        _ => {}
    }
}
