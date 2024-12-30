use std::{fmt::{self, Debug, Formatter}, pin::Pin, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use futures::{future::join_all, Stream};
use serde::Serialize;
use tokio::{sync::{mpsc::{Receiver, Sender}, oneshot, Mutex}, task::JoinHandle, time::{sleep, Duration}};
use tokio_stream::StreamExt;

use test_data_store::{
    scripts::{
        change_script_file_reader::ChangeScriptReader, ChangeHeaderRecord, ChangeScriptRecord, SequencedChangeScriptRecord, SourceChangeEvent
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

#[derive(Debug, Serialize)]
pub struct ScriptSourceChangeGeneratorExternalState {
    pub actual_time_ns_start: u64,
    pub error_messages: Vec<String>,
    pub ignore_scripted_pause_commands: bool,
    pub header_record: Option<ChangeHeaderRecord>,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub previous_record: Option<ProcessedChangeScriptRecord>,
    pub skips_remaining: u64,
    pub spacing_mode: SpacingMode,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl From<&mut ScriptSourceChangeGeneratorInternalState> for ScriptSourceChangeGeneratorExternalState {
    fn from(state: &mut ScriptSourceChangeGeneratorInternalState) -> Self {
        Self {
            actual_time_ns_start: state.actual_time_ns_start,
            error_messages: state.error_messages.clone(),
            ignore_scripted_pause_commands: state.ignore_scripted_pause_commands,
            header_record: state.header_record.clone(),
            next_record: state.next_record.clone(),
            previous_record: state.previous_record.clone(),
            skips_remaining: state.skips_remaining,
            spacing_mode: state.spacing_mode.clone(),
            status: state.status,
            steps_remaining: state.steps_remaining,
            test_run_source_id: state.test_run_source_id.clone(),
            time_mode: state.time_mode.clone(),
            virtual_time_ns_current: state.virtual_time_ns_current,
            virtual_time_ns_offset: state.virtual_time_ns_offset,
            virtual_time_ns_start: state.virtual_time_ns_start,
        }
    }
}

pub struct ScriptSourceChangeGeneratorInternalState {
    pub actual_time_ns_start: u64,
    pub change_stream: Pin<Box<dyn Stream<Item = Result<SequencedChangeScriptRecord, anyhow::Error>> + Send>>,
    pub change_tx_channel: Sender<DelayedChangeScriptRecordMessage>,
    pub delayer_tx_channel: Sender<DelayedChangeScriptRecordMessage>,
    pub dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>>,
    pub error_messages: Vec<String>,
    pub ignore_scripted_pause_commands: bool,
    pub header_record: Option<ChangeHeaderRecord>,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub previous_record: Option<ProcessedChangeScriptRecord>,
    pub skips_remaining: u64,
    pub spacing_mode: SpacingMode,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl Debug for ScriptSourceChangeGeneratorInternalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptSourceChangeGeneratorInternalState")
            .field("actual_time_ns_start", &self.actual_time_ns_start)
            .field("error_messages", &self.error_messages)
            .field("ignore_scripted_pause_commands", &self.ignore_scripted_pause_commands)
            .field("header_record", &self.header_record)
            .field("next_record", &self.next_record)
            .field("previous_record", &self.previous_record)
            .field("skips_remaining", &self.skips_remaining)
            .field("spacing_mode", &self.spacing_mode)
            .field("status", &self.status)
            .field("steps_remaining", &self.steps_remaining)
            .field("time_mode", &self.time_mode)
            .field("virtual_time_ns_current", &self.virtual_time_ns_current)
            .field("virtual_time_ns_offset", &self.virtual_time_ns_offset)
            .field("virtual_time_ns_start", &self.virtual_time_ns_start)
            .finish()
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
    pub state: ScriptSourceChangeGeneratorExternalState,
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
    script_processor_tx_channel: Sender<ScriptSourceChangeGeneratorMessage>,
    #[serde(skip_serializing)]
    _script_processor_thread_handle: Arc<Mutex<JoinHandle<anyhow::Result<()>>>>,
}

impl ScriptSourceChangeGenerator {
    pub async fn new(test_run_source_id: TestRunSourceId, common_config: CommonSourceChangeGeneratorDefinition, unique_config: ScriptSourceChangeGeneratorDefinition, input_storage: TestSourceStorage, output_storage: TestRunSourceStorage) -> anyhow::Result<Box<dyn SourceChangeGenerator + Send + Sync>> {
        let settings = ScriptSourceChangeGeneratorSettings::new(test_run_source_id, common_config, unique_config, input_storage, output_storage.clone()).await?;
        log::debug!("Creating ScriptSourceChangeGenerator from {:?}", &settings);

        let (script_processor_tx_channel, script_processor_rx_channel) = tokio::sync::mpsc::channel(100);
        let script_processor_thread_handle = tokio::spawn(script_processor_thread(script_processor_rx_channel, settings.clone()));

        Ok(Box::new(Self {
            settings,
            script_processor_tx_channel,
            _script_processor_thread_handle: Arc::new(Mutex::new(script_processor_thread_handle)),
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

        let r = self.script_processor_tx_channel.send(ScriptSourceChangeGeneratorMessage {
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
                        state: serde_json::to_value(player_response.state).unwrap(),
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
pub async fn script_processor_thread(mut command_rx_channel: Receiver<ScriptSourceChangeGeneratorMessage>, settings: ScriptSourceChangeGeneratorSettings) -> anyhow::Result<()>{
    log::info!("Script processor thread started for TestRunSource {} ...", settings.id);

    // The ScriptSourceChangeGenerator always starts with the first script record loaded and Paused.
    let (mut state, mut change_rx_channel) = match initialize(settings).await {
        Ok((state, change_rx_channel)) => (state, change_rx_channel),
        Err(e) => {
            let msg = format!("Error initializing ScriptSourceChangeGenerator: {:?}", e);
            log::error!("{}", msg);
            anyhow::bail!(msg);
        }
    };

    // Loop to process commands sent to the ScriptSourceChangeGenerator or read from the Change Stream.
    loop {
        log_test_script_player_state(&state, "Top of script processor loop");

        tokio::select! {
            // Always process all messages in the command channel and act on them first.
            biased;

            // Process messages from the command channel.
            command_message = command_rx_channel.recv() => {
                match command_message {
                    Some(command_message) => process_command_message(&mut state, command_message).await?,
                    None => {
                        transition_to_error_state(&mut state, "Command channel closed.", None);
                        break;
                    }
                }
            },

            // Process messages from the Change Stream.
            change_stream_message = change_rx_channel.recv(), if state.status.is_processing() => {
                match change_stream_message {
                    Some(change_stream_message) => process_change_stream_message(&mut state, change_stream_message).await?,
                    None => {
                        transition_to_error_state(&mut state, "Change stream channel closed.", None);
                        break;
                    }
                }
            },

            else => {
                log::error!("Script processor loop activated for {} but no command or change to process.", state.test_run_source_id);
            }
        }
    }

    log::info!("Script processor thread exiting for TestRunSource {} ...", state.test_run_source_id);    
    Ok(())
}

async fn initialize(settings: ScriptSourceChangeGeneratorSettings)  -> anyhow::Result<(ScriptSourceChangeGeneratorInternalState, Receiver<DelayedChangeScriptRecordMessage>)> {

    // Get the list of script files from the input storage.
    let script_files = match settings.input_storage.get_script_files().await {
        Ok(ds) => ds.source_change_script_files,
        Err(e) => {
            anyhow::bail!("Error getting script files from input storage: {:?}", e);
        }
    };

    // Create the ChangeStream from the script files; get the header and first record.
    let (change_stream, header, first_record) = match ChangeScriptReader::new(script_files) {
        Ok(reader) => {
            let header = reader.get_header();
            let mut change_stream = Box::pin(reader) as ChangeStream;
            let first_record = match change_stream.next().await {
                Some(Ok(seq_record)) => Some(seq_record),
                Some(Err(e)) => {
                    anyhow::bail!(format!("Error reading first ChangeStream record: {:?}", e));
                },
                None => None,
            };

            (change_stream, header, first_record)
        }
        Err(e) => {
            anyhow::bail!("Error creating ChangeStream: {:?}", e);
        }
    };
    log::info!("Loaded ChangeScript - Header: {:?}", header);

    // Set the virtual_time_ns_start based on the time mode and the script start time from the header.
    let virtual_time_ns_start = match settings.time_mode {
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

    // Create the set of dispatchers.
    let mut dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>> = Vec::new();
    for def in settings.dispatchers.iter() {
        match create_source_change_dispatcher(def, &settings.output_storage).await {
            Ok(dispatcher) => dispatchers.push(dispatcher),
            Err(e) => {
                anyhow::bail!("Error creating SourceChangeDispatcher: {:?}; Error: {:?}", def, e);
            }
        }
    }

    // Create the channels used for message passing.
    let (change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(100);
    let (delayer_tx_channel, delayer_rx_channel) = tokio::sync::mpsc::channel(100);
    let _ = tokio::spawn(delayer_thread(settings.id.clone(), delayer_rx_channel, change_tx_channel.clone()));

    let player_state = ScriptSourceChangeGeneratorInternalState {
        actual_time_ns_start: 0,
        change_stream: change_stream,
        change_tx_channel,
        delayer_tx_channel,
        dispatchers,
        error_messages: Vec::new(),
        ignore_scripted_pause_commands: settings.ignore_scripted_pause_commands,
        header_record: Some(header),
        next_record: first_record,
        previous_record: None,
        skips_remaining: 0,
        spacing_mode: settings.spacing_mode,
        status: SourceChangeGeneratorStatus::Paused,
        steps_remaining: 0,
        test_run_source_id: settings.id.clone(),
        time_mode: settings.time_mode,
        virtual_time_ns_current: 0,
        virtual_time_ns_offset: 0,
        virtual_time_ns_start,
    };

    Ok((player_state, change_rx_channel))
}

async fn process_command_message(state: &mut ScriptSourceChangeGeneratorInternalState, message: ScriptSourceChangeGeneratorMessage) -> anyhow::Result<()> {
    log::debug!("Received {:?} command message.", message.command);

    if let ScriptSourceChangeGeneratorCommand::GetState = message.command {
        let message_response = ScriptSourceChangeGeneratorMessageResponse {
            result: Ok(()),
            state: state.into(),
        };

        let r = message.response_tx.unwrap().send(message_response);
        if let Err(e) = r {
            log::error!("Error in ScriptSourceChangeGenerator sending message response back to caller: {:?}", e);
        }
    } else {
        let transition_response = match state.status {
            SourceChangeGeneratorStatus::Running => transition_from_running_state(state, &message.command),
            SourceChangeGeneratorStatus::Stepping => transition_from_stepping_state(state, &message.command),
            SourceChangeGeneratorStatus::Skipping => transition_from_skipping_state(state, &message.command),
            SourceChangeGeneratorStatus::Paused => transition_from_paused_state(state, &message.command),
            SourceChangeGeneratorStatus::Stopped => transition_from_stopped_state(state, &message.command),
            SourceChangeGeneratorStatus::Finished => transition_from_finished_state(state, &message.command),
            SourceChangeGeneratorStatus::Error => transition_from_error_state(state, &message.command),
        };

        if message.response_tx.is_some() {
            let message_response = ScriptSourceChangeGeneratorMessageResponse {
                result: transition_response,
                state: state.into(),
            };

            let r = message.response_tx.unwrap().send(message_response);
            if let Err(e) = r {
                log::error!("Error in ScriptSourceChangeGenerator sending message response back to caller: {:?}", e);
            }
        }    
    }

    log_test_script_player_state(&state, format!("Post {:?} command", message.command).as_str());

    Ok(())
}

async fn process_change_stream_message(state: &mut ScriptSourceChangeGeneratorInternalState, message: DelayedChangeScriptRecordMessage) -> anyhow::Result<()> {
    log::trace!("Received: {:?}", message);

    // Do nothing if the SourceChangeGenerator is already in an error state.
    if let SourceChangeGeneratorStatus::Error = state.status {
        anyhow::bail!("Ignoring load_next_change_stream_record call due to error state");
    }
    
    // Get the next record from the player state. Error if it is None.
    let next_record = match state.next_record.as_ref() {
        Some(record) => record.clone(),
        None => anyhow::bail!("player_state.next_record is None"),
    };

    // Transition to an error state if the channel message does not match the next record.
    if message.record_seq_num != next_record.seq {
        anyhow::bail!("Received DelayedChangeScriptRecordMessage with incorrect seq");
    } 

    // Time Shift.
    time_shift(state, &next_record);

    // Process the delayed record.
    match &next_record.record {
        ChangeScriptRecord::SourceChange(change_record) => {
            match state.status {
                SourceChangeGeneratorStatus::Running => {
                    // Dispatch the SourceChangeEvent.
                    dispatch_source_change_events(state, vec!(&change_record.source_change_event)).await;
                },
                SourceChangeGeneratorStatus::Stepping => {
                    if state.steps_remaining > 0 {
                        // Dispatch the SourceChangeEvent.
                        dispatch_source_change_events(state, vec!(&change_record.source_change_event)).await;

                        state.steps_remaining -= 1;
                        if state.steps_remaining == 0 {
                            transition_to_paused_state(state)?;
                        }
                    } else {
                        // Transition to an error state.
                        transition_to_error_state(state, "Stepping with no steps remaining", None);
                    }
                },
                SourceChangeGeneratorStatus::Skipping => {
                    if state.skips_remaining > 0 {
                        // Skip the SourceChangeEvent.
                        log::debug!("Skipping ChangeScriptRecord: {:?}", change_record);

                        state.skips_remaining -= 1;
                        if state.skips_remaining == 0 {
                            transition_to_paused_state(state)?;
                        }
                    } else {
                        // Transition to an error state.
                        transition_to_error_state(state, "Skipping with no skips remaining", None);
                    }
                },
                _ => {
                    // Transition to an error state.
                    transition_to_error_state(state, "Unexpected status for SourceChange processing", None);
                },
            }
        },
        ChangeScriptRecord::PauseCommand(_) => {
            // Process the PauseCommand only if the Player is not configured to ignore them.
            if state.ignore_scripted_pause_commands {
                log::debug!("Ignoring Change Script Pause Command: {:?}", next_record);
            } else {
                transition_to_paused_state(state).ok();
            }
        },
        ChangeScriptRecord::Label(label_record) => {
            log::debug!("Reached Source Change Script Label: {:?}", label_record);
        },
        ChangeScriptRecord::Finish(_) => {
            transition_to_finished_state(state);
        },
        ChangeScriptRecord::Header(header_record) => {
            // Transition to an error state.
            transition_to_error_state(state, &format!("Unexpected Change Script Header: {:?}", header_record), None);
        },
        ChangeScriptRecord::Comment(comment_record) => {
            // Transition to an error state.
            transition_to_error_state(state, &format!("Unexpected Change Script Comment: {:?}", comment_record), None);
        },
    };

    // if player_state.status.is_active() {
    //     load_next_change_stream_record(&mut player_state, change_stream.as_mut()).await
    //         .inspect_err(|e| transition_to_error_state(&mut player_state, "Error calling load_next_change_stream_record.", Some(e))).ok();
    // };

    Ok(())
}

async fn _load_next_change_stream_record(
    state: &mut ScriptSourceChangeGeneratorInternalState, 
    change_stream: Option<&mut ChangeStream>,
) -> anyhow::Result<()> {
    // Do nothing if the SourceChangeGenerator is already in an error state.
    if let SourceChangeGeneratorStatus::Error = state.status {
        anyhow::bail!("Ignoring load_next_change_stream_record call due to error state");
    }

    match change_stream {
        Some(stream) => {
            match stream.next().await {
                Some(Ok(seq_record)) => {
                    if state.next_record.is_some() {
                        // Assign the current next record to the previous record.
                        state.previous_record = Some(ProcessedChangeScriptRecord {
                            scripted: state.next_record.clone().unwrap(),
                        });
                    }
                
                    state.next_record = Some(seq_record);
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

async fn _schedule_next_change_stream_record(
    state: &mut ScriptSourceChangeGeneratorInternalState,
    change_tx_channel: Sender<DelayedChangeScriptRecordMessage>,
    delayer_tx_channel: Sender<DelayedChangeScriptRecordMessage>,
) -> anyhow::Result<()> {
    // Do nothing if the SourceChangeGenerator is already in an error state.
    if let SourceChangeGeneratorStatus::Error = state.status {
        anyhow::bail!("Ignoring load_next_change_stream_record call due to error state");
    }

    // Get the next record from the player state. Error if it is None.
    let next_record = match state.next_record.as_ref() {
        Some(record) => record.clone(),
        None => {
            anyhow::bail!("Received DelayedChangeScriptRecordMessage when player_state.next_record is None");
        }
    };
    
    // Processing of ChangeScriptRecord depends on the spacing mode settings.
    let delay_ns = match state.spacing_mode {
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
            std::cmp::max(0, next_record.offset_ns - state.virtual_time_ns_offset) as u64
        },
    };

    let delay_msg = DelayedChangeScriptRecordMessage {
        delay_ns,
        record_seq_num: next_record.seq,
        virtual_time_ns_replay: state.virtual_time_ns_current + delay_ns,
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

// async fn process_next_change_stream_record(
//     player_state: &mut ScriptSourceChangeGeneratorState, 
//     channel_message: DelayedChangeScriptRecordMessage, 
//     dispatchers: &mut Vec<Box<dyn SourceChangeDispatcher + Send>>
// ) -> anyhow::Result<()> {
//     // Do nothing if the SourceChangeGenerator is already in an error state.
//     if let SourceChangeGeneratorStatus::Error = player_state.status {
//         anyhow::bail!("Ignoring load_next_change_stream_record call due to error state");
//     }
    
//     // Get the next record from the player state. Error if it is None.
//     let next_record = match player_state.next_record.as_ref() {
//         Some(record) => record.clone(),
//         None => anyhow::bail!("player_state.next_record is None"),
//     };

//     // Transition to an error state if the channel message does not match the next record.
//     if channel_message.record_seq_num != next_record.seq {
//         anyhow::bail!("Received DelayedChangeScriptRecordMessage with incorrect seq");
//     } 

//     // Time Shift.
//     time_shift(player_state, &next_record);

//     // Process the delayed record.
//     match &next_record.record {
//         ChangeScriptRecord::SourceChange(change_record) => {
//             match player_state.status {
//                 SourceChangeGeneratorStatus::Running => {
//                     // Dispatch the SourceChangeEvent.
//                     dispatch_source_change_events(dispatchers, vec!(&change_record.source_change_event)).await;
//                 },
//                 SourceChangeGeneratorStatus::Stepping => {
//                     if player_state.steps_remaining > 0 {
//                         // Dispatch the SourceChangeEvent.
//                         dispatch_source_change_events(dispatchers, vec!(&change_record.source_change_event)).await;

//                         player_state.steps_remaining -= 1;
//                         if player_state.steps_remaining == 0 {
//                             transition_to_paused_state(player_state)?;
//                         }
//                     } else {
//                         // Transition to an error state.
//                         transition_to_error_state(player_state, "Stepping with no steps remaining", None);
//                     }
//                 },
//                 SourceChangeGeneratorStatus::Skipping => {
//                     if player_state.skips_remaining > 0 {
//                         // Skip the SourceChangeEvent.
//                         log::debug!("Skipping ChangeScriptRecord: {:?}", change_record);

//                         player_state.skips_remaining -= 1;
//                         if player_state.skips_remaining == 0 {
//                             transition_to_paused_state(player_state)?;
//                         }
//                     } else {
//                         // Transition to an error state.
//                         transition_to_error_state(player_state, "Skipping with no skips remaining", None);
//                     }
//                 },
//                 _ => {
//                     // Transition to an error state.
//                     transition_to_error_state(player_state, "Unexpected status for SourceChange processing", None);
//                 },
//             }
//         },
//         ChangeScriptRecord::PauseCommand(_) => {
//             // Process the PauseCommand only if the Player is not configured to ignore them.
//             if player_state.ignore_scripted_pause_commands {
//                 log::debug!("Ignoring Change Script Pause Command: {:?}", next_record);
//             } else {
//                 transition_to_paused_state(player_state).ok();
//             }
//         },
//         ChangeScriptRecord::Label(label_record) => {
//             log::debug!("Reached Source Change Script Label: {:?}", label_record);
//         },
//         ChangeScriptRecord::Finish(_) => {
//             transition_to_finished_state(player_state);
//         },
//         ChangeScriptRecord::Header(header_record) => {
//             // Transition to an error state.
//             transition_to_error_state(player_state, &format!("Unexpected Change Script Header: {:?}", header_record), None);
//         },
//         ChangeScriptRecord::Comment(comment_record) => {
//             // Transition to an error state.
//             transition_to_error_state(player_state, &format!("Unexpected Change Script Comment: {:?}", comment_record), None);
//         },
//     };

//     Ok(())
// }

async fn dispatch_source_change_events(
    state: &mut ScriptSourceChangeGeneratorInternalState,
    events: Vec<&SourceChangeEvent>
) {
    let dispatchers = &mut state.dispatchers;

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

fn time_shift(_state: &mut ScriptSourceChangeGeneratorInternalState, seq_record: &SequencedChangeScriptRecord) -> ChangeScriptRecord {
    let shifted_event = seq_record.record.clone();
    shifted_event
}

fn transition_from_paused_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Start => {
            state.status = SourceChangeGeneratorStatus::Running;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Step(steps) => {
            state.status = SourceChangeGeneratorStatus::Stepping;
            state.steps_remaining = *steps;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Skip(skips) => {
            state.status = SourceChangeGeneratorStatus::Skipping;
            state.skips_remaining = *skips;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Pause => {
            Err(ScriptSourceChangeGeneratorError::AlreadyPaused.into())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            state.status = SourceChangeGeneratorStatus::Stopped;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_running_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

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
            state.status = SourceChangeGeneratorStatus::Paused;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            state.status = SourceChangeGeneratorStatus::Stopped;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stepping_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Pause => {
            state.status = SourceChangeGeneratorStatus::Paused;
            state.steps_remaining = 0;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            state.status = SourceChangeGeneratorStatus::Stopped;
            state.steps_remaining = 0;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        },
        _ => Err(ScriptSourceChangeGeneratorError::CurrentlyStepping(state.steps_remaining).into())
    }
}

fn transition_from_skipping_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Pause => {
            state.status = SourceChangeGeneratorStatus::Paused;
            state.skips_remaining = 0;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            state.status = SourceChangeGeneratorStatus::Stopped;
            state.skips_remaining = 0;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        },
        _ => Err(ScriptSourceChangeGeneratorError::CurrentlySkipping(state.skips_remaining).into())
    }
}

fn transition_from_stopped_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    Err(ScriptSourceChangeGeneratorError::AlreadyStopped.into())
}

fn transition_from_finished_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    Err(ScriptSourceChangeGeneratorError::AlreadyFinished.into())
}

fn transition_from_error_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    Err(ScriptSourceChangeGeneratorError::Error(state.status).into())
}

fn transition_to_finished_state(state: &mut ScriptSourceChangeGeneratorInternalState) {
    log::debug!("Transitioning to Finished state from: {:?}", state.status);

    state.status = SourceChangeGeneratorStatus::Finished;
    state.skips_remaining = 0;
    state.steps_remaining = 0;
}

fn transition_to_error_state(state: &mut ScriptSourceChangeGeneratorInternalState, error_message: &str, error: Option<&anyhow::Error>) {    
    state.status = SourceChangeGeneratorStatus::Error;

    let msg = match error {
        Some(e) => format!("{}: {:?}", error_message, e),
        None => error_message.to_string(),
    };

    log_test_script_player_state(&state, &msg);

    state.error_messages.push(msg);
}

fn transition_to_paused_state(state: &mut ScriptSourceChangeGeneratorInternalState) -> anyhow::Result<()>{
    match state.status {
        SourceChangeGeneratorStatus::Running => transition_from_running_state(state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Stepping => transition_from_stepping_state(state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Skipping => transition_from_skipping_state(state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Paused => transition_from_paused_state(state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Stopped => transition_from_stopped_state(state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Finished => transition_from_finished_state(state,&ScriptSourceChangeGeneratorCommand::Pause),
        SourceChangeGeneratorStatus::Error => transition_from_error_state(state,&ScriptSourceChangeGeneratorCommand::Pause),
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
fn log_test_script_player_state(state: &ScriptSourceChangeGeneratorInternalState, msg: &str) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, state),
        log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, state),
        log::LevelFilter::Info => log::info!("{} - status:{:?}, error_message:{:?}, virtual_time_ns_start:{:?}, virtual_time_ns_current:{:?}, skips_remaining:{:?}, steps_remaining:{:?}",
            msg, state.status, state.error_messages, state.virtual_time_ns_start, state.virtual_time_ns_current, state.skips_remaining, state.steps_remaining),
        _ => {}
    }
}
