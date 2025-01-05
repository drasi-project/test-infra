use std::{fmt::{self, Debug, Formatter}, pin::Pin, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use futures::{future::join_all, Stream};
use serde::Serialize;
use time::{OffsetDateTime, format_description};
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
pub struct ScriptSourceChangeGeneratorStats {
    pub actual_start_time_ns: u64,
    pub actual_end_time_ns: u64,
    pub num_source_change_records: u64,
    pub num_skipped_source_change_records: u64,
    pub num_label_records: u64,
    pub num_pause_records: u64,
}

impl Default for ScriptSourceChangeGeneratorStats {
    fn default() -> Self {
        Self {
            actual_start_time_ns: 0,
            actual_end_time_ns: 0,
            num_source_change_records: 0,
            num_skipped_source_change_records: 0,
            num_label_records: 0,
            num_pause_records: 0,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ScriptSourceChangeGeneratorExternalState {
    pub error_messages: Vec<String>,
    pub ignore_scripted_pause_commands: bool,
    pub header_record: ChangeHeaderRecord,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub previous_record: Option<ProcessedChangeScriptRecord>,
    pub skips_remaining: u64,
    pub skips_spacing_mode: Option<SpacingMode>,
    pub spacing_mode: SpacingMode,
    pub status: SourceChangeGeneratorStatus,
    pub steps_remaining: u64,
    pub steps_spacing_mode: Option<SpacingMode>,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl From<&mut ScriptSourceChangeGeneratorInternalState> for ScriptSourceChangeGeneratorExternalState {
    fn from(state: &mut ScriptSourceChangeGeneratorInternalState) -> Self {
        Self {
            error_messages: state.error_messages.clone(),
            ignore_scripted_pause_commands: state.ignore_scripted_pause_commands,
            header_record: state.header_record.clone(),
            next_record: state.next_record.clone(),
            previous_record: state.previous_record.clone(),
            skips_remaining: state.skips_remaining,
            skips_spacing_mode: state.skips_spacing_mode.clone(),
            spacing_mode: state.spacing_mode.clone(),
            status: state.status,
            steps_remaining: state.steps_remaining,
            steps_spacing_mode: state.steps_spacing_mode.clone(),
            test_run_source_id: state.test_run_source_id.clone(),
            time_mode: state.time_mode.clone(),
            virtual_time_ns_current: state.virtual_time_ns_current,
            virtual_time_ns_offset: state.virtual_time_ns_offset,
            virtual_time_ns_start: state.virtual_time_ns_start,
        }
    }
}

pub struct ScriptSourceChangeGeneratorInternalState {
    pub change_stream: Pin<Box<dyn Stream<Item = Result<SequencedChangeScriptRecord, anyhow::Error>> + Send>>,
    pub change_tx_channel: Sender<ScheduledChangeScriptRecordMessage>,
    pub delayer_tx_channel: Sender<ScheduledChangeScriptRecordMessage>,
    pub dispatchers: Vec<Box<dyn SourceChangeDispatcher + Send>>,
    pub error_messages: Vec<String>,
    pub ignore_scripted_pause_commands: bool,
    pub header_record: ChangeHeaderRecord,
    pub next_record: Option<SequencedChangeScriptRecord>,
    pub output_storage: TestRunSourceStorage,
    pub previous_record: Option<ProcessedChangeScriptRecord>,
    pub skips_remaining: u64,
    pub skips_spacing_mode: Option<SpacingMode>,
    pub spacing_mode: SpacingMode,
    pub status: SourceChangeGeneratorStatus,
    pub stats: ScriptSourceChangeGeneratorStats,
    pub steps_remaining: u64,
    pub steps_spacing_mode: Option<SpacingMode>,
    pub test_run_source_id: TestRunSourceId,
    pub time_mode: TimeMode,
    pub virtual_time_ns_current: u64,
    pub virtual_time_ns_offset: u64,
    pub virtual_time_ns_start: u64,
}

impl Debug for ScriptSourceChangeGeneratorInternalState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptSourceChangeGeneratorInternalState")
            .field("error_messages", &self.error_messages)
            .field("ignore_scripted_pause_commands", &self.ignore_scripted_pause_commands)
            .field("header_record", &self.header_record)
            .field("next_record", &self.next_record)
            .field("previous_record", &self.previous_record)
            .field("skips_remaining", &self.skips_remaining)
            .field("skips_spacing_mode", &self.skips_spacing_mode)
            .field("spacing_mode", &self.spacing_mode)
            .field("status", &self.status)
            .field("stats", &self.stats)
            .field("steps_remaining", &self.steps_remaining)
            .field("steps_spacing_mode", &self.steps_spacing_mode)
            .field("time_mode", &self.time_mode)
            .field("virtual_time_ns_current", &self.virtual_time_ns_current)
            .field("virtual_time_ns_offset", &self.virtual_time_ns_offset)
            .field("virtual_time_ns_start", &self.virtual_time_ns_start)
            .finish()
    }
}

#[derive(Clone, Serialize)]
pub struct ScriptSourceChangeGeneratorResultSummary {
    pub actual_start_time: String,
    pub actual_start_time_ns: u64,
    pub actual_end_time: String,
    pub actual_end_time_ns: u64,
    pub run_duration_ns: u64,
    pub run_duration_sec: f64,
    pub num_source_change_records: u64,
    pub num_skipped_source_change: u64,
    pub num_label_records: u64,
    pub num_pause_records: u64,
    pub processing_rate: f64,
    pub test_run_source_id: String,
}

impl From<&mut ScriptSourceChangeGeneratorInternalState> for ScriptSourceChangeGeneratorResultSummary {
    fn from(state: &mut ScriptSourceChangeGeneratorInternalState) -> Self {
        let run_duration_ns = state.stats.actual_end_time_ns - state.stats.actual_start_time_ns;
        let run_duration_sec = run_duration_ns as f64 / 1_000_000_000.0;

        Self {
            actual_start_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.actual_start_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            actual_start_time_ns: state.stats.actual_start_time_ns,
            actual_end_time: OffsetDateTime::from_unix_timestamp_nanos(state.stats.actual_end_time_ns as i128).expect("Invalid timestamp")
                .format(&format_description::well_known::Rfc3339).unwrap(),
            actual_end_time_ns: state.stats.actual_end_time_ns,
            run_duration_ns,
            run_duration_sec,
            num_source_change_records: state.stats.num_source_change_records,
            num_skipped_source_change: state.stats.num_skipped_source_change_records,
            num_label_records: state.stats.num_label_records,
            num_pause_records: state.stats.num_pause_records,
            processing_rate: state.stats.num_source_change_records as f64 / run_duration_sec,
            test_run_source_id: state.test_run_source_id.to_string(),
        }
    }
}

impl Debug for ScriptSourceChangeGeneratorResultSummary {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let start_time = format!("{} ({} ns)", self.actual_start_time, self.actual_start_time_ns);
        let end_time = format!("{} ({} ns)", self.actual_end_time, self.actual_end_time_ns);
        let run_duration = format!("{} sec ({} ns)", self.run_duration_sec, self.run_duration_ns, );
        let source_change_records = format!("{} (skipped:{}, label:{}, pause:{})", 
            self.num_source_change_records, self.num_skipped_source_change, self.num_label_records, self.num_pause_records);
        let processing_rate = format!("{:.2} changes / sec", self.processing_rate);

        f.debug_struct("ScriptSourceChangeGeneratorResultSummary")
            .field("test_run_source_id", &self.test_run_source_id)
            .field("start_time", &start_time)
            .field("end_time", &end_time)
            .field("run_duration", &run_duration)
            .field("source_change_records", &source_change_records)
            .field("processing_rate", &processing_rate)
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
    Skip{skips: u64, spacing_mode: Option<SpacingMode>},
    // Command to start the ScriptSourceChangeGenerator.
    Start,
    // Command to step the ScriptSourceChangeGenerator forward a specified number of ChangeScriptRecords.
    Step{steps: u64, spacing_mode: Option<SpacingMode>},
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
pub struct ScheduledChangeScriptRecordMessage {
    pub delay_ns: u64,
    pub record_seq_num: u64,
    pub virtual_time_ns_replay: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessedChangeScriptRecord {
    pub dispatch_status: SourceChangeGeneratorStatus,
    pub scripted: SequencedChangeScriptRecord,
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

    async fn pause(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ScriptSourceChangeGeneratorCommand::Pause).await
    }

    async fn skip(&self, skips: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ScriptSourceChangeGeneratorCommand::Skip{skips, spacing_mode}).await
    }

    async fn start(&self) -> anyhow::Result<SourceChangeGeneratorCommandResponse> {
        self.send_command(ScriptSourceChangeGeneratorCommand::Start).await
    }

    async fn step(&self, steps: u64, spacing_mode: Option<SpacingMode>) -> anyhow::Result<SourceChangeGeneratorCommandResponse>  {
        self.send_command(ScriptSourceChangeGeneratorCommand::Step{steps, spacing_mode}).await
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
            // If initialization fails, don't dont transition to an error state, just log an error and exit the thread.
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
                    Some(command_message) => {
                        process_command_message(&mut state, command_message).await
                            .inspect_err(|e| transition_to_error_state(&mut state, "Error calling process_command_message.", Some(e))).ok();
                    }
                    None => {
                        transition_to_error_state(&mut state, "Command channel closed.", None);
                        break;
                    }
                }
            },

            // Process messages from the Change Stream.
            change_stream_message = change_rx_channel.recv(), if state.status.is_processing() => {
                match change_stream_message {
                    Some(change_stream_message) => {
                        process_change_stream_message(&mut state, change_stream_message).await
                            .inspect_err(|e| transition_to_error_state(&mut state, "Error calling process_change_stream_message", Some(e))).ok();
                    }
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

async fn initialize(settings: ScriptSourceChangeGeneratorSettings)  -> anyhow::Result<(ScriptSourceChangeGeneratorInternalState, Receiver<ScheduledChangeScriptRecordMessage>)> {
    log::info!("Initializing ScriptSourceChangeGenerator using {:?}", settings);

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
        change_stream: change_stream,
        change_tx_channel,
        delayer_tx_channel,
        dispatchers,
        error_messages: Vec::new(),
        ignore_scripted_pause_commands: settings.ignore_scripted_pause_commands,
        header_record: header,
        next_record: first_record,
        output_storage: settings.output_storage,
        previous_record: None,
        skips_remaining: 0,
        skips_spacing_mode: None,
        spacing_mode: settings.spacing_mode,
        status: SourceChangeGeneratorStatus::Paused,
        stats: ScriptSourceChangeGeneratorStats::default(),
        steps_remaining: 0,
        steps_spacing_mode: None,
        test_run_source_id: settings.id.clone(),
        time_mode: settings.time_mode,
        virtual_time_ns_current: 0,
        virtual_time_ns_offset: 0,
        virtual_time_ns_start: 0,
    };

    Ok((player_state, change_rx_channel))
}

async fn process_command_message(state: &mut ScriptSourceChangeGeneratorInternalState, message: ScriptSourceChangeGeneratorMessage) -> anyhow::Result<()> {
    log::debug!("Received command message: {:?}", message.command);

    if let ScriptSourceChangeGeneratorCommand::GetState = message.command {
        let message_response = ScriptSourceChangeGeneratorMessageResponse {
            result: Ok(()),
            state: state.into(),
        };

        let r = message.response_tx.unwrap().send(message_response);
        if let Err(e) = r {
            anyhow::bail!("Error sending message response back to caller: {:?}", e);
        }
    } else {
        let transition_response = match state.status {
            SourceChangeGeneratorStatus::Running => transition_from_running_state(state, &message.command).await,
            SourceChangeGeneratorStatus::Stepping => transition_from_stepping_state(state, &message.command).await,
            SourceChangeGeneratorStatus::Skipping => transition_from_skipping_state(state, &message.command).await,
            SourceChangeGeneratorStatus::Paused => transition_from_paused_state(state, &message.command).await,
            SourceChangeGeneratorStatus::Stopped => transition_from_stopped_state(state, &message.command),
            SourceChangeGeneratorStatus::Finished => transition_from_finished_state(state, &message.command),
            SourceChangeGeneratorStatus::Error => transition_from_error_state(state),
        };

        if message.response_tx.is_some() {
            let message_response = ScriptSourceChangeGeneratorMessageResponse {
                result: transition_response,
                state: state.into(),
            };

            let r = message.response_tx.unwrap().send(message_response);
            if let Err(e) = r {
                anyhow::bail!("Error sending message response back to caller: {:?}", e);
            }
        }    
    }

    Ok(())
}

async fn process_change_stream_message(state: &mut ScriptSourceChangeGeneratorInternalState, message: ScheduledChangeScriptRecordMessage) -> anyhow::Result<()> {
    log::trace!("Received change stream message: {:?}", message);
    
    // We could receive a message that was delayed before transitioning to an inactive state, so ignore it.
    // It will remain as the next record and be scheduled if the player is unpaused again.
    if !state.status.is_processing() {
        log::error!("Ignoring change stream message in non-processing state: {:?}", message);
        return Ok(());
    }

    // Get the next record from the player state. Error if it is None.
    let next_record = match state.next_record.as_ref() {
        Some(record) => record.clone(),
        None => anyhow::bail!("Received ScheduledChangeScriptRecordMessage when player_state.next_record is None")
    };

    // Return an error state if the scheduled record does not match the next record.
    if message.record_seq_num != next_record.seq {
       anyhow::bail!("Error processing change stream message - scheduled record (seq: {}) does not match next record (seq: {}).", 
        message.record_seq_num, next_record.seq);
    }

    // Time Shift.
    let shifted_record = time_shift(state, next_record)?;

    // Process the record.
    match &shifted_record.record {
        ChangeScriptRecord::SourceChange(change_record) => {
            state.stats.num_source_change_records += 1;

            match &state.status {
                SourceChangeGeneratorStatus::Running => {
                    // Dispatch the SourceChangeEvent.
                    dispatch_source_change_events(state, vec!(&change_record.source_change_event)).await;
                    load_next_change_stream_record(state).await?;  
                    schedule_next_change_stream_record(state).await?;
                },
                SourceChangeGeneratorStatus::Stepping => {
                    if state.steps_remaining > 0 {
                        // Dispatch the SourceChangeEvent.
                        dispatch_source_change_events(state, vec!(&change_record.source_change_event)).await;
                        
                        load_next_change_stream_record(state).await?;  

                        state.steps_remaining -= 1;
                        if state.steps_remaining == 0 {
                            state.status = SourceChangeGeneratorStatus::Paused;
                            state.steps_spacing_mode = None;
                        } else {
                            schedule_next_change_stream_record(state).await?;
                        }
                    } else {
                        // Transition to an error state.
                        transition_to_error_state(state, "Stepping with no steps remaining", None);
                    }
                },
                SourceChangeGeneratorStatus::Skipping => {
                    if state.skips_remaining > 0 {
                        // DON'T dispatch the SourceChangeEvent.
                        log::trace!("Skipping ChangeScriptRecord: {:?}", change_record);
                        state.stats.num_skipped_source_change_records += 1;

                        load_next_change_stream_record(state).await?;  

                        state.skips_remaining -= 1;
                        if state.skips_remaining == 0 {
                            state.status = SourceChangeGeneratorStatus::Paused;
                            state.skips_spacing_mode = None;
                        } else {
                            schedule_next_change_stream_record(state).await?;
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
            state.stats.num_pause_records += 1;

            // Process the PauseCommand only if the Player is not configured to ignore them.
            if state.ignore_scripted_pause_commands {
                log::debug!("Ignoring Change Script Pause Command: {:?}", shifted_record);
            } else {
                state.status = SourceChangeGeneratorStatus::Paused;
            }
        },
        ChangeScriptRecord::Label(label_record) => {
            state.stats.num_label_records += 1;

            log::debug!("Reached Source Change Script Label: {:?}", label_record);
        },
        ChangeScriptRecord::Finish(_) => {
            transition_to_finished_state(state).await;
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

    Ok(())
}

async fn load_next_change_stream_record(state: &mut ScriptSourceChangeGeneratorInternalState) -> anyhow::Result<()> {
    match state.change_stream.next().await {
        Some(Ok(seq_record)) => {
            state.previous_record = Some(ProcessedChangeScriptRecord {
                dispatch_status: state.status,
                scripted: state.next_record.clone().unwrap(),
            });

            state.next_record = Some(seq_record);
        },
        Some(Err(e)) => {
            anyhow::bail!(format!("Error reading ChangeScriptRecord: {:?}", e));
        },
        None => {
            anyhow::bail!("ChangeScriptReader.next() returned None, shouldn't be seeing this.");
        }
    };

    Ok(())
}

fn calculate_record_scheduling_delay(
    state: &mut ScriptSourceChangeGeneratorInternalState, 
    next_record: &SequencedChangeScriptRecord
) -> anyhow::Result<u64> {
    let delay_ns = match state.status {
        SourceChangeGeneratorStatus::Skipping => {
            match state.skips_spacing_mode {
                Some(SpacingMode::None) => 0,
                Some(SpacingMode::Fixed(nanos)) => nanos,
                Some(SpacingMode::Recorded) => if state.virtual_time_ns_offset > next_record.offset_ns { 0 } else { next_record.offset_ns - state.virtual_time_ns_offset },
                None => 0,
            }
        },
        SourceChangeGeneratorStatus::Stepping => {
            match state.steps_spacing_mode {
                Some(SpacingMode::None) => 0,
                Some(SpacingMode::Fixed(nanos)) => nanos,
                Some(SpacingMode::Recorded) => if state.virtual_time_ns_offset > next_record.offset_ns { 0 } else { next_record.offset_ns - state.virtual_time_ns_offset }
                None => match state.spacing_mode {
                    SpacingMode::None => 0,
                    SpacingMode::Fixed(nanos) => nanos,
                    SpacingMode::Recorded => if state.virtual_time_ns_offset > next_record.offset_ns { 0 } else { next_record.offset_ns - state.virtual_time_ns_offset }
                },
            }
        },
        SourceChangeGeneratorStatus::Running => match state.spacing_mode {
            SpacingMode::None => 0,
            SpacingMode::Fixed(nanos) => nanos,
            SpacingMode::Recorded => if state.virtual_time_ns_offset > next_record.offset_ns { 0 } else { next_record.offset_ns - state.virtual_time_ns_offset }
        },
        _ => anyhow::bail!("Calculating record delay for unexpected status: {:?}", state.status),
    };

    Ok(delay_ns)
}

async fn schedule_next_change_stream_record(state: &mut ScriptSourceChangeGeneratorInternalState) -> anyhow::Result<()> {

    // Get the next record from the player state. Error if it is None.
    let next_record = match state.next_record.as_ref() {
        Some(record) => record.clone(),
        None => anyhow::bail!("Received ScheduledChangeScriptRecordMessage when player_state.next_record is None")
    };

    let delay_ns = calculate_record_scheduling_delay(state, &next_record)?;

    let sch_msg = ScheduledChangeScriptRecordMessage {
        delay_ns,
        record_seq_num: next_record.seq,
        virtual_time_ns_replay: state.virtual_time_ns_current + delay_ns,
    };    

    // Take action based on size of the delay.
    // It doesn't make sense to delay for trivially small amounts of time, nor does it make sense to send 
    // a message to the delayer thread for relatively small delays. 
    // TODO: This figures might need to be adjusted, or made configurable.
    if delay_ns < 1_000 {
        // Process the record immediately.
        if let Err(e) = state.change_tx_channel.send(sch_msg).await {
            anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {:?}", e);
        }
    } else if delay_ns < 10_000_000 {
        // Sleep inproc, then process the record.
        sleep(Duration::from_nanos(delay_ns)).await;
        if let Err(e) = state.change_tx_channel.send(sch_msg).await {
            anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {:?}", e);
        }
    } else {
        if let Err(e) = state.delayer_tx_channel.send(sch_msg).await {
            anyhow::bail!("Error sending ScheduledChangeScriptRecordMessage: {:?}", e);
        }
    };

    Ok(())
}

async fn close_dispatchers(state: &mut ScriptSourceChangeGeneratorInternalState) {
    let dispatchers = &mut state.dispatchers;

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

// Function to log the Player State at varying levels of detail.
fn log_test_script_player_state(state: &ScriptSourceChangeGeneratorInternalState, msg: &str) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, state),
        log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, state),
        // log::LevelFilter::Info => log::info!("{} - status:{:?}, error_message:{:?}, virtual_time_ns_start:{:?}, virtual_time_ns_current:{:?}, skips_remaining:{:?}, steps_remaining:{:?}",
        //     msg, state.status, state.error_messages, state.virtual_time_ns_start, state.virtual_time_ns_current, state.skips_remaining, state.steps_remaining),
        _ => {}
    }
}

fn time_shift(state: &mut ScriptSourceChangeGeneratorInternalState, next_record: SequencedChangeScriptRecord) -> anyhow::Result<SequencedChangeScriptRecord> {
    
    let current_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // Time Mode controls how the updated virtual time is calculated.
    match state.time_mode {
        TimeMode::Live => {
            // Live - Use the actual current time.
            state.virtual_time_ns_current = current_time_ns;
            state.virtual_time_ns_offset = current_time_ns - state.virtual_time_ns_start;
        },
        TimeMode::Recorded => {
            // Recorded - Use the recorded time from the script.
            state.virtual_time_ns_current = state.virtual_time_ns_start + next_record.offset_ns;
            state.virtual_time_ns_offset = next_record.offset_ns;
        },
        TimeMode::Rebased(nanos) => {
            // Rebased - Use the rebased time provided in the config.
            state.virtual_time_ns_current = nanos + next_record.offset_ns;
            state.virtual_time_ns_offset = next_record.offset_ns;
        },
    };

    let shifted_change_record = match &next_record.record {
        ChangeScriptRecord::SourceChange(change_record) => {
            let mut shifted_change_record = change_record.clone();

            shifted_change_record.source_change_event.ts_ms = state.virtual_time_ns_current / 1_000_000;
            shifted_change_record.source_change_event.payload.source.ts_ms = state.virtual_time_ns_current / 1_000_000;
            shifted_change_record.source_change_event.payload.source.ts_sec = state.virtual_time_ns_current / 1_000_000_000;

            // TODO: Modify internal date times.

            ChangeScriptRecord::SourceChange(shifted_change_record)
        },
        ChangeScriptRecord::Label(_)
        | ChangeScriptRecord::PauseCommand(_) 
        | ChangeScriptRecord::Finish(_) => next_record.record.clone(),
        _ => anyhow::bail!("Unexpected ChangeScriptRecord type: {:?}", next_record.record),
    };

    let shifted_record = SequencedChangeScriptRecord {
        seq: next_record.seq,
        offset_ns: state.virtual_time_ns_current,
        record: shifted_change_record
    };

    state.next_record = Some(shifted_record.clone());

    Ok(shifted_record)
}

async fn transition_from_paused_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    // If we are unpausing for the first time, we need to initialize the start times based on time_mode config.
    if state.previous_record.is_none() && 
        matches!(command, ScriptSourceChangeGeneratorCommand::Start 
            | ScriptSourceChangeGeneratorCommand::Step { .. }
            | ScriptSourceChangeGeneratorCommand::Skip { .. }
        ) {
        state.stats.actual_start_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

        state.virtual_time_ns_start = match state.time_mode {
            TimeMode::Live => state.stats.actual_start_time_ns,
            TimeMode::Recorded => state.header_record.start_time.timestamp_nanos_opt().unwrap() as u64,
            TimeMode::Rebased(nanos) => nanos,
        };

        state.virtual_time_ns_current = state.virtual_time_ns_start;
        state.virtual_time_ns_offset = 0;
    }

    match command {
        ScriptSourceChangeGeneratorCommand::Start => {
            log::info!("Script Started for TestRunSource {}", state.test_run_source_id);
            
            state.status = SourceChangeGeneratorStatus::Running;
            schedule_next_change_stream_record(state).await?;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Step{steps, spacing_mode} => {
            log::info!("Script Stepping {} steps for TestRunSource {}", steps, state.test_run_source_id);

            state.status = SourceChangeGeneratorStatus::Stepping;
            state.steps_remaining = *steps;
            state.steps_spacing_mode = spacing_mode.clone();
            schedule_next_change_stream_record(state).await?;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Skip{skips, spacing_mode} => {
            log::info!("Script Skipping {} skips for TestRunSource {}", skips, state.test_run_source_id);

            state.status = SourceChangeGeneratorStatus::Skipping;
            state.skips_remaining = *skips;
            state.skips_spacing_mode = spacing_mode.clone();
            schedule_next_change_stream_record(state).await?;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            Ok(transition_to_stopped_state(state).await)
        },
        ScriptSourceChangeGeneratorCommand::GetState |
        ScriptSourceChangeGeneratorCommand::Pause => {
            Ok(())
        }
    }
}

async fn transition_from_running_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Step{..} => {
            Err(ScriptSourceChangeGeneratorError::PauseToStep.into())
        },
        ScriptSourceChangeGeneratorCommand::Skip{..} => {
            Err(ScriptSourceChangeGeneratorError::PauseToSkip.into())
        },
        ScriptSourceChangeGeneratorCommand::Pause => {
            state.status = SourceChangeGeneratorStatus::Paused;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            Ok(transition_to_stopped_state(state).await)
        },
        ScriptSourceChangeGeneratorCommand::GetState |
        ScriptSourceChangeGeneratorCommand::Start => {
            Ok(())
        }
    }
}

async fn transition_from_stepping_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Pause => {
            state.status = SourceChangeGeneratorStatus::Paused;
            state.steps_remaining = 0;
            state.steps_spacing_mode = None;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            Ok(transition_to_stopped_state(state).await)
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Skip {..} |
        ScriptSourceChangeGeneratorCommand::Step {..} |
        ScriptSourceChangeGeneratorCommand::Start
            => Err(ScriptSourceChangeGeneratorError::CurrentlyStepping(state.steps_remaining).into())
    }
}

async fn transition_from_skipping_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Transitioning from {:?} state via command: {:?}", state.status, command);

    match command {
        ScriptSourceChangeGeneratorCommand::Pause => {
            state.status = SourceChangeGeneratorStatus::Paused;
            state.skips_remaining = 0;
            state.skips_spacing_mode = None;
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Stop => {
            Ok(transition_to_stopped_state(state).await)
        },
        ScriptSourceChangeGeneratorCommand::GetState => {
            Ok(())
        },
        ScriptSourceChangeGeneratorCommand::Skip {..} |
        ScriptSourceChangeGeneratorCommand::Step {..} |
        ScriptSourceChangeGeneratorCommand::Start
            => Err(ScriptSourceChangeGeneratorError::CurrentlySkipping(state.skips_remaining).into())
    }
}

fn transition_from_stopped_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Attempting to transition from {:?} state via command: {:?}", state.status, command);

    Err(ScriptSourceChangeGeneratorError::AlreadyStopped.into())
}

fn transition_from_finished_state(state: &mut ScriptSourceChangeGeneratorInternalState, command: &ScriptSourceChangeGeneratorCommand) -> anyhow::Result<()> {
    log::debug!("Attempting to transition from {:?} state via command: {:?}", state.status, command);

    Err(ScriptSourceChangeGeneratorError::AlreadyFinished.into())
}

fn transition_from_error_state(state: &mut ScriptSourceChangeGeneratorInternalState) -> anyhow::Result<()> {
    log_test_script_player_state(state, &format!("Script Error for TestRunSource {}", state.test_run_source_id));

    Err(ScriptSourceChangeGeneratorError::Error(state.status).into())
}

async fn transition_to_finished_state(state: &mut ScriptSourceChangeGeneratorInternalState) {
    log::info!("Script Finished for TestRunSource {}", state.test_run_source_id);

    state.status = SourceChangeGeneratorStatus::Finished;
    state.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
    state.skips_remaining = 0;
    state.skips_spacing_mode = None;
    state.steps_remaining = 0;
    state.steps_spacing_mode = None;
    close_dispatchers(state).await;
    write_result_summary(state).await.ok();
}

async fn transition_to_stopped_state(state: &mut ScriptSourceChangeGeneratorInternalState) {
    log::info!("Script Stopped for TestRunSource {}", state.test_run_source_id);

    state.status = SourceChangeGeneratorStatus::Stopped;
    state.stats.actual_end_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
    state.skips_remaining = 0;
    state.skips_spacing_mode = None;
    state.steps_remaining = 0;
    state.steps_spacing_mode = None;
    close_dispatchers(state).await;
    write_result_summary(state).await.ok();
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

pub async fn delayer_thread(id: TestRunSourceId, mut delayer_rx_channel: Receiver<ScheduledChangeScriptRecordMessage>, change_tx_channel: Sender<ScheduledChangeScriptRecordMessage>) {
    log::info!("Delayer thread started for TestRunSource {} ...", id);

    loop {
        match delayer_rx_channel.recv().await {
            Some(message) => {
                // Sleep for the specified time before sending the message to the change_tx_channel.
                sleep(Duration::from_nanos(message.delay_ns)).await;

                if let Err(e) = change_tx_channel.send(message).await {
                    log::error!("Error sending ScheduledChangeScriptRecordMessage to change_tx_channel: {:?}", e);
                }
            },
            None => {
                log::error!("ChangeScriptRecord delayer channel closed.");
                break;
            }
        }
    }
}

pub async fn write_result_summary(state: &mut ScriptSourceChangeGeneratorInternalState) -> anyhow::Result<()> {

    let result_summary: ScriptSourceChangeGeneratorResultSummary = state.into();
    log::info!("Stats for TestRunSource:\n{:#?}", &result_summary);

    let result_summary_value = serde_json::to_value(result_summary).unwrap();
    match state.output_storage.write_result_summary(&result_summary_value).await {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!("Error writing result summary to output storage: {:?}", e);
            Err(e)
        }
    }
}
