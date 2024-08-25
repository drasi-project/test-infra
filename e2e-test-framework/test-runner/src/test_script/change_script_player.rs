use std::{fmt, path::PathBuf, str::FromStr, sync::Arc, time::SystemTime};

use serde::Serialize;
use tokio::sync::{mpsc::{Receiver, Sender}, oneshot, Mutex};
use tokio::sync::mpsc::error::TryRecvError::{Empty, Disconnected};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::{
    mask_secret, 
    config::{OutputType, ServiceSettings, SourceConfig, SourceConfigDefaults}, 
    source_change_dispatchers::{
        console_dispatcher::ConsoleSourceChangeEventDispatcher,
        dapr_dispatcher::DaprSourceChangeEventDispatcher,
        file_dispatcher::JsonlFileSourceChangeDispatcher,
        null_dispatcher::NullSourceChangeEventDispatcher,
    }
};
use crate::source_change_dispatchers::SourceChangeEventDispatcher;
use crate::test_script::change_script_reader::{ChangeScriptReader, ChangeScriptRecord, SequencedChangeScriptRecord};


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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ChangeScriptPlayerTimeMode {
    Live,
    Recorded,
    Rebased(u64),
}

impl FromStr for ChangeScriptPlayerTimeMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "live" => Ok(Self::Live),
            "recorded" => Ok(Self::Recorded),
            _ => {
                match chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(t) => Ok(Self::Rebased(t.timestamp_nanos_opt().unwrap() as u64)),
                    Err(e) => {
                        anyhow::bail!("Error parsing ChangeScriptPlayerTimeMode - value:{}, error:{}", s, e);
                    }
                }
            }
        }
    }
}

impl fmt::Display for ChangeScriptPlayerTimeMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Recorded => write!(f, "recorded"),
            Self::Rebased(time) => write!(f, "{}", time),
        }
    }
}

impl Default for ChangeScriptPlayerTimeMode {
    fn default() -> Self {
        Self::Recorded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ChangeScriptPlayerSpacingMode {
    None,
    Recorded,
    Fixed(u64),
}

// Implementation of FromStr for ReplayEventSpacingMode.
// For the Fixed variant, the spacing is specified as a string duration such as '5s' or '100n'.
// Supported units are seconds ('s'), milliseconds ('m'), microseconds ('u'), and nanoseconds ('n').
// If the string can't be parsed as a TimeDelta, an error is returned.
impl FromStr for ChangeScriptPlayerSpacingMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "none" => Ok(Self::None),
            "recorded" => Ok(Self::Recorded),
            _ => {
                // Parse the string as a number, followed by a time unit character.
                let (num_str, unit_str) = s.split_at(s.len() - 1);
                let num = match num_str.parse::<u64>() {
                    Ok(num) => num,
                    Err(e) => {
                        anyhow::bail!("Error parsing ChangeScriptPlayerSpacingMode: {}", e);
                    }
                };
                match unit_str {
                    "s" => Ok(Self::Fixed(num * 1000000000)),
                    "m" => Ok(Self::Fixed(num * 1000000)),
                    "u" => Ok(Self::Fixed(num * 1000)),
                    "n" => Ok(Self::Fixed(num)),
                    _ => {
                        anyhow::bail!("Invalid ChangeScriptPlayerSpacingMode: {}", s);
                    }
                }
            }
        }
    }
}

impl fmt::Display for ChangeScriptPlayerSpacingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Recorded => write!(f, "recorded"),
            Self::Fixed(d) => write!(f, "{}", d),
        }
    }
}

impl Default for ChangeScriptPlayerSpacingMode {
    fn default() -> Self {
        Self::Recorded
    }
}

// The ChangeScriptPlayerSettings struct holds the configuration settings that are used by the Change Script Player.
// It is created based on the SourceConfig either loaded from the Service config file or passed in to the Web API, and is 
// combined with the ServiceSettings and default values to create the final set of configuration.
// It is static and does not change during the execution of the Change Script Player, it is not used to track the active state of the Change Script Player.
#[derive(Debug, Serialize, Clone)]
pub struct ChangeScriptPlayerSettings {
    // The Test ID.
    pub test_id: String,

    // The Test Run ID.
    pub test_run_id: String,

    // The Test Storage Account where the Test Repo is located.
    pub test_storage_account: String,

    // The Test Storage Access Key where the Test Repo is located.
    #[serde(serialize_with = "mask_secret")]
    pub test_storage_access_key: String,

    // The Test Storage Container where the Test Repo is located.
    pub test_storage_container: String,

    // The Test Storage Path where the Test Repo is located.
    pub test_storage_path: String,

    // The Source ID for the Change Script Player.
    pub source_id: String,

    // The address of the change queue.
    pub change_queue_address: String,

    // The port of the change queue.
    pub change_queue_port: u16,

    // The PubSub topic for the change queue.
    pub change_queue_topic: String,
    
    // Flag to indicate if the Service should start the Change Script Player immediately after initialization.
    pub start_immediately: bool,

    // Whether the ChangeScriptPlayer should ignore scripted pause commands.
    pub ignore_scripted_pause_commands: bool,

    // SourceChangeEvent Time Mode for the Change Script Player.
    pub source_change_event_time_mode: ChangeScriptPlayerTimeMode,

    // SourceChangeEvent Spacing Mode for the Change Script Player.
    pub source_change_event_spacing_mode: ChangeScriptPlayerSpacingMode,

    // The path where data used and generated in the Change Script Player gets stored.
    pub data_cache_path: String,

    // The OutputType for Source Change Events.
    pub event_output: OutputType,

    // The OutputType for Change Script Player Telemetry data.
    pub telemetry_output: OutputType,

    // The OutputType for Change Script Player Log data.
    pub log_output: OutputType,
}

impl ChangeScriptPlayerSettings {
    // Function to create a new ChangeScriptPlayerSettings by combining a SourceConfig and the Service Settings.
    // The ChangeScriptPlayerSettings control the configuration and operation of a TestRun.   
    pub fn try_from_source_config(source_config: &SourceConfig, source_defaults: &SourceConfigDefaults, service_settings: &ServiceSettings) -> anyhow::Result<Self> {

        // If the SourceConfig doesnt contain a ReactivatorConfig, log and return an error.
        let reactivator_config = match &source_config.reactivator {
            Some(reactivator_config) => reactivator_config,
            None => {
                anyhow::bail!("No ReactivatorConfig provided in SourceConfig: {:?}", source_config);
            }
        };

        // If neither the SourceConfig nor the SourceDefaults contain a test_id, return an error.
        let test_id = match &source_config.test_id {
            Some(test_id) => test_id.clone(),
            None => {
                match &source_defaults.test_id {
                    Some(test_id) => test_id.clone(),
                    None => {
                        anyhow::bail!("No test_id provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a test_run_id, create one based on current time.
        let test_run_id = match &source_config.test_run_id {
            Some(test_run_id) => test_run_id.clone(),
            None => chrono::Utc::now().format("%Y%m%d%H%M%S").to_string()
        };

        // If the SourceConfig doesn't contain a test_storage_account value, use the default value.
        // If there is no default value, return an error.
        let test_storage_account = match &source_config.test_storage_account {
            Some(test_storage_account) => test_storage_account.clone(),
            None => {
                match &source_defaults.test_storage_account {
                    Some(test_storage_account) => test_storage_account.clone(),
                    None => {
                        anyhow::bail!("No test_storage_account provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a test_storage_access_key value, use the default value.
        // If there is no default value, return an error.
        let test_storage_access_key = match &source_config.test_storage_access_key {
            Some(test_storage_access_key) => test_storage_access_key.clone(),
            None => {
                match &source_defaults.test_storage_access_key {
                    Some(test_storage_access_key) => test_storage_access_key.clone(),
                    None => {
                        anyhow::bail!("No test_storage_access_key provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a test_storage_access_key value, use the default value.
        // If there is no default value, return an error.
        let test_storage_container = match &source_config.test_storage_container {
            Some(test_storage_container) => test_storage_container.clone(),
            None => {
                match &source_defaults.test_storage_container {
                    Some(test_storage_container) => test_storage_container.clone(),
                    None => {
                        anyhow::bail!("No test_storage_container provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a test_storage_path value, use the default value.
        // If there is no default value, return an error.
        let test_storage_path = match &source_config.test_storage_path {
            Some(test_storage_path) => test_storage_path.clone(),
            None => {
                match &source_defaults.test_storage_path {
                    Some(test_storage_path) => test_storage_path.clone(),
                    None => {
                        anyhow::bail!("No test_storage_path provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a source_id value, use the default value.
        // If there is no default value, return an error.
        let source_id = match &source_config.source_id {
            Some(source_id) => source_id.clone(),
            None => {
                match &source_defaults.source_id {
                    Some(source_id) => source_id.clone(),
                    None => {
                        anyhow::bail!("No source_id provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a change_queue_address value, use the default value.
        // If there is no default value, return an error.
        let change_queue_address = match &reactivator_config.change_queue_address {
            Some(change_queue_address) => change_queue_address.clone(),
            None => {
                match &source_defaults.reactivator.change_queue_address {
                    Some(change_queue_address) => change_queue_address.clone(),
                    None => {
                        anyhow::bail!("No change_queue_address provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a change_queue_port value, use the default value.
        // If there is no default value, return an error.
        let change_queue_port = match &reactivator_config.change_queue_port {
            Some(change_queue_port) => change_queue_port.clone(),
            None => {
                match &source_defaults.reactivator.change_queue_port {
                    Some(change_queue_port) => change_queue_port.clone(),
                    None => {
                        anyhow::bail!("No change_queue_port provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a change_queue_topic value, use the default value.
        // If there is no default value, return an error.
        let change_queue_topic = match &reactivator_config.change_queue_topic {
            Some(change_queue_topic) => change_queue_topic.clone(),
            None => {
                match &source_defaults.reactivator.change_queue_topic {
                    Some(change_queue_topic) => change_queue_topic.clone(),
                    None => {
                        anyhow::bail!("No change_queue_topic provided and no default value found.");
                    }
                }
            }
        };

        let spacing_mode = match &reactivator_config.spacing_mode {
            Some(mode) => ChangeScriptPlayerSpacingMode::from_str(&mode).unwrap(),
            None => ChangeScriptPlayerSpacingMode::from_str(&source_defaults.reactivator.spacing_mode).unwrap()
        };

        let time_mode = match &reactivator_config.time_mode {
            Some(mode) => ChangeScriptPlayerTimeMode::from_str(&mode).unwrap(),
            None => ChangeScriptPlayerTimeMode::from_str(&source_defaults.reactivator.time_mode).unwrap()
        };

        Ok(ChangeScriptPlayerSettings {
            test_id,
            test_run_id,
            test_storage_account,
            test_storage_access_key,
            test_storage_container,
            test_storage_path,
            source_id,
            change_queue_address,
            change_queue_port,
            change_queue_topic,
            ignore_scripted_pause_commands: reactivator_config.ignore_scripted_pause_commands.unwrap_or(source_defaults.reactivator.ignore_scripted_pause_commands),
            start_immediately: reactivator_config.start_immediately.unwrap_or(source_defaults.reactivator.start_immediately),
            source_change_event_time_mode: time_mode,
            source_change_event_spacing_mode: spacing_mode,
            data_cache_path: service_settings.data_cache_path.clone(),
            event_output: service_settings.event_output,
            telemetry_output: service_settings.telemetry_output,
            log_output: service_settings.log_output,
        })
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

#[derive(Debug, Clone, Serialize)]
pub struct ChangeScriptPlayerConfig {
    pub player_settings: ChangeScriptPlayerSettings,
    pub script_files: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChangeScriptPlayerState {
    // Status of the ChangeScriptPlayer.
    pub status: ChangeScriptPlayerStatus,

    // Error message if the ChangeScriptPlayer is in an Error state.
    pub error_message: Option<String>,

    // The time mode setting for the ChangeScriptPlayer.
    // This setting determines how the player will adjust the time values in the ChangeScriptRecords.
    pub time_mode: ChangeScriptPlayerTimeMode,

    // The spacing mode setting for the ChangeScriptPlayer.
    // This setting determines how the player will space out the processing of the ChangeScriptRecords.
    pub spacing_mode: ChangeScriptPlayerSpacingMode,

    // The 'starting' time for the player based on config and what is being replayed.
    pub start_replay_time: u64,

    // The 'current' time as it is in the player based on config and what is being replayed.
    // A None value means the current time is not set, and the player will always just use the system time.
    pub current_replay_time: u64,
    
    // Variable to manage the progress of skipping.
    // Holds the number of ChangeScriptRecords to skip before returning to a Paused state.
    pub skips_remaining: u64,

    // Variable to manage the progress of stepping.
    // Holds the number of ChangeScriptRecords to step through before returning to a Paused state.
    pub steps_remaining: u64,

    // The ChangeScriptRecord that is currently being delayed prior to processing.
    pub delayed_record: Option<ScheduledChangeScriptRecord>,    

    // The next ChangeScriptRecord to be processed.
    pub next_record: Option<SequencedChangeScriptRecord>,

    pub ignore_scripted_pause_commands: bool,
}

impl Default for ChangeScriptPlayerState {
    fn default() -> Self {
        Self {
            status: ChangeScriptPlayerStatus::Paused,
            error_message: None,
            start_replay_time: 0,
            current_replay_time: 0,
            delayed_record: None,
            next_record: None,
            skips_remaining: 0,
            steps_remaining: 0,
            time_mode: ChangeScriptPlayerTimeMode::Live,
            spacing_mode: ChangeScriptPlayerSpacingMode::None,
            ignore_scripted_pause_commands: false,
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
    // Command to process the delayed Change Script record. The param is the sequence nummber of the record to process.
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
    config: ChangeScriptPlayerConfig,
    player_tx_channel: Sender<ChangeScriptPlayerMessage>,
    _delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>,
    _player_thread_handle: Arc<Mutex<JoinHandle<()>>>,
    _delayer_thread_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl ChangeScriptPlayer {
    pub async fn new(config: ChangeScriptPlayerConfig) -> Self {
        let (player_tx_channel, player_rx_channel) = tokio::sync::mpsc::channel(100);
        let (delayer_tx_channel, delayer_rx_channel) = tokio::sync::mpsc::channel(100);

        let player_thread_handle = tokio::spawn(player_thread(player_rx_channel, delayer_tx_channel.clone(), config.clone()));
        let delayer_thread_handle = tokio::spawn(delayer_thread(delayer_rx_channel, player_tx_channel.clone()));

        Self {
            config,
            player_tx_channel,
            _delayer_tx_channel: delayer_tx_channel,
            _player_thread_handle: Arc::new(Mutex::new(player_thread_handle)),
            _delayer_thread_handle: Arc::new(Mutex::new(delayer_thread_handle)),
        }
    }

    pub fn get_id(&self) -> String {
        self.config.player_settings.test_run_id.clone()
    }

    pub fn get_config(&self) -> ChangeScriptPlayerConfig {
        self.config.clone()
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
pub async fn player_thread(mut player_rx_channel: Receiver<ChangeScriptPlayerMessage>, delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>, player_config: ChangeScriptPlayerConfig) {

    log::info!("ChangeScriptPlayer thread started...");

    // Initialize the ChangeScriptPlayer infrastructure.
    // Create a ChangeScriptReader to read the ChangeScript files.
    // Create a ChangeScriptPlayerState to hold the state of the ChangeScriptPlayer. The ChangeScriptPlayer always starts in a paused state.
    // Create a SourceChangeEventDispatcher to send SourceChangeEvents.
    let (mut player_state, mut test_script_reader, mut dispatcher) = match ChangeScriptReader::new(player_config.script_files.clone()) {
        Ok(mut reader) => {
            let header = reader.get_header();
            log::info!("Loaded ChangeScript. {:?}", header);

            let mut player_state = ChangeScriptPlayerState::default();
            player_state.ignore_scripted_pause_commands = player_config.player_settings.ignore_scripted_pause_commands;
            player_state.time_mode = ChangeScriptPlayerTimeMode::from(player_config.player_settings.source_change_event_time_mode.clone());
            player_state.spacing_mode = ChangeScriptPlayerSpacingMode::from(player_config.player_settings.source_change_event_spacing_mode.clone());
        
            // Set the start_replay_time based on the time mode and the script start time from the header.
            player_state.start_replay_time = match player_state.time_mode {
                ChangeScriptPlayerTimeMode::Live => {
                    // Use the current system time.
                    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
                },
                ChangeScriptPlayerTimeMode::Recorded => {
                    // Use the start time as provided in the Header.
                    header.start_time.timestamp_nanos_opt().unwrap() as u64
                },
                ChangeScriptPlayerTimeMode::Rebased(nanos) => {
                    // Use the rebased time as provided in the AppConfig.
                    nanos
                },
            };
            player_state.current_replay_time = player_state.start_replay_time;

            // Create a dispatcher to send SourceChangeEvents.
            let create_dispatcher_result = match player_config.player_settings.event_output {
                OutputType::Console => ConsoleSourceChangeEventDispatcher::new().map_err(|e| e),
                OutputType::File => JsonlFileSourceChangeDispatcher::new(&player_config).map_err(|e| e),
                OutputType::Publish => DaprSourceChangeEventDispatcher::new(&player_config).map_err(|e| e),
                OutputType::None => Ok(NullSourceChangeEventDispatcher::new()),
            };
        
            let dispatcher = match create_dispatcher_result {
                Ok(d) => d,
                Err(e) => {
                    transition_to_error_state(format!("Error creating dispatcher: {:?}", e).as_str(), &mut player_state);
                    NullSourceChangeEventDispatcher::new()
                }
            };
    
            // Read the first ChangeScriptRecord.
            read_next_sequenced_test_script_record(&mut player_state, &mut reader);

            (player_state, Some(reader), dispatcher)    
        },
        Err(e) => {
            let mut player_state = ChangeScriptPlayerState::default();
            transition_to_error_state(format!("Error creating ChangeScriptReader: {:?}", e).as_str(), &mut player_state);
            (player_state, None, NullSourceChangeEventDispatcher::new())
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
                    process_delayed_test_script_record(seq, &mut player_state, &mut dispatcher);
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
                    process_next_test_script_record(&mut player_state, &mut dispatcher, delayer_tx_channel.clone()).await;
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
    
    match test_script_reader.get_next_record() {
        Ok(seq_record) => {
            // Assign the next record to the player state.
            player_state.next_record = Some(seq_record);
        },
        Err(e) => {
            transition_to_error_state(format!("Error reading ChangeScriptRecord: {:?}", e).as_str(), player_state);
        }
    }
}

async fn process_next_test_script_record(mut player_state: &mut ChangeScriptPlayerState, dispatcher: &mut Box<dyn SourceChangeEventDispatcher>, delayer_tx_channel: Sender<DelayChangeScriptRecordMessage>) {

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
    log_sequeced_test_script_record("Next ChangeScriptRecord Pre-TimeShift", &seq_record);

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
        ChangeScriptPlayerSpacingMode::None => {
            // Process the record immediately.
            0
        },
        ChangeScriptPlayerSpacingMode::Fixed(nanos) => {
            nanos
        },
        ChangeScriptPlayerSpacingMode::Recorded => {
            // Delay the record based on the difference between the record's replay time and the current replay time.
            // Ensure the delay is not negative.
            std::cmp::max(0, next_record.replay_time_ns - player_state.current_replay_time) as u64
        },
    };

    // Take action based on size of the delay.
    // It doesnt make sense to delay for trivially small amounts of time, nor does it make sense to send 
    // a message to the delayer thread for relatively small delays. 
    // TODO: This figures might need to be adjusted, or made configurable.
    if delay < 1_000 { 
        // Process the record immediately.
        log_scheduled_test_script_record(
            format!("Minimal delay of {}ns; processing immediately", delay).as_str(), &next_record);
        resolve_test_script_record_effect(next_record, player_state, dispatcher);
    } else if delay < 10_000_000 {
        // Sleep inproc for efficiency, then process the record.
        log_scheduled_test_script_record(
            format!("Short delay of {}ns; processing after in-proc sleep", delay).as_str(), &next_record);
        std::thread::sleep(Duration::from_nanos(delay));
        resolve_test_script_record_effect(next_record, player_state, dispatcher);
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

fn process_delayed_test_script_record(delayed_record_seq: u64, player_state: &mut ChangeScriptPlayerState, dispatcher: &mut Box<dyn SourceChangeEventDispatcher>) {

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
        if player_state.time_mode == ChangeScriptPlayerTimeMode::Live {
            // Live Time Mode means we just use the current time as the record's Replay Time regardless of offset.
            let replay_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

            delayed_record.replay_time_ns = replay_time_ns;
            player_state.current_replay_time = replay_time_ns;
            log_test_script_player_state(
                format!("Shifted Player Current Replay Time to delayed record scheduled time: {}", replay_time_ns).as_str(),
                &player_state);
        };
    
        // Process the delayed record.
        resolve_test_script_record_effect(delayed_record, player_state, dispatcher);
    
        // Clear the delayed record.
        player_state.delayed_record = None;
    } else {
        log::error!("Should not be processing ProcessDelayedRecord when player_state.delayed_record is None.");
    };
}

fn resolve_test_script_record_effect(record: ScheduledChangeScriptRecord, player_state: &mut ChangeScriptPlayerState, dispatcher: &mut Box<dyn SourceChangeEventDispatcher>) {

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
                    let _ = dispatcher.dispatch_source_change_event(&change_record.source_change_event);
                },
                ChangeScriptPlayerStatus::Stepping => {
                    // Dispatch the SourceChangeEvent.
                    if player_state.steps_remaining > 0 {
                        let _ = dispatcher.dispatch_source_change_event(&change_record.source_change_event);

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

fn time_shift_test_script_record(player_state: &ChangeScriptPlayerState, seq_record: SequencedChangeScriptRecord) -> ScheduledChangeScriptRecord {

    let replay_time_ns = match player_state.time_mode {
        ChangeScriptPlayerTimeMode::Live => {
            // Live Time Mode means we just use the current time as the record's Replay Time regardless of offset.
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
        },
        ChangeScriptPlayerTimeMode::Recorded | ChangeScriptPlayerTimeMode::Rebased(_) => {
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
fn log_sequeced_test_script_record(msg: &str, record: &SequencedChangeScriptRecord) {
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
