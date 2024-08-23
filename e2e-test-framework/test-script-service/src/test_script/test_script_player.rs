use std::{fmt, path::PathBuf, str::FromStr, sync::Arc, time::SystemTime};

use serde::Serialize;
use tokio::sync::{mpsc::{Receiver, Sender}, oneshot, Mutex};
use tokio::sync::mpsc::error::TryRecvError::{Empty, Disconnected};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::{
    config::{OutputType, ServiceSettings, SourceConfig, SourceConfigDefaults}, 
    source_change_dispatchers::{
        console_dispatcher::ConsoleSourceChangeEventDispatcher,
        dapr_dispatcher::DaprSourceChangeEventDispatcher,
        file_dispatcher::JsonlFileSourceChangeDispatcher,
        null_dispatcher::NullSourceChangeEventDispatcher,
    }
};
use crate::source_change_dispatchers::SourceChangeEventDispatcher;
use crate::test_script::test_script_reader::{TestScriptReader, TestScriptRecord, SequencedTestScriptRecord};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum TestScriptPlayerTimeMode {
    Live,
    Recorded,
    Rebased(u64),
}

impl FromStr for TestScriptPlayerTimeMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "live" => Ok(Self::Live),
            "recorded" => Ok(Self::Recorded),
            _ => {
                match chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(t) => Ok(Self::Rebased(t.timestamp_nanos_opt().unwrap() as u64)),
                    Err(e) => Err(format!("Error parsing TestScriptPlayerTimeMode - value:{}, error:{}", s, e))
                }
            }
        }
    }
}

impl fmt::Display for TestScriptPlayerTimeMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Recorded => write!(f, "recorded"),
            Self::Rebased(time) => write!(f, "{}", time),
        }
    }
}

impl Default for TestScriptPlayerTimeMode {
    fn default() -> Self {
        Self::Recorded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum TestScriptPlayerSpacingMode {
    None,
    Recorded,
    Fixed(u64),
}

// Implementation of FromStr for ReplayEventSpacingMode.
// For the Fixed variant, the spacing is specified as a string duration such as '5s' or '100n'.
// Supported units are seconds ('s'), milliseconds ('m'), microseconds ('u'), and nanoseconds ('n').
// If the string can't be parsed as a TimeDelta, an error is returned.
impl FromStr for TestScriptPlayerSpacingMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(Self::None),
            "recorded" => Ok(Self::Recorded),
            _ => {
                // Parse the string as a number, followed by a time unit character.
                let (num_str, unit_str) = s.split_at(s.len() - 1);
                let num = match num_str.parse::<u64>() {
                    Ok(num) => num,
                    Err(e) => return Err(format!("Error parsing TestScriptPlayerSpacingMode: {}", e))
                };
                match unit_str {
                    "s" => Ok(Self::Fixed(num * 1000000000)),
                    "m" => Ok(Self::Fixed(num * 1000000)),
                    "u" => Ok(Self::Fixed(num * 1000)),
                    "n" => Ok(Self::Fixed(num)),
                    _ => Err(format!("Invalid TestScriptPlayerSpacingMode: {}", s))
                }
            }
        }
    }
}

impl fmt::Display for TestScriptPlayerSpacingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Recorded => write!(f, "recorded"),
            Self::Fixed(d) => write!(f, "{}", d),
        }
    }
}

impl Default for TestScriptPlayerSpacingMode {
    fn default() -> Self {
        Self::Recorded
    }
}

// The TestScriptPlayerSettings struct holds the configuration settings that are used by the Test Script Player.
// It is created based on the SourceConfig either loaded from the Service config file or passed in to the Web API, and is 
// combined with the ServiceSettings and default values to create the final set of configuration.
// It is static and does not change during the execution of the Test Script Player, it is not used to track the active state of the Test Script Player.
#[derive(Debug, Serialize, Clone)]
pub struct TestScriptPlayerSettings {
    // The Test ID.
    pub test_id: String,

    // The Test Run ID.
    pub test_run_id: String,

    // The Test Storage Account where the Test Repo is located.
    pub test_storage_account: String,

    // The Test Storage Access Key where the Test Repo is located.
    #[serde(skip_serializing)]
    pub test_storage_access_key: String,

    // The Test Storage Container where the Test Repo is located.
    pub test_storage_container: String,

    // The Test Storage Path where the Test Repo is located.
    pub test_storage_path: String,

    // The Source ID for the Test Script Player.
    pub source_id: String,

    // The address of the change queue.
    pub change_queue_address: String,

    // The port of the change queue.
    pub change_queue_port: u16,

    // The PubSub topic for the change queue.
    pub change_queue_topic: String,
    
    // Flag to indicate if the Service should start the Test Script Player immediately after initialization.
    pub start_immediately: bool,

    // Whether the TestScriptPlayer should ignore scripted pause commands.
    pub ignore_scripted_pause_commands: bool,

    // SourceChangeEvent Time Mode for the Test Script Player.
    pub source_change_event_time_mode: TestScriptPlayerTimeMode,

    // SourceChangeEvent Spacing Mode for the Test Script Player.
    pub source_change_event_spacing_mode: TestScriptPlayerSpacingMode,

    // The path where data used and generated in the Test Script Player gets stored.
    pub data_cache_path: String,

    // The OutputType for Source Change Events.
    pub event_output: OutputType,

    // The OutputType for Test Script Player Telemetry data.
    pub telemetry_output: OutputType,

    // The OutputType for Test Script Player Log data.
    pub log_output: OutputType,
}

impl TestScriptPlayerSettings {
    // Function to create a new TestScriptPlayerSettings by combining a SourceConfig and the Service Settings.
    // The TestScriptPlayerSettings control the configuration and operation of a TestRun.   
    pub fn try_from_source_config(source_config: &SourceConfig, source_defaults: &SourceConfigDefaults, service_settings: &ServiceSettings) -> Result<Self, String> {

        // If the SourceConfig doesnt contain a ReactivatorConfig, log and return an error.
        let reactivator_config = match &source_config.reactivator {
            Some(reactivator_config) => reactivator_config,
            None => {
                let err = format!("No ReactivatorConfig provided in SourceConfig: {:?}", source_config);
                log::error!("{}", err.as_str());
                return Err(err);    
            }
        };

        // If neither the SourceConfig nor the SourceDefaults contain a test_id, return an error.
        let test_id = match &source_config.test_id {
            Some(test_id) => test_id.clone(),
            None => {
                match &source_defaults.test_id {
                    Some(test_id) => test_id.clone(),
                    None => {
                        let err = format!("No test_id provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
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
                        let err = format!("No test_storage_account provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
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
                        let err = format!("No test_storage_access_key provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
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
                        let err = format!("No test_storage_container provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
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
                        let err = format!("No test_storage_path provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
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
                        let err = format!("No source_id provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
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
                        let err = format!("No change_queue_address provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
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
                        let err = format!("No change_queue_port provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
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
                        let err = format!("No change_queue_topic provided and no default value found.");
                        log::error!("{}", err.as_str());
                        return Err(err);    
                    }
                }
            }
        };

        let spacing_mode = match &reactivator_config.spacing_mode {
            Some(mode) => TestScriptPlayerSpacingMode::from_str(&mode).unwrap(),
            None => TestScriptPlayerSpacingMode::from_str(&source_defaults.reactivator.spacing_mode).unwrap()
        };

        let time_mode = match &reactivator_config.time_mode {
            Some(mode) => TestScriptPlayerTimeMode::from_str(&mode).unwrap(),
            None => TestScriptPlayerTimeMode::from_str(&source_defaults.reactivator.time_mode).unwrap()
        };

        Ok(TestScriptPlayerSettings {
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

// Enum of TestScriptPlayer status.
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
#[derive(Clone, Debug, PartialEq)]
pub enum TestScriptPlayerStatus {
    Running,
    Stepping,
    Skipping,
    Paused,
    Stopped,
    Finished,
    Error
}

impl Serialize for TestScriptPlayerStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        match self {
            TestScriptPlayerStatus::Running => serializer.serialize_str("Running"),
            TestScriptPlayerStatus::Stepping => serializer.serialize_str("Stepping"),
            TestScriptPlayerStatus::Skipping => serializer.serialize_str("Skipping"),
            TestScriptPlayerStatus::Paused => serializer.serialize_str("Paused"),
            TestScriptPlayerStatus::Stopped => serializer.serialize_str("Stopped"),
            TestScriptPlayerStatus::Finished => serializer.serialize_str("Finished"),
            TestScriptPlayerStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TestScriptPlayerConfig {
    pub player_settings: TestScriptPlayerSettings,
    pub script_files: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TestScriptPlayerState {
    // Status of the TestScriptPlayer.
    pub status: TestScriptPlayerStatus,

    // Error message if the TestScriptPlayer is in an Error state.
    pub error_message: Option<String>,

    // The time mode setting for the TestScriptPlayer.
    // This setting determines how the player will adjust the time values in the TestScriptRecords.
    pub time_mode: TestScriptPlayerTimeMode,

    // The spacing mode setting for the TestScriptPlayer.
    // This setting determines how the player will space out the processing of the TestScriptRecords.
    pub spacing_mode: TestScriptPlayerSpacingMode,

    // The 'starting' time for the player based on config and what is being replayed.
    pub start_replay_time: u64,

    // The 'current' time as it is in the player based on config and what is being replayed.
    // A None value means the current time is not set, and the player will always just use the system time.
    pub current_replay_time: u64,
    
    // Variable to manage the progress of skipping.
    // Holds the number of TestScriptRecords to skip before returning to a Paused state.
    pub skips_remaining: u64,

    // Variable to manage the progress of stepping.
    // Holds the number of TestScriptRecords to step through before returning to a Paused state.
    pub steps_remaining: u64,

    // The TestScriptRecord that is currently being delayed prior to processing.
    pub delayed_record: Option<ScheduledTestScriptRecord>,    

    // The next TestScriptRecord to be processed.
    pub next_record: Option<SequencedTestScriptRecord>,

    pub ignore_scripted_pause_commands: bool,
}

impl Default for TestScriptPlayerState {
    fn default() -> Self {
        Self {
            status: TestScriptPlayerStatus::Paused,
            error_message: None,
            start_replay_time: 0,
            current_replay_time: 0,
            delayed_record: None,
            next_record: None,
            skips_remaining: 0,
            steps_remaining: 0,
            time_mode: TestScriptPlayerTimeMode::Live,
            spacing_mode: TestScriptPlayerSpacingMode::None,
            ignore_scripted_pause_commands: false,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScheduledTestScriptRecord {
    pub replay_time_ns: u64,
    pub seq_record: SequencedTestScriptRecord,
}

impl ScheduledTestScriptRecord {
    pub fn new(record: SequencedTestScriptRecord, replay_time_ns: u64, ) -> Self {
        Self {
            replay_time_ns,
            seq_record: record,
        }
    }
}

// Enum of TestScriptPlayer commands sent from Web API handler functions.
#[derive(Debug)]
pub enum TestScriptPlayerCommand {
    // Command to start the TestScriptPlayer.
    Start,
    // Command to process the delayed test script record. The param is the sequence nummber of the record to process.
    // It is used as a guard to ensure the timer is only processed if the sequence number matches the expected sequence number.
    ProcessDelayedRecord(u64),
    // Command to step the TestScriptPlayer forward a specified number of TestScriptRecords.
    Step(u64),
    // Command to skip the TestScriptPlayer forward a specified number of TestScriptRecords.
    Skip(u64),
    // Command to pause the TestScriptPlayer.
    Pause,
    // Command to stop the TestScriptPlayer.
    Stop,
    // Command to get the current state of the TestScriptPlayer.
    GetState,
}

// Struct for messages sent to the TestScriptPlayer from the functions in the Web API.
#[derive(Debug)]
pub struct TestScriptPlayerMessage {
    // Command sent to the TestScriptPlayer.
    pub command: TestScriptPlayerCommand,
    // One-shot channel for TestScriptPlayer to send a response back to the caller.
    pub response_tx: Option<oneshot::Sender<TestScriptPlayerMessageResponse>>,
}

// A struct for the Response sent back from the TestScriptPlayer to the calling Web API handler.
#[derive(Debug)]
pub struct TestScriptPlayerMessageResponse {
    // Result of the command.
    pub result: Result<(), String>,
    // State of the TestScriptPlayer after the command.
    pub state: TestScriptPlayerState,
}

#[derive(Clone, Debug)]
pub struct DelayTestScriptRecordMessage {
    pub delay_ns: u64,
    pub delay_sequence: u64,
}

#[derive(Clone, Debug)]
pub struct TestScriptPlayer {
    // Channel used to send messages to the TestScriptPlayer thread.
    config: TestScriptPlayerConfig,
    player_tx_channel: Sender<TestScriptPlayerMessage>,
    _delayer_tx_channel: Sender<DelayTestScriptRecordMessage>,
    _player_thread_handle: Arc<Mutex<JoinHandle<()>>>,
    _delayer_thread_handle: Arc<Mutex<JoinHandle<()>>>,
}

impl TestScriptPlayer {
    pub async fn new(config: TestScriptPlayerConfig) -> Self {
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

    pub fn get_config(&self) -> TestScriptPlayerConfig {
        self.config.clone()
    }

    pub async fn get_state(&self) -> Result<TestScriptPlayerMessageResponse, String> {
        self.send_command(TestScriptPlayerCommand::GetState).await
    }

    pub async fn start(&self) -> Result<TestScriptPlayerMessageResponse, String> {
        self.send_command(TestScriptPlayerCommand::Start).await
    }

    pub async fn step(&self, steps: u64) -> Result<TestScriptPlayerMessageResponse, String>  {
        self.send_command(TestScriptPlayerCommand::Step(steps)).await
    }

    pub async fn skip(&self, skips: u64) -> Result<TestScriptPlayerMessageResponse, String>  {
        self.send_command(TestScriptPlayerCommand::Skip(skips)).await
    }

    pub async fn pause(&self) -> Result<TestScriptPlayerMessageResponse, String>  {
        self.send_command(TestScriptPlayerCommand::Pause).await
    }

    pub async fn stop(&self) -> Result<TestScriptPlayerMessageResponse, String>  {
        self.send_command(TestScriptPlayerCommand::Stop).await
    }

    async fn send_command(&self, command: TestScriptPlayerCommand) -> Result<TestScriptPlayerMessageResponse, String> {
        let (response_tx, response_rx) = oneshot::channel();

        let r = self.player_tx_channel.send(TestScriptPlayerMessage {
            command,
            response_tx: Some(response_tx),
        }).await;

        match r {
            Ok(_) => {
                Ok(response_rx.await.unwrap())
            },
            Err(e) => Err(format!("Error sending command to TestScriptPlayer: {:?}", e)),
        }
    }
}

// Function that defines the operation of the TestScriptPlayer thread.
// The TestScriptPlayer thread processes TestScriptPlayerCommands sent to it from the Web API handler functions.
// The Web API function communicate via a channel and provide oneshot channels for the TestScriptPlayer to send responses back.
pub async fn player_thread(mut player_rx_channel: Receiver<TestScriptPlayerMessage>, delayer_tx_channel: Sender<DelayTestScriptRecordMessage>, player_config: TestScriptPlayerConfig) {

    log::info!("TestScriptPlayer thread started...");

    // Initialize the TestScriptPlayer infrastructure.
    // Create a TestScriptReader to read the TestScript files.
    // Create a TestScriptPlayerState to hold the state of the TestScriptPlayer. The TestScriptPlayer always starts in a paused state.
    // Create a SourceChangeEventDispatcher to send SourceChangeEvents.
    let (mut player_state, mut test_script_reader, mut dispatcher) = match TestScriptReader::new(player_config.script_files.clone()) {
        Ok(mut reader) => {
            let header = reader.get_header();
            log::info!("Loaded TestScript. {:?}", header);

            let mut player_state = TestScriptPlayerState::default();
            player_state.ignore_scripted_pause_commands = player_config.player_settings.ignore_scripted_pause_commands;
            player_state.time_mode = TestScriptPlayerTimeMode::from(player_config.player_settings.source_change_event_time_mode.clone());
            player_state.spacing_mode = TestScriptPlayerSpacingMode::from(player_config.player_settings.source_change_event_spacing_mode.clone());
        
            // Set the start_replay_time based on the time mode and the script start time from the header.
            player_state.start_replay_time = match player_state.time_mode {
                TestScriptPlayerTimeMode::Live => {
                    // Use the current system time.
                    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
                },
                TestScriptPlayerTimeMode::Recorded => {
                    // Use the start time as provided in the Header.
                    header.start_time.timestamp_nanos_opt().unwrap() as u64
                },
                TestScriptPlayerTimeMode::Rebased(nanos) => {
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
    
            // Read the first TestScriptRecord.
            read_next_sequenced_test_script_record(&mut player_state, &mut reader);

            (player_state, Some(reader), dispatcher)    
        },
        Err(e) => {
            let mut player_state = TestScriptPlayerState::default();
            transition_to_error_state(format!("Error creating TestScriptReader: {:?}", e).as_str(), &mut player_state);
            (player_state, None, NullSourceChangeEventDispatcher::new())
        }
    };

    log::trace!("Starting TestScriptPlayer loop...");

    // Loop to process messages sent to the TestScriptPlayer and read data from test script.
    // Always process all messages in the command channel and act on them first.
    // If there are no messages and the TestScriptPlayer is running, stepping, or skipping, continue processing the test script.
    loop {
        log_test_script_player_state("Top of TestScriptPlayer loop", &player_state);

        // If the TestScriptPlayer needs a command to continue, block until a command is received.
        // Otherwise, try to receive a command message without blocking and then process the next test script record.
        let block_on_message = 
            (player_state.delayed_record.is_some() && (
                player_state.status == TestScriptPlayerStatus::Running 
                || player_state.status == TestScriptPlayerStatus::Stepping 
                || player_state.status == TestScriptPlayerStatus::Skipping
            ))
            || player_state.status == TestScriptPlayerStatus::Paused 
            || player_state.status == TestScriptPlayerStatus::Stopped 
            || player_state.status == TestScriptPlayerStatus::Finished 
            || player_state.status == TestScriptPlayerStatus::Error;
        
        let message: Option<TestScriptPlayerMessage> = if block_on_message {
            log::trace!("TestScriptPlayer Thread Blocked; waiting for a command message...");

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
            log::trace!("TestScriptPlayer Thread Unblocked; TRYing for a command message before processing next script record");

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

                if let TestScriptPlayerCommand::ProcessDelayedRecord(seq) = message.command {
                    process_delayed_test_script_record(seq, &mut player_state, &mut dispatcher);
                } else {
                    let transition_response = match player_state.status {
                        TestScriptPlayerStatus::Running => transition_from_running_state(&message.command, &mut player_state),
                        TestScriptPlayerStatus::Stepping => transition_from_stepping_state(&message.command, &mut player_state),
                        TestScriptPlayerStatus::Skipping => transition_from_skipping_state(&message.command, &mut player_state),
                        TestScriptPlayerStatus::Paused => transition_from_paused_state(&message.command, &mut player_state),
                        TestScriptPlayerStatus::Stopped => transition_from_stopped_state(&message.command, &mut player_state),
                        TestScriptPlayerStatus::Finished => transition_from_finished_state(&message.command, &mut player_state),
                        TestScriptPlayerStatus::Error => transition_from_error_state(&message.command, &mut player_state),
                    };
    
                    if message.response_tx.is_some() {
                        let message_response = TestScriptPlayerMessageResponse {
                            result: transition_response,
                            state: player_state.clone(),
                        };
        
                        let r = message.response_tx.unwrap().send(message_response);
                        if let Err(e) = r {
                            log::error!("Error in TestScriptPlayer sending message response back to caller: {:?}", e);
                        }
                    }
                }

                log_test_script_player_state(format!("Post {:?} command", message.command).as_str(), &player_state);
            },
            None => {
                // Only process the next TestScriptRecord if the player is not in an error state.
                if let TestScriptPlayerStatus::Error = player_state.status {
                    log_test_script_player_state("Trying to process Next TestScriptRecord, but Player in error state", &player_state);
                } else {
                    process_next_test_script_record(&mut player_state, &mut dispatcher, delayer_tx_channel.clone()).await;
                    read_next_sequenced_test_script_record(&mut player_state, &mut test_script_reader.as_mut().unwrap());
                }
            }
        }
    }
}

fn read_next_sequenced_test_script_record(player_state: &mut TestScriptPlayerState, test_script_reader: &mut TestScriptReader) {

    // Do nothing if the player is already in an error state.
    if let TestScriptPlayerStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring read_next_sequenced_test_script_record call due to error state", &player_state);
        return;
    }
    
    match test_script_reader.get_next_record() {
        Ok(seq_record) => {
            // Assign the next record to the player state.
            player_state.next_record = Some(seq_record);
        },
        Err(e) => {
            transition_to_error_state(format!("Error reading TestScriptRecord: {:?}", e).as_str(), player_state);
        }
    }
}

async fn process_next_test_script_record(mut player_state: &mut TestScriptPlayerState, dispatcher: &mut Box<dyn SourceChangeEventDispatcher>, delayer_tx_channel: Sender<DelayTestScriptRecordMessage>) {

    // Do nothing if the player is already in an error state.
    if let TestScriptPlayerStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring process_next_test_script_record call due to error state", &player_state);
        return;
    }

    // next_record should never be None.
    if player_state.next_record.is_none() {
        transition_to_error_state(format!("TestScriptReader should never return None for next_record").as_str(), player_state);
        return;
    }

    let seq_record = player_state.next_record.as_ref().unwrap().clone();
    log_sequeced_test_script_record("Next TestScriptRecord Pre-TimeShift", &seq_record);

    // Check if the TestScriptPlayer has finished processing all records.
    if let TestScriptRecord::Finish(_) = seq_record.record {
        transition_to_finished_state(&mut player_state);
        return;
    }

    // Time shift the TestScriptRecord based on the time mode settings.
    let next_record = time_shift_test_script_record(player_state, seq_record);
    log_scheduled_test_script_record("Next TestScriptRecord Post-TimeShift", &next_record);

    // Processing of TestScriptRecord depends on the spacing mode settings.
    let delay = match player_state.spacing_mode {
        TestScriptPlayerSpacingMode::None => {
            // Process the record immediately.
            0
        },
        TestScriptPlayerSpacingMode::Fixed(nanos) => {
            nanos
        },
        TestScriptPlayerSpacingMode::Recorded => {
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
        let delay_msg = DelayTestScriptRecordMessage {
            delay_ns: delay,
            delay_sequence: next_record.seq_record.seq,
        };

        // Setting the delayed record in the player state will cause the player to not process any more records until
        // the delayed record is processed.
        player_state.delayed_record = Some(next_record);

        // Send a message to the ChangeDispatchTimer to process the delayed record.
        let delay_send_res = delayer_tx_channel.send(delay_msg).await;
        if let Err(e) = delay_send_res {
            transition_to_error_state(format!("Error sending DelayTestScriptRecordMessage: {:?}", e).as_str(), player_state);
        }
    };
}

fn process_delayed_test_script_record(delayed_record_seq: u64, player_state: &mut TestScriptPlayerState, dispatcher: &mut Box<dyn SourceChangeEventDispatcher>) {

    // Do nothing if the player is already in an error state.
    if let TestScriptPlayerStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring process_delayed_test_script_record call due to error state", &player_state);
        return;
    }

    if let Some(mut delayed_record) = player_state.delayed_record.as_mut().cloned() {

        // Ensure the ProcessDelayedRecord command is only processed if the sequence number matches the expected sequence number.
        if delayed_record_seq != delayed_record.seq_record.seq {
            log_scheduled_test_script_record(
                format!("Received command to process incorrect TestScript Record with seq:{}", delayed_record_seq).as_str()
                , &delayed_record);
            return;
        }        

        log_scheduled_test_script_record("Processing Delayed TestScriptRecord", &delayed_record);

        // Adjust the time in the TestScriptRecord if the player is in Live Time Mode.
        // This will make sure the times are as accurate as possible.
        if player_state.time_mode == TestScriptPlayerTimeMode::Live {
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

fn resolve_test_script_record_effect(record: ScheduledTestScriptRecord, player_state: &mut TestScriptPlayerState, dispatcher: &mut Box<dyn SourceChangeEventDispatcher>) {

    // Do nothing if the player is already in an error state.
    if let TestScriptPlayerStatus::Error = player_state.status {
        log_test_script_player_state("Ignoring resolve_test_script_record_effect call due to error state", &player_state);
        return;
    }
        
    match &record.seq_record.record {
        TestScriptRecord::SourceChange(change_record) => {
            match player_state.status {
                TestScriptPlayerStatus::Running => {
                    // Dispatch the SourceChangeEvent.
                    let _ = dispatcher.dispatch_source_change_event(&change_record.source_change_event);
                },
                TestScriptPlayerStatus::Stepping => {
                    // Dispatch the SourceChangeEvent.
                    if player_state.steps_remaining > 0 {
                        let _ = dispatcher.dispatch_source_change_event(&change_record.source_change_event);

                        player_state.steps_remaining -= 1;
                        if player_state.steps_remaining == 0 {
                            let _ = transition_from_stepping_state(&TestScriptPlayerCommand::Pause, player_state);
                        }
                    }
                },
                TestScriptPlayerStatus::Skipping => {
                    // Skip the SourceChangeEvent.
                    if player_state.skips_remaining > 0 {
                        log::debug!("Skipping TestScriptRecord: {:?}", change_record);

                        player_state.skips_remaining -= 1;
                        if player_state.skips_remaining == 0 {
                            let _ = transition_from_skipping_state(&TestScriptPlayerCommand::Pause, player_state);
                        }
                    }
                },
                _ => {
                    log::warn!("Should not be processing TestScriptRecord when in state {:?}", player_state.status);
                },
            }
        },
        TestScriptRecord::PauseCommand(_) => {
            // Process the PauseCommand only if the Player is not configured to ignore them.
            if player_state.ignore_scripted_pause_commands {
                log::info!("Ignoring Test Script Pause Command: {:?}", record);
            } else {
                let _ = match player_state.status {
                    TestScriptPlayerStatus::Running => transition_from_running_state(&TestScriptPlayerCommand::Pause, player_state),
                    TestScriptPlayerStatus::Stepping => transition_from_stepping_state(&TestScriptPlayerCommand::Pause, player_state),
                    TestScriptPlayerStatus::Skipping => transition_from_skipping_state(&TestScriptPlayerCommand::Pause, player_state),
                    TestScriptPlayerStatus::Paused => transition_from_paused_state(&TestScriptPlayerCommand::Pause, player_state),
                    TestScriptPlayerStatus::Stopped => transition_from_stopped_state(&TestScriptPlayerCommand::Pause, player_state),
                    TestScriptPlayerStatus::Finished => transition_from_finished_state(&TestScriptPlayerCommand::Pause, player_state),
                    TestScriptPlayerStatus::Error => transition_from_error_state(&TestScriptPlayerCommand::Pause, player_state),
                };
            }
        },
        TestScriptRecord::Label(label_record) => {
            log::info!("Reached Test Script Label: {:?}", label_record);
        },
        TestScriptRecord::Finish(_) => {
            transition_to_finished_state(player_state);
        },
        TestScriptRecord::Header(header_record) => {
            // Only the first record should be a Header record, and this is handled in the TestScriptReader.
            log::warn!("Ignoring unexpected Test Script Header: {:?}", header_record);
        },
        TestScriptRecord::Comment(comment_record) => {
            // The TestScriptReader should not return Comment records.
            log::warn!("Ignoring unexpected Test Script Comment: {:?}", comment_record);
        },
    };
}

fn time_shift_test_script_record(player_state: &TestScriptPlayerState, seq_record: SequencedTestScriptRecord) -> ScheduledTestScriptRecord {

    let replay_time_ns = match player_state.time_mode {
        TestScriptPlayerTimeMode::Live => {
            // Live Time Mode means we just use the current time as the record's Replay Time regardless of offset.
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
        },
        TestScriptPlayerTimeMode::Recorded | TestScriptPlayerTimeMode::Rebased(_) => {
            // Recorded or Rebased Time Mode means we have to adjust the record's Replay Time based on the offset from
            // the start time. The player_state.start_replay_time has the base time regardless of the time mode.
            player_state.start_replay_time + seq_record.offset_ns
        },
    };

    // TODO: Also need to adjust the time values in the data based on the time mode settings.

    ScheduledTestScriptRecord::new(seq_record, replay_time_ns)
}

fn transition_from_paused_state(command: &TestScriptPlayerCommand, player_state: &mut TestScriptPlayerState) -> Result<(), String> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        TestScriptPlayerCommand::Start => {
            player_state.status = TestScriptPlayerStatus::Running;
            Ok(())
        },
        TestScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // The TestScriptPlayer is Paused, so we ignore the DispatchChangeEvent command.
            Ok(())
        },
        TestScriptPlayerCommand::Step(steps) => {
            player_state.status = TestScriptPlayerStatus::Stepping;
            player_state.steps_remaining = *steps;
            Ok(())
        },
        TestScriptPlayerCommand::Skip(skips) => {
            player_state.status = TestScriptPlayerStatus::Skipping;
            player_state.skips_remaining = *skips;
            Ok(())
        },
        TestScriptPlayerCommand::Pause => {
            Err("TestScriptPlayer is already Paused.".to_string())
        },
        TestScriptPlayerCommand::Stop => {
            player_state.status = TestScriptPlayerStatus::Stopped;
            Ok(())
        },
        TestScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_running_state(command: &TestScriptPlayerCommand, player_state: &mut TestScriptPlayerState) -> Result<(), String> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        TestScriptPlayerCommand::Start => {
            Err("TestScriptPlayer is already Running.".to_string())
        },
        TestScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while TestScriptPlayer is Running.");
            Ok(())
        },
        TestScriptPlayerCommand::Step(_) => {
            Err("TestScriptPlayer is currently Running. Pause before trying to Step.".to_string())
        },
        TestScriptPlayerCommand::Skip(_) => {
            Err("TestScriptPlayer is currently Running. Pause before trying to Skip.".to_string())
        },
        TestScriptPlayerCommand::Pause => {
            player_state.status = TestScriptPlayerStatus::Paused;
            Ok(())
        },
        TestScriptPlayerCommand::Stop => {
            player_state.status = TestScriptPlayerStatus::Stopped;
            Ok(())
        },
        TestScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stepping_state(command: &TestScriptPlayerCommand, player_state: &mut TestScriptPlayerState) -> Result<(), String> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        TestScriptPlayerCommand::Start => {
            Err(format!("TestScriptPlayer is currently Stepping ({} steps remaining). Pause before trying to Start.", player_state.steps_remaining))
        },
        TestScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while TestScriptPlayer is Stepping.");
            Ok(())
        },
        TestScriptPlayerCommand::Step(_) => {
            Err(format!("TestScriptPlayer is already Stepping ({} steps remaining).", player_state.steps_remaining))
        },
        TestScriptPlayerCommand::Skip(_) => {
            Err(format!("TestScriptPlayer is currently Stepping ({} steps remaining). Pause before trying to Skip.", player_state.steps_remaining))
        },
        TestScriptPlayerCommand::Pause => {
            player_state.status = TestScriptPlayerStatus::Paused;
            player_state.steps_remaining = 0;
            Ok(())
        },
        TestScriptPlayerCommand::Stop => {
            player_state.status = TestScriptPlayerStatus::Stopped;
            player_state.steps_remaining = 0;
            Ok(())
        },
        TestScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_skipping_state(command: &TestScriptPlayerCommand, player_state: &mut TestScriptPlayerState) -> Result<(), String> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    match command {
        TestScriptPlayerCommand::Start => {
            Err(format!("TestScriptPlayer is currently Skipping ({} skips remaining). Pause before trying to Start.", player_state.skips_remaining))
        },
        TestScriptPlayerCommand::ProcessDelayedRecord(_) => {
            // Should never get here.
            log::warn!("Ignoring DispatchChangeEvent command while TestScriptPlayer is Skipping.");
            Ok(())
        },
        TestScriptPlayerCommand::Step(_) => {
            Err(format!("TestScriptPlayer is currently Skipping ({} skips remaining). Pause before trying to Skip.", player_state.skips_remaining))
        },
        TestScriptPlayerCommand::Skip(_) => {
            Err(format!("TestScriptPlayer is already Skipping ({} skips remaining).", player_state.skips_remaining))
        },
        TestScriptPlayerCommand::Pause => {
            player_state.status = TestScriptPlayerStatus::Paused;
            player_state.skips_remaining = 0;
            Ok(())
        },
        TestScriptPlayerCommand::Stop => {
            player_state.status = TestScriptPlayerStatus::Stopped;
            player_state.skips_remaining = 0;
            Ok(())
        },
        TestScriptPlayerCommand::GetState => {
            Ok(())
        }
    }
}

fn transition_from_stopped_state(command: &TestScriptPlayerCommand, player_state: &mut TestScriptPlayerState) -> Result<(), String> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err("TestScriptPlayer is Stopped.".to_string())
}

fn transition_from_finished_state(command: &TestScriptPlayerCommand, player_state: &mut TestScriptPlayerState) -> Result<(), String> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err("TestScriptPlayer is Finished.".to_string())
}

fn transition_from_error_state(command: &TestScriptPlayerCommand, player_state: &mut TestScriptPlayerState) -> Result<(), String> {
    log::debug!("Transitioning from {:?} state via command: {:?}", player_state.status, command);

    Err(format!("TestScriptPlayer is in an Error state - {:?}.", player_state.status))
}

fn transition_to_finished_state(player_state: &mut TestScriptPlayerState) {
    player_state.status = TestScriptPlayerStatus::Finished;
    player_state.skips_remaining = 0;
    player_state.steps_remaining = 0;
}

fn transition_to_error_state(error_message: &str, player_state: &mut TestScriptPlayerState) {
    log::error!("{}", error_message);
    player_state.status = TestScriptPlayerStatus::Error;
    player_state.error_message = Some(error_message.to_string());
}

pub async fn delayer_thread(mut delayer_rx_channel: Receiver<DelayTestScriptRecordMessage>, player_tx_channel: Sender<TestScriptPlayerMessage>) {
    log::info!("TestScriptRecord Delayer thread started...");
    log::trace!("Starting TestScriptRecord Delayer loop...");

    loop {
        log::trace!("Top of TestScriptRecord Delayer loop.");

        match delayer_rx_channel.recv().await {
            Some(message) => {
                log::debug!("Processing DelayTestScriptRecordMessage: {:?}", message);

                // Sleep for the specified time.
                sleep(Duration::from_nanos(message.delay_ns)).await;

                // Send a DispatchChangeEvent command to the TestScriptPlayer.
                let msg = TestScriptPlayerMessage {
                    command: TestScriptPlayerCommand::ProcessDelayedRecord(message.delay_sequence),
                    response_tx: None,
                };
                let response = player_tx_channel.send(msg).await;

                match response {
                    Ok(_) => {
                        log::debug!("Sent ProcessDelayedRecord command to TestScriptPlayer.");
                    },
                    Err(e) => {
                        log::error!("Error sending ProcessDelayedRecord command to TestScriptPlayer: {:?}", e);
                    }
                }
            },
            None => {
                log::error!("TestScriptRecord delayer channel closed.");
                break;
            }
        }
    }
}

// Function to log the current Delayed TestScriptRecord at varying levels of detail.
fn log_scheduled_test_script_record(msg: &str, record: &ScheduledTestScriptRecord) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, record),
        log::LevelFilter::Debug => log::debug!("{} - seq:{:?}, offset_ns:{:?}, replay_time_ns:{:?}", 
            msg, record.seq_record.seq, record.seq_record.offset_ns, record.replay_time_ns),
        _ => {}
    }
}

// Function to log the current Next TestScriptRecord at varying levels of detail.
fn log_sequeced_test_script_record(msg: &str, record: &SequencedTestScriptRecord) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, record),
        log::LevelFilter::Debug => log::debug!("{} - seq:{:?}, offset_ns:{:?}", msg, record.seq, record.offset_ns),
        _ => {}
    }
}

// Function to log the Player State at varying levels of detail.
fn log_test_script_player_state(msg: &str, state: &TestScriptPlayerState) {
    match log::max_level() {
        log::LevelFilter::Trace => log::trace!("{} - {:#?}", msg, state),
        log::LevelFilter::Debug => log::debug!("{} - {:?}", msg, state),
        log::LevelFilter::Info => log::info!("{} - status:{:?}, error_message:{:?}, start_replay_time:{:?}, current_replay_time:{:?}, skips_remaining:{:?}, steps_remaining:{:?}",
            msg, state.status, state.error_message, state.start_replay_time, state.current_replay_time, state.skips_remaining, state.steps_remaining),
        _ => {}
    }
}
