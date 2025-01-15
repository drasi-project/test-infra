use std::{collections::HashMap, sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::SystemTime};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::{Receiver, Sender}, Notify, RwLock};

use test_data_store::{
    test_repo_storage::models::{CommonTestReactionDefinition, DaprResultQueueTestReactionDefinition}, 
    test_run_storage::TestRunReactionId
};

use super::{ReactionCollector, ReactionCollectorError, ReactionOutputRecord, ReactionCollectorMessage, ReactionCollectorStatus};

struct DaprStreamReadResult {
    dequeue_time_ns: u64,
    enqueue_time_ns: u64,
    error: Option<ReactionCollectorError>,
    id: String,
    record: Option<DaprStreamRecordContent>,
    seq: usize,
}

impl TryInto<ReactionCollectorMessage> for DaprStreamReadResult {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ReactionCollectorMessage, Self::Error> {
        match self.record {
            Some(record) => {
                let reaction_collector_event = ReactionOutputRecord {
                    result_data: serde_json::to_value(&record.data).unwrap(),
                    dequeue_time_ns: self.dequeue_time_ns,
                    enqueue_time_ns: self.enqueue_time_ns,
                    id: record.id,
                    seq: self.seq,
                    traceid: record.traceid,
                    traceparent: record.traceparent,
                };

                Ok(ReactionCollectorMessage::Record(reaction_collector_event))
            },
            None => {
                match self.error {
                    Some(e) => {
                        Ok(ReactionCollectorMessage::Error(e))
                    },
                    None => {
                        Err(anyhow::anyhow!("No record or error found in stream entry"))
                    }
                }
            }
        }
    }
}    

#[derive(Debug, Serialize, Deserialize)]
struct DaprStreamRecordContent {
    data: ResultStreamRecord,
    id: String,
    traceid: String,
    traceparent: String,
}

impl TryFrom<&str> for DaprStreamRecordContent {
    type Error = serde_json::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

impl TryFrom<&String> for DaprStreamRecordContent {
    type Error = serde_json::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ResultStreamRecord {
    #[serde(rename = "change")]
    Change(ChangeEvent),
    #[serde(rename = "control")]
    Control(ControlEvent),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BaseResultEvent {
    #[serde(rename = "queryId")]
    pub query_id: String,

    pub sequence: i64,

    #[serde(rename = "sourceTimeMs")]
    pub source_time_ms: i64,

    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangeEvent {
    #[serde(flatten)]
    pub base: BaseResultEvent,

    #[serde(rename = "addedResults")]
    pub added_results: Vec<HashMap<String, serde_json::Value>>,

    #[serde(rename = "updatedResults")]
    pub updated_results: Vec<UpdatePayload>,

    #[serde(rename = "deletedResults")]
    pub deleted_results: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatePayload {
    pub before: HashMap<String, serde_json::Value>,
    pub after: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ControlEvent {
    #[serde(flatten)]
    pub base: BaseResultEvent,

    #[serde(rename = "controlSignal")]
    pub control_signal: ControlSignal,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ControlSignal {
    #[serde(rename = "bootstrapStarted")]
    BootstrapStarted(BootstrapStartedSignal),
    #[serde(rename = "bootstrapCompleted")]
    BootstrapCompleted(BootstrapCompletedSignal),
    #[serde(rename = "running")]
    Running(RunningSignal),
    #[serde(rename = "stopped")]
    Stopped(StoppedSignal),
    #[serde(rename = "deleted")]
    Deleted(DeletedSignal),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BootstrapStartedSignal {
    // Additional fields can be added if necessary
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BootstrapCompletedSignal {
    // Additional fields can be added if necessary
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunningSignal {
    // Additional fields can be added if necessary
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoppedSignal {
    // Additional fields can be added if necessary
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeletedSignal {
    // Additional fields can be added if necessary
}

#[derive(Clone, Debug)]
pub struct DaprResultQueueCollectorSettings {
    pub host: String,
    pub port: u16,
    pub pubsub_name: String,
    pub pubsub_topic: String,
    pub reaction_id: String,
    pub test_run_reaction_id: TestRunReactionId,
}

impl DaprResultQueueCollectorSettings {
    pub fn new(id: TestRunReactionId, common_def: CommonTestReactionDefinition, unique_def: DaprResultQueueTestReactionDefinition) -> anyhow::Result<Self> {

        Ok(DaprResultQueueCollectorSettings {
            host: unique_def.host.clone().unwrap_or_else(|| "127.0.0.1".to_string()),
            port: unique_def.port.unwrap_or(6379),
            pubsub_name: unique_def.pubsub_name.clone().unwrap_or_else(|| "drasi-pubsub".to_string()),
            pubsub_topic: unique_def.pubsub_topic.clone().unwrap_or_else(|| format!("{}-results", common_def.test_reaction_id.clone())),
            reaction_id: common_def.test_reaction_id.clone(),
            test_run_reaction_id: id
        })
    }
}

#[allow(dead_code)]
pub struct DaprResultQueueCollector {
    notifier: Arc<Notify>,
    seq: Arc<AtomicUsize>,
    settings: DaprResultQueueCollectorSettings,
    status: Arc<RwLock<ReactionCollectorStatus>>,
}

impl DaprResultQueueCollector {
    pub async fn new(id: TestRunReactionId, common_def: CommonTestReactionDefinition, unique_def: DaprResultQueueTestReactionDefinition) -> anyhow::Result<Box<dyn ReactionCollector + Send + Sync>> {
        let settings = DaprResultQueueCollectorSettings::new(id, common_def, unique_def)?;
        log::trace!("Creating DaprResultQueueCollector with settings {:?}", settings);

        let notifier = Arc::new(Notify::new());
        let status = Arc::new(RwLock::new(ReactionCollectorStatus::Uninitialized));
        
        Ok(Box::new(Self {
            notifier,
            seq: Arc::new(AtomicUsize::new(0)),
            settings,
            status,
        }))
    }
}

#[async_trait]
impl ReactionCollector for DaprResultQueueCollector {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionCollectorMessage>> {
        log::trace!("Initializing DaprResultQueueCollector");

        let mut status = self.status.write().await;
        match *status {
            ReactionCollectorStatus::Uninitialized => {
                let (collector_tx_channel, collector_rx_channel) = tokio::sync::mpsc::channel(100);
                
                *status = ReactionCollectorStatus::Paused;

                tokio::spawn(reader_thread(self.seq.clone(), self.settings.clone(), self.status.clone(), self.notifier.clone(), collector_tx_channel));

                Ok(collector_rx_channel)
            },
            ReactionCollectorStatus::Running => {
                anyhow::bail!("Cant Init Collector, Collector currently Running");
            },
            ReactionCollectorStatus::Paused => {
                anyhow::bail!("Cant Init Collector, Collector currently Paused");
            },
            ReactionCollectorStatus::Stopped => {
                anyhow::bail!("Cant Init Collector, Collector currently Stopped");
            },            
            ReactionCollectorStatus::Error => {
                anyhow::bail!("Collector in Error state");
            },
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        log::trace!("Starting DaprResultQueueCollector");

        let mut status = self.status.write().await;
        match *status {
            ReactionCollectorStatus::Uninitialized => {
                anyhow::bail!("Cant Start Collector, Collector Uninitialized");
            },
            ReactionCollectorStatus::Running => {
                Ok(())
            },
            ReactionCollectorStatus::Paused => {
                *status = ReactionCollectorStatus::Running;
                self.notifier.notify_one();
                Ok(())
            },
            ReactionCollectorStatus::Stopped => {
                anyhow::bail!("Cant Start Collector, Collector already Stopped");
            },            
            ReactionCollectorStatus::Error => {
                anyhow::bail!("Collector in Error state");
            },
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {
        log::trace!("Pausing DaprResultQueueCollector");

        let mut status = self.status.write().await;
        match *status {
            ReactionCollectorStatus::Uninitialized => {
                anyhow::bail!("Cant Pause Collector, Collector Uninitialized");
            },
            ReactionCollectorStatus::Running => {
                *status = ReactionCollectorStatus::Paused;
                Ok(())
            },
            ReactionCollectorStatus::Paused => {
                Ok(())
            },
            ReactionCollectorStatus::Stopped => {
                anyhow::bail!("Cant Pause Collector, Collector already Stopped");
            },            
            ReactionCollectorStatus::Error => {
                anyhow::bail!("Collector in Error state");
            },
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {
        log::trace!("Stopping DaprResultQueueCollector");

        let mut status = self.status.write().await;
        match *status {
            ReactionCollectorStatus::Uninitialized => {
                anyhow::bail!("Collector not initialized, current status: Uninitialized");
            },
            ReactionCollectorStatus::Running => {
                *status = ReactionCollectorStatus::Stopped;
                Ok(())
            },
            ReactionCollectorStatus::Paused => {
                *status = ReactionCollectorStatus::Stopped;
                self.notifier.notify_one();
                Ok(())
            },
            ReactionCollectorStatus::Stopped => {
                Ok(())
            },            
            ReactionCollectorStatus::Error => {
                anyhow::bail!("Collector in Error state");
            },
        }
    }
}

async fn reader_thread(
    seq: Arc<AtomicUsize>, 
    settings: DaprResultQueueCollectorSettings, 
    status: Arc<RwLock<ReactionCollectorStatus>>, 
    notify: Arc<Notify>, reaction_collector_tx_channel: 
    Sender<ReactionCollectorMessage>) 
{

    let client_result = redis::Client::open(format!("redis://{}:{}", &settings.host, &settings.port));

    let client = match client_result {
        Ok(client) => {
            log::debug!("Created Dapr Client");
            client
        },
        Err(e) => {
            let msg = format!("Client creation error: {:?}", e);
            log::error!("{}", &msg);
            *status.write().await = ReactionCollectorStatus::Error;
            match reaction_collector_tx_channel.send(ReactionCollectorMessage::Error(ReactionCollectorError::DaprError(e))).await {
                Ok(_) => {},
                Err(e) => {
                    log::error!("Error sending error message: {:?}", e);
                }   
            }
            return;
        }
    };

    let con_result = client.get_multiplexed_async_connection().await;

    let mut con = match con_result {
        Ok(con) => {
            log::debug!("Connected to Dapr");
            con
        },
        Err(e) => {
            let msg = format!("Connection Error: {:?}", e);
            log::error!("{}", &msg);
            *status.write().await = ReactionCollectorStatus::Error;
            match reaction_collector_tx_channel.send(ReactionCollectorMessage::Error(ReactionCollectorError::DaprError(e))).await {
                Ok(_) => {},
                Err(e) => {
                    log::error!("Error sending error message: {:?}", e);
                }
            }
            return;
        }
    };

    let stream_key = &settings.queue_name;
    let mut stream_last_id = "0-0".to_string();
    let opts = StreamReadOptions::default().count(1).block(5000);

    loop {
        match *status.read().await {
            ReactionCollectorStatus::Uninitialized 
            | ReactionCollectorStatus::Stopped
            | ReactionCollectorStatus::Error => {
                return;
            },
            ReactionCollectorStatus::Paused => {
                notify.notified().await;
            },
            ReactionCollectorStatus::Running => {
                while *status.read().await == ReactionCollectorStatus::Running {
                    let read_result = read_stream(&mut con, seq.clone(), stream_key, &stream_last_id, &opts).await;
                    match read_result {
                        Ok(results) => {
                            for result in results {
                                stream_last_id = result.id.clone();

                                let reaction_collector_message: ReactionCollectorMessage = match result.try_into() {
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        log::error!("Error converting DaprStreamReadResult to ReactionCollectorMessage: {:?}", e);
                                        ReactionCollectorMessage::Error(ReactionCollectorError::ConversionError)
                                    }
                                };

                                match reaction_collector_tx_channel.send(reaction_collector_message).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        match e {
                                            tokio::sync::mpsc::error::SendError(msg) => {
                                                log::error!("Error sending change message: {:?}", msg);
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            log::error!("Error reading from Dapr stream: {:?}", e);
                        }
                    }
                };        
            },
        }
    }
}

async fn read_stream(con: &mut MultiplexedConnection, seq: Arc<AtomicUsize>, stream_key: &str, stream_last_id: &str, read_options: &StreamReadOptions) -> anyhow::Result<Vec<DaprStreamReadResult>>{

    let xread_result: DaprResult<StreamReadReply> = con.xread_options(&[stream_key], &[stream_last_id], read_options).await;

    let xread_result = match xread_result {
        Ok(xread_result) => {
            xread_result
        },
        Err(e) => {
            return Err(anyhow::anyhow!("Error reading from stream: {:?}", e));
        }
    };
    
    let dequeue_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

    let mut records: Vec<DaprStreamReadResult> = Vec::new();

    for key in xread_result.keys {
        let ids = &key.ids;

        for id in ids {
            let StreamId { id, map } = &id;

            let id = id.to_string();
            let enqueue_time_ns: u64 = id.split('-').next().unwrap().parse().unwrap();

            match map.get("data") {
                Some(data) => {
                    match data {
                        redis::Value::BulkString(bs_data) => {
                            match String::from_utf8(bs_data.to_vec()) {
                                Ok(s) => {
                                    match DaprStreamRecordContent::try_from(&s) {
                                        Ok(record) => {
                                            records.push(DaprStreamReadResult {
                                                id,
                                                seq: seq.fetch_add(1, Ordering::SeqCst),
                                                enqueue_time_ns: enqueue_time_ns * 1_000_000,
                                                dequeue_time_ns,
                                                record: Some(record),
                                                error: None,
                                            });                                            
                                        },
                                        Err(e) => {
                                            log::error!("Error: {:?}", e);
                                            records.push(DaprStreamReadResult {
                                                id,
                                                seq: seq.fetch_add(1, Ordering::SeqCst),
                                                enqueue_time_ns,
                                                dequeue_time_ns,
                                                record: None,
                                                error: Some(ReactionCollectorError::InvalidQueueData),
                                            });   
                                        }
                                    }
                                },
                                Err(e) => {
                                    log::error!("Error: {:?}", e);
                                    records.push(DaprStreamReadResult {
                                        id,
                                        seq: seq.fetch_add(1, Ordering::SeqCst),
                                        enqueue_time_ns,
                                        dequeue_time_ns,
                                        record: None,
                                        error: Some(ReactionCollectorError::InvalidQueueData),
                                    });   
                                }
                            }
                        },
                        _ => {
                            log::error!("Data is not a BulkString");
                            records.push(DaprStreamReadResult {
                                id,
                                seq: seq.fetch_add(1, Ordering::SeqCst),
                                enqueue_time_ns,
                                dequeue_time_ns,
                                record: None,
                                error: Some(ReactionCollectorError::InvalidQueueData),
                            });   
                        }
                    }
                },
                None => {
                    log::error!("No data found in stream entry");
                    records.push(DaprStreamReadResult {
                        id,
                        seq: seq.fetch_add(1, Ordering::SeqCst),
                        enqueue_time_ns,
                        dequeue_time_ns,
                        record: None,
                        error: Some(ReactionCollectorError::InvalidQueueData),
                    });   
                }
            };
        }
    }

    Ok(records)
}