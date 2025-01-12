use std::{sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::SystemTime};

use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, streams::{StreamId, StreamReadOptions, StreamReadReply}, AsyncCommands, RedisResult};
use serde::{Deserialize, Serialize};
use test_data_store::test_repo_storage::models::{CommonTestReactionDefinition, RedisResultQueueTestReactionDefinition};
use tokio::sync::{mpsc::{Receiver, Sender}, Notify, RwLock};

use super::{ReactionCollector, ReactionCollectorError, ReactionOutputRecord, ReactionCollectorMessage, ReactionCollectorStatus};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RedisStreamRecordContent {
    data: serde_json::Value,
    id: String,
    traceid: String,
    traceparent: String,
}

impl TryFrom<&str> for RedisStreamRecordContent {
    type Error = serde_json::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

impl TryFrom<&String> for RedisStreamRecordContent {
    type Error = serde_json::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        serde_json::from_str(value)
    }
}

struct RedisStreamReadResult {
    id: String,
    seq: usize,
    enqueue_time_ns: u64,
    dequeue_time_ns: u64,
    record: Option<RedisStreamRecordContent>,
    error: Option<ReactionCollectorError>,
}

#[derive(Clone, Debug)]
pub struct RedisResultQueueCollectorSettings {
    pub host: String,
    pub port: u16,
    pub queue_name: String,
    pub reaction_id: String,
}

impl RedisResultQueueCollectorSettings {
    pub fn new(common_def: CommonTestReactionDefinition, unique_def: RedisResultQueueTestReactionDefinition) -> anyhow::Result<Self> {

        let host = unique_def.host.clone().unwrap_or_else(|| "127.0.0.1".to_string());
        let port = unique_def.port.unwrap_or(6379);        
        let queue_name = unique_def.queue_name.clone().unwrap_or_else(|| format!("{}-change", common_def.test_reaction_id.clone()));

        Ok(RedisResultQueueCollectorSettings {
            host,
            port,
            queue_name,
            reaction_id: common_def.test_reaction_id.clone(),
        })
    }
}

#[allow(dead_code)]
pub struct RedisResultQueueCollector {
    notifier: Arc<Notify>,
    seq: Arc<AtomicUsize>,
    settings: RedisResultQueueCollectorSettings,
    status: Arc<RwLock<ReactionCollectorStatus>>,
}

impl RedisResultQueueCollector {
    pub async fn new(common_def: CommonTestReactionDefinition, unique_def: RedisResultQueueTestReactionDefinition) -> anyhow::Result<Box<dyn ReactionCollector + Send + Sync>> {
        let settings = RedisResultQueueCollectorSettings::new(common_def, unique_def)?;
        log::trace!("Creating RedisResultQueueCollector with settings {:?}", settings);

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
impl ReactionCollector for RedisResultQueueCollector {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionCollectorMessage>> {

        log::debug!("Initializing RedisResultQueueCollector");

        let mut status = self.status.write().await;
        match *status {
            ReactionCollectorStatus::Uninitialized => {
                let (change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(100);
                
                *status = ReactionCollectorStatus::Paused;

                tokio::spawn(reader_thread(self.seq.clone(), self.settings.clone(), self.status.clone(), self.notifier.clone(), change_tx_channel));

                Ok(change_rx_channel)
            },
            ReactionCollectorStatus::Running => {
                anyhow::bail!("Cant Init Reader, Reader currently Running");
            },
            ReactionCollectorStatus::Paused => {
                anyhow::bail!("Cant Init Reader, Reader currently Paused");
            },
            ReactionCollectorStatus::Stopped => {
                anyhow::bail!("Cant Init Reader, Reader currently Stopped");
            },            
            ReactionCollectorStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn start(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            ReactionCollectorStatus::Uninitialized => {
                anyhow::bail!("Cant Start Reader, Reader Uninitialized");
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
                anyhow::bail!("Cant Start Reader, Reader already Stopped");
            },            
            ReactionCollectorStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            ReactionCollectorStatus::Uninitialized => {
                anyhow::bail!("Cant Pause Reader, Reader Uninitialized");
            },
            ReactionCollectorStatus::Running => {
                *status = ReactionCollectorStatus::Paused;
                Ok(())
            },
            ReactionCollectorStatus::Paused => {
                Ok(())
            },
            ReactionCollectorStatus::Stopped => {
                anyhow::bail!("Cant Pause Reader, Reader already Stopped");
            },            
            ReactionCollectorStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            ReactionCollectorStatus::Uninitialized => {
                anyhow::bail!("Reader not initialized, current status: Uninitialized");
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
                anyhow::bail!("Reader in Error state");
            },
        }
    }
}

async fn reader_thread(seq: Arc<AtomicUsize>, settings: RedisResultQueueCollectorSettings, status: Arc<RwLock<ReactionCollectorStatus>>, notify: Arc<Notify>, change_tx_channel: Sender<ReactionCollectorMessage>) {

    let client_result = redis::Client::open(format!("redis://{}:{}", &settings.host, &settings.port));

    let client = match client_result {
        Ok(client) => {
            log::debug!("Created Redis Client");
            client
        },
        Err(e) => {
            let msg = format!("Client creation error: {:?}", e);
            log::error!("{}", &msg);
            *status.write().await = ReactionCollectorStatus::Error;
            match change_tx_channel.send(ReactionCollectorMessage::Error(ReactionCollectorError::RedisError(e))).await {
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
            log::debug!("Connected to Redis");
            con
        },
        Err(e) => {
            let msg = format!("Connection Error: {:?}", e);
            log::error!("{}", &msg);
            *status.write().await = ReactionCollectorStatus::Error;
            match change_tx_channel.send(ReactionCollectorMessage::Error(ReactionCollectorError::RedisError(e))).await {
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

                                match result.record {
                                    Some(content) => {
                                        let reaction_collector_event = ReactionOutputRecord {
                                            result_data: content.data,
                                            dequeue_time_ns: result.dequeue_time_ns,
                                            enqueue_time_ns: result.enqueue_time_ns,
                                            id: content.id,
                                            seq: result.seq,
                                            traceid: content.traceid,
                                            traceparent: content.traceparent,
                                        };
                                        match change_tx_channel.send(ReactionCollectorMessage::Event(reaction_collector_event)).await {
                                            Ok(_) => {},
                                            Err(e) => {
                                                let msg = format!("Error sending change message: {:?}", e);
                                                log::error!("{}", msg);
                                            }
                                        }
                                    },
                                    None => {
                                        match result.error {
                                            Some(e) => {
                                                log::error!("Error reading from Redis stream: {:?}", e);
                                                match change_tx_channel.send(ReactionCollectorMessage::Error(e)).await {
                                                    Ok(_) => {},
                                                    Err(e) => {
                                                        let msg = format!("Error sending error message: {:?}", e);
                                                        log::error!("{}", msg);
                                                    }
                                                }
                                                continue;
                                            },
                                            None => {
                                                log::error!("No record or error found in stream entry");
                                                continue;
                                            }
                                        }
                                    }
                                };
                            }
                        },
                        Err(e) => {
                            log::error!("Error reading from Redis stream: {:?}", e);
                            // match change_tx_channel.send(ReactionCollectorMessage::Error(e)).await {
                            //     Ok(_) => {},
                            //     Err(e) => {
                            //         let msg = format!("Error sending error message: {:?}", e);
                            //         log::error!("{}", msg);
                            //     }
                            // }
                        }
                    }
                };        
            },
        }
    }
}

async fn read_stream(con: &mut MultiplexedConnection, seq: Arc<AtomicUsize>, stream_key: &str, stream_last_id: &str, read_options: &StreamReadOptions) -> anyhow::Result<Vec<RedisStreamReadResult>>{

    let xread_result: RedisResult<StreamReadReply> = con.xread_options(&[stream_key], &[stream_last_id], read_options).await;

    let xread_result = match xread_result {
        Ok(xread_result) => {
            xread_result
        },
        Err(e) => {
            return Err(anyhow::anyhow!("Error reading from stream: {:?}", e));
        }
    };
    
    let dequeue_time_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

    let mut records: Vec<RedisStreamReadResult> = Vec::new();

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
                                    match RedisStreamRecordContent::try_from(&s) {
                                        Ok(record) => {
                                            records.push(RedisStreamReadResult {
                                                id,
                                                seq: seq.fetch_add(1, Ordering::SeqCst),
                                                enqueue_time_ns,
                                                dequeue_time_ns,
                                                record: Some(record),
                                                error: None,
                                            });                                            
                                        },
                                        Err(e) => {
                                            log::error!("Error: {:?}", e);
                                            records.push(RedisStreamReadResult {
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
                                    records.push(RedisStreamReadResult {
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
                            records.push(RedisStreamReadResult {
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
                    records.push(RedisStreamReadResult {
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
