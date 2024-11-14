use std::{sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::SystemTime};

use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, streams::{StreamId, StreamReadOptions, StreamReadReply}, AsyncCommands, RedisResult};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::{Receiver, Sender}, Notify, RwLock};

use crate::{config::RedisSourceChangeQueueReaderConfig, source::change_queue_readers::SourceChangeQueueReaderError};

use super::{SourceChangeQueueReader, SourceChangeQueueReaderMessage, SourceChangeQueueReaderStatus, SourceChangeQueueRecord};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RedisStreamRecordContent {
    data: Vec<serde_json::Value>,
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
    error: Option<SourceChangeQueueReaderError>,
}

#[derive(Clone, Debug)]
pub struct RedisSourceChangeQueueReaderSettings {
    pub host: String,
    pub port: u16,
    pub queue_name: String,
    pub source_id: String,
}

impl RedisSourceChangeQueueReaderSettings {
    pub fn new(config: &RedisSourceChangeQueueReaderConfig, source_id: String) -> anyhow::Result<Self> {

        let host = config.host.clone().unwrap_or_else(|| "127.0.0.1".to_string());
        let port = config.port.unwrap_or(6379);
        
        let queue_name = config
            .queue_name
            .clone()
            .unwrap_or_else(|| format!("{}-change", source_id));

        Ok(RedisSourceChangeQueueReaderSettings {
            host,
            port,
            queue_name,
            source_id,
        })
    }
}


#[allow(dead_code)]
pub struct RedisSourceChangeQueueReader {
    notifier: Arc<Notify>,
    seq: Arc<AtomicUsize>,
    settings: RedisSourceChangeQueueReaderSettings,
    status: Arc<RwLock<SourceChangeQueueReaderStatus>>,
}

impl RedisSourceChangeQueueReader {
    pub async fn new<S: Into<String>>(config: RedisSourceChangeQueueReaderConfig, source_id: S) -> anyhow::Result<Box<dyn SourceChangeQueueReader + Send + Sync>> {
        log::debug!("Creating RedisSourceChangeQueueReader from config {:?}", config);

        let settings = RedisSourceChangeQueueReaderSettings::new(&config,source_id.into())?;
        log::trace!("Creating RedisSourceChangeQueueReader with settings {:?}", settings);

        let notifier = Arc::new(Notify::new());
        let status = Arc::new(RwLock::new(SourceChangeQueueReaderStatus::Uninitialized));
        
        Ok(Box::new(RedisSourceChangeQueueReader {
            notifier,
            seq: Arc::new(AtomicUsize::new(0)),
            settings,
            status,
        }))
    }
}

#[async_trait]
impl SourceChangeQueueReader for RedisSourceChangeQueueReader {
    async fn init(&self) -> anyhow::Result<Receiver<SourceChangeQueueReaderMessage>> {

        log::debug!("Initializing RedisSourceChangeQueueReader");

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                let (change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(100);
                
                *status = SourceChangeQueueReaderStatus::Paused;

                tokio::spawn(reader_thread(self.seq.clone(), self.settings.clone(), self.status.clone(), self.notifier.clone(), change_tx_channel));

                Ok(change_rx_channel)
            },
            SourceChangeQueueReaderStatus::Running => {
                anyhow::bail!("Cant Init Reader, Reader currently Running");
            },
            SourceChangeQueueReaderStatus::Paused => {
                anyhow::bail!("Cant Init Reader, Reader currently Paused");
            },
            SourceChangeQueueReaderStatus::Stopped => {
                anyhow::bail!("Cant Init Reader, Reader currently Stopped");
            },            
            SourceChangeQueueReaderStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn start(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                anyhow::bail!("Cant Start Reader, Reader Uninitialized");
            },
            SourceChangeQueueReaderStatus::Running => {
                Ok(())
            },
            SourceChangeQueueReaderStatus::Paused => {
                *status = SourceChangeQueueReaderStatus::Running;
                self.notifier.notify_one();
                Ok(())
            },
            SourceChangeQueueReaderStatus::Stopped => {
                anyhow::bail!("Cant Start Reader, Reader already Stopped");
            },            
            SourceChangeQueueReaderStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                anyhow::bail!("Cant Pause Reader, Reader Uninitialized");
            },
            SourceChangeQueueReaderStatus::Running => {
                *status = SourceChangeQueueReaderStatus::Paused;
                Ok(())
            },
            SourceChangeQueueReaderStatus::Paused => {
                Ok(())
            },
            SourceChangeQueueReaderStatus::Stopped => {
                anyhow::bail!("Cant Pause Reader, Reader already Stopped");
            },            
            SourceChangeQueueReaderStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                anyhow::bail!("Reader not initialized, current status: Uninitialized");
            },
            SourceChangeQueueReaderStatus::Running => {
                *status = SourceChangeQueueReaderStatus::Stopped;
                Ok(())
            },
            SourceChangeQueueReaderStatus::Paused => {
                *status = SourceChangeQueueReaderStatus::Stopped;
                self.notifier.notify_one();
                Ok(())
            },
            SourceChangeQueueReaderStatus::Stopped => {
                Ok(())
            },            
            SourceChangeQueueReaderStatus::Error => {
                anyhow::bail!("Reader in Error state");
            },
        }
    }
}

async fn reader_thread(seq: Arc<AtomicUsize>, settings: RedisSourceChangeQueueReaderSettings, status: Arc<RwLock<SourceChangeQueueReaderStatus>>, notify: Arc<Notify>, change_tx_channel: Sender<SourceChangeQueueReaderMessage>) {

    let client_result = redis::Client::open(format!("redis://{}:{}", &settings.host, &settings.port));

    let client = match client_result {
        Ok(client) => {
            log::debug!("Created Redis Client");
            client
        },
        Err(e) => {
            let msg = format!("Client creation error: {:?}", e);
            log::error!("{}", &msg);
            *status.write().await = SourceChangeQueueReaderStatus::Error;
            match change_tx_channel.send(SourceChangeQueueReaderMessage::Error(SourceChangeQueueReaderError::RedisError(e))).await {
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
            *status.write().await = SourceChangeQueueReaderStatus::Error;
            match change_tx_channel.send(SourceChangeQueueReaderMessage::Error(SourceChangeQueueReaderError::RedisError(e))).await {
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
            SourceChangeQueueReaderStatus::Uninitialized 
            | SourceChangeQueueReaderStatus::Stopped
            | SourceChangeQueueReaderStatus::Error => {
                return;
            },
            SourceChangeQueueReaderStatus::Paused => {
                notify.notified().await;
            },
            SourceChangeQueueReaderStatus::Running => {
                while *status.read().await == SourceChangeQueueReaderStatus::Running {
                    let read_result = read_stream(&mut con, seq.clone(), stream_key, &stream_last_id, &opts).await;
                    match read_result {
                        Ok(results) => {
                            for result in results {
                                stream_last_id = result.id.clone();

                                match result.record {
                                    Some(content) => {
                                        let change_queue_record = SourceChangeQueueRecord {
                                            change_events: content.data,
                                            dequeue_time_ns: result.dequeue_time_ns,
                                            enqueue_time_ns: result.enqueue_time_ns,
                                            id: content.id,
                                            seq: result.seq,
                                            traceid: content.traceid,
                                            traceparent: content.traceparent,
                                        };
                                        match change_tx_channel.send(SourceChangeQueueReaderMessage::QueueRecord(change_queue_record)).await {
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
                                                match change_tx_channel.send(SourceChangeQueueReaderMessage::Error(e)).await {
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
                            // match change_tx_channel.send(SourceChangeQueueReaderMessage::Error(e)).await {
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
                                                error: Some(SourceChangeQueueReaderError::InvalidQueueData),
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
                                        error: Some(SourceChangeQueueReaderError::InvalidQueueData),
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
                                error: Some(SourceChangeQueueReaderError::InvalidQueueData),
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
                        error: Some(SourceChangeQueueReaderError::InvalidQueueData),
                    });   
                }
            };
        }
    }

    Ok(records)
}
