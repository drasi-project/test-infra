use std::{sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::SystemTime};

use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, streams::{StreamId, StreamReadOptions, StreamReadReply}, AsyncCommands, RedisResult};
use redis_stream_read_result::{RedisStreamReadResult, RedisStreamRecordData};
use test_data_store::{test_repo_storage::models::{CommonTestReactionDefinition, RedisResultQueueTestReactionDefinition}, test_run_storage::TestRunReactionId};
use tokio::sync::{mpsc::{Receiver, Sender}, Notify, RwLock};

use super::{ReactionHandler, ReactionHandlerError, ReactionHandlerMessage, ReactionHandlerStatus};

pub mod redis_stream_read_result;
pub mod result_stream_record;

#[derive(Clone, Debug)]
pub struct RedisResultQueueHandlerSettings {
    pub host: String,
    pub port: u16,
    pub queue_name: String,
    pub reaction_id: String,
    pub test_run_reaction_id: TestRunReactionId,
}

impl RedisResultQueueHandlerSettings {
    pub fn new(id: TestRunReactionId, common_def: CommonTestReactionDefinition, unique_def: RedisResultQueueTestReactionDefinition) -> anyhow::Result<Self> {

        let host = unique_def.host.clone().unwrap_or_else(|| "127.0.0.1".to_string());
        let port = unique_def.port.unwrap_or(6379);        
        let queue_name = unique_def.queue_name.clone().unwrap_or_else(|| format!("{}-results", common_def.test_reaction_id.clone()));

        Ok(RedisResultQueueHandlerSettings {
            host,
            port,
            queue_name,
            reaction_id: common_def.test_reaction_id.clone(),
            test_run_reaction_id: id
        })
    }
}

#[allow(dead_code)]
pub struct RedisResultQueueHandler {
    notifier: Arc<Notify>,
    seq: Arc<AtomicUsize>,
    settings: RedisResultQueueHandlerSettings,
    status: Arc<RwLock<ReactionHandlerStatus>>,
}

impl RedisResultQueueHandler {
    pub async fn new(id: TestRunReactionId, common_def: CommonTestReactionDefinition, unique_def: RedisResultQueueTestReactionDefinition) -> anyhow::Result<Box<dyn ReactionHandler + Send + Sync>> {
        let settings = RedisResultQueueHandlerSettings::new(id, common_def, unique_def)?;
        log::trace!("Creating RedisResultQueueHandler with settings {:?}", settings);

        let notifier = Arc::new(Notify::new());
        let status = Arc::new(RwLock::new(ReactionHandlerStatus::Uninitialized));
        
        Ok(Box::new(Self {
            notifier,
            seq: Arc::new(AtomicUsize::new(0)),
            settings,
            status,
        }))
    }
}

#[async_trait]
impl ReactionHandler for RedisResultQueueHandler {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
        log::debug!("Initializing RedisResultQueueHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ReactionHandlerStatus::Uninitialized => {
                    let (handler_tx_channel, handler_rx_channel) = tokio::sync::mpsc::channel(100);
                    
                    *status = ReactionHandlerStatus::Paused;
    
                    tokio::spawn(reader_thread(self.seq.clone(), self.settings.clone(), self.status.clone(), self.notifier.clone(), handler_tx_channel));
    
                    Ok(handler_rx_channel)
                },
                ReactionHandlerStatus::Running => {
                    anyhow::bail!("Cant Init Handler, Handler currently Running");
                },
                ReactionHandlerStatus::Paused => {
                    anyhow::bail!("Cant Init Handler, Handler currently Paused");
                },
                ReactionHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Init Handler, Handler currently Stopped");
                },            
                ReactionHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                },
            }    
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        log::debug!("Starting RedisResultQueueHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ReactionHandlerStatus::Uninitialized => {
                    anyhow::bail!("Can't Start Handler, Handler Uninitialized");
                },
                ReactionHandlerStatus::Running => {
                    Ok(())
                },
                ReactionHandlerStatus::Paused => {
                    *status = ReactionHandlerStatus::Running;
                    self.notifier.notify_one();
                    Ok(())
                },
                ReactionHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Start Handler, Handler already Stopped");
                },            
                ReactionHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                },
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {
        log::debug!("Pausing RedisResultQueueHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ReactionHandlerStatus::Uninitialized => {
                    anyhow::bail!("Cant Pause Handler, Handler Uninitialized");
                },
                ReactionHandlerStatus::Running => {
                    *status = ReactionHandlerStatus::Paused;
                    Ok(())
                },
                ReactionHandlerStatus::Paused => {
                    Ok(())
                },
                ReactionHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Pause Handler, Handler already Stopped");
                },            
                ReactionHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                },
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {
        log::debug!("Stopping RedisResultQueueHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ReactionHandlerStatus::Uninitialized => {
                    anyhow::bail!("Handler not initialized, current status: Uninitialized");
                },
                ReactionHandlerStatus::Running => {
                    *status = ReactionHandlerStatus::Stopped;
                    Ok(())
                },
                ReactionHandlerStatus::Paused => {
                    *status = ReactionHandlerStatus::Stopped;
                    self.notifier.notify_one();
                    Ok(())
                },
                ReactionHandlerStatus::Stopped => {
                    Ok(())
                },            
                ReactionHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                },
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }
}

async fn reader_thread(
    seq: Arc<AtomicUsize>, 
    settings: RedisResultQueueHandlerSettings, 
    status: Arc<RwLock<ReactionHandlerStatus>>, 
    notify: Arc<Notify>, reaction_handler_tx_channel: 
    Sender<ReactionHandlerMessage>) 
{
    log::debug!("Starting RedisResultQueueHandler Reader Thread");

    let client_result = redis::Client::open(format!("redis://{}:{}", &settings.host, &settings.port));

    let client = match client_result {
        Ok(client) => {
            log::debug!("Created Redis Client");
            client
        },
        Err(e) => {
            let msg = format!("Client creation error: {:?}", e);
            log::error!("{}", &msg);
            *status.write().await = ReactionHandlerStatus::Error;
            match reaction_handler_tx_channel.send(ReactionHandlerMessage::Error(ReactionHandlerError::RedisError(e))).await {
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
            *status.write().await = ReactionHandlerStatus::Error;
            match reaction_handler_tx_channel.send(ReactionHandlerMessage::Error(ReactionHandlerError::RedisError(e))).await {
                Ok(_) => {},
                Err(e) => {
                    log::error!("Error sending error message: {:?}", e);
                }
            }
            return;
        }
    };

    let stream_key = &settings.queue_name;
    let mut stream_last_id = format!("{}-0",SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string());
    let opts = StreamReadOptions::default().count(1).block(5000);

    loop {
        let current_status = {
            if let Ok(status) = status.try_read() {
                (*status).clone()
            } else {
                log::warn!("Could not acquire status lock in loop");
                continue;
            }
        };

        match current_status {
            ReactionHandlerStatus::Uninitialized 
            | ReactionHandlerStatus::Stopped
            | ReactionHandlerStatus::Error => {
                log::debug!("Uninitialized, Stopped, or Error, exiting");
                return;
            },
            ReactionHandlerStatus::Paused => {
                log::debug!("Paused, waiting for notify");
                notify.notified().await;
                log::debug!("Notified");
            },
            ReactionHandlerStatus::Running => {
                let read_result = read_stream(&mut con, seq.clone(), stream_key, &stream_last_id, &opts).await;
                match read_result {
                    Ok(results) => {
                        for result in results {
                            stream_last_id = result.id.clone();

                            let reaction_handler_message: ReactionHandlerMessage = match result.try_into() {
                                Ok(msg) => msg,
                                Err(e) => {
                                    log::error!("Error converting RedisStreamReadResult to ReactionHandlerMessage: {:?}", e);
                                    ReactionHandlerMessage::Error(ReactionHandlerError::ConversionError)
                                }
                            };

                            match reaction_handler_tx_channel.send(reaction_handler_message).await {
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
                        log::error!("Error reading from Redis stream: {:?}", e);
                    }
                }
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
                                    match RedisStreamRecordData::try_from(&s) {
                                        Ok(record) => {
                                            records.push(RedisStreamReadResult {
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
                                            records.push(RedisStreamReadResult {
                                                id,
                                                seq: seq.fetch_add(1, Ordering::SeqCst),
                                                enqueue_time_ns,
                                                dequeue_time_ns,
                                                record: None,
                                                error: Some(ReactionHandlerError::InvalidQueueData),
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
                                        error: Some(ReactionHandlerError::InvalidQueueData),
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
                                error: Some(ReactionHandlerError::InvalidQueueData),
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
                        error: Some(ReactionHandlerError::InvalidQueueData),
                    });   
                }
            };
        }
    }

    Ok(records)
}