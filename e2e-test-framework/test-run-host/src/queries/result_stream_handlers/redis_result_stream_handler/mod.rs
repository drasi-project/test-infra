use std::{sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::SystemTime};

use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, streams::{StreamId, StreamReadOptions, StreamReadReply}, AsyncCommands, RedisResult};
use redis_stream_read_result::{RedisStreamReadResult, RedisStreamRecordData};
use test_data_store::{test_repo_storage::models::RedisStreamResultStreamHandlerDefinition, test_run_storage::TestRunQueryId};
use tokio::sync::{mpsc::{Receiver, Sender}, Notify, RwLock};

use super::{ResultStreamHandler, ResultStreamHandlerError, ResultStreamHandlerMessage, ResultStreamHandlerStatus};

pub mod redis_stream_read_result;
pub mod result_stream_record;

#[derive(Clone, Debug)]
pub struct RedisResultStreamHandlerSettings {
    pub host: String,
    pub port: u16,
    pub process_old_entries: bool,
    pub query_id: String,
    pub stream_name: String,
    pub test_run_query_id: TestRunQueryId,
}

impl RedisResultStreamHandlerSettings {
    pub fn new(id: TestRunQueryId, definition: RedisStreamResultStreamHandlerDefinition) -> anyhow::Result<Self> {

        Ok(RedisResultStreamHandlerSettings {
            host: definition.host.clone().unwrap_or_else(|| "127.0.0.1".to_string()),
            port: definition.port.unwrap_or(6379),
            process_old_entries: definition.process_old_entries.unwrap_or(false),
            query_id: id.test_query_id.clone(),
            stream_name: definition.stream_name.clone().unwrap_or_else(|| format!("{}-results", id.test_query_id.clone())),
            test_run_query_id: id
        })
    }
}

#[allow(dead_code)]
pub struct RedisResultStreamHandler {
    notifier: Arc<Notify>,
    seq: Arc<AtomicUsize>,
    settings: RedisResultStreamHandlerSettings,
    status: Arc<RwLock<ResultStreamHandlerStatus>>,
}

impl RedisResultStreamHandler {
    pub async fn new(id: TestRunQueryId, definition: RedisStreamResultStreamHandlerDefinition) -> anyhow::Result<Box<dyn ResultStreamHandler + Send + Sync>> {
        let settings = RedisResultStreamHandlerSettings::new(id, definition)?;
        log::trace!("Creating RedisResultStreamHandler with settings {:?}", settings);

        let notifier = Arc::new(Notify::new());
        let status = Arc::new(RwLock::new(ResultStreamHandlerStatus::Uninitialized));
        
        Ok(Box::new(Self {
            notifier,
            seq: Arc::new(AtomicUsize::new(0)),
            settings,
            status,
        }))
    }
}

#[async_trait]
impl ResultStreamHandler for RedisResultStreamHandler {
    async fn init(&self) -> anyhow::Result<Receiver<ResultStreamHandlerMessage>> {
        log::debug!("Initializing RedisResultStreamHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ResultStreamHandlerStatus::Uninitialized => {
                    let (handler_tx_channel, handler_rx_channel) = tokio::sync::mpsc::channel(100);
                    
                    *status = ResultStreamHandlerStatus::Paused;
    
                    tokio::spawn(reader_thread(self.seq.clone(), self.settings.clone(), self.status.clone(), self.notifier.clone(), handler_tx_channel));
    
                    Ok(handler_rx_channel)
                },
                ResultStreamHandlerStatus::Running => {
                    anyhow::bail!("Cant Init Handler, Handler currently Running");
                },
                ResultStreamHandlerStatus::Paused => {
                    anyhow::bail!("Cant Init Handler, Handler currently Paused");
                },
                ResultStreamHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Init Handler, Handler currently Stopped");
                },            
                ResultStreamHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                },
            }    
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        log::debug!("Starting RedisResultStreamHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ResultStreamHandlerStatus::Uninitialized => {
                    anyhow::bail!("Can't Start Handler, Handler Uninitialized");
                },
                ResultStreamHandlerStatus::Running => {
                    Ok(())
                },
                ResultStreamHandlerStatus::Paused => {
                    *status = ResultStreamHandlerStatus::Running;
                    self.notifier.notify_one();
                    Ok(())
                },
                ResultStreamHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Start Handler, Handler already Stopped");
                },            
                ResultStreamHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                },
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {
        log::debug!("Pausing RedisResultStreamHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ResultStreamHandlerStatus::Uninitialized => {
                    anyhow::bail!("Cant Pause Handler, Handler Uninitialized");
                },
                ResultStreamHandlerStatus::Running => {
                    *status = ResultStreamHandlerStatus::Paused;
                    Ok(())
                },
                ResultStreamHandlerStatus::Paused => {
                    Ok(())
                },
                ResultStreamHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Pause Handler, Handler already Stopped");
                },            
                ResultStreamHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                },
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {
        log::debug!("Stopping RedisResultStreamHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                ResultStreamHandlerStatus::Uninitialized => {
                    anyhow::bail!("Handler not initialized, current status: Uninitialized");
                },
                ResultStreamHandlerStatus::Running => {
                    *status = ResultStreamHandlerStatus::Stopped;
                    Ok(())
                },
                ResultStreamHandlerStatus::Paused => {
                    *status = ResultStreamHandlerStatus::Stopped;
                    self.notifier.notify_one();
                    Ok(())
                },
                ResultStreamHandlerStatus::Stopped => {
                    Ok(())
                },            
                ResultStreamHandlerStatus::Error => {
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
    settings: RedisResultStreamHandlerSettings, 
    status: Arc<RwLock<ResultStreamHandlerStatus>>, 
    notify: Arc<Notify>, result_stream_handler_tx_channel: 
    Sender<ResultStreamHandlerMessage>) 
{
    log::debug!("Starting RedisResultStreamHandler Reader Thread");

    let client_result = redis::Client::open(format!("redis://{}:{}", &settings.host, &settings.port));

    let client = match client_result {
        Ok(client) => {
            log::debug!("Created Redis Client");
            client
        },
        Err(e) => {
            let msg = format!("Client creation error: {:?}", e);
            log::error!("{}", &msg);
            *status.write().await = ResultStreamHandlerStatus::Error;
            match result_stream_handler_tx_channel.send(ResultStreamHandlerMessage::Error(ResultStreamHandlerError::RedisError(e))).await {
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
            *status.write().await = ResultStreamHandlerStatus::Error;
            match result_stream_handler_tx_channel.send(ResultStreamHandlerMessage::Error(ResultStreamHandlerError::RedisError(e))).await {
                Ok(_) => {},
                Err(e) => {
                    log::error!("Error sending error message: {:?}", e);
                }
            }
            return;
        }
    };

    let stream_key = &settings.stream_name;
    let mut stream_last_id = match settings.process_old_entries {
        true => "0-0".to_string(),
        false =>  "$".to_string(),
    };
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
            ResultStreamHandlerStatus::Uninitialized 
            | ResultStreamHandlerStatus::Stopped
            | ResultStreamHandlerStatus::Error => {
                log::debug!("Uninitialized, Stopped, or Error, exiting");
                return;
            },
            ResultStreamHandlerStatus::Paused => {
                log::debug!("Paused, waiting for notify");
                notify.notified().await;
                log::debug!("Notified");
            },
            ResultStreamHandlerStatus::Running => {
                let read_result = read_stream(&mut con, seq.clone(), stream_key, &stream_last_id, &opts).await;
                match read_result {
                    Ok(results) => {
                        for result in results {
                            stream_last_id = result.id.clone();

                            let result_stream_handler_message: ResultStreamHandlerMessage = match result.try_into() {
                                Ok(msg) => msg,
                                Err(e) => {
                                    log::error!("Error converting RedisStreamReadResult to ResultStreamHandlerMessage: {:?}", e);
                                    ResultStreamHandlerMessage::Error(ResultStreamHandlerError::ConversionError)
                                }
                            };

                            match result_stream_handler_tx_channel.send(result_stream_handler_message).await {
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
                                                error: Some(ResultStreamHandlerError::InvalidStreamData),
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
                                        error: Some(ResultStreamHandlerError::InvalidStreamData),
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
                                error: Some(ResultStreamHandlerError::InvalidStreamData),
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
                        error: Some(ResultStreamHandlerError::InvalidStreamData),
                    });   
                }
            };
        }
    }

    Ok(records)
}