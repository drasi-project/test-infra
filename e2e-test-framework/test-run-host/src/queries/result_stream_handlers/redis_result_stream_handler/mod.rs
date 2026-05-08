// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::SystemTime,
};

use async_trait::async_trait;
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamId, StreamReadOptions, StreamReadReply},
    AsyncCommands, RedisResult,
};
use redis_stream_read_result::{RedisStreamReadResult, RedisStreamRecordData};
use test_data_store::{
    test_repo_storage::models::RedisStreamResultStreamHandlerDefinition,
    test_run_storage::TestRunQueryId,
};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Notify, RwLock,
};

use crate::queries::{
    QueryControlSignal, QueryHandlerError, QueryHandlerMessage, QueryHandlerStatus,
    QueryOutputHandler,
};

pub mod redis_stream_read_result;

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct RedisResultStreamHandlerSettings {
    pub host: String,
    pub port: u16,
    pub process_old_entries: bool,
    pub query_id: String,
    pub stream_name: String,
    pub test_run_query_id: TestRunQueryId,
}

impl RedisResultStreamHandlerSettings {
    pub fn new(
        id: TestRunQueryId,
        definition: RedisStreamResultStreamHandlerDefinition,
    ) -> anyhow::Result<Self> {
        Ok(RedisResultStreamHandlerSettings {
            host: definition
                .host
                .clone()
                .unwrap_or_else(|| "127.0.0.1".to_string()),
            port: definition.port.unwrap_or(6379),
            process_old_entries: definition.process_old_entries.unwrap_or(false),
            query_id: id.test_query_id.clone(),
            stream_name: definition
                .stream_name
                .clone()
                .unwrap_or_else(|| format!("{}-results", id.test_query_id.clone())),
            test_run_query_id: id,
        })
    }
}

#[allow(dead_code)]
pub struct RedisResultStreamHandler {
    notifier: Arc<Notify>,
    seq: Arc<AtomicUsize>,
    settings: RedisResultStreamHandlerSettings,
    status: Arc<RwLock<QueryHandlerStatus>>,
}

impl RedisResultStreamHandler {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        id: TestRunQueryId,
        definition: RedisStreamResultStreamHandlerDefinition,
    ) -> anyhow::Result<Box<dyn QueryOutputHandler + Send + Sync>> {
        let settings = RedisResultStreamHandlerSettings::new(id, definition)?;
        log::trace!("Creating RedisResultStreamHandler with settings {settings:?}");

        let notifier = Arc::new(Notify::new());
        let status = Arc::new(RwLock::new(QueryHandlerStatus::Uninitialized));

        Ok(Box::new(Self {
            notifier,
            seq: Arc::new(AtomicUsize::new(0)),
            settings,
            status,
        }))
    }
}

#[async_trait]
impl QueryOutputHandler for RedisResultStreamHandler {
    async fn init(&self) -> anyhow::Result<Receiver<QueryHandlerMessage>> {
        log::debug!("Initializing RedisResultStreamHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                QueryHandlerStatus::Uninitialized => {
                    let (handler_tx_channel, handler_rx_channel) = tokio::sync::mpsc::channel(100);

                    *status = QueryHandlerStatus::Paused;

                    tokio::spawn(reader_thread(
                        self.seq.clone(),
                        self.settings.clone(),
                        self.status.clone(),
                        self.notifier.clone(),
                        handler_tx_channel,
                    ));

                    Ok(handler_rx_channel)
                }
                QueryHandlerStatus::Running => {
                    anyhow::bail!("Cant Init Handler, Handler currently Running");
                }
                QueryHandlerStatus::Paused => {
                    anyhow::bail!("Cant Init Handler, Handler currently Paused");
                }
                QueryHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Init Handler, Handler currently Stopped");
                }
                QueryHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                }
                QueryHandlerStatus::BootstrapStarted
                | QueryHandlerStatus::BootstrapComplete
                | QueryHandlerStatus::Deleted => {
                    anyhow::bail!("Handler in invalid state for init: {:?}", *status);
                }
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn start(&self) -> anyhow::Result<()> {
        log::debug!("Starting RedisResultStreamHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                QueryHandlerStatus::Uninitialized => {
                    anyhow::bail!("Can't Start Handler, Handler Uninitialized");
                }
                QueryHandlerStatus::Running => Ok(()),
                QueryHandlerStatus::Paused => {
                    *status = QueryHandlerStatus::Running;
                    self.notifier.notify_one();
                    Ok(())
                }
                QueryHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Start Handler, Handler already Stopped");
                }
                QueryHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                }
                QueryHandlerStatus::BootstrapStarted
                | QueryHandlerStatus::BootstrapComplete
                | QueryHandlerStatus::Deleted => {
                    anyhow::bail!("Handler in invalid state for start: {:?}", *status);
                }
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn pause(&self) -> anyhow::Result<()> {
        log::debug!("Pausing RedisResultStreamHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                QueryHandlerStatus::Uninitialized => {
                    anyhow::bail!("Cant Pause Handler, Handler Uninitialized");
                }
                QueryHandlerStatus::Running => {
                    *status = QueryHandlerStatus::Paused;
                    Ok(())
                }
                QueryHandlerStatus::Paused => Ok(()),
                QueryHandlerStatus::Stopped => {
                    anyhow::bail!("Cant Pause Handler, Handler already Stopped");
                }
                QueryHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                }
                QueryHandlerStatus::BootstrapStarted
                | QueryHandlerStatus::BootstrapComplete
                | QueryHandlerStatus::Deleted => {
                    anyhow::bail!("Handler in invalid state for pause: {:?}", *status);
                }
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn stop(&self) -> anyhow::Result<()> {
        log::debug!("Stopping RedisResultStreamHandler");

        if let Ok(mut status) = self.status.try_write() {
            match *status {
                QueryHandlerStatus::Uninitialized => {
                    anyhow::bail!("Handler not initialized, current status: Uninitialized");
                }
                QueryHandlerStatus::Running => {
                    *status = QueryHandlerStatus::Stopped;
                    Ok(())
                }
                QueryHandlerStatus::Paused => {
                    *status = QueryHandlerStatus::Stopped;
                    self.notifier.notify_one();
                    Ok(())
                }
                QueryHandlerStatus::Stopped => Ok(()),
                QueryHandlerStatus::Error => {
                    anyhow::bail!("Handler in Error state");
                }
                QueryHandlerStatus::BootstrapStarted
                | QueryHandlerStatus::BootstrapComplete
                | QueryHandlerStatus::Deleted => {
                    anyhow::bail!("Handler in invalid state for stop: {:?}", *status);
                }
            }
        } else {
            anyhow::bail!("Could not acquire status lock");
        }
    }

    async fn status(&self) -> QueryHandlerStatus {
        *self.status.read().await
    }
}

async fn reader_thread(
    seq: Arc<AtomicUsize>,
    settings: RedisResultStreamHandlerSettings,
    status: Arc<RwLock<QueryHandlerStatus>>,
    notify: Arc<Notify>,
    result_stream_handler_tx_channel: Sender<QueryHandlerMessage>,
) {
    log::debug!("Starting RedisResultStreamHandler Reader Thread");

    let client_result =
        redis::Client::open(format!("redis://{}:{}", &settings.host, &settings.port));

    let client = match client_result {
        Ok(client) => {
            log::debug!("Created Redis Client");
            client
        }
        Err(e) => {
            let msg = format!("Client creation error: {e:?}");
            log::error!("{}", &msg);
            *status.write().await = QueryHandlerStatus::Error;
            match result_stream_handler_tx_channel
                .send(QueryHandlerMessage::Error(QueryHandlerError::new(
                    format!("Redis error: {e:?}"),
                    false,
                )))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    log::error!("Error sending error message: {e:?}");
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
        }
        Err(e) => {
            let msg = format!("Connection Error: {e:?}");
            log::error!("{}", &msg);
            *status.write().await = QueryHandlerStatus::Error;
            match result_stream_handler_tx_channel
                .send(QueryHandlerMessage::Error(QueryHandlerError::new(
                    format!("Redis error: {e:?}"),
                    false,
                )))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    log::error!("Error sending error message: {e:?}");
                }
            }
            return;
        }
    };

    let stream_key = &settings.stream_name;
    let mut stream_last_id = match settings.process_old_entries {
        true => "0-0".to_string(),
        false => "$".to_string(),
    };
    let opts = StreamReadOptions::default().count(1).block(5000);

    loop {
        let current_status = {
            if let Ok(status) = status.try_read() {
                *status
            } else {
                log::warn!("Could not acquire status lock in loop");
                continue;
            }
        };

        match current_status {
            QueryHandlerStatus::Uninitialized | QueryHandlerStatus::Error => {
                log::error!("Reader thread Uninitialized or Error, shutting down");
                return;
            }
            QueryHandlerStatus::Stopped => {
                log::debug!(
                    "Reader thread Stopped, sending StreamStopping message and shutting down"
                );

                match result_stream_handler_tx_channel
                    .send(QueryHandlerMessage::Control(QueryControlSignal::Stop))
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("Reader thread error sending StreamStopping message: {e:?}");
                    }
                }
                return;
            }
            QueryHandlerStatus::Paused => {
                log::debug!("Reader thread Paused, waiting to be notified");
                notify.notified().await;
                log::debug!("Notified");
            }
            QueryHandlerStatus::Running
            | QueryHandlerStatus::BootstrapStarted
            | QueryHandlerStatus::BootstrapComplete
            | QueryHandlerStatus::Deleted => {
                let read_result =
                    read_stream(&mut con, seq.clone(), stream_key, &stream_last_id, &opts).await;
                match read_result {
                    Ok(results) => {
                        for result in results {
                            stream_last_id = result.id.clone();

                            let result_stream_handler_message: QueryHandlerMessage = match result
                                .try_into()
                            {
                                Ok(msg) => msg,
                                Err(e) => {
                                    log::error!("Error converting RedisStreamReadResult to QueryHandlerMessage: {e:?}");
                                    QueryHandlerMessage::Error(QueryHandlerError::new(
                                        format!("Conversion error: {e:?}"),
                                        false,
                                    ))
                                }
                            };

                            match result_stream_handler_tx_channel
                                .send(result_stream_handler_message)
                                .await
                            {
                                Ok(_) => {}
                                Err(e) => match e {
                                    tokio::sync::mpsc::error::SendError(msg) => {
                                        log::error!("Error sending change message: {msg:?}");
                                    }
                                },
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error reading from Redis stream: {e:?}");
                    }
                }
            }
        }
    }
}

async fn read_stream(
    con: &mut MultiplexedConnection,
    seq: Arc<AtomicUsize>,
    stream_key: &str,
    stream_last_id: &str,
    read_options: &StreamReadOptions,
) -> anyhow::Result<Vec<RedisStreamReadResult>> {
    let xread_result: RedisResult<StreamReadReply> = con
        .xread_options(&[stream_key], &[stream_last_id], read_options)
        .await;

    let xread_result = match xread_result {
        Ok(xread_result) => xread_result,
        Err(e) => {
            return Err(anyhow::anyhow!("Error reading from stream: {e:?}"));
        }
    };

    let dequeue_time_ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    let mut records: Vec<RedisStreamReadResult> = Vec::new();

    for key in xread_result.keys {
        let ids = &key.ids;

        for id in ids {
            let StreamId { id, map } = &id;

            let id = id.to_string();
            let enqueue_time_ns = id.split('-').next().unwrap().parse::<u64>().unwrap() * 1_000_000;

            match map.get("data") {
                Some(data) => match data {
                    redis::Value::BulkString(bs_data) => {
                        match String::from_utf8(bs_data.to_vec()) {
                            Ok(s) => match RedisStreamRecordData::try_from(&s) {
                                Ok(record) => {
                                    records.push(RedisStreamReadResult {
                                        id,
                                        seq: seq.fetch_add(1, Ordering::SeqCst),
                                        enqueue_time_ns,
                                        dequeue_time_ns,
                                        record: Some(record),
                                        error: None,
                                    });
                                }
                                Err(e) => {
                                    log::error!("Error: {e:?}");
                                    records.push(RedisStreamReadResult {
                                        id,
                                        seq: seq.fetch_add(1, Ordering::SeqCst),
                                        enqueue_time_ns,
                                        dequeue_time_ns,
                                        record: None,
                                        error: Some(QueryHandlerError::new(
                                            "Invalid stream data".to_string(),
                                            false,
                                        )),
                                    });
                                }
                            },
                            Err(e) => {
                                log::error!("Error: {e:?}");
                                records.push(RedisStreamReadResult {
                                    id,
                                    seq: seq.fetch_add(1, Ordering::SeqCst),
                                    enqueue_time_ns,
                                    dequeue_time_ns,
                                    record: None,
                                    error: Some(QueryHandlerError::new(
                                        "Data is not a BulkString".to_string(),
                                        false,
                                    )),
                                });
                            }
                        }
                    }
                    _ => {
                        log::error!("Data is not a BulkString");
                        records.push(RedisStreamReadResult {
                            id,
                            seq: seq.fetch_add(1, Ordering::SeqCst),
                            enqueue_time_ns,
                            dequeue_time_ns,
                            record: None,
                            error: Some(QueryHandlerError::new(
                                "Data is not a BulkString".to_string(),
                                false,
                            )),
                        });
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
                        error: Some(QueryHandlerError::new(
                            "No data found in stream entry".to_string(),
                            false,
                        )),
                    });
                }
            };
        }
    }

    Ok(records)
}
