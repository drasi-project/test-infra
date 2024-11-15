use std::sync::Arc;

use async_trait::async_trait;
use test_runner::script_source::{SourceChangeEvent, SourceChangeEventPayload, SourceChangeEventSourceInfo};
use tokio::sync::{mpsc::{Receiver, Sender}, Notify, RwLock};

use crate::config::TestBeaconSourceChangeQueueReaderConfig;

use super::{SourceChangeQueueReader, SourceChangeQueueReaderMessage, SourceChangeQueueReaderStatus, SourceChangeQueueRecord};

#[derive(Clone, Debug)]
pub struct TestBeaconSourceChangeQueueReaderSettings {
    pub interval_ns: u64,
    pub record_count: usize,
    pub source_id: String,
}

impl TestBeaconSourceChangeQueueReaderSettings {
    pub fn new(config: &TestBeaconSourceChangeQueueReaderConfig, source_id: String) -> anyhow::Result<Self> {

        let interval_ns = config.interval_ns.unwrap_or(1000000000) as u64;
        let record_count = config.record_count.unwrap_or(100) as usize;
        
        Ok(TestBeaconSourceChangeQueueReaderSettings {
            interval_ns,
            record_count,
            source_id,
        })
    }
}

#[allow(dead_code)]
pub struct TestBeaconSourceChangeQueueReader {
    notifier: Arc<Notify>,
    records_generated: Arc<RwLock<u64>>,
    settings: TestBeaconSourceChangeQueueReaderSettings,
    status: Arc<RwLock<SourceChangeQueueReaderStatus>>,
}

impl TestBeaconSourceChangeQueueReader {
    pub async fn new<S: Into<String>>(config: TestBeaconSourceChangeQueueReaderConfig, source_id: S) -> anyhow::Result<Box<dyn SourceChangeQueueReader + Send + Sync>> {
        log::debug!("Creating TestBeaconSourceChangeQueueReader from config {:?}", config);

        let settings = TestBeaconSourceChangeQueueReaderSettings::new(&config,source_id.into())?;
        log::trace!("Creating TestBeaconSourceChangeQueueReader with settings {:?}", settings);

        let notifier = Arc::new(Notify::new());
        let records_generated = Arc::new(RwLock::new(0));
        let status = Arc::new(RwLock::new(SourceChangeQueueReaderStatus::Uninitialized));
        
        Ok(Box::new(TestBeaconSourceChangeQueueReader {
            notifier,
            records_generated,
            settings,
            status,
        }))
    }
}

#[async_trait]
impl SourceChangeQueueReader for TestBeaconSourceChangeQueueReader {
    async fn init(&self) -> anyhow::Result<Receiver<SourceChangeQueueReaderMessage>> {

        let mut status = self.status.write().await;
        match *status {
            SourceChangeQueueReaderStatus::Uninitialized => {
                let (change_tx_channel, change_rx_channel) = tokio::sync::mpsc::channel(100);
                
                *status = SourceChangeQueueReaderStatus::Paused;

                tokio::spawn(reader_thread(self.settings.clone(), self.status.clone(), self.notifier.clone(), change_tx_channel));

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

async fn reader_thread(settings: TestBeaconSourceChangeQueueReaderSettings, status: Arc<RwLock<SourceChangeQueueReaderStatus>>, notify: Arc<Notify>, change_tx_channel: Sender<SourceChangeQueueReaderMessage>) {

    let mut seq: usize = 0;

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

                    // If we have generated all the records, do nothing.
                    // Otherwise, generate a new record after the specified interval.
                    if seq < settings.record_count {
                        tokio::time::sleep(tokio::time::Duration::from_nanos(settings.interval_ns)).await;

                        let sce = SourceChangeEvent {  
                            op: "u".to_string(), 
                            ts_ms: 1724694923060, 
                            schema: "".to_string(), 
                            payload: SourceChangeEventPayload { 
                                source: SourceChangeEventSourceInfo {  
                                    lsn: 2, 
                                    ts_ms: 1724694923060, 
                                    ts_sec: 1724694923, 
                                    db: "facilities".to_string(), 
                                    table: "node".to_string()
                                }, 
                                before: serde_json::from_str(r#"{ "id": "room_01_01_02", "labels": ["Room"], "properties": { "name": "Room 01_01_02",  "temp": 72, "humidity": 42, "co2": 500}"#).unwrap(), 
                                after: serde_json::from_str(r#"{ "id": "room_01_01_02", "labels": ["Room"], "properties": { "name": "Room 01_01_02", "temp": 71, "humidity": 40, "co2": 495}}"#).unwrap()
                            }
                        };

                        let scr = SourceChangeQueueRecord {
                            change_events: vec![serde_json::to_value(sce).unwrap()],
                            dequeue_time_ns: 0,
                            enqueue_time_ns: 0,
                            id: "".to_string(),
                            seq: seq,
                            traceid: "".to_string(),
                            traceparent: "".to_string(),
                        };

                        match change_tx_channel.send(SourceChangeQueueReaderMessage::QueueRecord(scr)).await {
                            Ok(_) => {
                                seq += 1;
                            },
                            Err(e) => {
                                log::error!("Error sending change event to channel: {}", e);
                                return;
                            }
                        }
                    }
                };        
            },
        }
    }
}