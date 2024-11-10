use async_trait::async_trait;
use test_runner::script_source::{SourceChangeEvent, SourceChangeEventPayload, SourceChangeEventSourceInfo};

use crate::config::TestBeaconSourceChangeQueueReaderConfig;

use super::SourceChangeQueueReader;

#[derive(Debug)]
pub struct TestBeaconSourceChangeQueueReaderSettings {
    pub interval_ns: u64,
    pub record_count: u64,
    pub source_id: String,
}

impl TestBeaconSourceChangeQueueReaderSettings {
    pub fn new(config: &TestBeaconSourceChangeQueueReaderConfig, source_id: String) -> anyhow::Result<Self> {

        let interval_ns = config.interval_ns.unwrap_or(1000000000) as u64;
        let record_count = config.record_count.unwrap_or(100) as u64;
        
        Ok(TestBeaconSourceChangeQueueReaderSettings {
            interval_ns,
            record_count,
            source_id,
        })
    }
}

pub struct TestBeaconSourceChangeQueueReader {
    records_generated: u64,
    settings: TestBeaconSourceChangeQueueReaderSettings,
}

impl TestBeaconSourceChangeQueueReader {
    pub async fn new<S: Into<String>>(config: TestBeaconSourceChangeQueueReaderConfig, source_id: S) -> anyhow::Result<Box<dyn SourceChangeQueueReader + Send + Sync>> {
        log::debug!("Creating TestBeaconSourceChangeQueueReader from config {:?}", config);

        let settings = TestBeaconSourceChangeQueueReaderSettings::new(&config,source_id.into())?;
        log::trace!("Creating TestBeaconSourceChangeQueueReader with settings {:?}", settings);

        Ok(Box::new(TestBeaconSourceChangeQueueReader {
            records_generated:0,
            settings,
        }))
    }
}  

#[async_trait]
impl SourceChangeQueueReader for TestBeaconSourceChangeQueueReader {
    async fn get_next_change(&mut self) -> anyhow::Result<SourceChangeEvent> {
        log::trace!("RedisSourceChangeQueueReader - get_next_change");

        // If we have generated all the records, return an None.
        // Otherwise, generate a new record after the specified interval.
        if self.records_generated >= self.settings.record_count {
            anyhow::bail!("TestBeaconSourceChangeQueueReader.get_next_change should never return");
        } else {
            tokio::time::sleep(tokio::time::Duration::from_nanos(self.settings.interval_ns as u64)).await;

            self.records_generated += 1;

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
                    before: serde_json::from_str(r#"{ "id": "room_01_01_02", "labels": ["Room"], "properties": { "name": "Room 01_01_02",  "temp": 72, "humidity": 42, "co2": 500}"#)?, 
                    after: serde_json::from_str(r#"{ "id": "room_01_01_02", "labels": ["Room"], "properties": { "name": "Room 01_01_02", "temp": 71, "humidity": 40, "co2": 495}}"#)?
                }
            };

            Ok(sce)
        }
    }
}