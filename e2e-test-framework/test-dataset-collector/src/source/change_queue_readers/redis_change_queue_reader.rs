use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use test_runner::script_source::SourceChangeEvent;

use crate::config::RedisSourceChangeQueueReaderConfig;

use super::SourceChangeQueueReader;

#[derive(Debug)]
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
            .unwrap_or_else(|| format!("{}_change", source_id));

        Ok(RedisSourceChangeQueueReaderSettings {
            host,
            port,
            queue_name,
            source_id,
        })
    }
}

pub struct RedisSourceChangeQueueReader {
    _client: Client,
    con: MultiplexedConnection,
    index: usize,
    settings: RedisSourceChangeQueueReaderSettings,
}

impl RedisSourceChangeQueueReader {
    pub async fn new<S: Into<String>>(config: RedisSourceChangeQueueReaderConfig, source_id: S) -> anyhow::Result<Box<dyn SourceChangeQueueReader + Send + Sync>> {
        log::debug!("Creating RedisSourceChangeQueueReader from config {:?}", config);

        let settings = RedisSourceChangeQueueReaderSettings::new(&config,source_id.into())?;
        log::trace!("Creating RedisSourceChangeQueueReader with settings {:?}", settings);

        let _client = redis::Client::open(format!("redis://{}:{}/", &settings.host, &settings.port))?;
        let mut con = _client.get_multiplexed_async_connection().await?;

        // Get the current index of the last record in the redis queue
        let index:usize = con.llen(&settings.queue_name).await?;        

        Ok(Box::new(RedisSourceChangeQueueReader {
            _client,
            con,
            index,
            settings,
        }))
    }
}  

#[async_trait]
impl SourceChangeQueueReader for RedisSourceChangeQueueReader {
    async fn get_next_change(&mut self) -> anyhow::Result<SourceChangeEvent> {
        log::trace!("RedisSourceChangeQueueReader - get_next_change");

        let source_change_string: String = self.con.lindex(&self.settings.queue_name, self.index as isize).await?;

        let source_change = SourceChangeEvent::try_from(&source_change_string)?;

        self.index += 1;

        Ok(source_change)
    }
}