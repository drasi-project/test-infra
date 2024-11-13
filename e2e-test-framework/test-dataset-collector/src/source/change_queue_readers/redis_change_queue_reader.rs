use async_trait::async_trait;
use redis::{aio::MultiplexedConnection, streams::{StreamId, StreamKey, StreamReadOptions, StreamReadReply}, AsyncCommands, Client, RedisResult};
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
            .unwrap_or_else(|| format!("{}-change", source_id));

        Ok(RedisSourceChangeQueueReaderSettings {
            host,
            port,
            queue_name,
            source_id,
        })
    }
}

pub struct RedisSourceChangeQueueReader {
    cancel: bool,
    #[allow(dead_code)]
    client: Client,
    con: MultiplexedConnection,
    #[allow(dead_code)]
    settings: RedisSourceChangeQueueReaderSettings,
    stream_key: String,
    stream_last_id: String,
}

impl RedisSourceChangeQueueReader {
    pub async fn new<S: Into<String>>(config: RedisSourceChangeQueueReaderConfig, source_id: S) -> anyhow::Result<Box<dyn SourceChangeQueueReader + Send + Sync>> {
        log::debug!("Creating RedisSourceChangeQueueReader from config {:?}", config);

        let settings = RedisSourceChangeQueueReaderSettings::new(&config,source_id.into())?;
        log::trace!("Creating RedisSourceChangeQueueReader with settings {:?}", settings);

        let stream_key = settings.queue_name.clone();
        let stream_last_id = "1731333379331-0".to_string();

        let client = redis::Client::open(format!("redis://{}:{}", &settings.host, &settings.port))?;
        let multiplexed_connection = client.get_multiplexed_async_connection().await;
        let con = multiplexed_connection?;

        Ok(Box::new(RedisSourceChangeQueueReader {
            cancel: false,
            client,
            con,            
            settings,
            stream_key,
            stream_last_id,
        }))
    }
}

// #[async_trait]
// impl SourceChangeQueueReader for RedisSourceChangeQueueReader {
//     async fn get_next_change(&mut self) -> anyhow::Result<SourceChangeEvent> {
//         log::trace!("RedisSourceChangeQueueReader - get_next_change");

//         self.cancel = false;

//         // Process the result
//         // while !self.cancel {

//             let opts = StreamReadOptions::default().count(1).block(1000);

//             let result: RedisResult<StreamReadReply> = self.con.xread_options(&[&self.stream_key], &[&self.stream_last_id], &opts).await;
    
//             // Read the next item from the stream
//             match result {
//                 Ok(reply) => {

//                     // if reply.keys.len() == 0 {
//                     //     continue;
//                     // }

//                     let ids = &reply.keys[0].ids;
//                     let StreamId { id, map } = &ids[0];

//                     // Update last_id to the current entry’s ID
//                     self.stream_last_id = id.to_string();
    
//                     match map.get("data") {
//                         Some(data) => {
//                             match data {
//                                 redis::Value::BulkString(data) => {
//                                     let source_change_string = String::from_utf8(data.clone())?;
//                                     SourceChangeEvent::try_from(&source_change_string)?
//                                 },
//                                 _ => {
//                                     anyhow::bail!("No data found in stream entry")
//                                 }
//                             }
//                         },
//                         None => {
//                             anyhow::bail!("No data found in stream entry")
//                         }
//                     };
//                 },
//                 Err(e) => return Err(anyhow::Error::new(e)),
//             };
//         // }

//         anyhow::bail!("No data found in stream entry")
//     }

//     // async fn cancel_get_next_change(&mut self) -> anyhow::Result<()> {
//     //     log::trace!("RedisSourceChangeQueueReader - cancel_get_next_change");

//     //     self.block = false;

//     //     Ok(())
//     // }
// }

#[async_trait]
impl SourceChangeQueueReader for RedisSourceChangeQueueReader {
    async fn get_next_change(&mut self) -> anyhow::Result<SourceChangeEvent> {
        println!("Testing Redis Streams!");

        // let host: String = "localhost".to_string();
        // let port: u16 = 6379_u16;
        // let client: Client = redis::Client::open(format!("redis://{}:{}", host, port)).unwrap();
        // let con_result:Result<MultiplexedConnection, redis::RedisError>  = client.get_multiplexed_async_connection().await;
        // let mut con = match con_result {
        //     Ok(con) => {
        //         println!("Connected to Redis");
        //         con
        //     },
        //     Err(e) => {
        //         anyhow::bail!("Connection Error: {:?}", e);
        //     }
        // };
        
        let message_count:usize = 1;
        let block_ms:usize = 1000;
        let stream_key: String = "hello-world-change".to_string();
        let stream_last_id: String = "1731333379331-0".to_string();
        // let group_name: String = "drasi".to_string();
        // let consumer_name: String = "drasi".to_string();
    
        let opts: StreamReadOptions = StreamReadOptions::default().count(message_count).block(block_ms);
        // let opts = StreamReadOptions::default().count(message_count).block(block_ms).noack();
        // let opts = StreamReadOptions::default().count(message_count).block(block_ms).noack().group(group_name, consumer_name);
    
        let result: RedisResult<StreamReadReply> = self.con.xread_options(&[&stream_key], &[&stream_last_id], &opts).await;
    
        // Read the next item from the stream
        match result {
            Ok(reply) => {
    
                let StreamKey { key , ids   } = &reply.keys[0];
                // let ids = &reply.keys[0].ids;
                let StreamId { id, map } = &ids[0];
    
                println!("Stream Key: {}", key);
                println!("Stream ID: {}", id);
    
                match map.get("data") {
                    Some(data) => {
                        match data {
                            redis::Value::BulkString(data) => {
                                let source_change_string = String::from_utf8(data.clone());
                                match source_change_string {
                                    Ok(s) => {
                                        println!("Data: {}", s.clone());
                                        Ok(SourceChangeEvent::try_from(&s)?)
                                    },
                                    Err(e) => {
                                        anyhow::bail!("Error: {:?}", e);
                                    }
                                }
                            },
                            _ => {
                                anyhow::bail!("Data is not a BulkString");
                            }
                        }
                    },
                    None => {
                        anyhow::bail!("Data is not available");
                    }
                }
            },
            Err(e) => {
                anyhow::bail!("XREAD Error: {:?}", e);
            }   
        }
    }

    // async fn cancel_get_next_change(&mut self) -> anyhow::Result<()> {
    //     log::trace!("RedisSourceChangeQueueReader - cancel_get_next_change");

    //     self.block = false;

    //     Ok(())
    // }
}

async fn reader_thread(settings: RedisSourceChangeQueueReaderSettings, mut reader_tx_channel: Sender<SourceChangeReadererMessage>) {
}
