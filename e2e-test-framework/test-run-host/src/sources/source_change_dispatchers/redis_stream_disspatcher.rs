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

use async_trait::async_trait;
use hex;
use uuid::Uuid;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};
use serde::Serialize;

use test_data_store::{scripts::SourceChangeEvent, test_repo_storage::models::RedisStreamSourceChangeDispatcherDefinition, test_run_storage::TestRunSourceStorage};

use super::SourceChangeDispatcher;


#[derive(Debug, Clone, Serialize)]
pub struct SourceChangeQueueEvent {
    pub data: Vec<SourceChangeEvent>,
    pub datacontenttype: String,
    pub id: String,
    pub pubsubname: String,
    pub source: String,
    pub specversion: String,
    pub time: String,
    pub topic: String,
    #[serde(default)]
    pub traceid: String,
    pub traceparent: String,
    #[serde(default)]
    pub tracestate: String,
    #[serde(rename = "type")]
    pub event_type: String,
}

impl SourceChangeQueueEvent {
    /// Creates a new SourceChangeQueueEvent with automatically generated unique trace IDs
    pub fn new(
        data: Vec<SourceChangeEvent>,
        id: String,
        source_id: String,        
        time: String,
        traceid: String,
        traceparent: String,
    ) -> Self {
        SourceChangeQueueEvent {
            data,
            datacontenttype: "application/json".to_string(),
            id,
            pubsubname: "drasi-pubsub".to_string(),
            source: "drasi-test-service".to_string(),
            specversion: "1.0".to_string(),
            time,
            topic: source_id,
            traceid,
            traceparent,
            tracestate: "".to_string(),
            event_type: "com.dapr.event.sent".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisStreamSourceChangeDispatcherSettings {
    pub host: String,
    pub port: u16,
    pub stream_name: String,
}

impl RedisStreamSourceChangeDispatcherSettings {
    pub fn new(def: &RedisStreamSourceChangeDispatcherDefinition, source_id: String) -> anyhow::Result<Self> {
        Ok(Self {
            host: def.host.clone().unwrap_or("127.0.0.1".to_string()),
            port: def.port.unwrap_or(6379),
            stream_name: def.stream_name.clone().unwrap_or(format!("{}-change", source_id)),
        })
    }
}

pub struct RedisStreamSourceChangeDispatcher {
    connection: Option<MultiplexedConnection>,
    client: Option<Client>,
    rng: StdRng,
    settings: RedisStreamSourceChangeDispatcherSettings,
}

impl RedisStreamSourceChangeDispatcher {
    pub async fn new(def: &RedisStreamSourceChangeDispatcherDefinition, output_storage: &TestRunSourceStorage) -> anyhow::Result<Self> {
        log::debug!("Creating RedisStreamSourceChangeDispatcher from {:?}", def);

        let source_id = output_storage.id.test_source_id.clone();
        let settings = RedisStreamSourceChangeDispatcherSettings::new(def, source_id)?;
        log::trace!("Creating RedisStreamSourceChangeDispatcher with settings {:?}", settings);

        let redis_url = format!("redis://{}:{}", &settings.host, &settings.port);
        let client = match redis::Client::open(redis_url) {
            Ok(client) => {
                log::debug!("Created Redis Client");
                client
            },
            Err(e) => {
                anyhow::bail!("Failed to create Redis client: {:?}", e);
            }
        };
    
        let connection = match client.get_multiplexed_async_connection().await {
            Ok(con) => {
                log::debug!("Connected to Redis");
                con
            },
            Err(e) => {
                anyhow::bail!("Failed to connect to Redis: {:?}", e);
            }
        };

        Ok(RedisStreamSourceChangeDispatcher {
            connection: Some(connection),
            client: Some(client),
            rng: StdRng::from_os_rng(),
            settings,
        })
    }
}

#[async_trait]
impl SourceChangeDispatcher for RedisStreamSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        // Connection will be closed when dropped
        self.connection = None;
        self.client = None;
        Ok(())
    }

    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {
        log::trace!("Dispatching {} source change events to Redis stream", events.len());
                
        // Generate trace ID (16 random bytes for a root span)
        let mut trace_id_bytes = [0u8; 16];
        self.rng.fill(&mut trace_id_bytes);
        let trace_id_hex = hex::encode(trace_id_bytes);
        
        // Generate span ID (8 random bytes)
        let mut span_id_bytes = [0u8; 8];
        self.rng.fill(&mut span_id_bytes);
        let span_id_hex = hex::encode(span_id_bytes);
        
        // Format traceparent according to W3C trace context spec
        // Format: 00-<trace-id>-<span-id>-01
        // where 00 is version and 01 is flags (sampled)
        let traceparent = format!("00-{}-{}-01", trace_id_hex, span_id_hex);
        
        // For a root span, traceid follows the same format in this schema
        let traceid = traceparent.clone();
        
        // Generate a unique ID for the event
        let id = Uuid::new_v4().to_string();
        
        // Get current time in ISO 8601 format
        let now = chrono::Utc::now();
        let time = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
                
        let q_event = SourceChangeQueueEvent::new(
            events.iter().cloned().cloned().collect(),
            id,
            self.settings.stream_name.clone(),
            time,
            traceid,
            traceparent,
        );
                
        let q_event_json = serde_json::to_string(&q_event)?;

        let conn = self.connection.as_mut().unwrap();

        let result: String = conn
            .xadd(&self.settings.stream_name, "*", &[("data", &q_event_json)])
            .await?;

        log::trace!("Dispatched source change event to Redis Stream: {}", result);
        
        Ok(())
    }
}