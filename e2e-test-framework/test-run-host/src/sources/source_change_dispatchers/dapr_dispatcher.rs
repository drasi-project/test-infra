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

use drasi_comms_abstractions::comms::{Headers, Publisher};
use drasi_comms_dapr::comms::DaprHttpPublisher;

use test_data_store::{scripts::SourceChangeEvent, test_repo_storage::models::DaprSourceChangeDispatcherDefinition, test_run_storage::TestRunSourceStorage};

use super::SourceChangeDispatcher;

#[derive(Debug)]
pub struct DaprSourceChangeDispatcherSettings {
    pub host: String,
    pub port: u16,
    pub pubsub_name: String,
    pub pubsub_topic: String,
}

impl DaprSourceChangeDispatcherSettings {
    pub fn new(def: &DaprSourceChangeDispatcherDefinition, source_id: String) -> anyhow::Result<Self> {
        Ok(Self {
            host: def.host.clone().unwrap_or("127.0.0.1".to_string()),
            port: def.port.unwrap_or(3500),
            pubsub_name: def.pubsub_name.clone().unwrap_or("drasi-pubsub".to_string()),
            pubsub_topic: def.pubsub_topic.clone().unwrap_or(format!("{}-change", source_id)),
        })
    }
}

pub struct DaprSourceChangeDispatcher {
    _settings: DaprSourceChangeDispatcherSettings,
    publisher: Option<DaprHttpPublisher>,
}

impl DaprSourceChangeDispatcher {
    pub fn new(def: &DaprSourceChangeDispatcherDefinition, output_storage: &TestRunSourceStorage) -> anyhow::Result<Self> {
        log::debug!("Creating DaprSourceChangeDispatcher from {:?}, ", def);

        let source_id = output_storage.id.test_source_id.clone();
        let settings = DaprSourceChangeDispatcherSettings::new(def, source_id)?;
        log::trace!("Creating DaprSourceChangeDispatcher with settings {:?}, ", settings);

        let publisher = DaprHttpPublisher::new(
            settings.host.clone(),
            settings.port,
            settings.pubsub_name.clone(),
            settings.pubsub_topic.clone(),
        );

        Ok(DaprSourceChangeDispatcher {
            _settings: settings,
            publisher: Some(publisher),
        })
    }
}  

#[async_trait]
impl SourceChangeDispatcher for DaprSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        self.publisher = None;
        Ok(())
    }

    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {

        log::trace!("Dispatch source change events");

        let publisher = self.publisher.as_mut().unwrap();

        let data = serde_json::to_value(events)?;

        let headers: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        // let traceparent = "000".to_string();
        // headers.insert("traceparent".to_string(), traceparent.clone());
        let _headers = Headers::new(headers);

        match publisher.publish(data, _headers).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = format!("Error dispatching source change event: {:?}", e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            }
        }
    }
}