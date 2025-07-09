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
use chrono::prelude::*;

use test_data_store::{scripts::SourceChangeEvent, test_repo_storage::models::ConsoleSourceChangeDispatcherDefinition, test_run_storage::TestRunSourceStorage};

use super::SourceChangeDispatcher;

#[derive(Debug)]

pub struct ConsoleSourceChangeDispatcherSettings {
    pub date_time_format: String,
}

impl ConsoleSourceChangeDispatcherSettings {
    pub fn new(def: &ConsoleSourceChangeDispatcherDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            date_time_format: def.date_time_format.clone().unwrap_or("%Y-%m-%d %H:%M:%S%.f".to_string()),
        })
    }
}

pub struct ConsoleSourceChangeDispatcher {
    settings: ConsoleSourceChangeDispatcherSettings,
}

impl ConsoleSourceChangeDispatcher {
    pub fn new(def: &ConsoleSourceChangeDispatcherDefinition, _output_storage: &TestRunSourceStorage) -> anyhow::Result<Self> {
        log::debug!("Creating ConsoleSourceChangeDispatcher from {:?}, ", def);

        let settings = ConsoleSourceChangeDispatcherSettings::new(def)?;
        log::trace!("Creating ConsoleSourceChangeDispatcher with settings {:?}, ", settings);

        Ok(Self { settings })
    }
}  

#[async_trait]
impl SourceChangeDispatcher for ConsoleSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {

        log::trace!("Dispatch source change events");

        let time = Local::now().format(&self.settings.date_time_format);

        let event_list = events
            .iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join(",");
        
        println!("ConsoleSourceChangeDispatcher - Time: {}, SourceChangeEvents: [{}]", time, event_list);

        Ok(())
    }
}