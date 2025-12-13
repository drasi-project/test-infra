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

use anyhow::Ok;
use async_trait::async_trait;

use test_data_store::test_repo_storage::models::RecordCountStopTriggerDefinition;

use crate::reactions::{
    reaction_observer::ReactionObserverMetrics, reaction_output_handler::ReactionHandlerStatus,
};

use super::StopTrigger;

#[derive(Debug)]
pub struct RecordCountStopTriggerSettings {
    pub record_count: u64,
}

impl RecordCountStopTriggerSettings {
    pub fn new(cfg: &RecordCountStopTriggerDefinition) -> anyhow::Result<Self> {
        Ok(Self {
            record_count: cfg.record_count,
        })
    }
}

pub struct RecordCountStopTrigger {
    settings: RecordCountStopTriggerSettings,
}

impl RecordCountStopTrigger {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        def: &RecordCountStopTriggerDefinition,
    ) -> anyhow::Result<Box<dyn StopTrigger + Send + Sync>> {
        log::debug!("Creating RecordCountStopTrigger from {def:?}, ");

        let settings = RecordCountStopTriggerSettings::new(def)?;
        log::trace!("Creating RecordCountStopTrigger with settings {settings:?}, ");

        Ok(Box::new(Self { settings }))
    }
}

#[async_trait]
impl StopTrigger for RecordCountStopTrigger {
    async fn is_true(
        &self,
        _handler_status: &ReactionHandlerStatus,
        stats: &ReactionObserverMetrics,
    ) -> anyhow::Result<bool> {
        Ok(stats.reaction_invocation_count >= self.settings.record_count)
    }
}
