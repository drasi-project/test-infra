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

use std::str::FromStr;

use async_trait::async_trait;

use test_data_store::scripts::SourceChangeEvent;

// pub mod console_change_event_logger;
// pub mod script_change_event_logger;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartTimeMode {
    Live,
    FirstEvent,
    Rebased(u64),
}

impl FromStr for StartTimeMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "live" => Ok(Self::Live),
            "first_event" => Ok(Self::FirstEvent),
            _ => {
                match chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(t) => Ok(Self::Rebased(t.timestamp_nanos_opt().unwrap() as u64)),
                    Err(e) => {
                        anyhow::bail!("Error parsing StartTimeMode - value:{}, error:{}", s, e);
                    }
                }
            }
        }
    }
}

impl std::fmt::Display for StartTimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::FirstEvent => write!(f, "first_event"),
            Self::Rebased(time) => write!(f, "{}", time),
        }
    }
}

impl Default for StartTimeMode {
    fn default() -> Self {
        Self::FirstEvent
    }
}


#[derive(Debug, thiserror::Error)]
pub enum SourceChangeEventLoggerError {
    Io(#[from] std::io::Error),
    Serde(#[from]serde_json::Error),
}

impl std::fmt::Display for SourceChangeEventLoggerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
        }
    }
}

#[async_trait]
pub trait SourceChangeEventLogger : Send + Sync {
    async fn log_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()>;
}

#[async_trait]
impl SourceChangeEventLogger for Box<dyn SourceChangeEventLogger + Send + Sync> {
    async fn log_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {
        (**self).log_source_change_events(events).await
    }
}