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
use reqwest::Client;
use std::time::Duration;

use test_data_store::{
    scripts::SourceChangeEvent,
    test_repo_storage::models::DrasiServerApiSourceChangeDispatcherDefinition,
    test_run_storage::{TestRunDrasiServerId, TestRunSourceStorage},
};

use super::SourceChangeDispatcher;

#[derive(Debug)]
pub struct DrasiServerApiSourceChangeDispatcherSettings {
    pub drasi_server_id: TestRunDrasiServerId,
    pub source_id: String,
    pub timeout_seconds: u64,
    pub batch_events: bool,
}

impl DrasiServerApiSourceChangeDispatcherSettings {
    pub fn new(
        definition: &DrasiServerApiSourceChangeDispatcherDefinition,
        test_run_source_storage: &TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        // Parse the drasi_server_id from the definition
        let drasi_server_id = TestRunDrasiServerId::new(
            &test_run_source_storage.id.test_run_id,
            &definition.drasi_server_id,
        );

        Ok(Self {
            drasi_server_id,
            source_id: definition.source_id.clone(),
            timeout_seconds: definition.timeout_seconds.unwrap_or(30),
            batch_events: definition.batch_events.unwrap_or(true),
        })
    }
}

pub struct DrasiServerApiSourceChangeDispatcher {
    settings: DrasiServerApiSourceChangeDispatcherSettings,
    client: Client,
    test_run_host: Option<std::sync::Arc<crate::TestRunHost>>,
}

impl DrasiServerApiSourceChangeDispatcher {
    pub fn new(
        definition: &DrasiServerApiSourceChangeDispatcherDefinition,
        storage: &TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        log::debug!("Creating DrasiServerApiSourceChangeDispatcher from {definition:?}");

        let settings = DrasiServerApiSourceChangeDispatcherSettings::new(definition, storage)?;
        log::trace!("Creating DrasiServerApiSourceChangeDispatcher with settings {settings:?}");

        let client = Client::builder()
            .timeout(Duration::from_secs(settings.timeout_seconds))
            .build()?;

        Ok(Self {
            settings,
            client,
            test_run_host: None,
        })
    }

    pub fn set_test_run_host(&mut self, test_run_host: std::sync::Arc<crate::TestRunHost>) {
        self.test_run_host = Some(test_run_host);
    }

    async fn get_drasi_server_endpoint(&self) -> anyhow::Result<String> {
        let test_run_host = self
            .test_run_host
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("TestRunHost not set"))?;

        // Try to get the endpoint with retries to handle auto-start timing
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 10;
        const RETRY_DELAY_MS: u64 = 500;

        loop {
            match test_run_host
                .get_drasi_server_endpoint(&self.settings.drasi_server_id)
                .await
            {
                Ok(Some(endpoint)) => {
                    if attempts > 0 {
                        log::info!(
                            "Successfully connected to Drasi Server {} after {} attempts",
                            self.settings.drasi_server_id,
                            attempts + 1
                        );
                    }
                    return Ok(endpoint);
                }
                Ok(None) => {
                    attempts += 1;
                    if attempts >= MAX_ATTEMPTS {
                        return Err(anyhow::anyhow!(
                            "Drasi Server {} not found or not running after {} attempts",
                            self.settings.drasi_server_id,
                            MAX_ATTEMPTS
                        ));
                    }
                    log::debug!(
                        "Drasi Server {} not ready yet, retrying in {}ms (attempt {}/{})",
                        self.settings.drasi_server_id,
                        RETRY_DELAY_MS,
                        attempts,
                        MAX_ATTEMPTS
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[async_trait]
impl SourceChangeDispatcher for DrasiServerApiSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        log::debug!("Closing Drasi Server API source change dispatcher");
        Ok(())
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        log::trace!("Dispatching {} events to Drasi Server API", events.len());

        if events.is_empty() {
            return Ok(());
        }

        // Get the Drasi Server endpoint dynamically
        let base_endpoint = self.get_drasi_server_endpoint().await?;
        let url = format!(
            "{}/sources/{}/events",
            base_endpoint, self.settings.source_id
        );

        if self.settings.batch_events {
            // Log request body at debug level
            log::debug!(
                "Drasi Server API dispatcher sending batch request to {}: {}",
                url,
                serde_json::to_string_pretty(&events)
                    .unwrap_or_else(|e| format!("Failed to serialize: {e}"))
            );

            let response = match self.client.post(&url).json(&events).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    log::error!("Failed to connect to {url}: {e}");
                    return Err(e.into());
                }
            };

            let status = response.status();
            let response_body = response.text().await.unwrap_or_default();

            // Log response at debug level
            log::debug!(
                "Drasi Server API dispatcher received response from {url}: Status: {status}, Body: {response_body}"
            );

            if !status.is_success() {
                log::error!("Failed to dispatch events batch to {url}: {status} - {response_body}");
                anyhow::bail!("HTTP request failed with status: {status}");
            }

            log::trace!(
                "Successfully dispatched batch of {} events to {}",
                events.len(),
                url
            );
        } else {
            let event_count = events.len();
            for event in &events {
                // Log request body at debug level
                log::debug!(
                    "Drasi Server API dispatcher sending individual event to {}: {}",
                    url,
                    serde_json::to_string_pretty(event)
                        .unwrap_or_else(|e| format!("Failed to serialize: {e}"))
                );

                let response = self.client.post(&url).json(event).send().await?;

                let status = response.status();
                let response_body = response.text().await.unwrap_or_default();

                // Log response at debug level
                log::debug!(
                    "Drasi Server API dispatcher received response from {url}: Status: {status}, Body: {response_body}"
                );

                if !status.is_success() {
                    log::error!("Failed to dispatch event to {url}: {status} - {response_body}");
                    anyhow::bail!("HTTP request failed with status: {status}");
                }
            }

            log::trace!("Successfully dispatched {event_count} individual events to {url}");
        }

        Ok(())
    }
}
