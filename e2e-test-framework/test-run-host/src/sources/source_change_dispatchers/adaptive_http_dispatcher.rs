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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use test_data_store::{
    scripts::SourceChangeEvent, test_repo_storage::models::HttpSourceChangeDispatcherDefinition,
    test_run_storage::TestRunSourceStorage,
};

use super::SourceChangeDispatcher;
use crate::utils::{AdaptiveBatchConfig, AdaptiveBatcher};

use log::{debug, error, info};
use serde::{Deserialize, Serialize};

/// Convert SourceChangeEvent to Direct Format HttpChangeEvent
fn convert_to_direct_format(event: &SourceChangeEvent) -> Option<HttpChangeEvent> {
    // Parse the payload to extract element data
    let payload = serde_json::to_value(&event.payload).ok()?;

    // Determine operation type
    let operation = match event.op.as_str() {
        "i" => "insert",
        "u" => "update",
        "d" => "delete",
        _ => {
            error!("Unknown operation type: {}", event.op);
            return None;
        }
    };

    // Get timestamp in nanoseconds
    let timestamp = Some(event.reactivator_start_ns as i64);

    // For delete operations, we need to extract the ID from "before"
    if operation == "delete" {
        if let Some(before) = payload.get("before") {
            if let Some(id) = before.get("id").and_then(|v| v.as_str()) {
                let labels = before.get("labels").and_then(|v| v.as_array()).map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect::<Vec<String>>()
                });

                return Some(HttpChangeEvent {
                    operation: operation.to_string(),
                    element: None,
                    id: Some(id.to_string()),
                    labels,
                    timestamp,
                });
            }
        }
        error!("Delete operation missing 'before' data");
        return None;
    }

    // For insert/update, extract element from "after"
    let element_data = if operation == "update" || operation == "insert" {
        payload.get("after")
    } else {
        None
    };

    if let Some(elem_value) = element_data {
        // Extract element fields
        let id = elem_value
            .get("id")
            .and_then(|v| v.as_str())
            .map(String::from)?;

        let labels = elem_value
            .get("labels")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        let properties = elem_value
            .get("properties")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        // Determine element type based on table in source info
        let element_type = payload
            .get("source")
            .and_then(|s| s.get("table"))
            .and_then(|t| t.as_str())
            .unwrap_or("node");

        let element_type = match element_type {
            "relation" | "edge" => "relation",
            _ => "node",
        };

        // Internal SourceChangeEvent uses the CDC-style start_id/end_id.
        // Translate to from/to which is drasi-server's HTTP source plugin
        // DirectElement schema.
        let from = elem_value
            .get("start_id")
            .and_then(|v| v.as_str())
            .map(String::from);
        let to = elem_value
            .get("end_id")
            .and_then(|v| v.as_str())
            .map(String::from);

        let element = DirectElement {
            element_type: if from.is_some() && to.is_some() {
                "relation".to_string()
            } else {
                element_type.to_string()
            },
            id,
            labels,
            properties,
            from,
            to,
        };

        Some(HttpChangeEvent {
            operation: operation.to_string(),
            element: Some(element),
            id: None,
            labels: None,
            timestamp,
        })
    } else {
        error!("Insert/Update operation missing 'after' data");
        None
    }
}

/// Batch event request that wraps multiple events
#[derive(Debug, Serialize, Deserialize)]
struct BatchEventRequest {
    events: Vec<HttpChangeEvent>,
}

/// HTTP change event format matching Drasi Server's Direct Format
#[derive(Debug, Serialize, Deserialize, Clone)]
struct HttpChangeEvent {
    operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    element: Option<DirectElement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<i64>,
}

/// Direct element format for Drasi Server
#[derive(Debug, Serialize, Deserialize, Clone)]
struct DirectElement {
    #[serde(rename = "type")]
    element_type: String,
    id: String,
    labels: Vec<String>,
    properties: serde_json::Map<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<String>,
}

/// Response from the HTTP endpoint
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct EventResponse {
    success: bool,
    message: String,
    error: Option<String>,
}

pub struct AdaptiveHttpSourceChangeDispatcher {
    url: String,
    port: u16,
    endpoint: String,
    batch_endpoint: String,
    timeout_seconds: u64,
    source_id: String,
    adaptive_config: AdaptiveBatchConfig,
    // Channel for sending events to the batcher
    event_tx: Option<mpsc::Sender<SourceChangeEvent>>,
    // Handle to the background batcher task
    batcher_handle: Option<JoinHandle<anyhow::Result<()>>>,
    failure: Option<String>,
    client: Arc<Client>,
    batch_enabled: bool,
}

impl AdaptiveHttpSourceChangeDispatcher {
    pub fn new(
        definition: &HttpSourceChangeDispatcherDefinition,
        _storage: TestRunSourceStorage,
    ) -> anyhow::Result<Self> {
        info!("Creating AdaptiveHttpSourceChangeDispatcher");

        // Configure adaptive batching
        let mut adaptive_config = AdaptiveBatchConfig::default();

        // Check if we have explicit batch settings
        if let Some(batch_size) = definition.batch_size {
            adaptive_config.max_batch_size = batch_size as usize;
            adaptive_config.min_batch_size = (batch_size / 10).max(1) as usize;
        }
        if let Some(timeout_ms) = definition.batch_timeout_ms {
            adaptive_config.max_wait_time = Duration::from_millis(timeout_ms);
            adaptive_config.min_wait_time = Duration::from_millis(timeout_ms / 10);
        }

        // Check if adaptive mode is enabled (default true for adaptive dispatcher)
        let adaptive_enabled = definition.adaptive_enabled.unwrap_or(true);
        adaptive_config.adaptive_enabled = adaptive_enabled;

        // Determine if batch endpoint should be used
        let batch_enabled = definition.batch_events.unwrap_or(true);

        // Extract source_id from definition or use default
        let source_id = definition
            .source_id
            .clone()
            .unwrap_or_else(|| "test-source".to_string());

        // Construct endpoints
        let endpoint = if let Some(ep) = &definition.endpoint {
            ep.clone()
        } else {
            format!("/sources/{source_id}/events")
        };

        // For Drasi Server adaptive source, batch endpoint has /batch suffix
        let batch_endpoint = format!("{endpoint}/batch");

        // Create HTTP client with connection pooling (HTTP/1.1 for compatibility)
        let client = Client::builder()
            .timeout(Duration::from_secs(
                definition.timeout_seconds.unwrap_or(30),
            ))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(10)
            // Don't use http2_prior_knowledge as it can cause broken pipe errors
            .build()
            .unwrap_or_else(|_| Client::new());

        Ok(Self {
            url: definition.url.clone(),
            port: definition.port,
            endpoint,
            batch_endpoint,
            timeout_seconds: definition.timeout_seconds.unwrap_or(30),
            source_id,
            adaptive_config,
            event_tx: None,
            batcher_handle: None,
            failure: None,
            client: Arc::new(client),
            batch_enabled,
        })
    }

    fn start_batcher(&mut self) -> anyhow::Result<()> {
        if self.batcher_handle.is_some() {
            return Ok(()); // Already started
        }

        // Create channel for batching
        let (event_tx, event_rx) = mpsc::channel(1000);
        self.event_tx = Some(event_tx);

        // Clone values for the spawned task
        let url = self.url.clone();
        let port = self.port;
        let endpoint = self.endpoint.clone();
        let batch_endpoint = self.batch_endpoint.clone();
        let source_id = self.source_id.clone();
        let adaptive_config = self.adaptive_config.clone();
        let client = self.client.clone();
        let batch_enabled = self.batch_enabled;

        // Spawn batcher task
        let handle = tokio::spawn(async move {
            let mut batcher = AdaptiveBatcher::new(event_rx, adaptive_config);
            let mut successful_batches = 0u64;
            let mut total_events = 0u64;

            info!("Adaptive HTTP batcher started for source {source_id}");

            while let Some(batch) = batcher.next_batch().await {
                if batch.is_empty() {
                    continue;
                }

                let batch_size = batch.len();
                total_events += batch_size as u64;

                debug!("Adaptive HTTP batch ready with {batch_size} events");

                // Convert events to HttpChangeEvent Direct Format
                let http_events: Vec<HttpChangeEvent> = batch
                    .iter()
                    .map(|event| {
                        convert_to_direct_format(event).ok_or_else(|| {
                            anyhow::anyhow!(
                                "Failed to convert adaptive HTTP event with op={}",
                                event.op
                            )
                        })
                    })
                    .collect::<anyhow::Result<_>>()?;

                // Send batch or individual events
                let success = if batch_enabled && http_events.len() > 1 {
                    // Send as batch - Drasi Server adaptive source expects BatchEventRequest
                    let batch_url = format!("{url}:{port}{batch_endpoint}");
                    let batch_request = BatchEventRequest {
                        events: http_events.clone(),
                    };

                    // Log the batch being sent for debugging
                    info!(
                        "Sending batch of {} events to {}",
                        http_events.len(),
                        batch_url
                    );
                    debug!(
                        "Batch request: {}",
                        serde_json::to_string_pretty(&batch_request)
                            .unwrap_or_else(|_| "Failed to serialize".to_string())
                    );

                    match client.post(&batch_url).json(&batch_request).send().await {
                        Ok(response) => {
                            let status = response.status();
                            if status.is_success() {
                                debug!("Batch of {batch_size} events sent successfully");
                                true
                            } else {
                                // Get response body for debugging
                                let body = response
                                    .text()
                                    .await
                                    .unwrap_or_else(|_| "Failed to get response body".to_string());
                                error!(
                                    "Batch request failed with status: {status} - Response: {body}"
                                );
                                false
                            }
                        }
                        Err(e) => {
                            error!("Failed to send batch: {e}");
                            false
                        }
                    }
                } else {
                    // Send individual events
                    let single_url = format!("{url}:{port}{endpoint}");
                    let mut all_success = true;

                    for event in http_events {
                        match client.post(&single_url).json(&event).send().await {
                            Ok(response) => {
                                if !response.status().is_success() {
                                    error!(
                                        "Event request failed with status: {}",
                                        response.status()
                                    );
                                    all_success = false;
                                }
                            }
                            Err(e) => {
                                error!("Failed to send event: {e}");
                                all_success = false;
                            }
                        }
                    }
                    all_success
                };

                if success {
                    successful_batches += 1;
                } else {
                    anyhow::bail!(
                        "Adaptive HTTP batch send failed: source={source_id}, batch_size={batch_size}, successful_batches={successful_batches}"
                    );
                }

                if successful_batches.is_multiple_of(100) {
                    info!(
                        "Adaptive HTTP metrics - Successful: {successful_batches}, Total events: {total_events}"
                    );
                }
            }

            info!(
                "Adaptive HTTP batcher completed - Successful: {successful_batches}, Total events: {total_events}"
            );
            Ok(())
        });

        self.batcher_handle = Some(handle);
        Ok(())
    }

    async fn await_batcher(&mut self) -> anyhow::Result<()> {
        let result = if let Some(mut handle) = self.batcher_handle.take() {
            match tokio::time::timeout(
                Duration::from_secs(self.timeout_seconds.max(5)),
                &mut handle,
            )
            .await
            {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => Err(anyhow::anyhow!(
                    "Adaptive HTTP batcher task failed: {error}"
                )),
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    Err(anyhow::anyhow!(
                        "Timed out waiting for adaptive HTTP batcher to drain"
                    ))
                }
            }
        } else {
            Ok(())
        };
        if let Err(error) = result {
            self.failure = Some(error.to_string());
        }
        match &self.failure {
            Some(failure) => Err(anyhow::anyhow!(failure.clone())),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl SourceChangeDispatcher for AdaptiveHttpSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        info!("Closing AdaptiveHttpSourceChangeDispatcher");

        // Close the event channel to signal batcher to stop
        self.event_tx = None;

        self.await_batcher().await
    }

    async fn dispatch_source_change_events(
        &mut self,
        events: Vec<&SourceChangeEvent>,
    ) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        if let Some(failure) = &self.failure {
            anyhow::bail!(failure.clone());
        }
        if self
            .batcher_handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            self.event_tx = None;
            self.await_batcher().await?;
            anyhow::bail!("Adaptive HTTP batcher stopped unexpectedly");
        }

        // Start batcher if not already running
        if self.batcher_handle.is_none() {
            self.start_batcher()?;
        }

        // If we have a batch channel, use adaptive batching
        if let Some(ref tx) = self.event_tx {
            for event in events {
                if tx.send(event.clone()).await.is_err() {
                    self.event_tx = None;
                    self.await_batcher().await?;
                    anyhow::bail!("Adaptive HTTP batcher channel closed unexpectedly");
                }
            }
        } else {
            anyhow::bail!("Adaptive HTTP batcher not initialized");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::StatusCode, routing::post, Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    fn dispatcher(port: u16) -> AdaptiveHttpSourceChangeDispatcher {
        AdaptiveHttpSourceChangeDispatcher {
            url: "http://127.0.0.1".to_owned(),
            port,
            endpoint: "/events".to_owned(),
            batch_endpoint: "/events/batch".to_owned(),
            timeout_seconds: 1,
            source_id: "test-source".to_owned(),
            adaptive_config: AdaptiveBatchConfig {
                max_batch_size: 2,
                min_batch_size: 2,
                min_wait_time: Duration::from_millis(50),
                max_wait_time: Duration::from_millis(100),
                adaptive_enabled: false,
                ..Default::default()
            },
            event_tx: None,
            batcher_handle: None,
            failure: None,
            client: Arc::new(
                Client::builder()
                    .timeout(Duration::from_secs(1))
                    .build()
                    .unwrap(),
            ),
            batch_enabled: true,
        }
    }

    fn event(ordinal: u64) -> SourceChangeEvent {
        serde_json::from_value(json!({"op":"i", "reactivatorStart_ns":1, "reactivatorEnd_ns":1,
            "payload":{"source":{"db":"test","table":"node","ts_ns":1,"lsn":ordinal},
                "before":null,"after":{"id":format!("item-{ordinal}"),"labels":["Item"],"properties":{"ordinal":ordinal}}}
        })).unwrap()
    }

    fn server(app: Router) -> (u16, JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            axum::Server::from_tcp(listener)
                .unwrap()
                .serve(app.into_make_service())
                .await
                .unwrap();
        });
        (port, task)
    }

    #[tokio::test]
    async fn close_waits_for_final_batch_response() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let records = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/events/batch",
            post({
                let entered = entered.clone();
                let release = release.clone();
                let records = records.clone();
                move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let records = records.clone();
                    async move {
                        records
                            .fetch_add(body["events"].as_array().unwrap().len(), Ordering::SeqCst);
                        entered.notify_one();
                        release.notified().await;
                        StatusCode::OK
                    }
                }
            }),
        );
        let (port, task) = server(app);
        let mut dispatcher = dispatcher(port);
        dispatcher
            .dispatch_source_change_events(vec![&event(1), &event(2)])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        {
            let close = dispatcher.close();
            tokio::pin!(close);
            assert!(tokio::time::timeout(Duration::from_millis(20), &mut close)
                .await
                .is_err());
            release.notify_one();
            close.await.unwrap();
        }
        assert_eq!(records.load(Ordering::SeqCst), 2);
        assert!(dispatcher.event_tx.is_none());
        assert!(dispatcher.batcher_handle.is_none());
        dispatcher.close().await.unwrap();
        task.abort();
    }

    #[tokio::test]
    async fn failed_batch_propagates_and_cannot_restart_or_fallback() {
        let requests = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route(
            "/events/batch",
            post({
                let requests = requests.clone();
                move || {
                    let requests = requests.clone();
                    async move {
                        requests.fetch_add(1, Ordering::SeqCst);
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                }
            }),
        );
        let (port, task) = server(app);
        let mut dispatcher = dispatcher(port);
        dispatcher
            .dispatch_source_change_events(vec![&event(1), &event(2)])
            .await
            .unwrap();
        let error = dispatcher.close().await.unwrap_err().to_string();
        assert!(error.contains("batch send failed"));
        assert_eq!(dispatcher.close().await.unwrap_err().to_string(), error);
        assert_eq!(
            dispatcher
                .dispatch_source_change_events(vec![&event(3)])
                .await
                .unwrap_err()
                .to_string(),
            error
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        task.abort();
    }

    #[tokio::test]
    async fn malformed_event_is_not_silently_filtered() {
        let mut dispatcher = dispatcher(1);
        let mut invalid = event(1);
        invalid.op = "invalid".to_owned();
        dispatcher
            .dispatch_source_change_events(vec![&invalid, &event(2)])
            .await
            .unwrap();
        assert!(dispatcher
            .close()
            .await
            .unwrap_err()
            .to_string()
            .contains("Failed to convert"));
    }

    #[tokio::test]
    async fn failed_single_event_and_request_timeout_propagate() {
        let app = Router::new().route(
            "/events",
            post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
        let (port, task) = server(app);
        let mut first = dispatcher(port);
        first
            .dispatch_source_change_events(vec![&event(1)])
            .await
            .unwrap();
        assert!(first.close().await.is_err());
        task.abort();

        let app = Router::new().route(
            "/events",
            post(|| async {
                std::future::pending::<()>().await;
                StatusCode::OK
            }),
        );
        let (port, task) = server(app);
        let mut second = dispatcher(port);
        second.client = Arc::new(
            Client::builder()
                .timeout(Duration::from_millis(30))
                .build()
                .unwrap(),
        );
        second
            .dispatch_source_change_events(vec![&event(1)])
            .await
            .unwrap();
        assert!(second.close().await.is_err());
        task.abort();
    }

    #[tokio::test]
    async fn task_panic_is_latched() {
        let mut dispatcher = dispatcher(1);
        dispatcher.batcher_handle = Some(tokio::spawn(async { panic!("batcher panic") }));
        let error = dispatcher.close().await.unwrap_err().to_string();
        assert!(error.contains("batcher task failed"));
        assert_eq!(dispatcher.close().await.unwrap_err().to_string(), error);
    }

    #[tokio::test]
    async fn drain_timeout_aborts_task_instead_of_detaching() {
        let mut dispatcher = dispatcher(1);
        let handle = tokio::spawn(async { std::future::pending::<anyhow::Result<()>>().await });
        let abort = handle.abort_handle();
        dispatcher.batcher_handle = Some(handle);
        let error = dispatcher.close().await.unwrap_err().to_string();
        assert!(error.contains("Timed out"));
        assert!(abort.is_finished());
        assert_eq!(dispatcher.close().await.unwrap_err().to_string(), error);
    }
}
