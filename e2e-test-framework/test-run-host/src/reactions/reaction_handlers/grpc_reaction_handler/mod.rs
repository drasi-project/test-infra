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

use std::{net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use test_data_store::{
    test_repo_storage::models::GrpcReactionHandlerDefinition, test_run_storage::TestRunQueryId,
};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Notify, RwLock,
};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, error, info, trace};

use crate::grpc_converters::{convert_from_drasi_query_result, drasi};
use crate::reactions::reaction_output_handler::{
    ReactionHandlerMessage, ReactionHandlerPayload, ReactionHandlerStatus, ReactionHandlerType,
    ReactionInvocation, ReactionOutputHandler,
};

use drasi::v1::reaction_service_server::{ReactionService, ReactionServiceServer};
use drasi::v1::{
    ProcessResultsRequest, ProcessResultsResponse, QueryResult, ReactionHealthCheckResponse,
    StreamResultsResponse, SubscribeRequest,
};

#[derive(Clone, Debug)]
pub struct GrpcReactionHandlerSettings {
    pub host: String,
    pub port: u16,
    pub correlation_metadata_key: Option<String>,
    pub test_run_query_id: TestRunQueryId,
    pub query_ids: Vec<String>,
    pub include_initial_state: bool,
}

impl GrpcReactionHandlerSettings {
    pub fn new(
        id: TestRunQueryId,
        definition: GrpcReactionHandlerDefinition,
    ) -> anyhow::Result<Self> {
        Ok(GrpcReactionHandlerSettings {
            host: definition
                .host
                .clone()
                .unwrap_or_else(|| "0.0.0.0".to_string()),
            port: definition.port.unwrap_or(50052),
            correlation_metadata_key: definition.correlation_metadata_key,
            test_run_query_id: id,
            query_ids: definition.query_ids,
            include_initial_state: definition.include_initial_state.unwrap_or(false),
        })
    }

    pub fn instance_addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .expect("Invalid instance address")
    }
}

#[derive(Clone)]
struct GrpcInstanceImpl {
    tx: Sender<ReactionHandlerMessage>,
    settings: GrpcReactionHandlerSettings,
    invocation_count: Arc<RwLock<u64>>,
}

impl GrpcInstanceImpl {
    async fn process_query_result(&self, result: QueryResult) -> anyhow::Result<()> {
        let timestamp = chrono::Utc::now();

        // Convert Drasi QueryResult to internal format
        let json_results = convert_from_drasi_query_result(result.clone())?;

        // Process each result item as a separate invocation
        // This ensures stop triggers can fire mid-batch
        if json_results.is_empty() {
            // Handle empty results
            let mut count = self.invocation_count.write().await;
            *count += 1;
            let invocation_id = format!("grpc-invocation-{}", *count);
            drop(count);

            let payload = ReactionHandlerPayload {
                value: serde_json::json!({
                    "query_id": result.query_id,
                    "results": [],
                }),
                timestamp,
                invocation_id: Some(invocation_id),
                metadata: None,
            };

            let invocation = ReactionInvocation {
                handler_type: ReactionHandlerType::Grpc,
                payload,
            };

            let message = ReactionHandlerMessage::Invocation(invocation);
            self.tx
                .send(message)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send message to output handler: {e}"))?;
        } else {
            // Send each item as a separate invocation
            for json_result in json_results {
                let mut count = self.invocation_count.write().await;
                *count += 1;
                let invocation_id = format!("grpc-invocation-{}", *count);
                drop(count);

                let payload = ReactionHandlerPayload {
                    value: serde_json::json!({
                        "query_id": result.query_id.clone(),
                        "result": json_result,
                    }),
                    timestamp,
                    invocation_id: Some(invocation_id),
                    metadata: None,
                };

                let invocation = ReactionInvocation {
                    handler_type: ReactionHandlerType::Grpc,
                    payload,
                };

                let message = ReactionHandlerMessage::Invocation(invocation);
                self.tx.send(message).await.map_err(|e| {
                    anyhow::anyhow!("Failed to send message to output handler: {e}")
                })?;
            }
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl ReactionService for GrpcInstanceImpl {
    async fn process_results(
        &self,
        request: Request<ProcessResultsRequest>,
    ) -> Result<Response<ProcessResultsResponse>, Status> {
        trace!("Received ProcessResults request");
        let req = request.into_inner();

        if let Some(results) = req.results {
            debug!(
                "Processing query results for query_id: {}",
                results.query_id
            );
            let items_count = results.results.len() as u32;
            match self.process_query_result(results).await {
                Ok(_) => {
                    trace!("Successfully processed {} query result items", items_count);
                    let response = ProcessResultsResponse {
                        success: true,
                        message: "Results processed successfully".to_string(),
                        error: String::new(),
                        items_processed: items_count,
                    };
                    Ok(Response::new(response))
                }
                Err(e) => {
                    error!("Failed to process query results: {}", e);
                    let response = ProcessResultsResponse {
                        success: false,
                        message: "Failed to process results".to_string(),
                        error: e.to_string(),
                        items_processed: 0,
                    };
                    Ok(Response::new(response))
                }
            }
        } else {
            let response = ProcessResultsResponse {
                success: false,
                message: "No results provided".to_string(),
                error: "Missing results in request".to_string(),
                items_processed: 0,
            };
            Ok(Response::new(response))
        }
    }

    type StreamResultsStream =
        tokio_stream::wrappers::ReceiverStream<Result<StreamResultsResponse, Status>>;

    async fn stream_results(
        &self,
        request: Request<tonic::Streaming<QueryResult>>,
    ) -> Result<Response<Self::StreamResultsStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let self_clone = self.clone();

        tokio::spawn(async move {
            let mut batches_processed = 0u64;
            let mut items_processed = 0u64;

            while let Ok(Some(result)) = stream.message().await {
                let batch_item_count = result.results.len() as u64;
                items_processed += batch_item_count;
                batches_processed += 1;

                match self_clone.process_query_result(result).await {
                    Ok(_) => {
                        trace!(
                            "Processed batch {} with {} items",
                            batches_processed,
                            batch_item_count
                        );
                        let response = StreamResultsResponse {
                            success: true,
                            message: "Batch processed".to_string(),
                            error: String::new(),
                            batches_processed,
                            items_processed,
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let response = StreamResultsResponse {
                            success: false,
                            message: "Failed to process batch".to_string(),
                            error: e.to_string(),
                            batches_processed,
                            items_processed,
                        };
                        let _ = tx.send(Ok(response)).await;
                        break;
                    }
                }
            }

            debug!(
                "Stream completed: {} batches, {} total items",
                batches_processed, items_processed
            );
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    type SubscribeStream = tokio_stream::wrappers::ReceiverStream<Result<QueryResult, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();

        // Validate query IDs
        if !req.query_ids.is_empty() {
            let valid = req
                .query_ids
                .iter()
                .any(|id| self.settings.query_ids.contains(id));
            if !valid {
                return Err(Status::invalid_argument(
                    "None of the requested query IDs are configured for this handler",
                ));
            }
        }

        // For now, return an empty stream since we don't have a real Drasi Server to subscribe to
        // In a real implementation, this would establish a subscription to the Drasi Server
        let (_tx, rx) = tokio::sync::mpsc::channel(100);

        info!(
            "Subscription requested for queries: {:?} (not implemented)",
            req.query_ids
        );

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn health_check(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ReactionHealthCheckResponse>, Status> {
        let count = *self.invocation_count.read().await;

        let response = ReactionHealthCheckResponse {
            status: 1, // STATUS_HEALTHY = 1 from the proto
            message: format!("Reaction handler is healthy. Processed {count} invocations"),
            version: env!("CARGO_PKG_VERSION").to_string(),
            pending_items: 0, // We don't queue items in this implementation
        };

        Ok(Response::new(response))
    }
}

type GrpcServerJoinHandle =
    Arc<RwLock<Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>>>;

pub struct GrpcReactionHandler {
    instance_handle: GrpcServerJoinHandle,
    shutdown_notify: Arc<Notify>,
    settings: GrpcReactionHandlerSettings,
    tx: Arc<RwLock<Option<Sender<ReactionHandlerMessage>>>>,
    rx: Arc<RwLock<Option<Receiver<ReactionHandlerMessage>>>>,
    status: Arc<RwLock<ReactionHandlerStatus>>,
}

impl GrpcReactionHandler {
    pub async fn new(
        id: TestRunQueryId,
        definition: GrpcReactionHandlerDefinition,
    ) -> anyhow::Result<Self> {
        let settings = GrpcReactionHandlerSettings::new(id.clone(), definition)?;
        let (tx, rx) = channel(1000);

        Ok(Self {
            instance_handle: Arc::new(RwLock::new(None)),
            shutdown_notify: Arc::new(Notify::new()),
            settings,
            tx: Arc::new(RwLock::new(Some(tx))),
            rx: Arc::new(RwLock::new(Some(rx))),
            status: Arc::new(RwLock::new(ReactionHandlerStatus::Uninitialized)),
        })
    }
}

#[async_trait]
impl ReactionOutputHandler for GrpcReactionHandler {
    async fn init(&self) -> anyhow::Result<Receiver<ReactionHandlerMessage>> {
        let mut rx_lock = self.rx.write().await;
        rx_lock
            .take()
            .ok_or_else(|| anyhow::anyhow!("Receiver already taken"))
    }

    async fn start(&self) -> anyhow::Result<()> {
        let mut instance_handle = self.instance_handle.write().await;
        if instance_handle.is_some() {
            return Ok(()); // Already running
        }

        let tx_lock = self.tx.read().await;
        let tx = tx_lock
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Transmitter not available"))?
            .clone();

        let instance_impl = GrpcInstanceImpl {
            tx,
            settings: self.settings.clone(),
            invocation_count: Arc::new(RwLock::new(0)),
        };

        let addr = self.settings.instance_addr();
        let shutdown_notify_clone = self.shutdown_notify.clone();

        info!("Starting Drasi ReactionService instance on {}", addr);
        info!(
            "Instance configured for query_ids: {:?}",
            self.settings.query_ids
        );

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(ReactionServiceServer::new(instance_impl))
                .serve_with_shutdown(addr, async {
                    shutdown_notify_clone.notified().await;
                })
                .await
        });

        *instance_handle = Some(handle);
        *self.status.write().await = ReactionHandlerStatus::Running;

        // Give the instance time to start listening
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        info!(
            "Instance is now listening and ready to accept connections on {}",
            self.settings.instance_addr()
        );

        Ok(())
    }

    async fn pause(&self) -> anyhow::Result<()> {
        // gRPC instance doesn't support pause - just update status
        *self.status.write().await = ReactionHandlerStatus::Paused;
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        info!(
            "Shutting down Drasi ReactionService instance on {}",
            self.settings.instance_addr()
        );

        // Signal shutdown
        self.shutdown_notify.notify_one();

        // Wait for instance to stop
        let mut instance_handle = self.instance_handle.write().await;
        if let Some(handle) = instance_handle.take() {
            match tokio::time::timeout(std::time::Duration::from_secs(5), handle).await {
                Ok(Ok(Ok(_))) => {
                    debug!("Drasi ReactionService instance shut down successfully");
                }
                Ok(Ok(Err(e))) => {
                    error!(
                        "Drasi ReactionService instance error during shutdown: {}",
                        e
                    );
                }
                Ok(Err(e)) => {
                    error!("Task join error during shutdown: {}", e);
                }
                Err(_) => {
                    error!("Timeout waiting for Drasi ReactionService instance to shut down");
                }
            }
        }

        *self.status.write().await = ReactionHandlerStatus::Stopped;
        Ok(())
    }

    async fn status(&self) -> ReactionHandlerStatus {
        *self.status.read().await
    }

    async fn metrics(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "endpoint": format!("grpc://{}", self.settings.instance_addr()),
            "query_ids": self.settings.query_ids,
        }))
    }
}
