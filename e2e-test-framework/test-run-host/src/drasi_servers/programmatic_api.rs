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

use anyhow::{anyhow, Result};

use super::TestRunDrasiServer;
use crate::drasi_servers::api_models::*;
use drasi_lib::{ComponentStatus as DrasiComponentStatus, Query};

// NOTE: The drasi_lib API has changed significantly.
// Sources and reactions now require pre-built instances implementing Source and Reaction traits.
// Dynamic creation from JSON config is no longer supported for sources and reactions.
// Only queries can be created dynamically.

const UPDATE_NOT_IMPLEMENTED_MSG: &str =
    "Update operations are not available with the current DrasiLib API. \
     To update a component, delete it and recreate it with the new configuration.";

const SOURCE_NOT_SUPPORTED_MSG: &str =
    "Source management is no longer supported through the programmatic API. \
     Sources must be added as instances implementing the Source trait during DrasiLib construction.";

const REACTION_NOT_SUPPORTED_MSG: &str =
    "Reaction management is no longer supported through the programmatic API. \
     Reactions must be added as instances implementing the Reaction trait during DrasiLib construction.";

/// Helper function to convert DrasiComponentStatus to local ComponentStatus
fn convert_status(status: DrasiComponentStatus) -> ComponentStatus {
    match status {
        DrasiComponentStatus::Running => ComponentStatus::Running,
        DrasiComponentStatus::Stopped => ComponentStatus::Stopped,
        DrasiComponentStatus::Starting => ComponentStatus::Starting,
        DrasiComponentStatus::Stopping => ComponentStatus::Stopping,
        DrasiComponentStatus::Error => ComponentStatus::Error("Error".to_string()),
    }
}

impl TestRunDrasiServer {
    // ===== Sources Management =====
    // NOTE: Source management requires pre-built instances in the new drasi_lib API

    pub async fn list_sources(&self) -> Result<Vec<SourceInfo>> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let sources = core
                .list_sources()
                .await
                .map_err(|e| anyhow!("Failed to list sources: {e}"))?;

            Ok(sources
                .into_iter()
                .map(|(id, status)| SourceInfo {
                    name: id,
                    source_type: "unknown".to_string(),
                    status: convert_status(status),
                })
                .collect())
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn get_source(&self, source_id: &str) -> Result<SourceDetails> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let source_runtime = core
                .get_source_info(source_id)
                .await
                .map_err(|e| anyhow!("Failed to get source info: {e}"))?;

            Ok(SourceDetails {
                name: source_runtime.id.clone(),
                source_type: source_runtime.source_type.clone(),
                status: convert_status(source_runtime.status),
                auto_start: false, // Runtime info doesn't include auto_start
                properties: serde_json::json!({}), // Properties not available in new API
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn create_source(
        &self,
        _request: CreateSourceRequest,
    ) -> Result<SourceCreatedResponse> {
        Err(anyhow!(SOURCE_NOT_SUPPORTED_MSG))
    }

    pub async fn update_source(
        &self,
        _source_id: &str,
        _request: UpdateSourceRequest,
    ) -> Result<SourceDetails> {
        Err(anyhow!(UPDATE_NOT_IMPLEMENTED_MSG))
    }

    pub async fn delete_source(&self, source_id: &str) -> Result<()> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.remove_source(source_id)
                .await
                .map_err(|e| anyhow!("Failed to delete source: {e}"))?;
            Ok(())
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn start_source(&self, source_id: &str) -> Result<StatusResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.start_source(source_id)
                .await
                .map_err(|e| anyhow!("Failed to start source: {e}"))?;

            Ok(StatusResponse {
                status: "started".to_string(),
                message: Some(format!("Source '{source_id}' started successfully")),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn stop_source(&self, source_id: &str) -> Result<StatusResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.stop_source(source_id)
                .await
                .map_err(|e| anyhow!("Failed to stop source: {e}"))?;

            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!("Source '{source_id}' stopped successfully")),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    // ===== Queries Management =====

    pub async fn list_queries(&self) -> Result<Vec<QueryInfo>> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let queries = core
                .list_queries()
                .await
                .map_err(|e| anyhow!("Failed to list queries: {e}"))?;

            Ok(queries
                .into_iter()
                .map(|(id, status)| QueryInfo {
                    name: id,
                    sources: Vec::new(),
                    status: convert_status(status),
                })
                .collect())
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn get_query(&self, query_id: &str) -> Result<QueryDetails> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let query_runtime = core
                .get_query_info(query_id)
                .await
                .map_err(|e| anyhow!("Failed to get query info: {e}"))?;

            Ok(QueryDetails {
                name: query_runtime.id.clone(),
                query: query_runtime.query.clone(),
                source_subscriptions: query_runtime
                    .source_subscriptions
                    .iter()
                    .map(|sub| SourceSubscriptionConfig {
                        source_id: sub.source_id.clone(),
                        pipeline: sub.pipeline.clone(),
                    })
                    .collect(),
                status: convert_status(query_runtime.status),
                auto_start: false, // Runtime info doesn't include auto_start
                profiling: false,  // Runtime info doesn't include profiling
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn create_query(&self, request: CreateQueryRequest) -> Result<QueryCreatedResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            // Build the query configuration
            let mut builder = Query::cypher(&request.name)
                .query(&request.query)
                .auto_start(request.auto_start);

            // Add sources
            for source in &request.sources {
                builder = builder.from_source(source);
            }

            let query_config = builder.build();

            core.add_query(query_config)
                .await
                .map_err(|e| anyhow!("Failed to create query: {e}"))?;

            Ok(QueryCreatedResponse {
                name: request.name,
                message: "Query created successfully".to_string(),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn update_query(
        &self,
        _query_id: &str,
        _request: UpdateQueryRequest,
    ) -> Result<QueryDetails> {
        Err(anyhow!(UPDATE_NOT_IMPLEMENTED_MSG))
    }

    pub async fn delete_query(&self, query_id: &str) -> Result<()> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.remove_query(query_id)
                .await
                .map_err(|e| anyhow!("Failed to delete query: {e}"))?;
            Ok(())
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn start_query(&self, query_id: &str) -> Result<StatusResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.start_query(query_id)
                .await
                .map_err(|e| anyhow!("Failed to start query: {e}"))?;

            Ok(StatusResponse {
                status: "started".to_string(),
                message: Some(format!("Query '{query_id}' started successfully")),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn stop_query(&self, query_id: &str) -> Result<StatusResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.stop_query(query_id)
                .await
                .map_err(|e| anyhow!("Failed to stop query: {e}"))?;

            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!("Query '{query_id}' stopped successfully")),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn get_query_results(&self, query_id: &str) -> Result<serde_json::Value> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let results = core
                .get_query_results(query_id)
                .await
                .map_err(|e| anyhow!("Failed to get query results: {e}"))?;

            Ok(serde_json::json!({ "results": results }))
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    // ===== Reactions Management =====
    // NOTE: Reaction management requires pre-built instances in the new drasi_lib API

    pub async fn list_reactions(&self) -> Result<Vec<ReactionInfo>> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let reactions = core
                .list_reactions()
                .await
                .map_err(|e| anyhow!("Failed to list reactions: {e}"))?;

            Ok(reactions
                .into_iter()
                .map(|(id, status)| ReactionInfo {
                    name: id,
                    reaction_type: "unknown".to_string(),
                    queries: Vec::new(),
                    status: convert_status(status),
                })
                .collect())
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn get_reaction(&self, reaction_id: &str) -> Result<ReactionDetails> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let reaction_runtime = core
                .get_reaction_info(reaction_id)
                .await
                .map_err(|e| anyhow!("Failed to get reaction info: {e}"))?;

            Ok(ReactionDetails {
                name: reaction_runtime.id.clone(),
                reaction_type: reaction_runtime.reaction_type.clone(),
                queries: reaction_runtime.queries.clone(),
                status: convert_status(reaction_runtime.status),
                auto_start: false, // Runtime info doesn't include auto_start
                properties: serde_json::json!({}), // Properties not available in new API
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn create_reaction(
        &self,
        _request: CreateReactionRequest,
    ) -> Result<ReactionCreatedResponse> {
        Err(anyhow!(REACTION_NOT_SUPPORTED_MSG))
    }

    pub async fn update_reaction(
        &self,
        _reaction_id: &str,
        _request: UpdateReactionRequest,
    ) -> Result<ReactionDetails> {
        Err(anyhow!(UPDATE_NOT_IMPLEMENTED_MSG))
    }

    pub async fn delete_reaction(&self, reaction_id: &str) -> Result<()> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.remove_reaction(reaction_id)
                .await
                .map_err(|e| anyhow!("Failed to delete reaction: {e}"))?;
            Ok(())
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn start_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.start_reaction(reaction_id)
                .await
                .map_err(|e| anyhow!("Failed to start reaction: {e}"))?;

            Ok(StatusResponse {
                status: "started".to_string(),
                message: Some(format!("Reaction '{reaction_id}' started successfully")),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn stop_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            core.stop_reaction(reaction_id)
                .await
                .map_err(|e| anyhow!("Failed to stop reaction: {e}"))?;

            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!("Reaction '{reaction_id}' stopped successfully")),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }
}
