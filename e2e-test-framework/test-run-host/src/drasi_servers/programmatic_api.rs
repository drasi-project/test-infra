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
use drasi_server_core::{Query, Reaction, Source};

// NOTE: Update operations are not supported - components must be deleted and recreated
// Create, Delete, Start, and Stop ARE all supported via the DrasiServerCore API

const UPDATE_NOT_IMPLEMENTED_MSG: &str =
    "Update operations are not available with the current DrasiServerCore API. \
     To update a component, delete it and recreate it with the new configuration.";

impl TestRunDrasiServer {
    // ===== Sources Management =====

    pub async fn list_sources(&self) -> Result<Vec<SourceInfo>> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let sources = core
                .list_sources()
                .await
                .map_err(|e| anyhow!("Failed to list sources: {}", e))?;

            Ok(sources
                .into_iter()
                .map(|(id, status)| SourceInfo {
                    name: id,
                    source_type: "unknown".to_string(),
                    status: match status {
                        drasi_server_core::channels::ComponentStatus::Running => {
                            ComponentStatus::Running
                        }
                        drasi_server_core::channels::ComponentStatus::Stopped => {
                            ComponentStatus::Stopped
                        }
                        drasi_server_core::channels::ComponentStatus::Starting => {
                            ComponentStatus::Starting
                        }
                        drasi_server_core::channels::ComponentStatus::Stopping => {
                            ComponentStatus::Stopping
                        }
                        drasi_server_core::channels::ComponentStatus::Error => {
                            ComponentStatus::Error("Error".to_string())
                        }
                    },
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
                .map_err(|e| anyhow!("Failed to get source info: {}", e))?;

            Ok(SourceDetails {
                name: source_runtime.id.clone(),
                source_type: source_runtime.source_type.clone(),
                status: match source_runtime.status {
                    drasi_server_core::channels::ComponentStatus::Running => {
                        ComponentStatus::Running
                    }
                    drasi_server_core::channels::ComponentStatus::Stopped => {
                        ComponentStatus::Stopped
                    }
                    drasi_server_core::channels::ComponentStatus::Starting => {
                        ComponentStatus::Starting
                    }
                    drasi_server_core::channels::ComponentStatus::Stopping => {
                        ComponentStatus::Stopping
                    }
                    drasi_server_core::channels::ComponentStatus::Error => {
                        ComponentStatus::Error("Error".to_string())
                    }
                },
                auto_start: false, // Runtime info doesn't include auto_start
                properties: serde_json::to_value(&source_runtime.properties)
                    .map_err(|e| anyhow!("Failed to serialize properties: {}", e))?,
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn create_source(
        &self,
        request: CreateSourceRequest,
    ) -> Result<SourceCreatedResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            // Build the source configuration using the appropriate factory method
            let mut builder = match request.source_type.as_str() {
                "application" => Source::application(&request.name),
                "mock" => Source::mock(&request.name),
                "postgres" => Source::postgres(&request.name),
                "platform" => Source::platform(&request.name),
                _ => return Err(anyhow!("Unsupported source type: {}", request.source_type)),
            };

            builder = builder.auto_start(request.auto_start);

            // Add properties from the request
            if let Ok(props_map) = serde_json::from_value::<
                std::collections::HashMap<String, serde_json::Value>,
            >(request.properties)
            {
                for (key, value) in props_map {
                    builder = builder.with_property(key, value);
                }
            }

            let source_config = builder.build();

            core.create_source(source_config)
                .await
                .map_err(|e| anyhow!("Failed to create source: {}", e))?;

            Ok(SourceCreatedResponse {
                name: request.name,
                message: "Source created successfully".to_string(),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
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
                .map_err(|e| anyhow!("Failed to delete source: {}", e))?;
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
                .map_err(|e| anyhow!("Failed to start source: {}", e))?;

            Ok(StatusResponse {
                status: "started".to_string(),
                message: Some(format!("Source '{}' started successfully", source_id)),
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
                .map_err(|e| anyhow!("Failed to stop source: {}", e))?;

            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!("Source '{}' stopped successfully", source_id)),
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
                .map_err(|e| anyhow!("Failed to list queries: {}", e))?;

            Ok(queries
                .into_iter()
                .map(|(id, status)| QueryInfo {
                    name: id,
                    sources: Vec::new(),
                    status: match status {
                        drasi_server_core::channels::ComponentStatus::Running => {
                            ComponentStatus::Running
                        }
                        drasi_server_core::channels::ComponentStatus::Stopped => {
                            ComponentStatus::Stopped
                        }
                        drasi_server_core::channels::ComponentStatus::Starting => {
                            ComponentStatus::Starting
                        }
                        drasi_server_core::channels::ComponentStatus::Stopping => {
                            ComponentStatus::Stopping
                        }
                        drasi_server_core::channels::ComponentStatus::Error => {
                            ComponentStatus::Error("Error".to_string())
                        }
                    },
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
                .map_err(|e| anyhow!("Failed to get query info: {}", e))?;

            Ok(QueryDetails {
                name: query_runtime.id.clone(),
                query: query_runtime.query.clone(),
                sources: query_runtime.sources.clone(),
                status: match query_runtime.status {
                    drasi_server_core::channels::ComponentStatus::Running => {
                        ComponentStatus::Running
                    }
                    drasi_server_core::channels::ComponentStatus::Stopped => {
                        ComponentStatus::Stopped
                    }
                    drasi_server_core::channels::ComponentStatus::Starting => {
                        ComponentStatus::Starting
                    }
                    drasi_server_core::channels::ComponentStatus::Stopping => {
                        ComponentStatus::Stopping
                    }
                    drasi_server_core::channels::ComponentStatus::Error => {
                        ComponentStatus::Error("Error".to_string())
                    }
                },
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

            core.create_query(query_config)
                .await
                .map_err(|e| anyhow!("Failed to create query: {}", e))?;

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
                .map_err(|e| anyhow!("Failed to delete query: {}", e))?;
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
                .map_err(|e| anyhow!("Failed to start query: {}", e))?;

            Ok(StatusResponse {
                status: "started".to_string(),
                message: Some(format!("Query '{}' started successfully", query_id)),
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
                .map_err(|e| anyhow!("Failed to stop query: {}", e))?;

            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!("Query '{}' stopped successfully", query_id)),
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
                .map_err(|e| anyhow!("Failed to get query results: {}", e))?;

            Ok(serde_json::json!({ "results": results }))
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    // ===== Reactions Management =====

    pub async fn list_reactions(&self) -> Result<Vec<ReactionInfo>> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            let reactions = core
                .list_reactions()
                .await
                .map_err(|e| anyhow!("Failed to list reactions: {}", e))?;

            Ok(reactions
                .into_iter()
                .map(|(id, status)| ReactionInfo {
                    name: id,
                    reaction_type: "unknown".to_string(),
                    queries: Vec::new(),
                    status: match status {
                        drasi_server_core::channels::ComponentStatus::Running => {
                            ComponentStatus::Running
                        }
                        drasi_server_core::channels::ComponentStatus::Stopped => {
                            ComponentStatus::Stopped
                        }
                        drasi_server_core::channels::ComponentStatus::Starting => {
                            ComponentStatus::Starting
                        }
                        drasi_server_core::channels::ComponentStatus::Stopping => {
                            ComponentStatus::Stopping
                        }
                        drasi_server_core::channels::ComponentStatus::Error => {
                            ComponentStatus::Error("Error".to_string())
                        }
                    },
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
                .map_err(|e| anyhow!("Failed to get reaction info: {}", e))?;

            Ok(ReactionDetails {
                name: reaction_runtime.id.clone(),
                reaction_type: reaction_runtime.reaction_type.clone(),
                queries: reaction_runtime.queries.clone(),
                status: match reaction_runtime.status {
                    drasi_server_core::channels::ComponentStatus::Running => {
                        ComponentStatus::Running
                    }
                    drasi_server_core::channels::ComponentStatus::Stopped => {
                        ComponentStatus::Stopped
                    }
                    drasi_server_core::channels::ComponentStatus::Starting => {
                        ComponentStatus::Starting
                    }
                    drasi_server_core::channels::ComponentStatus::Stopping => {
                        ComponentStatus::Stopping
                    }
                    drasi_server_core::channels::ComponentStatus::Error => {
                        ComponentStatus::Error("Error".to_string())
                    }
                },
                auto_start: false, // Runtime info doesn't include auto_start
                properties: serde_json::to_value(&reaction_runtime.properties)
                    .map_err(|e| anyhow!("Failed to serialize properties: {}", e))?,
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }

    pub async fn create_reaction(
        &self,
        request: CreateReactionRequest,
    ) -> Result<ReactionCreatedResponse> {
        let core = self.drasi_core.read().await;
        if let Some(core) = core.as_ref() {
            // Build the reaction configuration - determine which builder to use based on type
            let mut builder = match request.reaction_type.as_str() {
                "log" => Reaction::log(&request.name),
                "application" => Reaction::application(&request.name),
                _ => {
                    return Err(anyhow!(
                        "Unsupported reaction type: {}",
                        request.reaction_type
                    ))
                }
            };

            builder = builder.auto_start(request.auto_start);

            // Add query subscriptions
            for query in &request.queries {
                builder = builder.subscribe_to(query);
            }

            // Add properties from the request
            if let Ok(props_map) = serde_json::from_value::<
                std::collections::HashMap<String, serde_json::Value>,
            >(request.properties)
            {
                for (key, value) in props_map {
                    builder = builder.with_property(key, value);
                }
            }

            let reaction_config = builder.build();

            core.create_reaction(reaction_config)
                .await
                .map_err(|e| anyhow!("Failed to create reaction: {}", e))?;

            Ok(ReactionCreatedResponse {
                name: request.name,
                message: "Reaction created successfully".to_string(),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
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
                .map_err(|e| anyhow!("Failed to delete reaction: {}", e))?;
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
                .map_err(|e| anyhow!("Failed to start reaction: {}", e))?;

            Ok(StatusResponse {
                status: "started".to_string(),
                message: Some(format!("Reaction '{}' started successfully", reaction_id)),
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
                .map_err(|e| anyhow!("Failed to stop reaction: {}", e))?;

            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!("Query '{}' stopped successfully", reaction_id)),
            })
        } else {
            Err(anyhow!("Drasi server not running"))
        }
    }
}
