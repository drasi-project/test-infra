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
use drasi_server_core::channels::ComponentStatus;
use drasi_server_core::config::{QueryConfig, ReactionConfig, SourceConfig, QueryLanguage};
use std::collections::HashMap;

use super::TestRunDrasiServer;
use crate::drasi_servers::api_models::{
    ComponentStatus as ApiComponentStatus, CreateQueryRequest, CreateReactionRequest,
    CreateSourceRequest, QueryCreatedResponse, QueryDetails, QueryInfo, ReactionCreatedResponse,
    ReactionDetails, ReactionInfo, SourceCreatedResponse, SourceDetails, SourceInfo,
    StatusResponse, UpdateQueryRequest, UpdateReactionRequest, UpdateSourceRequest,
};

impl TestRunDrasiServer {
    // ===== Sources Management =====

    pub async fn list_sources(&self) -> Result<Vec<SourceInfo>> {
        self.with_core(|core| async move {
            let sources = core.source_manager().list_sources().await;

            let mut source_infos = Vec::new();
            for (name, status) in sources {
                // We need to get the full source config to get the type
                if let Some(config) = core.source_manager().get_source_config(&name).await {
                    source_infos.push(SourceInfo {
                        name: config.id,
                        source_type: config.source_type,
                        status: convert_component_status(status),
                    });
                }
            }

            Ok(source_infos)
        })
        .await
    }

    pub async fn get_source(&self, source_id: &str) -> Result<SourceDetails> {
        let source_id = source_id.to_string();
        self.with_core(|core| async move {
            let source_runtime = core.source_manager().get_source(source_id.clone()).await?;

            // Get the config to find auto_start
            let config = core
                .source_manager()
                .get_source_config(&source_id)
                .await
                .ok_or_else(|| anyhow!("Source config not found"))?;

            Ok(SourceDetails {
                name: source_runtime.id,
                source_type: source_runtime.source_type,
                status: convert_component_status(source_runtime.status),
                auto_start: config.auto_start,
                properties: serde_json::to_value(source_runtime.properties)?,
            })
        })
        .await
    }

    pub async fn create_source(
        &self,
        request: CreateSourceRequest,
    ) -> Result<SourceCreatedResponse> {
        self.with_core(|core| async move {
            let config = SourceConfig {
                id: request.name.clone(),
                source_type: request.source_type,
                auto_start: request.auto_start,
                properties: serde_json::from_value(request.properties)?,
                bootstrap_provider: None,
            };

            core.source_manager().add_source(config).await?;

            Ok(SourceCreatedResponse {
                name: request.name.clone(),
                message: format!("Source '{}' created successfully", request.name),
            })
        })
        .await
    }

    pub async fn update_source(
        &self,
        source_id: &str,
        request: UpdateSourceRequest,
    ) -> Result<SourceDetails> {
        let source_id_str = source_id.to_string();
        let existing = self.get_source(&source_id_str).await?;

        let source_id_clone = source_id_str.clone();
        self.with_core(|core| async move {
            let config = SourceConfig {
                id: source_id_clone.clone(),
                source_type: request.source_type.unwrap_or(existing.source_type),
                auto_start: request.auto_start.unwrap_or(existing.auto_start),
                properties: if let Some(props) = request.properties {
                    serde_json::from_value(props)?
                } else {
                    // Properties from SourceDetails is a Value, need to convert to HashMap
                    serde_json::from_value(existing.properties)?
                },
                bootstrap_provider: None,
            };

            core.source_manager()
                .update_source(source_id_clone, config)
                .await?;
            Ok(())
        })
        .await?;

        self.get_source(&source_id_str).await
    }

    pub async fn delete_source(&self, source_id: &str) -> Result<()> {
        let source_id = source_id.to_string();
        self.with_core(|core| async move { core.source_manager().delete_source(source_id).await })
            .await
    }

    pub async fn start_source(&self, source_id: &str) -> Result<StatusResponse> {
        let source_id_str = source_id.to_string();
        self.with_core(|core| async move {
            core.source_manager()
                .start_source(source_id_str.clone())
                .await?;
            Ok(StatusResponse {
                status: "started".to_string(),
                message: Some(format!("Source '{}' started successfully", source_id_str)),
            })
        })
        .await
    }

    pub async fn stop_source(&self, source_id: &str) -> Result<StatusResponse> {
        let source_id_str = source_id.to_string();
        self.with_core(|core| async move {
            core.source_manager()
                .stop_source(source_id_str.clone())
                .await?;
            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!("Source '{}' stopped successfully", source_id_str)),
            })
        })
        .await
    }

    // ===== Queries Management =====

    pub async fn list_queries(&self) -> Result<Vec<QueryInfo>> {
        self.with_core(|core| async move {
            let queries = core.query_manager().list_queries().await;

            let mut query_infos = Vec::new();
            for (name, status) in queries {
                // We need to get the full query config to get sources
                if let Some(config) = core.query_manager().get_query_config(&name).await {
                    query_infos.push(QueryInfo {
                        name: config.id,
                        sources: config.sources,
                        status: convert_component_status(status),
                    });
                }
            }

            Ok(query_infos)
        })
        .await
    }

    pub async fn get_query(&self, query_id: &str) -> Result<QueryDetails> {
        let query_id = query_id.to_string();
        self.with_core(|core| async move {
            let query_runtime = core.query_manager().get_query(query_id.clone()).await?;

            // Get the config to find auto_start
            let config = core
                .query_manager()
                .get_query_config(&query_id)
                .await
                .ok_or_else(|| anyhow!("Query config not found"))?;

            Ok(QueryDetails {
                name: query_runtime.id,
                query: query_runtime.query,
                sources: query_runtime.sources,
                status: convert_component_status(query_runtime.status),
                auto_start: config.auto_start,
                profiling: query_runtime
                    .properties
                    .get("profiling")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            })
        })
        .await
    }

    pub async fn create_query(&self, request: CreateQueryRequest) -> Result<QueryCreatedResponse> {
        self.with_core(|core| async move {
            let mut properties = HashMap::new();
            if request.profiling {
                properties.insert("profiling".to_string(), serde_json::Value::Bool(true));
            }

            let config = QueryConfig {
                id: request.name.clone(),
                query: request.query,
                query_language: QueryLanguage::Cypher,
                sources: request.sources,
                auto_start: request.auto_start,
                properties,
                joins: None,
            };

            core.query_manager().add_query(config).await?;

            Ok(QueryCreatedResponse {
                name: request.name.clone(),
                message: format!("Query '{}' created successfully", request.name),
            })
        })
        .await
    }

    pub async fn update_query(
        &self,
        query_id: &str,
        request: UpdateQueryRequest,
    ) -> Result<QueryDetails> {
        let query_id_str = query_id.to_string();
        let existing = self.get_query(&query_id_str).await?;

        let query_id_clone = query_id_str.clone();
        self.with_core(|core| async move {
            let mut properties = HashMap::new();
            if let Some(profiling) = request.profiling {
                properties.insert("profiling".to_string(), serde_json::Value::Bool(profiling));
            }

            let config = QueryConfig {
                id: query_id_clone.clone(),
                query: request.query.unwrap_or(existing.query),
                query_language: QueryLanguage::Cypher,
                sources: request.sources.unwrap_or(existing.sources),
                auto_start: request.auto_start.unwrap_or(existing.auto_start),
                properties,
                joins: None,
            };

            core.query_manager()
                .update_query(query_id_clone, config)
                .await?;
            Ok(())
        })
        .await?;

        self.get_query(&query_id_str).await
    }

    pub async fn delete_query(&self, query_id: &str) -> Result<()> {
        let query_id = query_id.to_string();
        self.with_core(|core| async move { core.query_manager().delete_query(query_id).await })
            .await
    }

    pub async fn start_query(&self, query_id: &str) -> Result<StatusResponse> {
        let _query_id_str = query_id.to_string();
        self.with_core(|_core| async move {
            // Note: Starting a query through the manager requires a SourceChangeReceiver
            // which is typically managed internally by DrasiServerCore
            // For now, return an error indicating this operation is not supported
            Err(anyhow!(
                "Starting queries is managed automatically by DrasiServerCore"
            ))
        })
        .await
    }

    pub async fn stop_query(&self, query_id: &str) -> Result<StatusResponse> {
        let query_id_str = query_id.to_string();
        self.with_core(|core| async move {
            core.query_manager()
                .stop_query(query_id_str.clone())
                .await?;
            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!("Query '{}' stopped successfully", query_id_str)),
            })
        })
        .await
    }
    pub async fn get_query_results(&self, query_id: &str) -> Result<serde_json::Value> {
        let query_id = query_id.to_string();
        self.with_core(|core| async move {
            let results = core.query_manager().get_query_results(&query_id).await?;
            Ok(serde_json::Value::Array(results))
        })
        .await
    }

    // ===== Reactions Management =====

    pub async fn list_reactions(&self) -> Result<Vec<ReactionInfo>> {
        self.with_core(|core| async move {
            let reactions = core.reaction_manager().list_reactions().await;

            let mut reaction_infos = Vec::new();
            for (name, status) in reactions {
                // We need to get the full reaction config to get type and queries
                if let Some(config) = core.reaction_manager().get_reaction_config(&name).await {
                    reaction_infos.push(ReactionInfo {
                        name: config.id,
                        reaction_type: config.reaction_type,
                        queries: config.queries,
                        status: convert_component_status(status),
                    });
                }
            }

            Ok(reaction_infos)
        })
        .await
    }

    pub async fn get_reaction(&self, reaction_id: &str) -> Result<ReactionDetails> {
        let reaction_id = reaction_id.to_string();
        self.with_core(|core| async move {
            let reaction_runtime = core
                .reaction_manager()
                .get_reaction(reaction_id.clone())
                .await?;

            // Get the config to find auto_start
            let config = core
                .reaction_manager()
                .get_reaction_config(&reaction_id)
                .await
                .ok_or_else(|| anyhow!("Reaction config not found"))?;

            Ok(ReactionDetails {
                name: reaction_runtime.id,
                reaction_type: reaction_runtime.reaction_type,
                queries: reaction_runtime.queries,
                status: convert_component_status(reaction_runtime.status),
                auto_start: config.auto_start,
                properties: serde_json::to_value(reaction_runtime.properties)?,
            })
        })
        .await
    }

    pub async fn create_reaction(
        &self,
        request: CreateReactionRequest,
    ) -> Result<ReactionCreatedResponse> {
        self.with_core(|core| async move {
            let config = ReactionConfig {
                id: request.name.clone(),
                reaction_type: request.reaction_type,
                queries: request.queries,
                auto_start: request.auto_start,
                properties: serde_json::from_value(request.properties)?,
            };

            core.reaction_manager().add_reaction(config).await?;

            Ok(ReactionCreatedResponse {
                name: request.name.clone(),
                message: format!("Reaction '{}' created successfully", request.name),
            })
        })
        .await
    }

    pub async fn update_reaction(
        &self,
        reaction_id: &str,
        request: UpdateReactionRequest,
    ) -> Result<ReactionDetails> {
        let reaction_id_str = reaction_id.to_string();
        let existing = self.get_reaction(&reaction_id_str).await?;

        let reaction_id_clone = reaction_id_str.clone();
        self.with_core(|core| async move {
            let config = ReactionConfig {
                id: reaction_id_clone.clone(),
                reaction_type: request.reaction_type.unwrap_or(existing.reaction_type),
                queries: request.queries.unwrap_or(existing.queries),
                auto_start: request.auto_start.unwrap_or(existing.auto_start),
                properties: if let Some(props) = request.properties {
                    serde_json::from_value(props)?
                } else {
                    // Properties from ReactionDetails is a Value, need to convert to HashMap
                    serde_json::from_value(existing.properties)?
                },
            };

            core.reaction_manager()
                .update_reaction(reaction_id_clone, config)
                .await?;
            Ok(())
        })
        .await?;

        self.get_reaction(&reaction_id_str).await
    }

    pub async fn delete_reaction(&self, reaction_id: &str) -> Result<()> {
        let reaction_id = reaction_id.to_string();
        self.with_core(
            |core| async move { core.reaction_manager().delete_reaction(reaction_id).await },
        )
        .await
    }

    pub async fn start_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let _reaction_id_str = reaction_id.to_string();
        self.with_core(|_core| async move {
            // Note: Starting a reaction through the manager requires a QueryResultReceiver
            // which is typically managed internally by DrasiServerCore
            // For now, return an error indicating this operation is not supported
            Err(anyhow!(
                "Starting reactions is managed automatically by DrasiServerCore"
            ))
        })
        .await
    }

    pub async fn stop_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let reaction_id_str = reaction_id.to_string();
        self.with_core(|core| async move {
            core.reaction_manager()
                .stop_reaction(reaction_id_str.clone())
                .await?;
            Ok(StatusResponse {
                status: "stopped".to_string(),
                message: Some(format!(
                    "Reaction '{}' stopped successfully",
                    reaction_id_str
                )),
            })
        })
        .await
    }
}

// Helper function to convert ComponentStatus
fn convert_component_status(status: ComponentStatus) -> ApiComponentStatus {
    match status {
        ComponentStatus::Starting => ApiComponentStatus::Starting,
        ComponentStatus::Running => ApiComponentStatus::Running,
        ComponentStatus::Stopping => ApiComponentStatus::Stopping,
        ComponentStatus::Stopped => ApiComponentStatus::Stopped,
        ComponentStatus::Error => ApiComponentStatus::Error("Component error".to_string()),
    }
}
