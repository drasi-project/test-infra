// Copyright 2025 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use anyhow::{anyhow, Result};
use drasi_lib::{ComponentStatus, QueryConfig, QueryLanguage};
use drasi_reaction_application::ApplicationReaction;
use drasi_source_application::{ApplicationSource, ApplicationSourceConfig};

use super::TestRunDrasiLibInstance;
use crate::drasi_lib_instances::api_models::{
    ComponentStatus as ApiComponentStatus, CreateQueryRequest, CreateReactionRequest,
    CreateSourceRequest, QueryCreatedResponse, QueryDetails, QueryInfo, ReactionCreatedResponse,
    ReactionDetails, ReactionInfo, SourceCreatedResponse, SourceDetails, SourceInfo,
    StatusResponse, UpdateQueryRequest, UpdateReactionRequest, UpdateSourceRequest,
};

impl TestRunDrasiLibInstance {
    pub async fn list_sources(&self) -> Result<Vec<SourceInfo>> {
        self.with_core(|core| async move {
            let sources = core.list_sources().await?;
            let mut infos = Vec::new();
            for (name, status) in sources {
                if let Ok(info) = core.get_source_info(&name).await {
                    infos.push(SourceInfo {
                        name: info.id,
                        source_type: info.source_type,
                        status: convert_component_status(status),
                    });
                }
            }
            Ok(infos)
        })
        .await
    }

    pub async fn get_source(&self, source_id: &str) -> Result<SourceDetails> {
        let source_id = source_id.to_string();
        self.with_core(|core| async move {
            let info = core.get_source_info(&source_id).await?;
            Ok(SourceDetails {
                name: info.id,
                source_type: info.source_type,
                status: convert_component_status(info.status),
                auto_start: true,
                properties: serde_json::to_value(info.properties)?,
            })
        })
        .await
    }

    pub async fn create_source(
        &self,
        request: CreateSourceRequest,
    ) -> Result<SourceCreatedResponse> {
        if request.source_type != "application" {
            return Err(anyhow!(
                "Unsupported drasi-lib instance source kind '{}'",
                request.source_type
            ));
        }
        let properties = serde_json::from_value(request.properties.clone()).unwrap_or_default();
        let (source, handle) =
            ApplicationSource::new(request.name.clone(), ApplicationSourceConfig { properties })?;
        self.with_core(|core| async move { core.add_source(source).await.map_err(Into::into) })
            .await?;
        self.source_handles
            .write()
            .await
            .insert(request.name.clone(), handle);
        Ok(SourceCreatedResponse {
            name: request.name.clone(),
            message: format!("Source '{}' created successfully", request.name),
        })
    }

    pub async fn update_source(
        &self,
        source_id: &str,
        request: UpdateSourceRequest,
    ) -> Result<SourceDetails> {
        self.delete_source(source_id).await?;
        let create = CreateSourceRequest {
            name: source_id.to_string(),
            source_type: request
                .source_type
                .unwrap_or_else(|| "application".to_string()),
            auto_start: request.auto_start.unwrap_or(true),
            properties: request.properties.unwrap_or_else(|| serde_json::json!({})),
        };
        self.create_source(create).await?;
        self.get_source(source_id).await
    }

    pub async fn delete_source(&self, source_id: &str) -> Result<()> {
        let id = source_id.to_string();
        self.with_core(
            |core| async move { core.remove_source(&id, false).await.map_err(Into::into) },
        )
        .await?;
        self.source_handles.write().await.remove(source_id);
        Ok(())
    }

    pub async fn start_source(&self, source_id: &str) -> Result<StatusResponse> {
        let id = source_id.to_string();
        self.with_core(|core| async move { core.start_source(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Source '{source_id}' started successfully")),
        })
    }

    pub async fn stop_source(&self, source_id: &str) -> Result<StatusResponse> {
        let id = source_id.to_string();
        self.with_core(|core| async move { core.stop_source(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "stopped".to_string(),
            message: Some(format!("Source '{source_id}' stopped successfully")),
        })
    }

    pub async fn list_queries(&self) -> Result<Vec<QueryInfo>> {
        self.with_core(|core| async move {
            let queries = core.list_queries().await?;
            let mut infos = Vec::new();
            for (name, status) in queries {
                if let Ok(info) = core.get_query_info(&name).await {
                    infos.push(QueryInfo {
                        name: info.id,
                        sources: info
                            .source_subscriptions
                            .into_iter()
                            .map(|s| s.source_id)
                            .collect(),
                        status: convert_component_status(status),
                    });
                }
            }
            Ok(infos)
        })
        .await
    }

    pub async fn get_query(&self, query_id: &str) -> Result<QueryDetails> {
        let id = query_id.to_string();
        self.with_core(|core| async move {
            let info = core.get_query_info(&id).await?;
            let config = core.get_query_config(&id).await?;
            Ok(QueryDetails {
                name: info.id,
                query: info.query,
                sources: info
                    .source_subscriptions
                    .into_iter()
                    .map(|s| s.source_id)
                    .collect(),
                status: convert_component_status(info.status),
                auto_start: config.auto_start,
                profiling: false,
            })
        })
        .await
    }

    pub async fn create_query(&self, request: CreateQueryRequest) -> Result<QueryCreatedResponse> {
        let config = query_config_from_request(
            &request.name,
            request.query,
            request.sources,
            request.auto_start,
        );
        let name = request.name.clone();
        self.with_core(|core| async move { core.add_query(config).await.map_err(Into::into) })
            .await?;
        Ok(QueryCreatedResponse {
            name: name.clone(),
            message: format!("Query '{name}' created successfully"),
        })
    }

    pub async fn update_query(
        &self,
        query_id: &str,
        request: UpdateQueryRequest,
    ) -> Result<QueryDetails> {
        let existing = self.get_query(query_id).await?;
        let config = query_config_from_request(
            query_id,
            request.query.unwrap_or(existing.query),
            request.sources.unwrap_or(existing.sources),
            request.auto_start.unwrap_or(existing.auto_start),
        );
        let id = query_id.to_string();
        self.with_core(
            |core| async move { core.update_query(&id, config).await.map_err(Into::into) },
        )
        .await?;
        self.get_query(query_id).await
    }

    pub async fn delete_query(&self, query_id: &str) -> Result<()> {
        let id = query_id.to_string();
        self.with_core(|core| async move { core.remove_query(&id).await.map_err(Into::into) })
            .await
    }

    pub async fn start_query(&self, query_id: &str) -> Result<StatusResponse> {
        let id = query_id.to_string();
        self.with_core(|core| async move { core.start_query(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Query '{query_id}' started successfully")),
        })
    }

    pub async fn stop_query(&self, query_id: &str) -> Result<StatusResponse> {
        let id = query_id.to_string();
        self.with_core(|core| async move { core.stop_query(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "stopped".to_string(),
            message: Some(format!("Query '{query_id}' stopped successfully")),
        })
    }

    pub async fn list_reactions(&self) -> Result<Vec<ReactionInfo>> {
        self.with_core(|core| async move {
            let reactions = core.list_reactions().await?;
            let mut infos = Vec::new();
            for (name, status) in reactions {
                if let Ok(info) = core.get_reaction_info(&name).await {
                    infos.push(ReactionInfo {
                        name: info.id,
                        reaction_type: info.reaction_type,
                        queries: info.queries,
                        status: convert_component_status(status),
                    });
                }
            }
            Ok(infos)
        })
        .await
    }

    pub async fn get_reaction(&self, reaction_id: &str) -> Result<ReactionDetails> {
        let id = reaction_id.to_string();
        self.with_core(|core| async move {
            let info = core.get_reaction_info(&id).await?;
            Ok(ReactionDetails {
                name: info.id,
                reaction_type: info.reaction_type,
                queries: info.queries,
                status: convert_component_status(info.status),
                auto_start: true,
                properties: serde_json::to_value(info.properties)?,
            })
        })
        .await
    }

    pub async fn create_reaction(
        &self,
        request: CreateReactionRequest,
    ) -> Result<ReactionCreatedResponse> {
        if request.reaction_type != "application" {
            return Err(anyhow!(
                "Unsupported drasi-lib instance reaction kind '{}'",
                request.reaction_type
            ));
        }
        let (reaction, handle) = ApplicationReaction::builder(request.name.clone())
            .with_queries(request.queries.clone())
            .with_auto_start(request.auto_start)
            .build();
        let name = request.name.clone();
        self.with_core(|core| async move { core.add_reaction(reaction).await.map_err(Into::into) })
            .await?;
        self.reaction_handles
            .write()
            .await
            .insert(name.clone(), handle);
        Ok(ReactionCreatedResponse {
            name: name.clone(),
            message: format!("Reaction '{name}' created successfully"),
        })
    }

    pub async fn update_reaction(
        &self,
        reaction_id: &str,
        request: UpdateReactionRequest,
    ) -> Result<ReactionDetails> {
        self.delete_reaction(reaction_id).await?;
        let create = CreateReactionRequest {
            name: reaction_id.to_string(),
            reaction_type: request
                .reaction_type
                .unwrap_or_else(|| "application".to_string()),
            queries: request.queries.unwrap_or_default(),
            auto_start: request.auto_start.unwrap_or(true),
            properties: request.properties.unwrap_or_else(|| serde_json::json!({})),
        };
        self.create_reaction(create).await?;
        self.get_reaction(reaction_id).await
    }

    pub async fn delete_reaction(&self, reaction_id: &str) -> Result<()> {
        let id = reaction_id.to_string();
        self.with_core(
            |core| async move { core.remove_reaction(&id, false).await.map_err(Into::into) },
        )
        .await?;
        self.reaction_handles.write().await.remove(reaction_id);
        Ok(())
    }

    pub async fn start_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let id = reaction_id.to_string();
        self.with_core(|core| async move { core.start_reaction(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Reaction '{reaction_id}' started successfully")),
        })
    }

    pub async fn stop_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let id = reaction_id.to_string();
        self.with_core(|core| async move { core.stop_reaction(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "stopped".to_string(),
            message: Some(format!("Reaction '{reaction_id}' stopped successfully")),
        })
    }
}

fn query_config_from_request(
    id: &str,
    query: String,
    sources: Vec<String>,
    auto_start: bool,
) -> QueryConfig {
    QueryConfig {
        id: id.to_string(),
        query,
        query_language: QueryLanguage::Cypher,
        middleware: Vec::new(),
        sources: sources
            .into_iter()
            .map(|source_id| drasi_lib::config::SourceSubscriptionConfig {
                source_id,
                nodes: Vec::new(),
                relations: Vec::new(),
                pipeline: Vec::new(),
            })
            .collect(),
        auto_start,
        joins: None,
        enable_bootstrap: true,
        bootstrap_buffer_size: 10_000,
        priority_queue_capacity: None,
        dispatch_buffer_capacity: None,
        dispatch_mode: None,
        storage_backend: None,
        recovery_policy: None,
    }
}

fn convert_component_status(status: ComponentStatus) -> ApiComponentStatus {
    match status {
        ComponentStatus::Running => ApiComponentStatus::Running,
        ComponentStatus::Stopped => ApiComponentStatus::Stopped,
        ComponentStatus::Starting => ApiComponentStatus::Starting,
        ComponentStatus::Stopping => ApiComponentStatus::Stopping,
        ComponentStatus::Error => ApiComponentStatus::Error("Component error".to_string()),
        ComponentStatus::Reconfiguring => ApiComponentStatus::Starting,
        ComponentStatus::Added | ComponentStatus::Removed => ApiComponentStatus::Stopped,
    }
}
