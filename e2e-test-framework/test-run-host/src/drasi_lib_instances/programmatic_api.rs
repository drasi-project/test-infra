// Copyright 2025 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use anyhow::{anyhow, Context, Result};
use drasi_lib::{computation::v1::*, ComponentStatus, DrasiLib, QueryConfig};
use drasi_reaction_application::NativeApplicationReaction;
use drasi_source_application::{ApplicationSourceHandle, NativeApplicationSource};
use std::{
    collections::{BTreeSet, HashMap},
    num::NonZeroUsize,
};

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
                let info = core.get_source_info(&name).await?;
                let (source_type, _) = component_properties(info.source_type, info.properties)?;
                infos.push(SourceInfo {
                    name: info.id,
                    source_type,
                    status: convert_component_status(status),
                });
            }
            Ok(infos)
        })
        .await
    }

    pub async fn get_source(&self, source_id: &str) -> Result<SourceDetails> {
        let source_id = source_id.to_string();
        self.with_core(|core| async move {
            let info = core.get_source_info(&source_id).await?;
            let auto_start = component_auto_start(&core, &source_id)?;
            let (source_type, properties) =
                component_properties(info.source_type, info.properties)?;
            Ok(SourceDetails {
                name: info.id,
                source_type,
                status: convert_component_status(info.status),
                auto_start,
                properties,
            })
        })
        .await
    }

    pub async fn create_source(
        &self,
        request: CreateSourceRequest,
    ) -> Result<SourceCreatedResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        if request.source_type != "application" {
            return Err(anyhow!(
                "Unsupported drasi-lib instance source kind '{}'",
                request.source_type
            ));
        }
        let scope = self.id.to_string();
        let name = request.name.clone();
        let handle = self
            .with_core(|core| async move {
                Self::add_native_application_source(
                    &core,
                    &scope,
                    &name,
                    &request.properties,
                    request.auto_start,
                )
                .await
            })
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
        let _lifecycle = self.lifecycle.lock().await;
        let existing = self.get_source(source_id).await?;
        let create = CreateSourceRequest {
            name: source_id.to_string(),
            source_type: request.source_type.unwrap_or(existing.source_type),
            auto_start: request.auto_start.unwrap_or(existing.auto_start),
            properties: request.properties.unwrap_or(existing.properties),
        };
        anyhow::ensure!(
            create.source_type == "application",
            "unsupported source kind '{}'",
            create.source_type
        );
        application_source_properties(&create.properties)?;
        let core = self
            .get_drasi_lib()
            .await
            .context("drasi-lib instance is not running")?;
        core.remove_source(source_id, false).await?;
        self.source_handles.write().await.remove(source_id);
        let handle = Self::add_native_application_source(
            &core,
            &self.id.to_string(),
            source_id,
            &create.properties,
            create.auto_start,
        )
        .await?;
        self.source_handles
            .write()
            .await
            .insert(source_id.to_string(), handle);
        self.get_source(source_id).await
    }

    pub async fn delete_source(&self, source_id: &str) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = source_id.to_string();
        self.with_core(
            |core| async move { core.remove_source(&id, false).await.map_err(Into::into) },
        )
        .await?;
        self.source_handles.write().await.remove(source_id);
        Ok(())
    }

    pub async fn start_source(&self, source_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = source_id.to_string();
        self.with_core(|core| async move { core.start_source(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Source '{source_id}' started successfully")),
        })
    }

    pub async fn stop_source(&self, source_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
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
                let info = core.get_query_info(&name).await?;
                infos.push(QueryInfo {
                    name: info.id,
                    inputs: info
                        .inputs
                        .into_iter()
                        .map(|input| input.component)
                        .collect(),
                    status: convert_component_status(status),
                });
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
                inputs: info
                    .inputs
                    .into_iter()
                    .map(|input| input.component)
                    .collect(),
                status: convert_component_status(info.status),
                auto_start: config.auto_start,
                profiling: false,
            })
        })
        .await
    }

    pub async fn create_query(&self, request: CreateQueryRequest) -> Result<QueryCreatedResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let config = query_config_from_request(
            &request.name,
            request.query,
            request.inputs,
            request.auto_start,
        );
        let name = request.name.clone();
        self.with_core(|core| async move { Self::add_native_query(&core, config).await })
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
        let _lifecycle = self.lifecycle.lock().await;
        let existing = self.get_query(query_id).await?;
        let config = query_config_from_request(
            query_id,
            request.query.unwrap_or(existing.query),
            request.inputs.unwrap_or(existing.inputs),
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
        let _lifecycle = self.lifecycle.lock().await;
        let id = query_id.to_string();
        self.with_core(|core| async move { core.remove_query(&id).await.map_err(Into::into) })
            .await
    }

    pub async fn start_query(&self, query_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = query_id.to_string();
        self.with_core(|core| async move { core.start_query(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Query '{query_id}' started successfully")),
        })
    }

    pub async fn stop_query(&self, query_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
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
                let info = core.get_reaction_info(&name).await?;
                let (reaction_type, _) = component_properties(info.reaction_type, info.properties)?;
                let queries = if reaction_type == "application" {
                    reaction_inputs(&core, &name)?
                } else {
                    info.queries
                };
                infos.push(ReactionInfo {
                    name: info.id,
                    reaction_type,
                    queries,
                    status: convert_component_status(status),
                });
            }
            Ok(infos)
        })
        .await
    }

    pub async fn get_reaction(&self, reaction_id: &str) -> Result<ReactionDetails> {
        let id = reaction_id.to_string();
        self.with_core(|core| async move {
            let info = core.get_reaction_info(&id).await?;
            let auto_start = component_auto_start(&core, &id)?;
            let (reaction_type, properties) =
                component_properties(info.reaction_type, info.properties)?;
            let queries = if reaction_type == "application" {
                reaction_inputs(&core, &id)?
            } else {
                info.queries
            };
            Ok(ReactionDetails {
                name: info.id,
                reaction_type,
                queries,
                status: convert_component_status(info.status),
                auto_start,
                properties,
            })
        })
        .await
    }

    pub async fn create_reaction(
        &self,
        request: CreateReactionRequest,
    ) -> Result<ReactionCreatedResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        if request.reaction_type != "application" {
            return Err(anyhow!(
                "Unsupported drasi-lib instance reaction kind '{}'",
                request.reaction_type
            ));
        }
        let name = request.name.clone();
        let receiver = self
            .with_core(|core| async move {
                Self::add_native_application_reaction(
                    &core,
                    &request.name,
                    &request.queries,
                    request.auto_start,
                    &request.properties,
                )
                .await
            })
            .await?;
        self.reaction_receivers
            .write()
            .await
            .insert(name.clone(), receiver);
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
        let _lifecycle = self.lifecycle.lock().await;
        let existing = self.get_reaction(reaction_id).await?;
        let create = CreateReactionRequest {
            name: reaction_id.to_string(),
            reaction_type: request.reaction_type.unwrap_or(existing.reaction_type),
            queries: request.queries.unwrap_or(existing.queries),
            auto_start: request.auto_start.unwrap_or(existing.auto_start),
            properties: request.properties.unwrap_or(existing.properties),
        };
        anyhow::ensure!(
            create.reaction_type == "application",
            "unsupported reaction kind '{}'",
            create.reaction_type
        );
        application_properties(&create.properties)?;
        for query in &create.queries {
            ComponentId::try_new(query.as_str())?;
        }
        anyhow::ensure!(
            self.reaction_receivers.read().await.contains_key(reaction_id),
            "reaction '{reaction_id}' has an application-owned receiver; stop its observer and explicitly replace the reaction binding"
        );
        let core = self
            .get_drasi_lib()
            .await
            .context("drasi-lib instance is not running")?;
        core.remove_reaction(reaction_id, false).await?;
        self.reaction_receivers.write().await.remove(reaction_id);
        let receiver = Self::add_native_application_reaction(
            &core,
            reaction_id,
            &create.queries,
            create.auto_start,
            &create.properties,
        )
        .await?;
        self.reaction_receivers
            .write()
            .await
            .insert(reaction_id.to_string(), receiver);
        self.get_reaction(reaction_id).await
    }

    pub async fn delete_reaction(&self, reaction_id: &str) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = reaction_id.to_string();
        self.with_core(
            |core| async move { core.remove_reaction(&id, false).await.map_err(Into::into) },
        )
        .await?;
        self.reaction_receivers.write().await.remove(reaction_id);
        Ok(())
    }

    pub(super) async fn add_native_application_reaction(
        core: &DrasiLib,
        id: &str,
        queries: &[String],
        auto_start: bool,
        properties: &serde_json::Value,
    ) -> Result<tokio::sync::mpsc::Receiver<InputEnvelope>> {
        let properties = application_properties(properties)?;
        let capacity = std::num::NonZeroUsize::new(1000).context("reaction capacity")?;
        let (reaction, receiver) = NativeApplicationReaction::channel(
            id,
            QueryChangeCodec::schema().descriptor().clone(),
            capacity,
        )?;
        let input = reaction.input();
        let mut batch = ComponentBatch::builder()
            .sink(Box::new(reaction.with_properties(properties)))
            .input_merge(
                input.component.clone(),
                InputMergePolicy::EventTimeAcrossStreams,
            )
            .lifecycle_policy(input.component.clone(), LifecyclePolicy { auto_start })
            .build()?;
        for query in queries {
            batch
                .definition
                .boundary_relationships
                .push(DesiredRelationship {
                    definition: EdgeDefinition::new(
                        Endpoint::new(
                            ComponentId::try_new(query.as_str())?,
                            PortId::try_new("out")?,
                        ),
                        input.clone(),
                    ),
                    policy: RelationshipPolicy::default(),
                    pipe: DesiredPipe::Bounded {
                        capacity: capacity.get(),
                    },
                });
        }
        let report = core.add_components(batch).await?;
        anyhow::ensure!(
            report.committed,
            "reaction declaration was rejected: {report:?}"
        );
        Ok(receiver)
    }

    pub async fn start_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = reaction_id.to_string();
        self.with_core(|core| async move { core.start_reaction(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Reaction '{reaction_id}' started successfully")),
        })
    }

    pub async fn stop_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = reaction_id.to_string();
        self.with_core(|core| async move { core.stop_reaction(&id).await.map_err(Into::into) })
            .await?;
        Ok(StatusResponse {
            status: "stopped".to_string(),
            message: Some(format!("Reaction '{reaction_id}' stopped successfully")),
        })
    }
}

impl TestRunDrasiLibInstance {
    pub(super) async fn add_native_application_source(
        core: &DrasiLib,
        scope: &str,
        id: &str,
        properties: &serde_json::Value,
        auto_start: bool,
    ) -> Result<ApplicationSourceHandle> {
        let properties = application_source_properties(properties)?;
        let stream = StreamId::try_new(format!("{id}/out"))?;
        let (source, handle) = NativeApplicationSource::channel(
            scope,
            core.computation_control()?
                .desired_snapshot()
                .id
                .to_string(),
            id,
            stream.clone(),
            NonZeroUsize::new(1000).context("application input capacity")?,
        )?;
        let output = source.output();
        let batch = ComponentBatch::builder()
            .source(Box::new(source.with_properties(properties)))
            .lifecycle_policy(output.component.clone(), LifecyclePolicy { auto_start })
            .bind_stream(output, stream)
            .build()?;
        let report = core.add_components(batch).await?;
        anyhow::ensure!(
            report.committed,
            "source declaration was rejected: {report:?}"
        );
        Ok(handle)
    }

    pub(super) async fn add_native_query(core: &DrasiLib, config: QueryConfig) -> Result<()> {
        let mut builder = core.computation_pipeline()?;
        for input in &config.inputs {
            builder = builder.native_source(Endpoint::new(
                ComponentId::try_new(input.component.as_str())?,
                PortId::try_new(input.port.as_deref().unwrap_or("out"))?,
            ))?;
        }
        let report = core.add_components(builder.query(config).build()?).await?;
        anyhow::ensure!(
            report.committed,
            "query declaration was rejected: {report:?}"
        );
        Ok(())
    }
}

fn application_properties(value: &serde_json::Value) -> Result<HashMap<String, serde_json::Value>> {
    serde_json::from_value(value.clone()).context("Application properties must be an object")
}

fn application_source_properties(
    value: &serde_json::Value,
) -> Result<HashMap<String, serde_json::Value>> {
    let properties = application_properties(value)?;
    anyhow::ensure!(
        !properties.contains_key("durability"),
        "the embedded test host uses volatile native application input; legacy WAL durability is unsupported, and durable input requires an explicit native SourceAdmission binding"
    );
    Ok(properties)
}

fn component_auto_start(core: &DrasiLib, id: &str) -> Result<bool> {
    core.computation_control()?
        .desired_snapshot()
        .lifecycle_policies
        .get(&ComponentId::try_new(id)?)
        .map(|policy| policy.auto_start)
        .context("component lifecycle policy is unavailable")
}

fn component_properties(
    implementation: String,
    mut properties: HashMap<String, serde_json::Value>,
) -> Result<(String, serde_json::Value)> {
    if implementation == "native"
        && properties.get("kind").and_then(serde_json::Value::as_str) == Some("application")
    {
        let properties = properties
            .remove("properties")
            .context("native application properties are unavailable")?;
        anyhow::ensure!(
            properties.is_object(),
            "native application properties must be an object"
        );
        Ok(("application".into(), properties))
    } else {
        Ok((implementation, serde_json::to_value(properties)?))
    }
}

fn reaction_inputs(core: &DrasiLib, id: &str) -> Result<Vec<String>> {
    let snapshot = core.computation_control()?.desired_snapshot();
    Ok(snapshot
        .edges
        .iter()
        .map(|edge| &edge.definition)
        .chain(
            snapshot
                .unbound_relationships
                .iter()
                .map(|edge| &edge.definition),
        )
        .filter(|edge| edge.to.component.as_str() == id)
        .map(|edge| edge.from.component.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

pub(super) fn query_config_from_request(
    id: &str,
    query: String,
    inputs: Vec<String>,
    auto_start: bool,
) -> QueryConfig {
    // Embedded application sources are live-only inputs with no snapshot provider, so
    // their queries explicitly opt out of bootstrap rather than relying on a default.
    let mut builder = drasi_lib::Query::cypher(id)
        .query(query)
        .auto_start(auto_start)
        .enable_bootstrap(false);
    for input in inputs {
        builder = builder.from_input(input);
    }
    builder.build()
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
