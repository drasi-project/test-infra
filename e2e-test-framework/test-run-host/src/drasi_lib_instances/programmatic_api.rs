// Copyright 2025 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use anyhow::{anyhow, Context, Result};
use drasi_lib::{computation::v1::*, ComponentStatus, DrasiLib, QueryConfig};
use std::collections::{BTreeSet, HashMap};

use super::application_factories::ApplicationFactory;
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
                    id: info.id.to_string(),
                    name: info.name.to_string(),
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
            let source_id = core.component_by_name(&source_id)?.id().to_string();
            let info = core.get_source_info(&source_id).await?;
            let auto_start = component_auto_start(&core, &source_id)?;
            let (source_type, properties) =
                component_properties(info.source_type, info.properties)?;
            Ok(SourceDetails {
                id: info.id.to_string(),
                name: info.name.to_string(),
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
        let name = request.name.clone();
        let id = self
            .with_core(|core| async move {
                self.add_native_application_source(
                    &core,
                    &name,
                    &request.properties,
                    request.auto_start,
                )
                .await
            })
            .await?;
        Ok(SourceCreatedResponse {
            id: id.to_string(),
            name: request.name.clone(),
            message: format!("Source '{}' declaration accepted", request.name),
        })
    }

    pub async fn update_source(
        &self,
        source_id: &str,
        _request: UpdateSourceRequest,
    ) -> Result<SourceDetails> {
        let _lifecycle = self.lifecycle.lock().await;
        self.get_source(source_id).await?;
        anyhow::bail!(
            "application sources have creation-only factories; explicitly disconnect dependents, delete the source, and create a new binding with a new UUID"
        )
    }

    pub async fn delete_source(&self, source_id: &str) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = source_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.remove_source(&id, false).await.map_err(Into::into)
        })
        .await?;
        self.source_handles.write().await.remove(source_id);
        Ok(())
    }

    pub async fn start_source(&self, source_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = source_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.start_source(&id).await.map_err(Into::into)
        })
        .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Source '{source_id}' started successfully")),
        })
    }

    pub async fn stop_source(&self, source_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = source_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.stop_source(&id).await.map_err(Into::into)
        })
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
                    id: info.id.to_string(),
                    name: info.name.to_string(),
                    inputs: info
                        .inputs
                        .into_iter()
                        .map(|input| reference_name(&core, &input.component))
                        .collect::<Result<_>>()?,
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
            let id = core.component_by_name(&id)?.id().to_string();
            let info = core.get_query_info(&id).await?;
            let config = core.get_query_config(&id).await?;
            Ok(QueryDetails {
                id: info.id.to_string(),
                name: info.name.to_string(),
                query: info.query,
                inputs: info
                    .inputs
                    .into_iter()
                    .map(|input| reference_name(&core, &input.component))
                    .collect::<Result<_>>()?,
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
        )?;
        let name = request.name.clone();
        let id = self
            .with_core(|core| async move { Self::add_native_query(&core, config).await })
            .await?;
        Ok(QueryCreatedResponse {
            id: id.to_string(),
            name: name.clone(),
            message: format!("Query '{name}' declaration accepted"),
        })
    }

    pub async fn update_query(
        &self,
        query_id: &str,
        request: UpdateQueryRequest,
    ) -> Result<QueryDetails> {
        let _lifecycle = self.lifecycle.lock().await;
        let existing = self.get_query(query_id).await?;
        let preserve_inputs = request.inputs.is_none();
        let mut config = query_config_from_request(
            query_id,
            request.query.unwrap_or(existing.query),
            request.inputs.unwrap_or(existing.inputs),
            request.auto_start.unwrap_or(existing.auto_start),
        )?;
        let id = query_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            if preserve_inputs {
                config.inputs = core.get_query_config(&id).await?.inputs;
            }
            core.update_query(&id, config).await.map_err(Into::into)
        })
        .await?;
        self.get_query(query_id).await
    }

    pub async fn delete_query(&self, query_id: &str) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = query_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.remove_query(&id).await.map_err(Into::into)
        })
        .await
    }

    pub async fn start_query(&self, query_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = query_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.start_query(&id).await.map_err(Into::into)
        })
        .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Query '{query_id}' started successfully")),
        })
    }

    pub async fn stop_query(&self, query_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = query_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.stop_query(&id).await.map_err(Into::into)
        })
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
                        .iter()
                        .map(|reference| reference_name(&core, reference))
                        .collect::<Result<_>>()?
                };
                infos.push(ReactionInfo {
                    id: info.id.to_string(),
                    name: info.name.to_string(),
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
            let id = core.component_by_name(&id)?.id().to_string();
            let info = core.get_reaction_info(&id).await?;
            let auto_start = component_auto_start(&core, &id)?;
            let (reaction_type, properties) =
                component_properties(info.reaction_type, info.properties)?;
            let queries = if reaction_type == "application" {
                reaction_inputs(&core, &id)?
            } else {
                info.queries
                    .iter()
                    .map(|reference| reference_name(&core, reference))
                    .collect::<Result<_>>()?
            };
            Ok(ReactionDetails {
                id: info.id.to_string(),
                name: info.name.to_string(),
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
        let id = self
            .with_core(|core| async move {
                self.add_native_application_reaction(
                    &core,
                    &request.name,
                    &request.queries,
                    request.auto_start,
                    &request.properties,
                )
                .await
            })
            .await?;
        Ok(ReactionCreatedResponse {
            id: id.to_string(),
            name: name.clone(),
            message: format!("Reaction '{name}' declaration accepted"),
        })
    }

    pub async fn update_reaction(
        &self,
        reaction_id: &str,
        _request: UpdateReactionRequest,
    ) -> Result<ReactionDetails> {
        let _lifecycle = self.lifecycle.lock().await;
        self.get_reaction(reaction_id).await?;
        anyhow::bail!(
            "application reactions have creation-only factories; stop the observer, explicitly delete the reaction, and create a new receiver binding with a new UUID"
        )
    }

    pub async fn delete_reaction(&self, reaction_id: &str) -> Result<()> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = reaction_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.remove_reaction(&id, false).await.map_err(Into::into)
        })
        .await?;
        self.reaction_receivers.write().await.remove(reaction_id);
        Ok(())
    }

    pub(super) async fn add_native_application_reaction(
        &self,
        core: &DrasiLib,
        name: &str,
        queries: &[String],
        auto_start: bool,
        properties: &serde_json::Value,
    ) -> Result<ComponentId> {
        let properties = application_properties(properties)?;
        let name = ComponentName::try_new(name)?;
        let factory = ApplicationFactory::reaction(self.reaction_receivers.clone());
        let mut inputs = Vec::new();
        for query in queries {
            let id = core.component_by_name(query)?.id().clone();
            core.get_query_info(id.as_str()).await?;
            inputs.push(id);
        }
        let current = core.desired_configuration()?;
        let request = core.new_configuration_request_id().await?;
        let graph_id = current.desired.topology.graph_id;
        let receipt = core
            .create_managed_components(current.revision, request, move |ids| {
                let id = ids.allocate()?;
                (|| -> Result<_> {
                    let mut topology = ComputationGraph::empty(graph_id)?
                        .snapshot()
                        .select(GraphSelection::All)?;
                    let specification = factory.specification_from_config(
                        id.clone(),
                        serde_json::json!({"slot": name, "properties": properties}),
                    )?;
                    let mut addition =
                        ComponentAddition::declaration(name, specification).auto_start(auto_start);
                    addition.definition.input_merge = InputMergePolicy::EventTimeAcrossStreams;
                    topology.components.push(addition.definition);
                    let input = Endpoint::new(id.clone(), PortId::try_new("in")?);
                    for query in inputs {
                        topology.relationships.push(DesiredRelationship {
                            definition: EdgeDefinition::new(
                                Endpoint::new(query, PortId::try_new("out")?),
                                input.clone(),
                            ),
                            policy: RelationshipPolicy::default(),
                            pipe: DesiredPipe::Bounded { capacity: 1000 },
                        });
                    }
                    Ok(topology)
                })()
                .map_err(|source| GraphError::Component {
                    component: id,
                    operation: "declare application reaction",
                    source,
                })
            })
            .await?;
        let id = accepted_id(&receipt)?;
        await_created(core, &id).await?;
        Ok(id)
    }

    pub async fn start_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = reaction_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.start_reaction(&id).await.map_err(Into::into)
        })
        .await?;
        Ok(StatusResponse {
            status: "started".to_string(),
            message: Some(format!("Reaction '{reaction_id}' started successfully")),
        })
    }

    pub async fn stop_reaction(&self, reaction_id: &str) -> Result<StatusResponse> {
        let _lifecycle = self.lifecycle.lock().await;
        let id = reaction_id.to_string();
        self.with_core(|core| async move {
            let id = core.component_by_name(&id)?.id().to_string();
            core.stop_reaction(&id).await.map_err(Into::into)
        })
        .await?;
        Ok(StatusResponse {
            status: "stopped".to_string(),
            message: Some(format!("Reaction '{reaction_id}' stopped successfully")),
        })
    }
}

impl TestRunDrasiLibInstance {
    pub(super) async fn add_native_application_source(
        &self,
        core: &DrasiLib,
        name: &str,
        properties: &serde_json::Value,
        auto_start: bool,
    ) -> Result<ComponentId> {
        let properties = application_source_properties(properties)?;
        let name = ComponentName::try_new(name)?;
        let factory = ApplicationFactory::source(self.source_handles.clone());
        let current = core.desired_configuration()?;
        let request = core.new_configuration_request_id().await?;
        let graph_id = current.desired.topology.graph_id;
        let receipt = core
            .create_managed_components(current.revision, request, move |ids| {
                let id = ids.allocate()?;
                (|| -> Result<_> {
                    let mut topology = ComputationGraph::empty(graph_id)?
                        .snapshot()
                        .select(GraphSelection::All)?;
                    let stream = StreamId::try_new(format!("{name}/out"))?;
                    let specification = factory.specification_from_config(
                        id.clone(),
                        serde_json::json!({"slot": name, "properties": properties}),
                    )?;
                    topology.components.push(
                        ComponentAddition::declaration(name, specification)
                            .auto_start(auto_start)
                            .bind_stream(PortId::try_new("out")?, stream)
                            .definition,
                    );
                    Ok(topology)
                })()
                .map_err(|source| GraphError::Component {
                    component: id,
                    operation: "declare application source",
                    source,
                })
            })
            .await?;
        let id = accepted_id(&receipt)?;
        await_created(core, &id).await?;
        Ok(id)
    }

    pub(super) async fn add_native_query(
        core: &DrasiLib,
        config: QueryConfig,
    ) -> Result<ComponentId> {
        Ok(core.add_query_with_handle(config).await?.id().clone())
    }
}

fn accepted_id(receipt: &drasi_lib::management::AcceptanceReceipt) -> Result<ComponentId> {
    let identities = &receipt
        .creation
        .as_ref()
        .context("creation receipt omitted identities")?
        .components;
    anyhow::ensure!(
        identities.len() == 1,
        "single creation returned an unexpected identity count"
    );
    Ok(identities[0].id.clone())
}

async fn await_created(core: &DrasiLib, id: &ComponentId) -> Result<()> {
    core.reconcile_desired_state().await?;
    let handle = core.computation_component(id.as_str())?;
    tokio::time::timeout(std::time::Duration::from_secs(10), handle.wait_created())
        .await
        .with_context(|| format!("accepted component {id} did not finish construction"))??;
    Ok(())
}

fn reference_name(
    core: &DrasiLib,
    reference: &drasi_lib::config::ComponentReference,
) -> Result<String> {
    match reference {
        drasi_lib::config::ComponentReference::Name(name) => Ok(name.to_string()),
        drasi_lib::config::ComponentReference::Id { id } => core
            .computation_control()?
            .desired_snapshot()
            .component(id)
            .map(|component| component.name.to_string())
            .with_context(|| format!("bound input {id} is no longer declared")),
    }
}

pub(super) fn application_properties(
    value: &serde_json::Value,
) -> Result<HashMap<String, serde_json::Value>> {
    serde_json::from_value(value.clone()).context("Application properties must be an object")
}

pub(super) fn application_source_properties(
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
    if matches!(
        implementation.as_str(),
        super::application_factories::SOURCE | super::application_factories::REACTION
    ) || (implementation == "native"
        && properties.get("kind").and_then(serde_json::Value::as_str) == Some("application"))
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
    snapshot
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
        .map(|edge| {
            snapshot
                .component(&edge.from.component)
                .map(|component| component.name.to_string())
                .context("reaction input is no longer declared")
        })
        .collect::<Result<BTreeSet<_>>>()
        .map(|names| names.into_iter().collect())
}

pub(super) fn query_config_from_request(
    id: &str,
    query: String,
    inputs: Vec<String>,
    auto_start: bool,
) -> Result<QueryConfig> {
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
