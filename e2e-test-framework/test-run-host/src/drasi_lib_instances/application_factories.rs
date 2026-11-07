// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
    sync::Arc,
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use drasi_lib::computation::v1::*;
use drasi_reaction_application::NativeApplicationReaction;
use drasi_source_application::{ApplicationSourceHandle, NativeApplicationSource};
use tokio::sync::{mpsc, RwLock};

pub(super) const SOURCE: &str = "test-infra/application-source";
pub(super) const REACTION: &str = "test-infra/application-reaction";

type Sources = Arc<RwLock<HashMap<String, ApplicationSourceHandle>>>;
type Reactions = Arc<RwLock<HashMap<String, mpsc::Receiver<InputEnvelope>>>>;

enum Binding {
    Source(Sources),
    Reaction(Reactions),
}

pub(super) struct ApplicationFactory {
    descriptor: FactoryDescriptor,
    binding: Binding,
}

impl ApplicationFactory {
    pub(super) fn source(bindings: Sources) -> Self {
        Self::new(SOURCE, ComponentRole::Source, Binding::Source(bindings))
    }

    pub(super) fn reaction(bindings: Reactions) -> Self {
        Self::new(REACTION, ComponentRole::Sink, Binding::Reaction(bindings))
    }

    fn new(name: &str, role: ComponentRole, binding: Binding) -> Self {
        Self {
            descriptor: FactoryDescriptor {
                implementation: ImplementationIdentity::try_new(name, "1").expect("fixed identity"),
                role,
                configuration_version: 1,
                configuration: ConfigurationSchema {
                    fields: BTreeMap::from([
                        (
                            "slot".into(),
                            ConfigurationField {
                                value_type: ConfigurationType::String,
                                required: true,
                                secret: false,
                            },
                        ),
                        (
                            "properties".into(),
                            ConfigurationField {
                                value_type: ConfigurationType::Object,
                                required: true,
                                secret: false,
                            },
                        ),
                    ]),
                    allow_additional: false,
                },
                dependencies: BTreeMap::new(),
            },
            binding,
        }
    }
}

#[async_trait]
impl ComponentFactory for ApplicationFactory {
    fn descriptor(&self) -> &FactoryDescriptor {
        &self.descriptor
    }

    fn specification_from_config(
        &self,
        id: ComponentId,
        configuration: serde_json::Value,
    ) -> Result<ComponentSpecification> {
        let source = self.descriptor.role == ComponentRole::Source;
        let schema = if source {
            GraphChangeCodec::schema()
        } else {
            QueryChangeCodec::schema()
        };
        Ok(ComponentSpecification {
            descriptor: ComponentDescriptor::try_new(
                id,
                vec![PortDescriptor::new(
                    PortId::try_new(if source { "out" } else { "in" })?,
                    if source {
                        PortDirection::Output
                    } else {
                        PortDirection::Input
                    },
                    schema.descriptor().clone(),
                    PipeRequirements::default(),
                )],
            )?
            .with_semantic_kind(if source {
                ComponentSemanticKind::Source
            } else {
                ComponentSemanticKind::Reaction
            }),
            role: self.descriptor.role,
            completion: (!source).then_some(SinkCompletion::Accepted),
            implementation: self.descriptor.implementation.clone(),
            configuration_version: 1,
            configuration: configuration
                .as_object()
                .context("application configuration must be an object")?
                .iter()
                .map(|(key, value)| {
                    (
                        Arc::from(key.as_str()),
                        ConfigurationValue::Literal(value.clone()),
                    )
                })
                .collect(),
            dependencies: BTreeMap::new(),
        })
    }

    fn validate(&self, specification: &ComponentSpecification) -> Result<()> {
        let Some(ConfigurationValue::Literal(slot)) = specification.configuration.get("slot")
        else {
            anyhow::bail!("application test slot must be a literal name");
        };
        ComponentName::try_new(
            slot.as_str()
                .context("application test slot must be a string")?,
        )?;
        if let Some(ConfigurationValue::Literal(properties)) =
            specification.configuration.get("properties")
        {
            if self.descriptor.role == ComponentRole::Source {
                super::programmatic_api::application_source_properties(properties)?;
            } else {
                super::programmatic_api::application_properties(properties)?;
            }
        }
        Ok(())
    }

    async fn create(
        &self,
        context: ConstructionContext,
    ) -> Result<ConstructedComponent, ComponentCreationError> {
        let create = async {
            let slot = context.configuration()["slot"]
                .as_str()
                .context("application slot missing")?
                .to_owned();
            let properties = &context.configuration()["properties"];
            let capacity = NonZeroUsize::new(1000).context("application channel capacity")?;
            match &self.binding {
                Binding::Source(bindings) => {
                    let properties =
                        super::programmatic_api::application_source_properties(properties)?;
                    let (source, handle) = NativeApplicationSource::channel(
                        context.instance_id.to_string(),
                        context.graph_id.to_string(),
                        context.component_id.to_string(),
                        StreamId::try_new(format!("{slot}/out"))?,
                        capacity,
                    )?;
                    let mut bindings = bindings.write().await;
                    anyhow::ensure!(
                        !bindings.contains_key(&slot),
                        "application source slot is already bound"
                    );
                    bindings.insert(slot, handle);
                    Ok(ConstructedComponent::source(Box::new(
                        source.with_properties(properties),
                    )))
                }
                Binding::Reaction(bindings) => {
                    let properties = super::programmatic_api::application_properties(properties)?;
                    let (reaction, receiver) = NativeApplicationReaction::channel(
                        context.component_id.to_string(),
                        QueryChangeCodec::schema().descriptor().clone(),
                        capacity,
                    )?;
                    let mut bindings = bindings.write().await;
                    anyhow::ensure!(
                        !bindings.contains_key(&slot),
                        "application reaction slot is already bound"
                    );
                    bindings.insert(slot, receiver);
                    Ok(ConstructedComponent::sink(Box::new(
                        reaction.with_properties(properties),
                    )))
                }
            }
        };
        create.await.map_err(ComponentCreationError::terminal)
    }
}
