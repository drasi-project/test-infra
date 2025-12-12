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
use http_reaction_handler::HttpReactionHandler;
use serde::Serialize;
use tokio::sync::mpsc::Receiver;

use test_data_store::{
    test_repo_storage::models::ReactionHandlerDefinition, test_run_storage::TestRunQueryId,
};

use crate::common::OutputHandlerMessage;

pub mod drasi_server_callback_handler;
pub mod drasi_server_channel_handler;
pub mod grpc_reaction_handler;
pub mod http_reaction_handler;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ReactionHandlerStatus {
    Uninitialized,
    Running,
    Paused,
    Stopped,
    Error,
}

impl Serialize for ReactionHandlerStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ReactionHandlerStatus::Uninitialized => serializer.serialize_str("Uninitialized"),
            ReactionHandlerStatus::Running => serializer.serialize_str("Running"),
            ReactionHandlerStatus::Paused => serializer.serialize_str("Paused"),
            ReactionHandlerStatus::Stopped => serializer.serialize_str("Stopped"),
            ReactionHandlerStatus::Error => serializer.serialize_str("Error"),
        }
    }
}

#[async_trait]
pub trait ReactionHandler: Send + Sync {
    async fn init(&self) -> anyhow::Result<Receiver<OutputHandlerMessage>>;
    async fn pause(&self) -> anyhow::Result<()>;
    async fn start(&self) -> anyhow::Result<()>;
    async fn stop(&self) -> anyhow::Result<()>;
}

#[async_trait]
impl ReactionHandler for Box<dyn ReactionHandler + Send + Sync> {
    async fn init(&self) -> anyhow::Result<Receiver<OutputHandlerMessage>> {
        (**self).init().await
    }

    async fn pause(&self) -> anyhow::Result<()> {
        (**self).pause().await
    }

    async fn start(&self) -> anyhow::Result<()> {
        (**self).start().await
    }

    async fn stop(&self) -> anyhow::Result<()> {
        (**self).stop().await
    }
}

pub async fn create_reaction_handler(
    id: TestRunQueryId,
    definition: ReactionHandlerDefinition,
) -> anyhow::Result<Box<dyn crate::reactions::ReactionOutputHandler + Send + Sync>> {
    match definition {
        ReactionHandlerDefinition::Http(definition) => {
            HttpReactionHandler::new(id, definition).await
        }
        ReactionHandlerDefinition::EventGrid(_) => {
            unimplemented!("EventGridReactionHandler is not implemented yet")
        }
        ReactionHandlerDefinition::Grpc(definition) => Ok(Box::new(
            grpc_reaction_handler::GrpcReactionHandler::new(id, definition).await?,
        )
            as Box<dyn crate::reactions::ReactionOutputHandler + Send + Sync>),
        ReactionHandlerDefinition::DrasiServerCallback(definition) => {
            drasi_server_callback_handler::DrasiServerCallbackHandler::new(id, definition).await
        }
        ReactionHandlerDefinition::DrasiServerChannel(definition) => {
            drasi_server_channel_handler::DrasiServerChannelHandler::new(id, definition).await
        }
    }
}
