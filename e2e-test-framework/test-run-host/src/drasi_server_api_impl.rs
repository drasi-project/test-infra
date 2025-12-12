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
use test_data_store::test_run_storage::TestRunDrasiServerId;

use crate::TestRunHost;

// Re-export the component types for API consistency
pub use crate::drasi_servers::api_models::{
    CreateQueryRequest, CreateReactionRequest, CreateSourceRequest, QueryCreatedResponse,
    QueryDetails, QueryInfo, ReactionCreatedResponse, ReactionDetails, ReactionInfo,
    SourceCreatedResponse, SourceDetails, SourceInfo, StatusResponse, UpdateQueryRequest,
    UpdateReactionRequest, UpdateSourceRequest,
};

impl TestRunHost {
    // ===== Sources API =====

    pub async fn list_drasi_server_sources(&self, server_id: &str) -> Result<Vec<SourceInfo>> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.list_sources().await
    }

    pub async fn get_drasi_server_source(
        &self,
        server_id: &str,
        source_id: &str,
    ) -> Result<SourceDetails> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.get_source(source_id).await
    }

    pub async fn create_drasi_server_source(
        &self,
        server_id: &str,
        request: CreateSourceRequest,
    ) -> Result<SourceCreatedResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.create_source(request).await
    }

    pub async fn update_drasi_server_source(
        &self,
        server_id: &str,
        source_id: &str,
        request: UpdateSourceRequest,
    ) -> Result<SourceDetails> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.update_source(source_id, request).await
    }

    pub async fn delete_drasi_server_source(&self, server_id: &str, source_id: &str) -> Result<()> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.delete_source(source_id).await
    }

    pub async fn start_drasi_server_source(
        &self,
        server_id: &str,
        source_id: &str,
    ) -> Result<StatusResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.start_source(source_id).await
    }

    pub async fn stop_drasi_server_source(
        &self,
        server_id: &str,
        source_id: &str,
    ) -> Result<StatusResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.stop_source(source_id).await
    }

    // ===== Queries API =====

    pub async fn list_drasi_server_queries(&self, server_id: &str) -> Result<Vec<QueryInfo>> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.list_queries().await
    }

    pub async fn get_drasi_server_query(
        &self,
        server_id: &str,
        query_id: &str,
    ) -> Result<QueryDetails> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.get_query(query_id).await
    }

    pub async fn create_drasi_server_query(
        &self,
        server_id: &str,
        request: CreateQueryRequest,
    ) -> Result<QueryCreatedResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.create_query(request).await
    }

    pub async fn update_drasi_server_query(
        &self,
        server_id: &str,
        query_id: &str,
        request: UpdateQueryRequest,
    ) -> Result<QueryDetails> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.update_query(query_id, request).await
    }

    pub async fn delete_drasi_server_query(&self, server_id: &str, query_id: &str) -> Result<()> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.delete_query(query_id).await
    }

    pub async fn start_drasi_server_query(
        &self,
        server_id: &str,
        query_id: &str,
    ) -> Result<StatusResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.start_query(query_id).await
    }

    pub async fn stop_drasi_server_query(
        &self,
        server_id: &str,
        query_id: &str,
    ) -> Result<StatusResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.stop_query(query_id).await
    }

    pub async fn get_drasi_server_query_results(
        &self,
        server_id: &str,
        query_id: &str,
    ) -> Result<serde_json::Value> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.get_query_results(query_id).await
    }

    // ===== Reactions API =====

    pub async fn list_drasi_server_reactions(&self, server_id: &str) -> Result<Vec<ReactionInfo>> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.list_reactions().await
    }

    pub async fn get_drasi_server_reaction(
        &self,
        server_id: &str,
        reaction_id: &str,
    ) -> Result<ReactionDetails> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.get_reaction(reaction_id).await
    }

    pub async fn create_drasi_server_reaction(
        &self,
        server_id: &str,
        request: CreateReactionRequest,
    ) -> Result<ReactionCreatedResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.create_reaction(request).await
    }

    pub async fn update_drasi_server_reaction(
        &self,
        server_id: &str,
        reaction_id: &str,
        request: UpdateReactionRequest,
    ) -> Result<ReactionDetails> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.update_reaction(reaction_id, request).await
    }

    pub async fn delete_drasi_server_reaction(
        &self,
        server_id: &str,
        reaction_id: &str,
    ) -> Result<()> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.delete_reaction(reaction_id).await
    }

    pub async fn start_drasi_server_reaction(
        &self,
        server_id: &str,
        reaction_id: &str,
    ) -> Result<StatusResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.start_reaction(reaction_id).await
    }

    pub async fn stop_drasi_server_reaction(
        &self,
        server_id: &str,
        reaction_id: &str,
    ) -> Result<StatusResponse> {
        let server_id = TestRunDrasiServerId::try_from(server_id)?;
        let test_runs = self.test_runs.read().await;
        let test_run = test_runs
            .get(&server_id.test_run_id)
            .ok_or_else(|| anyhow!("TestRun {} not found", server_id.test_run_id))?;
        let server = test_run
            .drasi_servers
            .get(&server_id.test_drasi_server_id)
            .ok_or_else(|| anyhow!("Drasi Server {server_id} not found"))?;

        server.stop_reaction(reaction_id).await
    }
}
