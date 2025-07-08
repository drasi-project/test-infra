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

use std::{collections::HashSet, sync::Arc};

use axum::{
    extract::{Extension, Path},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize, Serializer};

use serde_json::{json, Value};
use test_data_store::{
    scripts::{NodeRecord, RelationRecord},
    test_repo_storage::models::SpacingMode,
};
use test_run_host::{
    sources::{bootstrap_data_generators::BootstrapData, TestRunSourceConfig},
    TestRunHost, TestRunHostStatus,
};

use super::TestServiceWebApiError;

pub fn get_sources_routes() -> Router {
    Router::new()
        .route(
            "/sources",
            get(get_source_list_handler).post(post_source_handler),
        )
        .route("/sources/:id", get(get_source_handler))
        .route("/sources/:id/bootstrap", post(source_bootstrap_handler))
        .route(
            "/sources/:id/pause",
            post(source_change_generator_pause_handler),
        )
        .route(
            "/sources/:id/reset",
            post(source_change_generator_reset_handler),
        )
        .route(
            "/sources/:id/skip",
            post(source_change_generator_skip_handler),
        )
        .route(
            "/sources/:id/start",
            post(source_change_generator_start_handler),
        )
        .route(
            "/sources/:id/step",
            post(source_change_generator_step_handler),
        )
        .route(
            "/sources/:id/stop",
            post(source_change_generator_stop_handler),
        )
}

pub async fn get_source_list_handler(
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_source_list");

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let sources = test_run_host.get_test_source_ids().await?;
    Ok(Json(sources).into_response())
}

pub async fn get_source_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_source: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let source_state = test_run_host.get_test_source_state(&id).await?;
    Ok(Json(source_state).into_response())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestSkipConfig {
    #[serde(default)]
    pub num_skips: u64,
    pub spacing_mode: Option<SpacingMode>,
}

impl Default for TestSkipConfig {
    fn default() -> Self {
        TestSkipConfig {
            num_skips: 1,
            spacing_mode: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceBootstrapRequestBody {
    // #[serde(rename = "queryId")]
    // pub query_id: String,
    // #[serde(rename = "queryNodeId")]
    // pub query_node_id: String,
    #[serde(rename = "nodeLabels")]
    pub node_labels: Vec<String>,
    #[serde(rename = "relLabels")]
    pub rel_labels: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SourceBootstrapResponseBody {
    pub nodes: Vec<Node>,
    pub rels: Vec<Relation>,
}

impl SourceBootstrapResponseBody {
    pub fn new(data: BootstrapData) -> Self {
        let mut body = Self {
            nodes: Vec::new(),
            rels: Vec::new(),
        };

        for (_, nodes) in data.nodes {
            body.nodes
                .extend(nodes.iter().map(|node| Node::from_script_record(node)));
        }
        for (_, rels) in data.rels {
            body.rels
                .extend(rels.iter().map(|rel| Relation::from_script_record(rel)));
        }

        body
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(serialize_with = "serialize_properties")]
    pub properties: Value,
}

impl Node {
    fn from_script_record(record: &NodeRecord) -> Self {
        Self {
            id: record.id.clone(),
            labels: record.labels.clone(),
            properties: record.properties.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Relation {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default, rename = "startId")]
    pub start_id: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "startLabel")]
    pub start_label: Option<String>,
    #[serde(default, rename = "endId")]
    pub end_id: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "endLabel")]
    pub end_label: Option<String>,
    #[serde(serialize_with = "serialize_properties")]
    pub properties: Value,
}

impl Relation {
    fn from_script_record(record: &RelationRecord) -> Self {
        Self {
            id: record.id.clone(),
            labels: record.labels.clone(),
            start_id: record.start_id.clone(),
            start_label: record.start_label.clone(),
            end_id: record.end_id.clone(),
            end_label: record.end_label.clone(),
            properties: record.properties.clone(),
        }
    }
}

fn serialize_properties<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        // If properties is Null, serialize it as an empty object `{}`.
        Value::Null => {
            let empty_obj = json!({});
            empty_obj.serialize(serializer)
        }
        // Otherwise, serialize the value as-is.
        _ => value.serialize(serializer),
    }
}

pub async fn source_bootstrap_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
    body: Json<SourceBootstrapRequestBody>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_bootstrap");

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let bootstrap_body = body.0;

    let node_labels: HashSet<String> = bootstrap_body.node_labels.into_iter().collect();
    let rel_labels: HashSet<String> = bootstrap_body.rel_labels.into_iter().collect();
    log::debug!(
        "Source: {:?}, Node Labels: {:?}, Rel Labels: {:?}",
        id,
        node_labels,
        rel_labels
    );

    let response = test_run_host
        .get_source_bootstrap_data(&id, &node_labels, &rel_labels)
        .await;
    match response {
        Ok(data) => Ok(Json(SourceBootstrapResponseBody::new(data)).into_response()),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

pub async fn source_change_generator_pause_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_pause: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_source_pause(&id).await;
    match response {
        Ok(source) => Ok(Json(source.state).into_response()),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

pub async fn source_change_generator_reset_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_reset: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_source_reset(&id).await;
    match response {
        Ok(source) => Ok(Json(source.state).into_response()),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

pub async fn source_change_generator_skip_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
    body: Json<Option<TestSkipConfig>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_skip: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let skips_body = body.0.unwrap_or_default();

    let response = test_run_host
        .test_source_skip(&id, skips_body.num_skips, skips_body.spacing_mode)
        .await;
    match response {
        Ok(source) => Ok(Json(source.state).into_response()),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

pub async fn source_change_generator_start_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_start: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_source_start(&id).await;
    match response {
        Ok(source) => Ok(Json(source.state).into_response()),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestStepConfig {
    #[serde(default)]
    pub num_steps: u64,
    pub spacing_mode: Option<SpacingMode>,
}

impl Default for TestStepConfig {
    fn default() -> Self {
        TestStepConfig {
            num_steps: 1,
            spacing_mode: None,
        }
    }
}

pub async fn source_change_generator_step_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
    body: Json<Option<TestStepConfig>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_step: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let steps_body = body.0.unwrap_or_default();

    let response = test_run_host
        .test_source_step(&id, steps_body.num_steps, steps_body.spacing_mode)
        .await;
    match response {
        Ok(source) => Ok(Json(source.state).into_response()),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

pub async fn source_change_generator_stop_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - source_change_generator_stop: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_source_stop(&id).await;
    match response {
        Ok(source) => Ok(Json(source.state).into_response()),
        Err(e) => Err(TestServiceWebApiError::AnyhowError(e)),
    }
}

pub async fn post_source_handler(
    test_run_host: Extension<Arc<TestRunHost>>,
    body: Json<TestRunSourceConfig>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_source");

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let source_config = body.0;

    match test_run_host.add_test_source(source_config).await {
        Ok(id) => match test_run_host.get_test_source_state(&id.to_string()).await {
            Ok(source) => Ok(Json(source).into_response()),
            Err(_) => Err(TestServiceWebApiError::NotFound(
                "TestRunSource".to_string(),
                id.to_string(),
            )),
        },
        Err(e) => {
            let msg = format!("Error creating Source: {}", e);
            log::error!("{}", &msg);
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}
