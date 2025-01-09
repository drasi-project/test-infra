use std::{collections::HashSet, sync::Arc};

use axum::{response::IntoResponse, Extension, Json};
use serde::{Deserialize, Serialize, Serializer };
use serde_json::{json, Value};

use test_data_store::scripts::{NodeRecord, RelationRecord};
use test_run_host::{bootstrap_data_generators::BootstrapData, TestRunner, TestRunnerStatus};

use super::TestServiceWebApiError;

#[derive(Debug, Serialize, Deserialize)]
pub struct AcquireRequestBody {
    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "queryNodeId")]
    pub query_node_id: String,
    #[serde(rename = "nodeLabels")]
    pub node_labels: Vec<String>,
    #[serde(rename = "relLabels")]
    pub rel_labels: Vec<String>
}

#[derive(Debug, Serialize, Deserialize)]
struct AcquireResponseBody {
    pub nodes: Vec<Node>,
    pub rels: Vec<Relation>
}

impl AcquireResponseBody {
    pub fn new(data: BootstrapData) -> Self {

        let mut body = Self {
            nodes: Vec::new(),
            rels: Vec::new()
        };

        for (_, nodes) in data.nodes {
            body.nodes.extend(nodes.iter().map(|node| Node::from_script_record(node)));
        }
        for (_, rels) in data.rels {
            body.rels.extend(rels.iter().map(|rel| Relation::from_script_record(rel)));
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
    pub properties: Value
}

impl Node {
    fn from_script_record(record: &NodeRecord) -> Self {
        Self {
            id: record.id.clone(),
            labels: record.labels.clone(),
            properties: record.properties.clone()
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
    pub properties: Value
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
            properties: record.properties.clone()
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
        },
        // Otherwise, serialize the value as-is.
        _ => value.serialize(serializer),
    }
}

pub async fn post_acquire_handler(
    test_runner: Extension<Arc<TestRunner>>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_acquire");

    // If the TestRunner is an Error state, return an error and a description of the error.
    if let TestRunnerStatus::Error(msg) = &test_runner.get_status().await? {
        return Err(TestServiceWebApiError::TestRunnerError(msg.to_string()));
    }

    let acquire_body: AcquireRequestBody = serde_json::from_value(body.0)?;
    let query_id = format!("{}.{}", acquire_body.query_node_id.clone(), acquire_body.query_id.clone());
    let node_labels: HashSet<String> = acquire_body.node_labels.into_iter().collect();
    let rel_labels: HashSet<String> = acquire_body.rel_labels.into_iter().collect();
    log::debug!("Query ID: {}, Node Labels: {:?}, Rel Labels: {:?}", query_id, node_labels, rel_labels);

    let result = test_runner.get_bootstrap_data_for_query(&query_id, &node_labels, &rel_labels).await;
    match result {
        Ok(data) => {
            Ok(Json(AcquireResponseBody::new(data)).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}