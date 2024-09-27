use std::collections::HashSet;

use axum::{http::StatusCode, response::IntoResponse, Extension, Json};
use serde::{Deserialize, Serialize, Serializer };
use serde_json::{json, Value};

use test_runner::{script_source::bootstrap_script_file_reader::{BootstrapScriptReader, BootstrapScriptRecord, NodeRecord, RelationRecord}, SharedTestRunner, TestRunnerStatus};

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
    pub fn new() -> Self {
        AcquireResponseBody {
            nodes: Vec::new(),
            rels: Vec::new()
        }
    }

    pub fn add_data(&mut self, reader: BootstrapScriptReader) -> anyhow::Result<()>{

        for record in reader {
            match record?.record {
                BootstrapScriptRecord::Node(node) => {
                    self.nodes.push(Node::from_script_record(node));
                },
                BootstrapScriptRecord::Relation(rel) => {
                    self.rels.push(Relation::from_script_record(rel));
                },
                BootstrapScriptRecord::Finish(_) => break,
                _ => {}
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Node {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(serialize_with = "serialize_properties")] 
    pub properties: Value
}

impl Node {
    fn from_script_record(record: NodeRecord) -> Self {
        Self {
            id: record.id.clone(),
            labels: record.labels.clone(),
            properties: record.properties.clone()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Relation {
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
    fn from_script_record(record: RelationRecord) -> Self {
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

pub async fn acquire_handler(
    state: Extension<SharedTestRunner>,
    body: Json<AcquireRequestBody>,
) -> impl IntoResponse {
    log::info!("Processing call - acquire_handler");

    // TODO: The issue we have is that the local repo can include many datasets for different tests
    // and those datasets can have the same types of nodes and relations. The acquire request
    // is not able to differentiate between the datasets because the API assumes there is only a
    // single source served by the proxy.
    // For now we will accept this as a known limitation and look to change the API in the future
    // as we rework the source architecture.

    let requested_labels = body.0.node_labels.into_iter().chain(body.0.rel_labels.into_iter()).collect::<Vec<String>>();

    // Limit the scope of the Read Lock to the error check and dataset lookup.
    let dataset = {
        let state = state.read().await;

        // If the TestRunner is an Error state, return an error and a description of the error.
        if let TestRunnerStatus::Error(msg) = &state.get_status() {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Identify the dataset that has the most matching labels.
        // Create a hashset of the requested node and relation labels.
        let label_set = HashSet::from_iter(requested_labels.clone());

        match state.match_bootstrap_dataset(&label_set) {
            Ok(Some(dataset)) => dataset,
            Ok(None) => return Json(AcquireResponseBody::new()).into_response(),
            Err(e) => {
                let msg = format!("Error matching dataset: {}", e);
                log::error!("{}", msg);
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
            }
        }
    };

    // Read the boostrap data for each node and relation type and return the response.
    let mut response = AcquireResponseBody::new();

    let bootstrap_data = dataset.content.bootstrap_script_files.unwrap();

    for label in &requested_labels {
        if let Some(bootstrap_files) = bootstrap_data.get(label) {
            if bootstrap_files.len() == 0 {
                continue;
            }

            // Create a new BootstrapScriptReader and read the content.
            match BootstrapScriptReader::new(bootstrap_files.clone()) {
                Ok(reader) => {
                    
                    let header = reader.get_header();
                    log::debug!("Loaded BootstrapScript. {:?}", header);
                
                    match response.add_data(reader) {
                        Ok(_) => {},
                        Err(e) => {
                            let msg = format!("Error reading bootstrap script: {}", e);
                            log::error!("{}", msg);
                            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();        
                        }
                    }
                },
                Err(e) => {
                    let msg = format!("Error creating BootstrapScriptReader: {}", e);
                    log::error!("{}", msg);
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
                }
            }
        }
    }
    
    Json(response).into_response()
}