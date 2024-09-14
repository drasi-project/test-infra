use std::collections::HashSet;

use axum::{http::StatusCode, response::IntoResponse, Extension, Json};
use serde::{Serialize, Deserialize};

use crate::{
    script_source::bootstrap_script_file_reader::{BootstrapScriptReader, BootstrapScriptRecord, NodeRecord, RelationRecord}, 
    runner::{TestRunnerStatus, SharedTestRunner}
};

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct AcquireRequestBody {
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
    pub id: String,
    pub labels: Vec<String>,
    pub properties: serde_json::Value
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
    pub id: String,
    pub labels: Vec<String>,
    #[serde(rename = "startId")]
    pub start_id: String,
    #[serde(rename = "startLabel", skip_serializing_if = "Option::is_none")]
    pub start_label: Option<String>,
    #[serde(rename = "endId")]
    pub end_id: String,
    #[serde(rename = "endLabel", skip_serializing_if = "Option::is_none")]
    pub end_label: Option<String>,
    pub properties: serde_json::Value
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

pub(super) async fn acquire_handler(
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
        if let TestRunnerStatus::Errorz(msg) = &state.status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Identify the dataset that has the most matching labels.
        // Create a hashset of the requested node and relation labels.
        let label_set = HashSet::from_iter(requested_labels.clone());

        match state.test_repo_cache.get_dataset_for_bootstrap(&label_set) {
            Some(dataset) => dataset,
            None => return Json(AcquireResponseBody::new()).into_response()
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