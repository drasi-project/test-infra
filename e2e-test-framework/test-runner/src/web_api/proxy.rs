use std::collections::HashSet;

use axum::{http::StatusCode, response::IntoResponse, Extension, Json};
use serde::{Serialize, Deserialize};

use crate::{ServiceStatus, SharedState};

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
    pub rels: Vec<Rel>
}

impl AcquireResponseBody {
    pub fn new() -> Self {
        AcquireResponseBody {
            nodes: Vec::new(),
            rels: Vec::new()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Node {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: serde_json::Value
}

#[derive(Debug, Serialize, Deserialize)]
struct Rel {
    pub id: String,
    pub labels: Vec<String>,
    #[serde(rename = "startId")]
    pub start_id: String,
    #[serde(rename = "startLabel")]
    pub start_label: String,
    #[serde(rename = "endId")]
    pub end_id: String,
    #[serde(rename = "endLabel")]
    pub end_label: String,
    pub properties: serde_json::Value
}

pub(super) async fn acquire_handler(
    state: Extension<SharedState>,
    body: Json<AcquireRequestBody>,
) -> impl IntoResponse {
    log::info!("Processing call - acquire_handler");

    // TODO: The issue we have is that the local repo can include many datasets for different tests
    // and those datasets can have the same types of nodes and relations. The acquire request
    // is not able to differentiate between the datasets because the API assumes there is only a
    // single source served by the proxy.
    // For now we will accept this as a known limitation and look to change the API in the future
    // as we rework the source architecture.

    let node_labels = body.0.node_labels;
    let rel_labels = body.0.rel_labels;

    // Limit the scope of the Read Lock to the error check and dataset lookup.
    let _dataset = {
        let state = state.read().await;

        // Check if the service is an Error state.
        if let ServiceStatus::Error(msg) = &state.service_status {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(msg)).into_response();
        }

        // Iterate through the datasets in the local test repo and identify the dataset that
        // has the most matching labels.

        // Create a hashset of the requested node and relation labels.
        let requested_labels = node_labels.into_iter().chain(rel_labels.into_iter()).collect::<HashSet<_>>();

        let mut best_match = None;
        let mut best_match_count = 0;

        for (id, ds) in &state.test_repo.as_ref().unwrap().data_sets {
            let match_count = ds.count_bootstrap_type_intersection(&requested_labels);

            if match_count > best_match_count {
                best_match = Some(id);
                best_match_count = match_count;
            }
        }

        match best_match {
            Some(id) => state.test_repo.as_ref().unwrap().data_sets.get(id).unwrap().clone(),
            None => return Json(AcquireResponseBody::new()).into_response()
        }
    };

    // Read the boostrap data for each node and relation type and return the response.
    let result = AcquireResponseBody::new();
    
    Json(result).into_response()
}