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

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::{select, signal};

use crate::Params;

#[derive(Debug, Serialize)]
pub struct SourceBootstrapRequestBody {
    #[serde(rename = "nodeLabels")]
    pub node_labels: Vec<String>,
    #[serde(rename = "relLabels")]
    pub rel_labels: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceBootstrapResponseBody {
    pub nodes: Vec<SourceElement>,
    pub rels: Vec<SourceElement>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum SourceElement {
    Node {
        id: String,
        labels: Vec<String>,
        properties: Map<String, Value>,
    },
    Relation {
        id: String,
        labels: Vec<String>,
        properties: Map<String, Value>,
        #[serde(rename = "startId")]
        start_id: String,
        #[serde(rename = "endId")]
        end_id: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AcquireRequestBody {
    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "queryNodeId")]
    pub query_node_id: String,
    #[serde(rename = "nodeLabels")]
    pub node_labels: Vec<String>,
    #[serde(rename = "relLabels")]
    pub rel_labels: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AcquireResponseBody {
    pub nodes: Vec<SourceElement>,
    pub rels: Vec<SourceElement>,
}

impl From<SourceBootstrapResponseBody> for AcquireResponseBody {
    fn from(data: SourceBootstrapResponseBody) -> Self {
        Self {
            nodes: data.nodes,
            rels: data.rels,
        }
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Error)]
pub enum TestProxyWebApiError {
    #[error("Error: {0}")]
    AnyhowError(anyhow::Error),
    #[error("Error: {0}")]
    ReqwestError(reqwest::Error),
    #[error("Error: {0}")]
    SerdeJsonError(serde_json::Error),
}

impl From<anyhow::Error> for TestProxyWebApiError {
    fn from(error: anyhow::Error) -> Self {
        TestProxyWebApiError::AnyhowError(error)
    }
}

impl From<reqwest::Error> for TestProxyWebApiError {
    fn from(error: reqwest::Error) -> Self {
        TestProxyWebApiError::ReqwestError(error)
    }
}

impl From<serde_json::Error> for TestProxyWebApiError {
    fn from(error: serde_json::Error) -> Self {
        TestProxyWebApiError::SerdeJsonError(error)
    }
}

impl IntoResponse for TestProxyWebApiError {
    fn into_response(self) -> Response {
        match self {
            TestProxyWebApiError::AnyhowError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            }
            TestProxyWebApiError::ReqwestError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            }
            TestProxyWebApiError::SerdeJsonError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            }
        }
    }
}

pub(crate) async fn start_web_api(cfg: Params) {
    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.proxy_port));

    let app = Router::new()
        .route("/acquire", post(post_acquire_handler))
        .layer(axum::extract::Extension(Arc::new(cfg)));

    log::info!("\n\nTest Proxy Web API listening on http://{}", addr);

    let server = axum::Server::bind(&addr).serve(app.into_make_service());

    // Graceful shutdown when receiving `Ctrl+C` or SIGTERM
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    log::info!("\n\nPress CTRL-C to stop the Test Proxy...\n\n");

    if let Err(err) = graceful.await {
        log::error!("Test Proxy error: {}", err);
    }
}

async fn shutdown_signal() {
    // Create two tasks: one for Ctrl+C and another for SIGTERM
    // Either will trigger a shutdown
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        log::info!("\nReceived Ctrl+C, shutting down...");
    };

    // Listen for SIGTERM (Docker stop signal)
    let sigterm = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("Failed to listen for SIGTERM");
            sigterm.recv().await;
            log::info!("\nReceived SIGTERM, shutting down...");
        }
        #[cfg(not(unix))]
        futures::future::pending::<()>().await; // Fallback for non-Unix systems
    };

    // Wait for either signal
    select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }

    log::info!("Cleaning up resources...");
    // TODO: Perform cleanup here...
    log::info!("Resources cleaned up.");
}

pub async fn post_acquire_handler(
    cfg: Extension<Arc<Params>>,
    body: Json<Value>,
) -> anyhow::Result<impl IntoResponse, TestProxyWebApiError> {
    log::debug!("Processing call - post_acquire - {:?}", body);

    let url = format!(
        "http://{}:{}/test_run_host/sources/{}/bootstrap",
        cfg.test_service_host, cfg.test_service_port, cfg.test_run_source_id
    );

    let acquire_body: AcquireRequestBody = serde_json::from_value(body.0)?;
    let src_req_body = SourceBootstrapRequestBody {
        node_labels: acquire_body.node_labels,
        rel_labels: acquire_body.rel_labels,
    };
    log::debug!("Calling Test Service - {:?}, with {:?}", url, src_req_body);

    let client = Client::new();
    let response = client
        .post(url)
        .json(&src_req_body)
        .header("Content-Type", "application/json")
        .send()
        .await?;

    match response.json::<SourceBootstrapResponseBody>().await {
        Ok(data) => {
            log::debug!(
                "Response from Test Service - #nodes:{:?}, #rels:{:?}",
                data.nodes.len(),
                data.rels.len()
            );
            Ok(Json(AcquireResponseBody::from(data)).into_response())
        }
        Err(e) => {
            log::error!("Error: {:?}", e);
            Err(e.into())
        }
    }
}
