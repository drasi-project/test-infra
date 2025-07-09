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
    routing::get,
    Json, Router,
};
use thiserror::Error;
use tokio::{select, signal};

use crate::Params;

#[derive(Debug, Error)]
pub enum TestReactivatorWebApiError {
    #[error("Error: {0}")]
    AnyhowError(anyhow::Error),
}

impl From<anyhow::Error> for TestReactivatorWebApiError {
    fn from(error: anyhow::Error) -> Self {
        TestReactivatorWebApiError::AnyhowError(error)
    }
}

impl IntoResponse for TestReactivatorWebApiError {
    fn into_response(self) -> Response {
        match self {
            TestReactivatorWebApiError::AnyhowError(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, Json(e.to_string())).into_response()
            }
        }
    }
}

pub(crate) async fn start_web_api(cfg: Params) {
    let addr = SocketAddr::from(([0, 0, 0, 0], cfg.proxy_port));

    let app = Router::new()
        .route("/", get(get_handler))
        .layer(axum::extract::Extension(Arc::new(cfg)));

    log::info!("\n\nTest Reactivator Web API listening on http://{}", addr);

    let server = axum::Server::bind(&addr).serve(app.into_make_service());

    // Graceful shutdown when receiving `Ctrl+C` or SIGTERM
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    log::info!("\n\nPress CTRL-C to stop the Test Reactivator...\n\n");

    if let Err(err) = graceful.await {
        log::error!("Test Reactivator error: {}", err);
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

pub async fn get_handler(
    cfg: Extension<Arc<Params>>,
) -> anyhow::Result<impl IntoResponse, TestReactivatorWebApiError> {
    log::debug!("Processing call - get_handler - {:?}", cfg);

    Ok((StatusCode::OK, Json("Test Reactivator Web API")))
}
