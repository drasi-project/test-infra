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

use base64::prelude::*;
use std::sync::Arc;

use anyhow::Context;
use axum::{ extract::{Extension, Path}, http::{header, StatusCode}, response::{Html, IntoResponse}, routing::{get, post}, Json, Router };
use tokio::{fs, io::AsyncReadExt};

use test_run_host::{queries::{query_result_observer::QueryResultObserverStatus, TestRunQueryConfig}, TestRunHost, TestRunHostStatus};

use super::TestServiceWebApiError;

pub fn get_queries_routes() -> Router {
    Router::new()
        .route("/queries", get(get_query_list_handler).post(post_query_handler))
        .route("/queries/:id", get(get_query_handler))
        .route("/queries/:id/profile", get(get_query_result_profile_handler))  
        .route("/queries/:id/pause", post(query_observer_pause_handler))
        .route("/queries/:id/reset", post(query_observer_reset_handler))
        .route("/queries/:id/start", post(query_observer_start_handler))
        .route("/queries/:id/stop", post(query_observer_stop_handler))  
}

pub async fn get_query_list_handler(
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_query_list");

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let queries = test_run_host.get_test_query_ids().await?;
    Ok(Json(queries).into_response())
}


pub async fn get_query_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_query: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let query_state = test_run_host.get_test_query_state(&id).await?;
    Ok(Json(query_state).into_response())
}

const IMAGE_EXTENSIONS: &[&str] = &["jpg", "jpeg", "png", "gif", "bmp", "webp"];

pub async fn get_query_result_profile_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_query_profile: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    // Check the status of the query
    let query_state = test_run_host.get_test_query_state(&id).await?;
    if query_state.query_observer.status != QueryResultObserverStatus::Stopped {
        return Err(TestServiceWebApiError::NotReady(format!("Query {} is not finished. No Result Profile available", id)));
    }  

    let query_results = test_run_host.get_test_query_result_logger_output(&id).await?;

    let mut image_paths = Vec::new();
    for query_result in query_results {
        if let Some(output_folder_path) = query_result.output_folder_path {

            let mut dir_entries = fs::read_dir(&output_folder_path).await.context("Failed to read directory")?;

            while let Some(entry) = dir_entries.next_entry().await? {
                let path = entry.path();
                
                // Check if it's a file and has an image extension
                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        if let Some(ext_str) = ext.to_str() {
                            if IMAGE_EXTENSIONS.contains(&ext_str.to_lowercase().as_str()) {
                                image_paths.push(path);
                            }
                        }
                    }
                }
            }
        }
    }

    let mut image_tags = String::new();
    
    for path in image_paths {
        match fs::File::open(&path).await {
            Ok(mut file) => {
                let mut contents = Vec::new();
                match file.read_to_end(&mut contents).await {
                    Ok(_) => {
                        let base64_image = BASE64_STANDARD.encode(&contents);
                        let img_tag = format!(
                            r#"<img src="data:image/png;base64,{}" alt="Image from {}" style="max-width: 300px; margin: 10px;">"#,
                            base64_image,
                            path.display()
                        );
                        image_tags.push_str(&img_tag);
                    }
                    Err(e) => {
                        image_tags.push_str(&format!(
                            "<p>Error reading {}: {}</p>",
                            path.display(),
                            e
                        ));
                    }
                }
            }
            Err(e) => {
                image_tags.push_str(&format!(
                    "<p>Could not open {}: {}</p>",
                    path.display(),
                    e
                ));
            }
        }
    }

    // Construct the full HTML
    let html_content = format!(
        r#"<!DOCTYPE html>
        <html>
            <head>
                <title>Multiple Images</title>
            </head>
            <body>
                <h1>My Image Gallery</h1>
                <div style="display: flex; flex-wrap: wrap;">
                    {}
                </div>
            </body>
        </html>"#,
        image_tags
    );

    // Return the response
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html")],
        Html(html_content),
    ).into_response())
}

pub async fn query_observer_pause_handler (
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - query_observer_pause: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_query_pause(&id).await;
    match response {
        Ok(query) => {
            Ok(Json(query.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn query_observer_reset_handler (
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - query_observer_reset: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_query_reset(&id).await;
    match response {
        Ok(query) => {
            Ok(Json(query.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn query_observer_start_handler (
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - query_observer_start: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_query_start(&id).await;
    match response {
        Ok(query) => {
            Ok(Json(query.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn query_observer_stop_handler (
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - query_observer_stop: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_query_stop(&id).await;
    match response {
        Ok(query) => {
            Ok(Json(query.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn post_query_handler (
    test_run_host: Extension<Arc<TestRunHost>>,
    body: Json<TestRunQueryConfig>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_query");

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let query_config = body.0;

    match test_run_host.add_test_query(query_config).await {
        Ok(id) => {
            match test_run_host.get_test_query_state(&id.to_string()).await {
                Ok(query) => {
                    Ok(Json(query).into_response())
                },
                Err(_) => {
                    Err(TestServiceWebApiError::NotFound("TestRunQuery".to_string(), id.to_string()))
                }
            }
        },
        Err(e) => {
            let msg = format!("Error creating Query: {}", e);
            log::error!("{}", &msg);
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}