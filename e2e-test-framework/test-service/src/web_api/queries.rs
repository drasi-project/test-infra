use std::sync::Arc;

use axum::{ extract::{Extension, Path}, response::IntoResponse, routing::{get, post}, Json, Router };

use test_run_host::{queries::TestRunQueryConfig, TestRunHost, TestRunHostStatus};

use super::TestServiceWebApiError;

pub fn get_queries_routes() -> Router {
    Router::new()
        .route("/queries", get(get_query_list_handler).post(post_query_handler))
        .route("/queries/:id", get(get_query_handler))
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