use std::sync::Arc;

use axum::{
    extract::{Extension, Path}, response::IntoResponse, routing::{get, post}, Json, Router
};
use test_run_host::{reactions::TestRunReactionConfig, TestRunHost, TestRunHostStatus};

use super::TestServiceWebApiError;

pub fn get_reactions_routes() -> Router {
    Router::new()
        .route("/reactions", get(get_reaction_list_handler).post(post_reaction_handler))
        .route("/reactions/:id", get(get_reaction_handler))
        .route("/reactions/:id/pause", post(reaction_observer_pause_handler))
        .route("/reactions/:id/reset", post(reaction_observer_reset_handler))
        .route("/reactions/:id/start", post(reaction_observer_start_handler))
        .route("/reactions/:id/stop", post(reaction_observer_stop_handler))        
}

pub async fn get_reaction_list_handler(
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_reaction_list");

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let reactions = test_run_host.get_test_reaction_ids().await?;
    Ok(Json(reactions).into_response())
}

pub async fn get_reaction_handler(
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - get_reaction: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let reaction_state = test_run_host.get_test_reaction_state(&id).await?;
    Ok(Json(reaction_state).into_response())
}

pub async fn reaction_observer_pause_handler (
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - reaction_observer_pause: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_reaction_pause(&id).await;
    match response {
        Ok(reaction) => {
            Ok(Json(reaction.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn reaction_observer_reset_handler (
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - reaction_observer_reset: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_reaction_reset(&id).await;
    match response {
        Ok(reaction) => {
            Ok(Json(reaction.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn reaction_observer_start_handler (
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - reaction_observer_start: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_reaction_start(&id).await;
    match response {
        Ok(reaction) => {
            Ok(Json(reaction.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn reaction_observer_stop_handler (
    Path(id): Path<String>,
    test_run_host: Extension<Arc<TestRunHost>>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - reaction_observer_stop: {}", id);

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let response = test_run_host.test_reaction_stop(&id).await;
    match response {
        Ok(reaction) => {
            Ok(Json(reaction.state).into_response())
        },
        Err(e) => {
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}

pub async fn post_reaction_handler (
    test_run_host: Extension<Arc<TestRunHost>>,
    body: Json<TestRunReactionConfig>,
) -> anyhow::Result<impl IntoResponse, TestServiceWebApiError> {
    log::info!("Processing call - post_reaction");

    // If the TestRunHost is an Error state, return an error and a description of the error.
    if let TestRunHostStatus::Error(msg) = &test_run_host.get_status().await? {
        return Err(TestServiceWebApiError::TestRunHostError(msg.to_string()));
    }

    let reaction_config = body.0;

    match test_run_host.add_test_reaction(reaction_config).await {
        Ok(id) => {
            match test_run_host.get_test_reaction_state(&id.to_string()).await {
                Ok(reaction) => {
                    Ok(Json(reaction).into_response())
                },
                Err(_) => {
                    Err(TestServiceWebApiError::NotFound("TestRunReaction".to_string(), id.to_string()))
                }
            }
        },
        Err(e) => {
            let msg = format!("Error creating Reaction: {}", e);
            log::error!("{}", &msg);
            Err(TestServiceWebApiError::AnyhowError(e))
        }
    }
}