// Copyright 2026 The Drasi Authors.
// Licensed under the Apache License, Version 2.0.

use super::*;
use test_data_store::test_run_storage::TestRunId;

fn state(capacity: usize) -> (HttpInstanceState, Receiver<ReactionHandlerMessage>) {
    let (tx, receiver) = tokio::sync::mpsc::channel(capacity);
    (
        HttpInstanceState {
            tx,
            settings: HttpReactionHandlerSettings {
                host: "127.0.0.1".into(),
                port: 0,
                path: "/reaction".into(),
                correlation_header: None,
                test_run_query_id: TestRunQueryId::new(&TestRunId::new("repo", "test", "run"), "q"),
            },
        },
        receiver,
    )
}

#[tokio::test]
async fn native_batch_ack_waits_for_every_bounded_observer_admission() {
    let (state, mut receiver) = state(1);
    let body = serde_json::json!({"batch":[
        {"queryId":"q","sequenceId":1,"operation":"ADD","after":{"value":1}},
        {"queryId":"q","sequenceId":2,"operation":"UPDATE","before":{"value":1},"after":{"value":2}},
    ]}).to_string();
    let delivery = handle_reaction(
        State(state),
        Method::POST,
        HeaderMap::new(),
        "/reaction/batch".parse().unwrap(),
        body,
    );
    tokio::pin!(delivery);
    assert!(futures::poll!(&mut delivery).is_pending());
    assert!(matches!(
        receiver.recv().await,
        Some(ReactionHandlerMessage::Invocation(_))
    ));
    let response = delivery.await.into_response();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(matches!(
        receiver.recv().await,
        Some(ReactionHandlerMessage::Invocation(_))
    ));
}

#[tokio::test]
async fn closed_observer_and_invalid_batches_are_not_successful_http_deliveries() {
    let (state, receiver) = state(1);
    drop(receiver);
    for (body, status) in [
        (
            serde_json::json!({"batch":[{"operation":"ADD","after":{"value":1}}]}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (serde_json::json!({"batch":[]}), StatusCode::BAD_REQUEST),
        (
            serde_json::json!({"results":"not-an-array"}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = handle_reaction(
            State(state.clone()),
            Method::POST,
            HeaderMap::new(),
            "/reaction/batch".parse().unwrap(),
            body.to_string(),
        )
        .await
        .into_response();
        assert_eq!(response.status(), status);
    }
}
