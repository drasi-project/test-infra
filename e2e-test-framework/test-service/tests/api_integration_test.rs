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

#![allow(clippy::unwrap_used)]

use axum::http::StatusCode;
use serde_json::json;

const BASE_URL: &str = "http://localhost:8080";

#[tokio::test]
#[ignore] // Run with `cargo test -- --ignored` when service is running
async fn test_service_info_endpoint() {
    let client = reqwest::Client::new();
    let response = client.get(format!("{BASE_URL}/")).send().await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("data_collector").is_some());
    assert!(body.get("data_store").is_some());
    assert!(body.get("test_run_host").is_some());
}

#[tokio::test]
#[ignore]
async fn test_direct_source_endpoints_return_404() {
    let client = reqwest::Client::new();

    // These legacy endpoints should no longer exist
    let endpoints = vec![
        "/test_run_host/test_source",
        "/test_run_host/test_sources",
        "/test_run_host/test_query",
        "/test_run_host/test_queries",
        "/test_run_host/test_reaction",
        "/test_run_host/test_reactions",
        "/test_run_host/drasi_server",
        "/test_run_host/drasi_servers",
    ];

    for endpoint in endpoints {
        let response = client
            .get(format!("{BASE_URL}{endpoint}"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "Endpoint {endpoint} should return 404"
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_test_run_crud_operations() {
    let client = reqwest::Client::new();

    // Create a test run
    let test_run_config = json!({
        "test_id": "test_api_test",
        "test_repo_id": "test_repo",
        "test_run_id": "run_001",
        "sources": [],
        "queries": [],
        "reactions": [],
        "drasi_servers": []
    });

    let response = client
        .post(format!("{BASE_URL}/api/test_runs"))
        .json(&test_run_config)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let created: serde_json::Value = response.json().await.unwrap();
    let run_id = created["id"].as_str().unwrap();

    // List test runs
    let response = client
        .get(format!("{BASE_URL}/api/test_runs"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let runs: Vec<serde_json::Value> = response.json().await.unwrap();
    assert!(runs.iter().any(|r| r["id"] == run_id));

    // Get specific test run
    let response = client
        .get(format!("{BASE_URL}/api/test_runs/{run_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let run: serde_json::Value = response.json().await.unwrap();
    assert_eq!(run["id"], run_id);

    // Delete test run
    let response = client
        .delete(format!("{BASE_URL}/api/test_runs/{run_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
#[ignore]
async fn test_test_run_source_operations() {
    let client = reqwest::Client::new();

    // First create a test run
    let test_run_config = json!({
        "test_id": "test_source_ops",
        "test_repo_id": "test_repo",
        "test_run_id": "run_002",
        "sources": [],
        "queries": [],
        "reactions": [],
        "drasi_servers": []
    });

    let response = client
        .post(format!("{BASE_URL}/api/test_runs"))
        .json(&test_run_config)
        .send()
        .await
        .unwrap();

    let created: serde_json::Value = response.json().await.unwrap();
    let run_id = created["id"].as_str().unwrap();

    // List sources (should be empty)
    let response = client
        .get(format!("{BASE_URL}/api/test_runs/{run_id}/sources"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let sources: Vec<serde_json::Value> = response.json().await.unwrap();
    assert_eq!(sources.len(), 0);

    // Create a source within the test run
    let source_config = json!({
        "source_id": "test_source_001",
        "source_change_generator": {
            "kind": "Script",
            "config": {}
        },
        "source_change_dispatcher": {
            "kind": "Console"
        }
    });

    let response = client
        .post(format!("{BASE_URL}/api/test_runs/{run_id}/sources"))
        .json(&source_config)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);

    // Clean up
    client
        .delete(format!("{BASE_URL}/api/test_runs/{run_id}"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_test_run_query_operations() {
    let client = reqwest::Client::new();

    // Create a test run
    let test_run_config = json!({
        "test_id": "test_query_ops",
        "test_repo_id": "test_repo",
        "test_run_id": "run_003",
        "sources": [],
        "queries": [],
        "reactions": [],
        "drasi_servers": []
    });

    let response = client
        .post(format!("{BASE_URL}/api/test_runs"))
        .json(&test_run_config)
        .send()
        .await
        .unwrap();

    let created: serde_json::Value = response.json().await.unwrap();
    let run_id = created["id"].as_str().unwrap();

    // List queries (should be empty)
    let response = client
        .get(format!("{BASE_URL}/api/test_runs/{run_id}/queries"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let queries: Vec<serde_json::Value> = response.json().await.unwrap();
    assert_eq!(queries.len(), 0);

    // Clean up
    client
        .delete(format!("{BASE_URL}/api/test_runs/{run_id}"))
        .send()
        .await
        .unwrap();
}

#[tokio::test]
#[ignore]
async fn test_repos_endpoint_exists() {
    let client = reqwest::Client::new();

    // Test that repos endpoint still exists
    let response = client
        .get(format!("{BASE_URL}/test_repos"))
        .send()
        .await
        .unwrap();

    // Should return OK (empty list) or similar success status
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}
