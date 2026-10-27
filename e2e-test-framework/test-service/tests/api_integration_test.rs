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

use axum::http::StatusCode;
use serde_json::json;

mod common;

use common::TestService;

#[tokio::test]
async fn test_service_info_endpoint() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;
    let response = client.get(format!("{base_url}/")).send().await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("data_collector").is_some());
    assert!(body.get("data_store").is_some());
    assert!(body.get("test_run_host").is_some());
    service.shutdown().await
}

#[tokio::test]
async fn test_direct_source_endpoints_return_404() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;

    // These legacy endpoints should no longer exist
    let endpoints = vec![
        "/test_run_host/test_source",
        "/test_run_host/test_sources",
        "/test_run_host/test_query",
        "/test_run_host/test_queries",
        "/test_run_host/test_reaction",
        "/test_run_host/test_reactions",
        "/test_run_host/drasi_lib_instance",
        "/test_run_host/drasi_lib_instances",
    ];

    for endpoint in endpoints {
        let response = client
            .get(format!("{base_url}{endpoint}"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "Endpoint {endpoint} should return 404"
        );
    }
    service.shutdown().await
}

#[tokio::test]
async fn test_test_run_crud_operations() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;

    // Create a test run
    let test_run_config = json!({
        "test_id": "api_test",
        "test_repo_id": "test_repo",
        "test_run_id": "run_001",
        "sources": [],
        "queries": [],
        "reactions": [],
        "drasi_lib_instances": []
    });

    let response = client
        .post(format!("{base_url}/api/test_runs"))
        .json(&test_run_config)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let created: serde_json::Value = response.json().await.unwrap();
    let run_id = created["id"].as_str().unwrap();

    // List test runs
    let response = client
        .get(format!("{base_url}/api/test_runs"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let runs: Vec<serde_json::Value> = response.json().await.unwrap();
    assert!(runs.iter().any(|r| r["id"] == run_id));

    // Get specific test run
    let response = client
        .get(format!("{base_url}/api/test_runs/{run_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let run: serde_json::Value = response.json().await.unwrap();
    assert_eq!(run["id"], run_id);

    // Delete test run
    let response = client
        .delete(format!("{base_url}/api/test_runs/{run_id}"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = client
        .get(format!("{base_url}/api/test_runs/{run_id}"))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    service.shutdown().await
}

#[tokio::test]
async fn test_test_run_source_operations() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;

    // First create a test run
    let test_run_config = json!({
        "test_id": "api_test",
        "test_repo_id": "test_repo",
        "test_run_id": "run_002",
        "sources": [],
        "queries": [],
        "reactions": [],
        "drasi_lib_instances": []
    });

    let response = client
        .post(format!("{base_url}/api/test_runs"))
        .json(&test_run_config)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let created: serde_json::Value = response.json().await.unwrap();
    let run_id = created["id"].as_str().unwrap();

    // List sources (should be empty)
    let response = client
        .get(format!("{base_url}/api/test_runs/{run_id}/sources"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let sources: Vec<serde_json::Value> = response.json().await.unwrap();
    assert_eq!(sources.len(), 0);

    // Create a source within the test run
    let source_config = json!({
        "test_source_id": "test_source_001",
        "start_mode": "manual"
    });

    let response = client
        .post(format!("{base_url}/api/test_runs/{run_id}/sources"))
        .json(&source_config)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let created_source: serde_json::Value = response.json().await?;
    assert_eq!(created_source["id"], format!("{run_id}.test_source_001"));

    let response = client
        .get(format!("{base_url}/api/test_runs/{run_id}/sources"))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let sources: Vec<String> = response.json().await?;
    assert_eq!(sources, vec![format!("{run_id}.test_source_001")]);

    let response = client
        .get(format!(
            "{base_url}/api/test_runs/{run_id}/sources/test_source_001"
        ))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    // Clean up
    let response = client
        .delete(format!("{base_url}/api/test_runs/{run_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = client
        .get(format!(
            "{base_url}/api/test_runs/{run_id}/sources/test_source_001"
        ))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    service.shutdown().await
}

#[tokio::test]
async fn test_test_run_query_operations() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;

    // Create a test run
    let test_run_config = json!({
        "test_id": "api_test",
        "test_repo_id": "test_repo",
        "test_run_id": "run_003",
        "sources": [],
        "queries": [],
        "reactions": [],
        "drasi_lib_instances": []
    });

    let response = client
        .post(format!("{base_url}/api/test_runs"))
        .json(&test_run_config)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let created: serde_json::Value = response.json().await.unwrap();
    let run_id = created["id"].as_str().unwrap();

    // List queries (should be empty)
    let response = client
        .get(format!("{base_url}/api/test_runs/{run_id}/queries"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let queries: Vec<serde_json::Value> = response.json().await.unwrap();
    assert_eq!(queries.len(), 0);

    // Clean up
    let response = client
        .delete(format!("{base_url}/api/test_runs/{run_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    service.shutdown().await
}

#[tokio::test]
async fn test_repos_endpoint_exists() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;

    // Test that repos endpoint still exists
    let response = client
        .get(format!("{base_url}/test_repos"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let repos: serde_json::Value = response.json().await?;
    assert_eq!(repos, json!(["test_repo"]));
    service.shutdown().await
}
