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
use serde_json::Value;

mod common;

use common::TestService;

#[tokio::test]
async fn test_openapi_json_endpoint() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;
    let response = client
        .get(format!("{base_url}/api-docs/openapi.json"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let openapi: Value = response.json().await.unwrap();

    // Verify it's a valid OpenAPI document
    assert_eq!(openapi["openapi"], "3.0.3");
    assert!(openapi.get("info").is_some());
    assert!(openapi.get("paths").is_some());
    service.shutdown().await
}

#[tokio::test]
async fn test_swagger_ui_endpoint() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;
    let response = client.get(format!("{base_url}/docs")).send().await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    // Should return HTML content for Swagger UI
    let content_type = response.headers().get("content-type").unwrap();
    assert!(content_type.to_str().unwrap().contains("text/html"));
    service.shutdown().await
}

#[tokio::test]
async fn test_only_approved_top_level_paths() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;
    let response = client
        .get(format!("{base_url}/api-docs/openapi.json"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let openapi: Value = response.json().await.unwrap();
    let paths = openapi["paths"].as_object().unwrap();

    // Check that only approved top-level paths exist
    for (path, _) in paths {
        assert!(
            path == "/" || path.starts_with("/api/test_runs") || path.starts_with("/test_repos"),
            "Unexpected path in API: {path}"
        );

        // Ensure no legacy direct access paths exist
        assert!(
            !path.starts_with("/test_run_host"),
            "Legacy path found: {path}"
        );
    }
    service.shutdown().await
}

#[tokio::test]
async fn test_only_approved_tags() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;
    let response = client
        .get(format!("{base_url}/api-docs/openapi.json"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let openapi: Value = response.json().await.unwrap();
    let tags = openapi["tags"].as_array().unwrap();

    let allowed_tags = ["service", "test-runs", "repos"];

    for tag in tags {
        let tag_name = tag["name"].as_str().unwrap();
        assert!(
            allowed_tags.contains(&tag_name),
            "Unexpected tag in API documentation: {tag_name}"
        );
    }

    // Verify only these tags exist
    assert_eq!(tags.len(), allowed_tags.len(), "Unexpected number of tags");
    service.shutdown().await
}

#[tokio::test]
async fn test_test_run_nested_paths_documented() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;
    let response = client
        .get(format!("{base_url}/api-docs/openapi.json"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let openapi: Value = response.json().await.unwrap();
    let paths = openapi["paths"].as_object().unwrap();

    // Verify test run nested endpoints are documented
    let expected_nested_paths = vec![
        "/api/test_runs/{run_id}/sources",
        "/api/test_runs/{run_id}/sources/{source_id}",
        "/api/test_runs/{run_id}/queries",
        "/api/test_runs/{run_id}/queries/{query_id}",
        "/api/test_runs/{run_id}/reactions",
        "/api/test_runs/{run_id}/reactions/{reaction_id}",
        "/api/test_runs/{run_id}/drasi_lib_instances",
        "/api/test_runs/{run_id}/drasi_lib_instances/{instance_id}",
    ];

    for expected_path in expected_nested_paths {
        assert!(
            paths.contains_key(expected_path),
            "Missing nested path in documentation: {expected_path}"
        );
    }
    service.shutdown().await
}

#[tokio::test]
async fn test_no_direct_resource_schemas() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;
    let response = client
        .get(format!("{base_url}/api-docs/openapi.json"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let openapi: Value = response.json().await.unwrap();
    let schemas = openapi["components"]["schemas"].as_object().unwrap();

    // These legacy schemas should not be present.
    let forbidden_schemas = [
        "SourceStateResponse",
        "QueryStateResponse",
        "ReactionStateResponse",
        "SourceBootstrapResponseBody",
    ];

    for forbidden in forbidden_schemas {
        assert!(
            !schemas.contains_key(forbidden),
            "Legacy schema {forbidden} should not be in OpenAPI document"
        );
    }
    service.shutdown().await
}

#[tokio::test]
async fn test_repository_endpoints_documented() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let client = &service.client;
    let base_url = &service.base_url;
    let response = client
        .get(format!("{base_url}/api-docs/openapi.json"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let openapi: Value = response.json().await.unwrap();
    let paths = openapi["paths"].as_object().unwrap();

    // Ensure repository endpoints are still documented
    assert!(
        paths.contains_key("/test_repos"),
        "Repository endpoint missing"
    );
    service.shutdown().await
}
