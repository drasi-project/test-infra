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

use serde::Serialize;
#[allow(unused_imports)]
use serde_json::json;
use utoipa::{OpenApi, ToSchema};

use crate::web_api::{
    repo, test_runs, DataCollectorStateResponse, TestDataStoreStateResponse,
    TestRunHostStateResponse, TestRunSummary, TestServiceStateResponse,
};

/// Standard error response for all API endpoints
#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({"error": "Resource not found", "details": "TestSource with ID source-123 not found"}))]
pub struct ErrorResponse {
    /// Error message
    pub error: String,
    /// Additional error details
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::web_api::get_service_info_handler,
        // Repository endpoints
        repo::get_test_repo_list_handler,
        repo::get_test_repo_handler,
        repo::post_test_repo_handler,
        repo::get_test_repo_test_list_handler,
        repo::get_test_repo_test_handler,
        repo::post_test_repo_test_handler,
        repo::get_test_repo_test_source_list_handler,
        repo::get_test_repo_test_source_handler,
        repo::post_test_repo_test_source_handler,
        // Test Run endpoints
        test_runs::create_test_run,
        test_runs::list_test_runs,
        test_runs::get_test_run,
        test_runs::delete_test_run,
        test_runs::start_test_run,
        test_runs::stop_test_run,
        // Test Run Source endpoints
        test_runs::list_test_run_sources,
        test_runs::create_test_run_source,
        test_runs::get_test_run_source,
        test_runs::delete_test_run_source,
        test_runs::start_test_run_source,
        test_runs::stop_test_run_source,
        test_runs::pause_test_run_source,
        test_runs::reset_test_run_source,
        // Test Run Query endpoints
        test_runs::list_test_run_queries,
        test_runs::create_test_run_query,
        test_runs::get_test_run_query,
        test_runs::delete_test_run_query,
        test_runs::start_test_run_query,
        test_runs::stop_test_run_query,
        test_runs::pause_test_run_query,
        test_runs::reset_test_run_query,
        // Test Run Reaction endpoints
        test_runs::list_test_run_reactions,
        test_runs::create_test_run_reaction,
        test_runs::get_test_run_reaction,
        test_runs::delete_test_run_reaction,
        test_runs::start_test_run_reaction,
        test_runs::stop_test_run_reaction,
        test_runs::pause_test_run_reaction,
        test_runs::reset_test_run_reaction,
        // Test Run drasi-lib instance endpoints
        test_runs::list_test_run_drasi_lib_instances,
        test_runs::create_test_run_drasi_lib_instance,
        test_runs::get_test_run_drasi_lib_instance,
        test_runs::delete_test_run_drasi_lib_instance,
    ),
    components(
        schemas(
            // Common schemas
            ErrorResponse,
            // Service state schemas
            TestServiceStateResponse,
            TestDataStoreStateResponse,
            TestRunHostStateResponse,
            TestRunSummary,
            DataCollectorStateResponse,
            // Repository schemas
            repo::TestRepoResponse,
            repo::TestPostBody,
            repo::TestResponse,
            repo::TestSourcePostBody,
            repo::TestSourceResponse,
            // Test Run schemas
            test_runs::TestRunCreatedResponse,
            test_runs::TestRunInfo,
        )
    ),
    tags(
        (name = "service", description = "Test Service general information"),
        (name = "test-runs", description = "Test Run management API - hierarchical structure for organizing test components"),
        (name = "repos", description = "Test repository management API")
    ),
    info(
        title = "Drasi Test Service API",
        version = "0.1.0",
        description = "REST API for controlling the Drasi Test Service and managing test resources",
        contact(
            name = "Drasi Team",
            url = "https://github.com/drasi-project"
        )
    )
)]
pub struct ApiDoc;
