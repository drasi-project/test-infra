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

//! Completion handlers that execute when a TestRun finishes.
//!
//! Handlers implement the `CompletionHandler` trait and receive a summary
//! of component completion states. They can perform actions like logging
//! results, uploading files, sending notifications, or terminating services.

use std::sync::Arc;

use async_trait::async_trait;

use test_data_store::{test_run_storage::TestRunId, TestDataStore};

use super::types::ComponentCompletionSummary;

pub mod log;
pub mod sha256_determinism;

pub use log::LogCompletionHandler;
pub use sha256_determinism::Sha256DeterminismCompletionHandler;

/// Trait for handlers that execute when a TestRun completes.
#[async_trait]
pub trait CompletionHandler: Send + Sync {
    /// Handle test run completion.
    ///
    /// # Parameters
    /// - `test_run_id`: The ID of the completed test run
    /// - `completion_summary`: Summary of which components finished/stopped/errored
    ///
    /// # Errors
    /// Returns an error if the handler fails to execute. Errors are logged but
    /// do not prevent other handlers from executing (continue-on-error semantics).
    async fn handle_completion(
        &self,
        test_run_id: &str,
        completion_summary: &ComponentCompletionSummary,
    ) -> anyhow::Result<()>;
}

/// Create a completion handler from a test definition.
///
/// Handlers are constructed once per TestRun when the run is added to the
/// host. The factory receives the test-data store and the run id so handlers
/// that need to write artifacts (verdict files, etc.) can resolve their own
/// storage paths without being passed a back-reference to the host.
pub fn create_completion_handler(
    config: &test_data_store::test_repo_storage::models::CompletionHandlerDefinition,
    data_store: Arc<TestDataStore>,
    test_run_id: TestRunId,
) -> anyhow::Result<Box<dyn CompletionHandler>> {
    use test_data_store::test_repo_storage::models::CompletionHandlerDefinition;

    match config {
        CompletionHandlerDefinition::Log(log_config) => {
            let _ = (&data_store, &test_run_id);
            Ok(Box::new(LogCompletionHandler::new(log_config)))
        }
        CompletionHandlerDefinition::Sha256Determinism(cfg) => Ok(Box::new(
            Sha256DeterminismCompletionHandler::new(cfg, data_store, test_run_id),
        )),
    }
}
