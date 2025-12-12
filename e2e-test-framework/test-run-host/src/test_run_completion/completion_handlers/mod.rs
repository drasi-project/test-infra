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

use async_trait::async_trait;

use super::types::ComponentCompletionSummary;

pub mod log;

pub use log::LogCompletionHandler;

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
/// Completion handlers are defined in the test definition (like stop triggers)
/// because they define how the test completes - they're intrinsic to the test itself.
///
/// # MVP Implementation
/// Currently only supports LogCompletionHandler. Future handlers will be
/// added as additional enum variants are implemented.
pub fn create_completion_handler(
    config: &test_data_store::test_repo_storage::models::CompletionHandlerDefinition,
) -> anyhow::Result<Box<dyn CompletionHandler>> {
    use test_data_store::test_repo_storage::models::CompletionHandlerDefinition;

    match config {
        CompletionHandlerDefinition::Log(log_config) => {
            Ok(Box::new(LogCompletionHandler::new(log_config)))
        }
    }
}
