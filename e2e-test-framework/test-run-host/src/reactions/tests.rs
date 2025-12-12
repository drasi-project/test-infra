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

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::reactions::output_loggers::{JsonlFileOutputLoggerConfig, OutputLoggerConfig};
    use std::sync::Arc;
    use tempfile::TempDir;
    use test_data_store::{
        test_repo_storage::models::{HttpReactionHandlerDefinition, ReactionHandlerDefinition},
        test_run_storage::{TestRunId, TestRunReactionId, TestRunReactionStorage},
        TestDataStore,
    };

    async fn setup_test_env() -> anyhow::Result<(
        Arc<TestDataStore>,
        TestRunReactionId,
        TestRunReactionStorage,
        TempDir,
    )> {
        let temp_dir = TempDir::new()?;
        let data_store = Arc::new(TestDataStore::new_temp(None).await?);

        let test_run_id = TestRunId::new("test-repo", "test-001", "run-001");
        let reaction_id = TestRunReactionId::new(&test_run_id, "reaction-001");

        let reaction_storage = data_store
            .get_test_run_reaction_storage(&reaction_id)
            .await?;

        Ok((data_store, reaction_id, reaction_storage, temp_dir))
    }

    #[tokio::test]
    async fn test_reaction_observer_with_logging() -> anyhow::Result<()> {
        let (_data_store, reaction_id, reaction_storage, _temp_dir) = setup_test_env().await?;

        // Create a reaction handler definition
        let handler_def = ReactionHandlerDefinition::Http(HttpReactionHandlerDefinition {
            host: Some("localhost".to_string()),
            port: Some(8080),
            path: Some("/callback".to_string()),
            correlation_header: None,
        });

        // Configure JSONL logger
        let logger_config = OutputLoggerConfig::JsonlFile(JsonlFileOutputLoggerConfig {
            max_lines_per_file: Some(10000),
        });

        // Create reaction observer with logger
        let observer = reaction_observer::ReactionObserver::new(
            reaction_id.clone(),
            handler_def,
            reaction_storage.clone(),
            vec![logger_config],
            vec![],                                              // stop_triggers
            None,                                                // test_run_overrides
            crate::test_run_completion::LifecycleTx::disabled(), // lifecycle_tx
        )
        .await?;

        // Test state transitions
        let state = observer.get_state().await?;
        assert_eq!(
            state.state.status,
            reaction_observer::ReactionObserverStatus::Stopped
        );

        // Start the observer
        let result = observer.start().await?;
        assert!(result.result.is_ok());
        assert_eq!(
            result.state.status,
            reaction_observer::ReactionObserverStatus::Running
        );

        // Verify logger output directory was created
        let log_dir = reaction_storage.reaction_output_path.join("outputs");
        assert!(log_dir.exists() || reaction_storage.reaction_output_path.exists());

        // Stop the observer
        let result = observer.stop().await?;
        assert!(result.result.is_ok());
        assert_eq!(
            result.state.status,
            reaction_observer::ReactionObserverStatus::Stopped
        );

        // Verify logger results are available
        assert!(!result.state.logger_results.is_empty());
        assert_eq!(result.state.logger_results[0].logger_name, "JsonlFile");

        Ok(())
    }

    #[tokio::test]
    async fn test_reaction_with_multiple_loggers() -> anyhow::Result<()> {
        let (_data_store, reaction_id, reaction_storage, _temp_dir) = setup_test_env().await?;

        let handler_def = ReactionHandlerDefinition::Http(HttpReactionHandlerDefinition {
            host: Some("localhost".to_string()),
            port: Some(8080),
            path: Some("/callback".to_string()),
            correlation_header: None,
        });

        // Configure multiple loggers as OutputLoggerConfig
        let output_loggers = vec![
            OutputLoggerConfig::JsonlFile(JsonlFileOutputLoggerConfig {
                max_lines_per_file: Some(10000),
            }),
            OutputLoggerConfig::Console(output_loggers::ConsoleOutputLoggerConfig {
                date_time_format: None,
            }),
        ];

        // Create test reaction definition with stop triggers
        let test_reaction_def =
            test_data_store::test_repo_storage::models::TestReactionDefinition {
                test_reaction_id: "reaction-001".to_string(),
                output_handler: Some(handler_def.clone()),
                stop_triggers: Some(vec![]), // Empty stop triggers for this test
            };

        // Create test run reaction
        let definition = TestRunReactionDefinition {
            id: reaction_id.clone(),
            start_immediately: false,
            reaction_handler_definition: handler_def,
            test_reaction_definition: test_reaction_def,
            test_run_overrides: None,
            output_loggers,
        };

        let reaction = TestRunReaction::new(
            definition,
            reaction_storage.clone(),
            crate::test_run_completion::LifecycleTx::disabled(),
        )
        .await?;

        // Start and verify
        reaction.start_reaction_observer().await?;
        let state = reaction.get_state().await?;
        assert_eq!(
            state.reaction_observer.status,
            reaction_observer::ReactionObserverStatus::Running
        );

        // Stop and check logger results
        reaction.stop_reaction_observer().await?;
        let state = reaction.get_state().await?;
        assert_eq!(state.reaction_observer.logger_results.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_reaction_logger_output_paths() -> anyhow::Result<()> {
        let (_data_store, reaction_id, reaction_storage, _temp_dir) = setup_test_env().await?;

        let handler_def = ReactionHandlerDefinition::Http(HttpReactionHandlerDefinition {
            host: Some("localhost".to_string()),
            port: Some(8080),
            path: Some("/callback".to_string()),
            correlation_header: None,
        });

        // Configure logger
        let logger_config = OutputLoggerConfig::JsonlFile(JsonlFileOutputLoggerConfig {
            max_lines_per_file: Some(10000),
        });

        let observer = reaction_observer::ReactionObserver::new(
            reaction_id.clone(),
            handler_def,
            reaction_storage.clone(),
            vec![logger_config],
            vec![],                                              // stop_triggers
            None,                                                // test_run_overrides
            crate::test_run_completion::LifecycleTx::disabled(), // lifecycle_tx
        )
        .await?;

        // Start observer to create directories
        observer.start().await?;

        // Verify output directory was created
        let log_dir = reaction_storage.reaction_output_path.join("outputs");
        assert!(log_dir.exists() || reaction_storage.reaction_output_path.exists());

        observer.stop().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_reaction_state_with_start_immediately() -> anyhow::Result<()> {
        let (_data_store, reaction_id, reaction_storage, _temp_dir) = setup_test_env().await?;

        let handler_def = ReactionHandlerDefinition::Http(HttpReactionHandlerDefinition {
            host: Some("localhost".to_string()),
            port: Some(8080),
            path: Some("/callback".to_string()),
            correlation_header: None,
        });

        // Create test reaction definition
        let test_reaction_def =
            test_data_store::test_repo_storage::models::TestReactionDefinition {
                test_reaction_id: "reaction-001".to_string(),
                output_handler: Some(handler_def.clone()),
                stop_triggers: Some(vec![]), // Empty stop triggers for this test
            };

        let definition = TestRunReactionDefinition {
            id: reaction_id.clone(),
            start_immediately: true, // Should start immediately
            reaction_handler_definition: handler_def,
            test_reaction_definition: test_reaction_def,
            test_run_overrides: None,
            output_loggers: vec![],
        };

        let reaction = TestRunReaction::new(
            definition,
            reaction_storage.clone(),
            crate::test_run_completion::LifecycleTx::disabled(),
        )
        .await?;

        // With the new design, reactions don't auto-start themselves
        // TestRunHost is responsible for starting reactions with start_immediately=true
        let state = reaction.get_state().await?;
        assert_eq!(
            state.reaction_observer.status,
            reaction_observer::ReactionObserverStatus::Stopped
        );
        assert!(state.start_immediately);

        // Manually start the reaction to test it can be started
        reaction.start_reaction_observer().await?;

        // Now it should be running
        let state = reaction.get_state().await?;
        assert_eq!(
            state.reaction_observer.status,
            reaction_observer::ReactionObserverStatus::Running
        );

        // Clean up
        reaction.stop_reaction_observer().await?;

        Ok(())
    }
}
