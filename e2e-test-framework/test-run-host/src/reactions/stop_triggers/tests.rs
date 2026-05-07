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

use crate::reactions::reaction_observer::ReactionObserverMetrics;
use crate::reactions::reaction_output_handler::ReactionHandlerStatus;
use crate::reactions::stop_triggers::*;
use test_data_store::test_repo_storage::models::{
    RecordCountStopTriggerDefinition, RecordSequenceNumberStopTriggerDefinition,
    StopTriggerDefinition,
};

#[tokio::test]
async fn test_record_count_stop_trigger_below_threshold() {
    let definition = RecordCountStopTriggerDefinition { record_count: 10 };

    let trigger = RecordCountStopTrigger::new(&definition).unwrap();
    let handler_status = ReactionHandlerStatus::Running;
    let metrics = ReactionObserverMetrics {
        reaction_invocation_count: 5,
        ..Default::default()
    };

    // Test with count below threshold
    assert!(!trigger.is_true(&handler_status, &metrics).await.unwrap());
}

#[tokio::test]
async fn test_record_count_stop_trigger_at_threshold() {
    let definition = RecordCountStopTriggerDefinition { record_count: 10 };

    let trigger = RecordCountStopTrigger::new(&definition).unwrap();
    let handler_status = ReactionHandlerStatus::Running;
    let metrics = ReactionObserverMetrics {
        reaction_invocation_count: 10,
        ..Default::default()
    };

    // Test with count at threshold
    assert!(trigger.is_true(&handler_status, &metrics).await.unwrap());
}

#[tokio::test]
async fn test_record_count_stop_trigger_above_threshold() {
    let definition = RecordCountStopTriggerDefinition { record_count: 10 };

    let trigger = RecordCountStopTrigger::new(&definition).unwrap();
    let handler_status = ReactionHandlerStatus::Running;
    let metrics = ReactionObserverMetrics {
        reaction_invocation_count: 15,
        ..Default::default()
    };

    // Test with count above threshold
    assert!(trigger.is_true(&handler_status, &metrics).await.unwrap());
}

#[tokio::test]
async fn test_never_stop_trigger() {
    // Test that NeverStopTrigger always returns false
    let trigger = NeverStopTrigger;
    let handler_status = ReactionHandlerStatus::Running;

    // Test with various metrics
    let metrics = ReactionObserverMetrics {
        reaction_invocation_count: 0,
        ..Default::default()
    };
    assert!(!trigger.is_true(&handler_status, &metrics).await.unwrap());

    let metrics = ReactionObserverMetrics {
        reaction_invocation_count: 1000,
        ..Default::default()
    };
    assert!(!trigger.is_true(&handler_status, &metrics).await.unwrap());
}

#[tokio::test]
async fn test_create_stop_trigger_factory() {
    // Test RecordCount variant
    let record_count_def =
        StopTriggerDefinition::RecordCount(RecordCountStopTriggerDefinition { record_count: 20 });
    let trigger = create_stop_trigger(&record_count_def).await;
    assert!(trigger.is_ok());

    // Test RecordSequenceNumber variant (should return NeverStopTrigger for reactions)
    let seq_num_def =
        StopTriggerDefinition::RecordSequenceNumber(RecordSequenceNumberStopTriggerDefinition {
            record_sequence_number: 100,
        });
    let trigger = create_stop_trigger(&seq_num_def).await.unwrap();

    // Verify it never triggers (NeverStopTrigger behavior)
    let handler_status = ReactionHandlerStatus::Running;
    let metrics = ReactionObserverMetrics::default();
    assert!(!trigger.is_true(&handler_status, &metrics).await.unwrap());
}

#[tokio::test]
async fn test_stop_trigger_with_different_handler_states() {
    let definition = RecordCountStopTriggerDefinition { record_count: 5 };

    let trigger = RecordCountStopTrigger::new(&definition).unwrap();
    let metrics = ReactionObserverMetrics {
        reaction_invocation_count: 10,
        ..Default::default()
    };

    // Test with different handler states - the trigger should work regardless
    let running_status = ReactionHandlerStatus::Running;
    assert!(trigger.is_true(&running_status, &metrics).await.unwrap());

    let stopped_status = ReactionHandlerStatus::Stopped;
    assert!(trigger.is_true(&stopped_status, &metrics).await.unwrap());

    let uninitialized_status = ReactionHandlerStatus::Uninitialized;
    assert!(trigger
        .is_true(&uninitialized_status, &metrics)
        .await
        .unwrap());
}
