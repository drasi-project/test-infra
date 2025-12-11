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

// Test infrastructure module - allow unwraps for performance metrics code
#![allow(clippy::unwrap_used)]

//! Performance metrics output logger for measuring reaction throughput
//!
//! This logger tracks timing information and record counts to calculate
//! performance metrics like records per second. It writes a summary file
//! when the test run ends with detailed performance statistics.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use test_data_store::test_run_storage::{TestRunReactionId, TestRunReactionStorage};

use crate::common::HandlerRecord;

use super::{OutputLogger, OutputLoggerResult};

/// Performance metrics data structure
#[derive(Debug, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Timestamp in nanoseconds when first record was received
    pub start_time_ns: u64,
    /// Timestamp in nanoseconds when test run ended
    pub end_time_ns: u64,
    /// Total duration in nanoseconds
    pub duration_ns: u64,
    /// Total number of records processed
    pub record_count: u64,
    /// Records processed per second
    pub records_per_second: f64,
    /// Test run reaction identifier
    pub test_run_reaction_id: String,
    /// Timestamp when metrics were written
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl std::fmt::Display for PerformanceMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Performance Metrics for {}: {} records in {:.3}s ({:.2} records/sec)",
            self.test_run_reaction_id,
            self.record_count,
            self.duration_ns as f64 / 1_000_000_000.0,
            self.records_per_second
        )
    }
}

/// Configuration for the performance metrics output logger
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerformanceMetricsOutputLoggerConfig {
    /// Optional custom filename for the metrics output
    pub filename: Option<String>,
}

/// Performance metrics output logger implementation
pub struct PerformanceMetricsOutputLogger {
    /// Timestamp in nanoseconds when first record was received
    start_time_ns: Option<u64>,
    /// Timestamp in nanoseconds when test run ended
    end_time_ns: u64,
    /// Total number of records received
    record_count: u64,
    /// Test run reaction identifier
    test_run_reaction_id: TestRunReactionId,
    /// Storage abstraction for writing output files
    output_storage: TestRunReactionStorage,
    /// Path where metrics file will be written
    output_path: PathBuf,
}

impl PerformanceMetricsOutputLogger {
    /// Create a new performance metrics output logger
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        test_run_reaction_id: TestRunReactionId,
        config: &PerformanceMetricsOutputLoggerConfig,
        output_storage: &TestRunReactionStorage,
    ) -> anyhow::Result<Box<dyn OutputLogger + Send + Sync>> {
        log::info!(
            "PerformanceMetricsOutputLogger::new() called for {test_run_reaction_id} with config {config:?}"
        );

        // Generate output filename
        let filename = config.filename.clone().unwrap_or_else(|| {
            format!(
                "performance_metrics_{}.json",
                chrono::Utc::now().format("%Y%m%d_%H%M%S")
            )
        });

        // Create output directory
        let output_dir = output_storage
            .reaction_output_path
            .join("performance_metrics");
        log::info!("PerformanceMetricsOutputLogger checking/creating directory: {output_dir:?}");

        if !output_dir.exists() {
            log::info!("Creating directory: {output_dir:?}");
            tokio::fs::create_dir_all(&output_dir).await?;
        } else {
            log::info!("Directory already exists: {output_dir:?}");
        }

        // Set the output path
        let output_path = output_dir.join(&filename);

        log::info!("PerformanceMetricsOutputLogger created with output path: {output_path:?}");

        Ok(Box::new(Self {
            start_time_ns: None,
            end_time_ns: 0,
            record_count: 0,
            test_run_reaction_id,
            output_storage: output_storage.clone(),
            output_path,
        }))
    }

    /// Get current time in nanoseconds since UNIX epoch
    fn get_current_time_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_nanos() as u64
    }
}

#[async_trait]
impl OutputLogger for PerformanceMetricsOutputLogger {
    async fn log_handler_record(&mut self, _record: &HandlerRecord) -> anyhow::Result<()> {
        // Set start time on first record
        if self.start_time_ns.is_none() {
            self.start_time_ns = Some(Self::get_current_time_ns());
            log::debug!(
                "PerformanceMetricsOutputLogger: First record received at {} ns",
                self.start_time_ns.unwrap()
            );
        }

        // Increment record count
        self.record_count += 1;

        // Log every 1000 records for debugging
        if self.record_count % 1000 == 0 {
            log::debug!(
                "PerformanceMetricsOutputLogger: Processed {} records",
                self.record_count
            );
        }

        Ok(())
    }

    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult> {
        log::info!(
            "PerformanceMetricsOutputLogger: Ending test run for {} with {} records",
            self.test_run_reaction_id,
            self.record_count
        );

        // Capture end time
        self.end_time_ns = Self::get_current_time_ns();

        // Calculate metrics
        let start_time = self.start_time_ns.unwrap_or(self.end_time_ns);
        let duration_ns = if self.start_time_ns.is_some() {
            self.end_time_ns - start_time
        } else {
            0
        };

        let duration_seconds = duration_ns as f64 / 1_000_000_000.0;
        let records_per_second = if duration_seconds > 0.0 {
            self.record_count as f64 / duration_seconds
        } else {
            0.0
        };

        // Create metrics struct
        let metrics = PerformanceMetrics {
            start_time_ns: start_time,
            end_time_ns: self.end_time_ns,
            duration_ns,
            record_count: self.record_count,
            records_per_second,
            test_run_reaction_id: self.test_run_reaction_id.to_string(),
            timestamp: chrono::Utc::now(),
        };

        log::info!("{metrics}");

        // Write metrics to file
        let metrics_json = serde_json::to_string_pretty(&metrics)?;
        log::info!(
            "PerformanceMetricsOutputLogger writing {} bytes to {:?}",
            metrics_json.len(),
            self.output_path
        );

        match tokio::fs::write(&self.output_path, metrics_json.as_bytes()).await {
            Ok(_) => log::info!("Successfully wrote metrics to {:?}", self.output_path),
            Err(e) => {
                log::error!("Failed to write metrics to {:?}: {}", self.output_path, e);
                return Err(e.into());
            }
        }

        // Get the parent directory for the output folder path
        let output_folder = self
            .output_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.output_storage.reaction_output_path.clone());

        Ok(OutputLoggerResult {
            has_output: true,
            logger_name: "PerformanceMetrics".to_string(),
            output_folder_path: Some(output_folder),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::common::HandlerPayload;
    use tempfile::TempDir;
    use test_data_store::test_run_storage::{TestRunId, TestRunReactionStorage};

    async fn create_test_logger() -> (PerformanceMetricsOutputLogger, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let test_run_id = TestRunId::new("test_repo", "test_id", "test_run_001");
        let test_run_reaction_id = TestRunReactionId::new(&test_run_id, "reaction_001");

        let reaction_storage = TestRunReactionStorage {
            id: test_run_reaction_id.clone(),
            path: temp_dir.path().to_path_buf(),
            reaction_output_path: temp_dir.path().join("output"),
        };

        let _config = PerformanceMetricsOutputLoggerConfig {
            filename: Some("test_metrics.json".to_string()),
        };

        // Create output directory
        let output_dir = reaction_storage
            .reaction_output_path
            .join("performance_metrics");
        tokio::fs::create_dir_all(&output_dir).await.unwrap();

        let logger = PerformanceMetricsOutputLogger {
            start_time_ns: None,
            end_time_ns: 0,
            record_count: 0,
            test_run_reaction_id,
            output_storage: reaction_storage,
            output_path: output_dir.join("test_metrics.json"),
        };

        (logger, temp_dir)
    }

    #[tokio::test]
    async fn test_first_record_sets_start_time() {
        let (mut logger, _temp_dir) = create_test_logger().await;

        assert!(logger.start_time_ns.is_none());

        let record = HandlerRecord {
            id: "test_id".to_string(),
            sequence: 1,
            created_time_ns: 1000,
            processed_time_ns: 2000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"test": "data"}),
            },
        };

        logger.log_handler_record(&record).await.unwrap();

        assert!(logger.start_time_ns.is_some());
        assert_eq!(logger.record_count, 1);
    }

    #[tokio::test]
    async fn test_multiple_records_increment_count() {
        let (mut logger, _temp_dir) = create_test_logger().await;

        let record = HandlerRecord {
            id: "test_id".to_string(),
            sequence: 1,
            created_time_ns: 1000,
            processed_time_ns: 2000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"test": "data"}),
            },
        };

        for i in 0..5 {
            let mut r = record.clone();
            r.sequence = i;
            logger.log_handler_record(&r).await.unwrap();
        }

        assert_eq!(logger.record_count, 5);
        assert!(logger.start_time_ns.is_some());
    }

    #[tokio::test]
    async fn test_end_test_run_produces_metrics() {
        let (mut logger, temp_dir) = create_test_logger().await;

        // Simulate some records
        let record = HandlerRecord {
            id: "test_id".to_string(),
            sequence: 1,
            created_time_ns: 1000,
            processed_time_ns: 2000,
            traceparent: None,
            tracestate: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({"test": "data"}),
            },
        };

        // Add some records
        for _ in 0..100 {
            logger.log_handler_record(&record).await.unwrap();
        }

        // Force a specific start time for predictable testing
        logger.start_time_ns = Some(1_000_000_000);

        // Sleep briefly to ensure some time passes
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let result = logger.end_test_run().await.unwrap();

        assert!(result.has_output);
        assert_eq!(result.logger_name, "PerformanceMetrics");
        assert!(result.output_folder_path.is_some());

        // Verify the metrics file was created
        let metrics_path = temp_dir
            .path()
            .join("output")
            .join("performance_metrics")
            .join("test_metrics.json");
        assert!(metrics_path.exists());

        // Read and verify metrics content
        let metrics_content = std::fs::read_to_string(metrics_path).unwrap();
        let metrics: PerformanceMetrics = serde_json::from_str(&metrics_content).unwrap();

        assert_eq!(metrics.record_count, 100);
        assert!(metrics.duration_ns > 0);
        assert!(metrics.records_per_second > 0.0);
    }

    #[tokio::test]
    async fn test_no_records_case() {
        let (mut logger, _temp_dir) = create_test_logger().await;

        let result = logger.end_test_run().await.unwrap();

        assert!(result.has_output);
        assert_eq!(result.logger_name, "PerformanceMetrics");

        // Even with no records, metrics should be written
        assert_eq!(logger.record_count, 0);
    }
}
