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

//! Order-sensitive streaming SHA-256 over a reaction's canonical payload.
//!
//! For each incoming `HandlerRecord`, the logger projects to a canonical
//! payload (`payload.request_body` for `ReactionInvocation`,
//! `payload.reaction_output` for `ReactionOutput`, otherwise the whole
//! `payload` object), re-serialises it as compact JSON with recursively
//! sorted keys, and feeds the bytes (plus a trailing newline) into a running
//! `Sha256`. On `end_test_run` the digest is finalised and published via
//! `OutputLoggerResult.summary` as `{ "sha256": "<hex>", "record_count": N }`.
//!
//! The hash is intentionally order-sensitive: emission order is part of the
//! contract being verified, not noise. Tests where reaction-output ordering
//! is not deterministic (e.g. cross-transport joins) should not enable this
//! logger.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use test_data_store::test_run_storage::TestRunReactionId;

use crate::common::{HandlerPayload, HandlerRecord};

use super::{OutputLogger, OutputLoggerResult};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DeterminismHashOutputLoggerConfig {}

pub struct DeterminismHashOutputLogger {
    test_run_reaction_id: TestRunReactionId,
    hasher: Sha256,
    record_count: u64,
}

impl DeterminismHashOutputLogger {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        test_run_reaction_id: TestRunReactionId,
        _config: &DeterminismHashOutputLoggerConfig,
    ) -> anyhow::Result<Box<dyn OutputLogger + Send + Sync>> {
        log::debug!("Creating DeterminismHashOutputLogger for {test_run_reaction_id}");
        Ok(Box::new(Self {
            test_run_reaction_id,
            hasher: Sha256::new(),
            record_count: 0,
        }))
    }
}

#[async_trait]
impl OutputLogger for DeterminismHashOutputLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult> {
        let digest = std::mem::replace(&mut self.hasher, Sha256::new()).finalize();
        let hex_sha = hex::encode(digest);
        log::info!(
            "DeterminismHashOutputLogger finalised for {}: sha256={} record_count={}",
            self.test_run_reaction_id,
            hex_sha,
            self.record_count
        );
        Ok(OutputLoggerResult {
            has_output: true,
            logger_name: "DeterminismHash".to_string(),
            output_folder_path: None,
            summary: Some(json!({
                "sha256": hex_sha,
                "record_count": self.record_count,
            })),
        })
    }

    async fn log_handler_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()> {
        let canonical = canonical_payload_bytes(record)?;
        self.hasher.update(&canonical);
        self.hasher.update(b"\n");
        self.record_count += 1;
        Ok(())
    }
}

/// Project a `HandlerRecord` to the canonical payload bytes that participate
/// in the determinism hash.
///
/// Matches the historical bash pipeline:
///   - `ReactionInvocation` -> `request_body`
///   - `ReactionOutput`     -> `reaction_output`
///   - anything else        -> the entire `payload` object (serde-tagged form)
///
/// The chosen value is re-serialised to compact JSON with recursively sorted
/// keys (equivalent of `jq -cS`) so logically identical objects always hash
/// to the same bytes regardless of in-memory field order.
pub(crate) fn canonical_payload_bytes(record: &HandlerRecord) -> anyhow::Result<Vec<u8>> {
    let projected: Value = match &record.payload {
        HandlerPayload::ReactionInvocation { request_body, .. } => request_body.clone(),
        HandlerPayload::ReactionOutput { reaction_output } => reaction_output.clone(),
        _ => serde_json::to_value(&record.payload)?,
    };
    let canonical = sort_json_keys(projected);
    Ok(serde_json::to_vec(&canonical)?)
}

fn sort_json_keys(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted: std::collections::BTreeMap<String, Value> =
                std::collections::BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k, sort_json_keys(v));
            }
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sort_json_keys).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{HandlerPayload, HandlerRecord};
    use serde_json::json;
    use test_data_store::test_run_storage::{TestRunId, TestRunReactionId};

    fn make_record(seq: u64, payload: HandlerPayload) -> HandlerRecord {
        HandlerRecord {
            id: format!("rec-{seq}"),
            sequence: seq,
            created_time_ns: seq * 1_000_000,
            processed_time_ns: seq * 1_000_000 + 500,
            traceparent: None,
            tracestate: None,
            payload,
        }
    }

    #[test]
    fn canonical_payload_uses_request_body_for_invocations() {
        let rec = make_record(
            1,
            HandlerPayload::ReactionInvocation {
                reaction_type: "http".into(),
                query_id: "q1".into(),
                request_method: "POST".into(),
                request_path: "/r".into(),
                request_body: json!({ "b": 2, "a": 1 }),
                headers: Default::default(),
            },
        );
        let bytes = canonical_payload_bytes(&rec).unwrap();
        // Keys recursively sorted -> a before b.
        assert_eq!(std::str::from_utf8(&bytes).unwrap(), r#"{"a":1,"b":2}"#);
    }

    #[test]
    fn canonical_payload_uses_reaction_output_for_outputs() {
        let rec = make_record(
            2,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "z": [3, 2, 1], "a": { "y": 1, "x": 2 } }),
            },
        );
        let bytes = canonical_payload_bytes(&rec).unwrap();
        // Array order is preserved; nested object keys are sorted.
        assert_eq!(
            std::str::from_utf8(&bytes).unwrap(),
            r#"{"a":{"x":2,"y":1},"z":[3,2,1]}"#
        );
    }

    #[tokio::test]
    async fn streaming_hash_is_stable_for_same_record_sequence() {
        let test_run_id = TestRunId::new("repo", "test", "run");
        let reaction_id = TestRunReactionId::new(&test_run_id, "r");

        let mut logger_a =
            DeterminismHashOutputLogger::new(reaction_id.clone(), &Default::default()).unwrap();
        let mut logger_b =
            DeterminismHashOutputLogger::new(reaction_id, &Default::default()).unwrap();

        let records = vec![
            make_record(
                0,
                HandlerPayload::ReactionOutput {
                    reaction_output: json!({ "value": 1 }),
                },
            ),
            make_record(
                1,
                HandlerPayload::ReactionOutput {
                    reaction_output: json!({ "value": 2 }),
                },
            ),
            make_record(
                2,
                HandlerPayload::ReactionOutput {
                    reaction_output: json!({ "value": 3 }),
                },
            ),
        ];

        for r in &records {
            logger_a.log_handler_record(r).await.unwrap();
            logger_b.log_handler_record(r).await.unwrap();
        }
        let a = logger_a.end_test_run().await.unwrap();
        let b = logger_b.end_test_run().await.unwrap();
        assert_eq!(a.summary, b.summary);
        let sha = a.summary.as_ref().unwrap()["sha256"].as_str().unwrap();
        assert_eq!(sha.len(), 64);
        assert_eq!(
            a.summary.as_ref().unwrap()["record_count"].as_u64(),
            Some(3)
        );
    }

    #[tokio::test]
    async fn streaming_hash_differs_when_record_order_changes() {
        let test_run_id = TestRunId::new("repo", "test", "run");
        let reaction_id = TestRunReactionId::new(&test_run_id, "r");

        let r1 = make_record(
            0,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "value": 1 }),
            },
        );
        let r2 = make_record(
            1,
            HandlerPayload::ReactionOutput {
                reaction_output: json!({ "value": 2 }),
            },
        );

        let mut forward =
            DeterminismHashOutputLogger::new(reaction_id.clone(), &Default::default()).unwrap();
        forward.log_handler_record(&r1).await.unwrap();
        forward.log_handler_record(&r2).await.unwrap();
        let f = forward.end_test_run().await.unwrap();

        let mut reverse =
            DeterminismHashOutputLogger::new(reaction_id, &Default::default()).unwrap();
        reverse.log_handler_record(&r2).await.unwrap();
        reverse.log_handler_record(&r1).await.unwrap();
        let r = reverse.end_test_run().await.unwrap();

        assert_ne!(f.summary, r.summary);
    }
}
