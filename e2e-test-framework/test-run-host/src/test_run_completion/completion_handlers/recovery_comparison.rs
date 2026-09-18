use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{ensure, Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use test_data_store::{
    test_repo_storage::models::{
        RecoveryResultVerificationHandlerConfig,
    },
    test_run_storage::{TestRunId, TestRunReactionId},
    TestDataStore,
};

use super::CompletionHandler;
use crate::{
    recovery_comparison::{compare, Artifact, Capture, Report, Verdict},
    test_run_completion::types::ComponentCompletionSummary,
};

pub struct RecoveryResultVerificationCompletionHandler {
    config: RecoveryResultVerificationHandlerConfig,
    data_store: Arc<TestDataStore>,
    test_run_id: TestRunId,
    created_at_ns: u128,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletionEvidence {
    test_run_id: String,
    recorded_at_ns: u128,
    complete: bool,
    evidence: String,
}

impl RecoveryResultVerificationCompletionHandler {
    pub fn new(
        config: &RecoveryResultVerificationHandlerConfig,
        data_store: Arc<TestDataStore>,
        test_run_id: TestRunId,
    ) -> Result<Self> {
        ensure!(
            !config.baseline_path.trim().is_empty(),
            "RecoveryResultVerification baseline_path is required"
        );
        ensure!(
            !config.workload_fingerprint.trim().is_empty(),
            "RecoveryResultVerification workload_fingerprint is required"
        );
        ensure!(
            !config.queries.is_empty(),
            "RecoveryResultVerification requires queries"
        );
        let mut queries = BTreeSet::new();
        let mut reactions = BTreeSet::new();
        for query in &config.queries {
            ensure!(
                !query.query_id.trim().is_empty() && queries.insert(&query.query_id),
                "RecoveryResultVerification requires unique nonempty query IDs"
            );
            ensure!(
                !query.test_reaction_id.trim().is_empty()
                    && reactions.insert(&query.test_reaction_id),
                "RecoveryResultVerification requires one distinct reaction per query"
            );
            ensure!(
                !query.config_fingerprint.trim().is_empty(),
                "query configuration fingerprint is required"
            );
            ensure!(
                query.identity_contract.is_some() == query.identity_pointer.is_some(),
                "identity contract and pointer must both be present or absent"
            );
            ensure!(
                query.snapshot_path.is_none() || query.snapshot_url.is_none(),
                "choose snapshot_path or snapshot_url, not both"
            );
        }
        Ok(Self {
            config: config.clone(),
            data_store,
            test_run_id,
            created_at_ns: now_ns()?,
        })
    }

    async fn evaluate(&self, root: &Path, summary: &ComponentCompletionSummary) -> Result<Report> {
        let capture = self.capture_evidence(root, summary).await?;
        let mut queries = Vec::new();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        for (index, query) in self.config.queries.iter().enumerate() {
            let reaction_id = TestRunReactionId::new(&self.test_run_id, &query.test_reaction_id);
            let outputs = summary
                .reaction_logger_outputs
                .get(&reaction_id)
                .with_context(|| {
                    format!("no finalized logger results for {}", query.test_reaction_id)
                })?;
            let loggers: Vec<_> = outputs
                .iter()
                .filter(|output| output.logger_name == "JsonlFile" && output.has_output)
                .collect();
            ensure!(
                loggers.len() == 1,
                "reaction {} requires exactly one finalized JsonlFile logger",
                query.test_reaction_id
            );
            let folder = loggers[0]
                .output_folder_path
                .as_ref()
                .context("JsonlFile output folder missing")?;
            let files = event_files(folder).await?;
            let snapshot = match (&query.snapshot_path, &query.snapshot_url) {
                (Some(path), _) => Some(root.join(path)),
                (_, Some(url)) => {
                    let response: Value = client
                        .get(url)
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                    let path = root.join(format!("recovery_snapshot_{index}.json"));
                    tokio::fs::write(&path, serde_json::to_vec_pretty(&response)?).await?;
                    Some(path)
                }
                _ => None,
            };
            queries.push(json!({
                "query_id": query.query_id, "config_fingerprint": query.config_fingerprint,
                "identity_contract": query.identity_contract, "identity_pointer": query.identity_pointer,
                "query_id_pointer": query.query_id_pointer, "payload_pointer": query.payload_pointer,
                "event_files": files, "snapshot_file": snapshot
            }));
        }
        let manifest_path = root.join("recovery_capture.json");
        let manifest = json!({"schema_version": 1, "workload_fingerprint": self.config.workload_fingerprint, "capture": capture, "queries": queries});
        tokio::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?).await?;
        let artifact =
            tokio::task::spawn_blocking(move || crate::recovery_capture::load(&manifest_path))
                .await??;
        tokio::fs::write(
            root.join("recovery_actual.json"),
            serde_json::to_vec_pretty(&artifact)?,
        )
        .await?;

        let baseline_path = Path::new(&self.config.baseline_path);
        let baseline_path = if baseline_path.is_absolute() {
            baseline_path.to_owned()
        } else {
            let storage = self
                .data_store
                .get_test_storage(&self.test_run_id.test_repo_id, &self.test_run_id.test_id)
                .await?;
            storage.path.join(baseline_path)
        };
        let bytes = tokio::fs::read(&baseline_path)
            .await
            .with_context(|| format!("reading baseline {}", baseline_path.display()))?;
        tokio::task::spawn_blocking(move || {
            let baseline: Artifact = serde_json::from_slice(&bytes)?;
            compare(&baseline, &artifact)
        })
        .await?
    }

    async fn capture_evidence(
        &self,
        root: &Path,
        summary: &ComponentCompletionSummary,
    ) -> Result<Capture> {
        if summary.has_errors() || summary.sources_stopped > 0 {
            return Ok(Capture {
                complete: false,
                evidence:
                    "Component errors or prematurely stopped sources prevent verified completion"
                        .to_owned(),
            });
        }
        let Some(path) = &self.config.capture_evidence_path else {
            return Ok(Capture { complete: false, evidence: "No terminal-boundary evidence configured; logger finalization alone is insufficient".to_owned() });
        };
        let bytes = match tokio::fs::read(root.join(path)).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Capture {
                    complete: false,
                    evidence: "Terminal-boundary evidence file is missing".to_owned(),
                })
            }
            Err(error) => return Err(error.into()),
        };
        let evidence: CompletionEvidence = serde_json::from_slice(&bytes)?;
        ensure!(
            evidence.test_run_id == self.test_run_id.to_string(),
            "completion evidence belongs to a different test run"
        );
        ensure!(
            evidence.recorded_at_ns >= self.created_at_ns && evidence.recorded_at_ns <= now_ns()?,
            "completion evidence is stale or future-dated"
        );
        ensure!(
            !evidence.evidence.trim().is_empty(),
            "completion evidence description is required"
        );
        Ok(Capture {
            complete: evidence.complete,
            evidence: evidence.evidence,
        })
    }
}

#[async_trait]
impl CompletionHandler for RecoveryResultVerificationCompletionHandler {
    async fn handle_completion(
        &self,
        test_run_id: &str,
        summary: &ComponentCompletionSummary,
    ) -> Result<()> {
        ensure!(
            test_run_id == self.test_run_id.to_string(),
            "RecoveryResultVerification invoked for the wrong run"
        );
        let storage = self
            .data_store
            .get_test_run_storage(&self.test_run_id)
            .await?;
        let root = storage.path.canonicalize()?;
        let result = self.evaluate(&root, summary).await;
        let body = match &result {
            Ok(report) => {
                json!({"test_run_id": test_run_id, "enforced": self.config.enforce, "verdict": report.verdict, "comparison": report})
            }
            Err(error) => {
                json!({"test_run_id": test_run_id, "enforced": self.config.enforce, "verdict": "invalid", "error": format!("{error:#}")})
            }
        };
        tokio::fs::write(
            root.join("recovery_verdict.json"),
            serde_json::to_vec_pretty(&body)?,
        )
        .await?;
        match result {
            Ok(report) if report.verdict == Verdict::Passed => Ok(()),
            result => {
                let message = match result {
                    Ok(report) => format!(
                        "RecoveryResultVerification verdict: {:?}; see recovery_verdict.json",
                        report.verdict
                    ),
                    Err(error) => format!("RecoveryResultVerification invalid: {error:#}"),
                };
                if self.config.enforce {
                    anyhow::bail!(message);
                }
                log::warn!("[{test_run_id}] advisory {message}");
                Ok(())
            }
        }
    }
}

fn now_ns() -> Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())
}

async fn event_files(folder: &Path) -> Result<Vec<PathBuf>> {
    let mut entries = tokio::fs::read_dir(folder).await?;
    let mut files = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(index) = name
            .strip_prefix("outputs_")
            .and_then(|name| name.strip_suffix(".jsonl"))
        {
            let index: usize = index.parse().context("invalid JsonlFile chunk number")?;
            files.push((index, entry.path().canonicalize()?));
        }
    }
    files.sort_by_key(|(index, _)| *index);
    ensure!(!files.is_empty(), "no finalized JsonlFile chunks found");
    for (expected, (actual, _)) in files.iter().enumerate() {
        ensure!(
            expected == *actual,
            "missing or repeated JsonlFile chunk: expected {expected}, found {actual}"
        );
    }
    Ok(files.into_iter().map(|(_, path)| path).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::{HandlerPayload, HandlerRecord},
        reactions::output_loggers::{create_output_logger, OutputLoggerConfig},
        test_run_completion::completion_handlers::create_completion_handler,
    };
    use std::collections::HashMap;
    use test_data_store::{
        test_repo_storage::models::CompletionHandlerDefinition, TestDataStoreConfig,
    };

    struct Fixture {
        _directory: tempfile::TempDir,
        store: Arc<TestDataStore>,
        run: TestRunId,
        root: PathBuf,
        summary: ComponentCompletionSummary,
        config: Value,
    }

    async fn fixture() -> Fixture {
        fixture_with_events(&[1, 2]).await
    }

    async fn fixture_with_events(identities: &[u64]) -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            TestDataStore::new(TestDataStoreConfig {
                data_store_path: Some(directory.path().to_string_lossy().to_string()),
                delete_on_start: Some(false),
                delete_on_stop: Some(false),
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let run = TestRunId::new("repo", "test", "run");
        let root = store.get_test_run_storage(&run).await.unwrap().path;
        let reaction = TestRunReactionId::new(&run, "receiver");
        let storage = store
            .get_test_run_reaction_storage(&reaction)
            .await
            .unwrap();
        let logger_config: OutputLoggerConfig =
            serde_json::from_value(json!({"kind": "JsonlFile", "max_lines_per_file": 1})).unwrap();
        let mut logger = create_output_logger(reaction.clone(), &logger_config, &storage)
            .await
            .unwrap();
        for identity in identities {
            logger.log_handler_record(&HandlerRecord {
                id: format!("received-{identity}"), sequence: 99,
                created_time_ns: 0, processed_time_ns: 0, traceparent: None, tracestate: None,
                payload: HandlerPayload::ReactionOutput { reaction_output: json!({
                    "query_id": "items", "producer_id": identity, "result": {"value": identity}
                }) },
            }).await.unwrap();
        }
        let logger_result = logger.end_test_run().await.unwrap();
        let summary = ComponentCompletionSummary {
            drasi_lib_instances_stopped: 0,
            drasi_lib_instances_error: 0,
            sources_finished: 1,
            sources_stopped: 0,
            sources_error: 0,
            queries_stopped: 0,
            queries_error: 0,
            reactions_stopped: 1,
            reactions_error: 0,
            component_finish_times: HashMap::new(),
            reaction_logger_outputs: HashMap::from([(reaction, vec![logger_result])]),
        };
        let baseline = root.join("baseline.json");
        tokio::fs::write(&baseline, json!({
            "schema_version": 1, "workload_fingerprint": "fixture-v1",
            "capture": {"complete": true, "evidence": "Synthetic fixture with known terminal boundary"},
            "queries": [{"query_id": "items", "config_fingerprint": "items-v1", "identity_contract": "fixture-producer-v1",
                "events": [{"identity": "1", "payload": {"value": 1}}, {"identity": "2", "payload": {"value": 2}}],
                "snapshot": [{"value": 1}, {"value": 2}]}]
        }).to_string()).await.unwrap();
        tokio::fs::write(
            root.join("snapshot.json"),
            json!({"success":true, "data":[{"value":2}, {"value":1}]}).to_string(),
        )
        .await
        .unwrap();
        let config = json!({
            "kind": "RecoveryResultVerification", "baseline_path": baseline,
            "workload_fingerprint": "fixture-v1",
            "capture_evidence_path": "boundary.json",
            "queries": [{"query_id": "items", "test_reaction_id": "receiver", "config_fingerprint": "items-v1",
                "identity_contract": "fixture-producer-v1", "identity_pointer": "/payload/reaction_output/producer_id",
                "query_id_pointer": "/payload/reaction_output/query_id", "payload_pointer": "/payload/reaction_output/result",
                "snapshot_path": "snapshot.json"}]
        });
        Fixture {
            _directory: directory,
            store,
            run,
            root,
            summary,
            config,
        }
    }

    async fn execute(fixture: &Fixture, evidence: Option<Value>) -> (Result<()>, Value) {
        let definition: CompletionHandlerDefinition =
            serde_json::from_value(fixture.config.clone()).unwrap();
        let handler =
            create_completion_handler(&definition, fixture.store.clone(), fixture.run.clone())
                .unwrap();
        if let Some(mut evidence) = evidence {
            if evidence.get("recorded_at_ns").is_none() {
                evidence["recorded_at_ns"] = json!(now_ns().unwrap() as u64);
            }
            tokio::fs::write(fixture.root.join("boundary.json"), evidence.to_string())
                .await
                .unwrap();
        }
        let result = handler
            .handle_completion(&fixture.run.to_string(), &fixture.summary)
            .await;
        let verdict = serde_json::from_slice(
            &tokio::fs::read(fixture.root.join("recovery_verdict.json"))
                .await
                .unwrap(),
        )
        .unwrap();
        (result, verdict)
    }

    fn evidence() -> Value {
        json!({"test_run_id":"repo.test.run", "complete":true, "evidence":"Synthetic producer finished; all delivery boundary events and final snapshot captured"})
    }

    #[tokio::test]
    async fn recovery_result_verification_accepts_legacy_config_name() {
        let mut fixture = fixture().await;
        fixture.config["kind"] = json!("RecoveryComparison");
        let definition: CompletionHandlerDefinition =
            serde_json::from_value(fixture.config.clone()).unwrap();
        assert_eq!(
            serde_json::to_value(&definition).unwrap()["kind"],
            "RecoveryResultVerification"
        );
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        result.unwrap();
        assert_eq!(verdict["verdict"], "passed");
    }

    #[tokio::test]
    async fn recovery_comparison_factory_reads_real_finalized_logger_and_passes() {
        let fixture = fixture().await;
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        result.unwrap();
        assert_eq!(verdict["verdict"], "passed");
        assert_eq!(verdict["enforced"], true);
        assert_eq!(verdict["comparison"]["queries"][0]["delivery"]["duplicates"], json!({}));
        assert_eq!(
            verdict["comparison"]["queries"][0]["delivery"]["reordered"],
            false
        );
        assert!(fixture.root.join("recovery_actual.json").exists());
        assert!(fixture.root.join("recovery_capture.json").exists());
    }

    #[tokio::test]
    async fn recovery_comparison_mismatch_is_a_handler_error() {
        let fixture = fixture_with_events(&[2, 1, 1]).await;
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        assert!(result.is_err());
        assert_eq!(verdict["verdict"], "failed");
    }

    #[tokio::test]
    async fn recovery_comparison_rejects_policy_configuration() {
        let mut fixture = fixture().await;
        fixture.config["policy"] = json!({"delivery":"at_least_once","allow_reordering":true});
        assert!(serde_json::from_value::<CompletionHandlerDefinition>(fixture.config).is_err());
    }

    #[tokio::test]
    async fn recovery_comparison_missing_evidence_is_not_completion() {
        let fixture = fixture().await;
        let (result, verdict) = execute(&fixture, None).await;
        assert!(result.is_err());
        assert_eq!(verdict["verdict"], "inconclusive");
    }

    #[tokio::test]
    async fn recovery_comparison_advisory_preserves_nonpass_verdict() {
        let mut fixture = fixture().await;
        fixture.config["enforce"] = json!(false);
        let (result, verdict) = execute(&fixture, None).await;
        result.unwrap();
        assert_eq!(verdict["verdict"], "inconclusive");
        assert_eq!(verdict["enforced"], false);
    }

    #[tokio::test]
    async fn recovery_comparison_stale_or_wrong_run_evidence_rejected() {
        let mut stale = evidence();
        stale["recorded_at_ns"] = json!(0);
        for boundary in [
            stale,
            json!({"test_run_id":"other.test.run", "complete":true, "evidence":"wrong run"}),
        ] {
            let fixture = fixture().await;
            let (result, verdict) = execute(&fixture, Some(boundary)).await;
            assert!(result.is_err());
            assert_eq!(verdict["verdict"], "invalid");
        }
    }

    #[tokio::test]
    async fn recovery_comparison_component_errors_override_boundary_claim() {
        let mut fixture = fixture().await;
        fixture.summary.sources_error = 1;
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        assert!(result.is_err());
        assert_eq!(verdict["verdict"], "inconclusive");
    }

    #[tokio::test]
    async fn recovery_comparison_missing_logs_never_passes() {
        let mut fixture = fixture().await;
        fixture.summary.reaction_logger_outputs.clear();
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        assert!(result.is_err());
        assert_eq!(verdict["verdict"], "invalid");
        assert!(verdict["error"]
            .as_str()
            .unwrap()
            .contains("no finalized logger"));
    }

    #[tokio::test]
    async fn recovery_comparison_failed_snapshot_is_not_empty() {
        let fixture = fixture().await;
        tokio::fs::write(
            fixture.root.join("snapshot.json"),
            json!({"success":false,"data":[]}).to_string(),
        )
        .await
        .unwrap();
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        assert!(result.is_err());
        assert_eq!(verdict["verdict"], "invalid");
    }

    #[tokio::test]
    async fn recovery_comparison_missing_baseline_is_invalid() {
        let mut fixture = fixture().await;
        fixture.config["baseline_path"] = json!(fixture.root.join("missing-baseline.json"));
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        assert!(result.is_err());
        assert_eq!(verdict["verdict"], "invalid");
        assert!(verdict["error"]
            .as_str()
            .unwrap()
            .contains("reading baseline"));
    }

    #[tokio::test]
    async fn recovery_comparison_missing_producer_identity_stays_inconclusive() {
        let fixture = fixture().await;
        let folder = fixture
            .summary
            .reaction_logger_outputs
            .values()
            .next()
            .unwrap()[0]
            .output_folder_path
            .as_ref()
            .unwrap();
        let path = folder.join("outputs_00000.jsonl");
        let mut event: Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        event["payload"]["reaction_output"]
            .as_object_mut()
            .unwrap()
            .remove("producer_id");
        tokio::fs::write(path, event.to_string()).await.unwrap();
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        assert!(result.is_err());
        assert_eq!(verdict["verdict"], "inconclusive");
        assert_eq!(
            verdict["comparison"]["queries"][0]["state"]["verdict"],
            "passed"
        );
    }

    #[tokio::test]
    async fn recovery_comparison_fetches_snapshot_before_comparing() {
        let mut fixture = fixture().await;
        let app = axum::Router::new().route(
            "/snapshot",
            axum::routing::get(|| async {
                axum::Json(json!({"success":true,"data":[{"value":1},{"value":2}]}))
            }),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}/snapshot", listener.local_addr().unwrap());
        let server = axum::Server::from_tcp(listener)
            .unwrap()
            .serve(app.into_make_service());
        let task = tokio::spawn(server);
        fixture.config["queries"][0]
            .as_object_mut()
            .unwrap()
            .remove("snapshot_path");
        fixture.config["queries"][0]["snapshot_url"] = json!(url);
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        task.abort();
        result.unwrap();
        assert_eq!(verdict["verdict"], "passed");
        assert!(fixture.root.join("recovery_snapshot_0.json").exists());
    }

    #[tokio::test]
    async fn recovery_comparison_missing_rotated_chunk_rejected() {
        let fixture = fixture().await;
        let folder = fixture
            .summary
            .reaction_logger_outputs
            .values()
            .next()
            .unwrap()[0]
            .output_folder_path
            .as_ref()
            .unwrap();
        tokio::fs::remove_file(folder.join("outputs_00001.jsonl"))
            .await
            .unwrap();
        let (result, verdict) = execute(&fixture, Some(evidence())).await;
        assert!(result.is_err());
        assert_eq!(verdict["verdict"], "invalid");
    }
}
