use crate::recovery_comparison::{Artifact, Capture, Event, QueryArtifact};
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    workload_fingerprint: String,
    capture: Capture,
    queries: Vec<QueryFiles>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryFiles {
    query_id: String,
    config_fingerprint: String,
    identity_contract: Option<String>,
    event_files: Vec<PathBuf>,
    query_id_pointer: String,
    payload_pointer: String,
    identity_pointer: Option<String>,
    snapshot_file: Option<PathBuf>,
}

pub fn load(path: &Path) -> Result<Artifact> {
    let manifest: Manifest = serde_json::from_reader(BufReader::new(File::open(path)?))?;
    ensure!(
        manifest.schema_version == 1,
        "unsupported capture schema version"
    );
    let directory = path.parent().unwrap_or(Path::new("."));
    let mut queries = Vec::new();
    for query in manifest.queries {
        ensure!(
            !query.event_files.is_empty(),
            "query {}: event files required",
            query.query_id
        );
        ensure!(
            query.identity_contract.is_some() == query.identity_pointer.is_some(),
            "query {}: identity contract and pointer must both be provided or both absent",
            query.query_id
        );
        for pointer in [&query.query_id_pointer, &query.payload_pointer]
            .into_iter()
            .chain(query.identity_pointer.iter())
        {
            ensure!(
                pointer.is_empty() || pointer.starts_with('/'),
                "invalid JSON pointer: {pointer}"
            );
        }
        let mut events = Vec::new();
        let mut files = std::collections::BTreeSet::new();
        for filename in &query.event_files {
            let filename = directory.join(filename);
            let canonical_path = filename
                .canonicalize()
                .with_context(|| format!("opening {}", filename.display()))?;
            ensure!(
                files.insert(canonical_path),
                "repeated event file: {}",
                filename.display()
            );
            for (line_number, line) in BufReader::new(File::open(&filename)?).lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let event = (|| -> Result<Option<Event>> {
                    let record: Value = serde_json::from_str(&line)?;
                    parse_event(&record, &query)
                })()
                .with_context(|| format!("{}:{}", filename.display(), line_number + 1))?;
                if let Some(event) = event {
                    events.push(event);
                }
            }
        }
        let snapshot = query
            .snapshot_file
            .as_ref()
            .map(|filename| -> Result<Vec<Value>> {
                let filename = directory.join(filename);
                let response: Value =
                    serde_json::from_reader(BufReader::new(File::open(&filename)?))
                        .with_context(|| format!("reading snapshot {}", filename.display()))?;
                snapshot_rows(&response)
            })
            .transpose()?;
        queries.push(QueryArtifact {
            query_id: query.query_id,
            config_fingerprint: query.config_fingerprint,
            identity_contract: query.identity_contract,
            events,
            snapshot,
        });
    }
    let artifact = Artifact {
        schema_version: manifest.schema_version,
        workload_fingerprint: manifest.workload_fingerprint,
        capture: manifest.capture,
        queries,
    };
    crate::recovery_comparison::validate(&artifact)?;
    Ok(artifact)
}

fn parse_event(record: &Value, query: &QueryFiles) -> Result<Option<Event>> {
    ensure!(
        record
            .pointer(&query.query_id_pointer)
            .and_then(Value::as_str)
            == Some(query.query_id.as_str()),
        "query ID missing or different from {}",
        query.query_id
    );
    if query.query_id_pointer == "/payload/request_body/query_id"
        && query.payload_pointer == "/payload/request_body/result"
        && record
            .pointer("/payload/request_body")
            .and_then(Value::as_object)
            .is_some_and(|body| {
                body.len() == 2
                    && body
                        .get("results")
                        .and_then(Value::as_array)
                        .is_some_and(Vec::is_empty)
            })
    {
        return Ok(None);
    }
    let payload = record
        .pointer(&query.payload_pointer)
        .context("result payload pointer not found")?
        .clone();
    let identity = match query
        .identity_pointer
        .as_ref()
        .and_then(|pointer| record.pointer(pointer))
    {
        None | Some(Value::Null) => None,
        Some(value @ Value::String(text)) if !text.trim().is_empty() => {
            Some(serde_json::to_string(value)?)
        }
        Some(value @ Value::Number(number)) if number.as_u64().is_some() => {
            Some(serde_json::to_string(value)?)
        }
        Some(_) => bail!("identity must be a nonempty string or unsigned integer"),
    };
    Ok(Some(Event { identity, payload }))
}

fn snapshot_rows(response: &Value) -> Result<Vec<Value>> {
    ensure!(
        response.get("success") == Some(&Value::Bool(true)),
        "snapshot API response is not successful"
    );
    response
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .context("snapshot data must be an array")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn query() -> QueryFiles {
        serde_json::from_value(json!({
            "query_id": "items", "config_fingerprint": "items-v1",
            "identity_contract": null, "identity_pointer": null,
            "event_files": ["events.jsonl"], "snapshot_file": null,
            "query_id_pointer": "/payload/request_body/query_id",
            "payload_pointer": "/payload/request_body/result"
        }))
        .unwrap()
    }

    #[test]
    fn legacy_receiver_sequence_is_not_producer_identity() {
        let event = parse_event(&json!({"sequence": 42, "payload": {"query_id": "unknown",
            "request_body": {"query_id": "items", "result": {"type": "ADD", "after": {"ordinal": 1}}}}}), &query()).unwrap().unwrap();
        assert_eq!(event.identity, None);
        assert_eq!(event.payload["after"]["ordinal"], 1);
    }

    #[test]
    fn wrong_query_or_missing_payload_is_invalid() {
        assert!(parse_event(
            &json!({"payload": {"request_body": {"query_id": "other", "result": {}}}}),
            &query()
        )
        .is_err());
        assert!(parse_event(
            &json!({"payload": {"request_body": {"query_id": "items"}}}),
            &query()
        )
        .is_err());
    }

    #[test]
    fn absent_identity_is_unknown_and_invalid_identity_is_rejected() {
        let mut query = query();
        query.identity_contract = Some("fixture-producer-id".to_owned());
        query.identity_pointer = Some("/producer_id".to_owned());
        let mut record = json!({"payload": {"request_body": {"query_id": "items", "result": {}}}});
        assert!(parse_event(&record, &query)
            .unwrap()
            .unwrap()
            .identity
            .is_none());
        record["producer_id"] = json!([]);
        assert!(parse_event(&record, &query).is_err());
    }

    #[test]
    fn failed_snapshot_is_not_empty_state() {
        assert!(snapshot_rows(&json!({"success": false, "data": []})).is_err());
        assert!(snapshot_rows(&json!({"success": true, "data": {}})).is_err());
        assert!(snapshot_rows(&json!({"success": true, "data": []}))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn only_known_empty_grpc_envelopes_are_skipped() {
        let heartbeat = json!({"payload":{"request_body":{"query_id":"items","results":[]}}});
        assert!(parse_event(&heartbeat, &query()).unwrap().is_none());
        for body in [
            json!({"query_id":"other","results":[]}),
            json!({"results":[]}),
            json!({"query_id":"items"}),
            json!({"query_id":"items","results":[{"value":1}]}),
            json!({"query_id":"items","results":null}),
            json!({"query_id":"items","results":[],"unexpected":true}),
        ] {
            assert!(parse_event(&json!({"payload":{"request_body":body}}), &query()).is_err());
        }
        let mut custom = query();
        custom.payload_pointer = "/payload/request_body/request_body".to_owned();
        assert!(parse_event(&heartbeat, &custom).is_err());
        let real = json!({"payload":{"request_body":{"query_id":"items","result":{"results":[]}}}});
        assert_eq!(
            parse_event(&real, &query()).unwrap().unwrap().payload,
            json!({"results":[]})
        );
        let mixed = json!({"payload":{"request_body":{"query_id":"items","result":{"value":1},"results":[]}}});
        assert_eq!(
            parse_event(&mixed, &query()).unwrap().unwrap().payload,
            json!({"value":1})
        );
    }

    #[test]
    fn import_preserves_explicit_file_order_and_relative_paths() {
        let directory = tempfile::tempdir().unwrap();
        for (filename, identity) in [("second.jsonl", "second"), ("first.jsonl", "first")] {
            std::fs::write(
                directory.path().join(filename),
                json!({"producer_id": identity, "query": "items", "result": {"value": identity}})
                    .to_string(),
            )
            .unwrap();
        }
        let manifest = json!({"schema_version": 1, "workload_fingerprint": "fixture",
        "capture": {"complete": false, "evidence": "test fixture"}, "queries": [{
            "query_id": "items", "config_fingerprint": "items-v1", "identity_contract": "fixture-v1",
            "identity_pointer": "/producer_id", "query_id_pointer": "/query", "payload_pointer": "/result",
            "event_files": ["second.jsonl", "first.jsonl"], "snapshot_file": null
        }]});
        let path = directory.path().join("capture.json");
        std::fs::write(&path, manifest.to_string()).unwrap();
        let artifact = load(&path).unwrap();
        assert_eq!(
            artifact.queries[0].events[0].identity.as_deref(),
            Some("\"second\"")
        );
        assert_eq!(
            artifact.queries[0].events[1].identity.as_deref(),
            Some("\"first\"")
        );
        assert!(!artifact.capture.complete);
    }

    #[test]
    fn import_skips_interleaved_heartbeats_without_changing_real_events() {
        let directory = tempfile::tempdir().unwrap();
        let heartbeat = json!({"payload":{"request_body":{"query_id":"items","results":[]}}});
        let first = json!({"producer_id":"first","payload":{"request_body":{"query_id":"items","result":{"value":1}}}});
        let second = json!({"producer_id":"second","payload":{"request_body":{"query_id":"items","result":{"value":2}}}});
        let records = directory.path().join("events.jsonl");
        let manifest = json!({"schema_version":1,"workload_fingerprint":"fixture",
            "capture":{"complete":false,"evidence":"Synthetic fixture without terminal evidence"},
            "queries":[{"query_id":"items","config_fingerprint":"items-v1",
                "identity_contract":"fixture-v1","identity_pointer":"/producer_id",
                "query_id_pointer":"/payload/request_body/query_id",
                "payload_pointer":"/payload/request_body/result",
                "event_files":["events.jsonl"],"snapshot_file":null}]});
        let path = directory.path().join("capture.json");
        std::fs::write(&path, manifest.to_string()).unwrap();
        std::fs::write(&records, format!("{first}\n{second}\n")).unwrap();
        let baseline = load(&path).unwrap();
        std::fs::write(
            &records,
            format!("{heartbeat}\n{first}\n{heartbeat}\n{second}\n{heartbeat}\n"),
        )
        .unwrap();
        let recovered = load(&path).unwrap();
        assert_eq!(
            serde_json::to_value(&baseline).unwrap(),
            serde_json::to_value(&recovered).unwrap()
        );
        let report = crate::recovery_comparison::compare(&baseline, &recovered).unwrap();
        assert_eq!(
            report.queries[0].delivery.verdict,
            crate::recovery_comparison::Verdict::Passed
        );
        assert_eq!(report.queries[0].delivery.actual_observations, 2);
        assert!(!recovered.capture.complete);

        std::fs::write(&records, format!("{heartbeat}\n{heartbeat}\n")).unwrap();
        assert!(load(&path).unwrap().queries[0].events.is_empty());
        for malformed in [
            "{truncated".to_owned(),
            json!({"payload":{"request_body":{"query_id":"items"}}}).to_string(),
        ] {
            std::fs::write(&records, format!("{heartbeat}\n{malformed}\n")).unwrap();
            let error = load(&path).unwrap_err();
            assert!(format!("{error:#}").contains("events.jsonl:2"));
        }
    }
}
