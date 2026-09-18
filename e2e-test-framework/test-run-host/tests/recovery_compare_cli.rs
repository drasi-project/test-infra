use serde_json::{json, Value};
use std::path::Path;
use std::process::{Command, Output};

fn fixture(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../examples/recovery_comparison")
        .join(name);
    serde_json::from_reader(std::fs::File::open(path).unwrap()).unwrap()
}

fn run(baseline: &Value, recovered: &Value) -> Output {
    let directory = tempfile::tempdir().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_recovery-compare"));
    for (filename, value) in [
        ("baseline.json", baseline),
        ("recovered.json", recovered),
    ] {
        let path = directory.path().join(filename);
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        command.arg(path);
    }
    command.output().unwrap()
}

#[test]
fn example_rejects_duplicates_and_reordering() {
    let output = run(
        &fixture("baseline.json"),
        &fixture("recovered.json"),
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["verdict"], "failed");
    assert_eq!(report["queries"][0]["delivery"]["duplicates"]["1:0"], 1);
    assert_eq!(report["queries"][0]["delivery"]["reordered"], true);
    assert_eq!(report["queries"][0]["state"]["verdict"], "passed");
}

#[test]
fn identical_ordered_delivery_passes() {
    let baseline = fixture("baseline.json");
    let output = run(&baseline, &baseline);
    assert_eq!(output.status.code(), Some(0));
}

#[test]
fn duplicate_and_reorder_each_fail_independently() {
    let baseline = fixture("baseline.json");
    let mut recovered = baseline.clone();
    recovered["queries"][0]["events"].as_array_mut().unwrap().push(baseline["queries"][0]["events"][0].clone());
    assert_eq!(run(&baseline, &recovered).status.code(), Some(1));
    recovered = baseline.clone();
    recovered["queries"][0]["events"].as_array_mut().unwrap().reverse();
    assert_eq!(run(&baseline, &recovered).status.code(), Some(1));
}

#[test]
fn policy_argument_is_rejected() {
    let output = Command::new(env!("CARGO_BIN_EXE_recovery-compare"))
        .args(["baseline.json", "recovered.json", "policy.json"])
        .output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Usage:"));
}

#[test]
fn incomplete_and_unknown_identity_exit_two() {
    let mut recovered = fixture("recovered.json");
    recovered["capture"]["complete"] = json!(false);
    let output = run(
        &fixture("baseline.json"),
        &recovered,
    );
    assert_eq!(output.status.code(), Some(2));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["verdict"], "inconclusive");

    recovered = fixture("recovered.json");
    recovered["queries"][0]["events"][0]["identity"] = Value::Null;
    let output = run(
        &fixture("baseline.json"),
        &recovered,
    );
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn incompatible_baseline_and_invalid_schema_do_not_emit_success_report() {
    let mut recovered = fixture("recovered.json");
    recovered["workload_fingerprint"] = json!("different-input-script");
    let output = run(
        &fixture("baseline.json"),
        &recovered,
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("workload fingerprints differ"));

    recovered = fixture("recovered.json");
    recovered["typo"] = json!(true);
    let output = run(
        &fixture("baseline.json"),
        &recovered,
    );
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
}

#[test]
fn cli_import_marks_legacy_identity_unavailable() {
    let directory = tempfile::tempdir().unwrap();
    let events = directory.path().join("events.jsonl");
    std::fs::write(
        &events,
        json!({"sequence": 1, "payload": {"request_body": {
            "query_id": "items", "result": {"type": "ADD", "after": {"Ordinal": 1}}
        }}})
        .to_string(),
    )
    .unwrap();
    let manifest = json!({
        "schema_version": 1, "workload_fingerprint": "synthetic-one-item-v1",
        "capture": {"complete": false, "evidence": "Legacy capture lacks a verified terminal boundary"},
        "queries": [{"query_id": "items", "config_fingerprint": "items-v1",
            "identity_contract": null, "identity_pointer": null,
            "query_id_pointer": "/payload/request_body/query_id",
            "payload_pointer": "/payload/request_body/result",
            "event_files": ["events.jsonl"], "snapshot_file": null}]
    });
    let path = directory.path().join("capture.json");
    std::fs::write(&path, manifest.to_string()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_recovery-compare"))
        .arg("--import")
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success());
    let artifact: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(artifact["queries"][0]["events"][0]["identity"].is_null());
    assert_eq!(artifact["capture"]["complete"], false);

    std::fs::write(&events, "{truncated").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_recovery-compare"))
        .arg("--import")
        .arg(directory.path().join("capture.json"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("events.jsonl:1"));
}
