use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub schema_version: u32,
    pub workload_fingerprint: String,
    pub capture: Capture,
    pub queries: Vec<QueryArtifact>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub complete: bool,
    pub evidence: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryArtifact {
    pub query_id: String,
    pub config_fingerprint: String,
    pub identity_contract: Option<String>,
    pub events: Vec<Event>,
    pub snapshot: Option<Vec<Value>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub identity: Option<String>,
    pub payload: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Passed,
    Failed,
    Inconclusive,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub schema_version: u32,
    pub verdict: Verdict,
    pub workload_fingerprint: String,
    pub reasons: Vec<String>,
    pub missing_queries: Vec<String>,
    pub unexpected_queries: Vec<String>,
    pub queries: Vec<QueryReport>,
}

#[derive(Debug, Serialize)]
pub struct QueryReport {
    pub query_id: String,
    pub delivery: DeliveryReport,
    pub state: StateReport,
}

#[derive(Debug, Serialize)]
pub struct DeliveryReport {
    pub verdict: Verdict,
    pub reason: Option<String>,
    pub expected_observations: usize,
    pub actual_observations: usize,
    pub missing: Vec<String>,
    pub unexpected: Vec<String>,
    pub duplicates: BTreeMap<String, usize>,
    pub conflicting: Vec<String>,
    pub reordered: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct StateReport {
    pub verdict: Verdict,
    pub reason: Option<String>,
    pub missing: Vec<RowDifference>,
    pub unexpected: Vec<RowDifference>,
}

#[derive(Debug, Serialize)]
pub struct RowDifference {
    pub row: Value,
    pub count: usize,
}

pub fn compare(baseline: &Artifact, recovery: &Artifact) -> Result<Report> {
    validate(baseline)?;
    validate(recovery)?;
    ensure!(
        baseline.workload_fingerprint == recovery.workload_fingerprint,
        "workload fingerprints differ; choose a matching baseline"
    );
    let expected = query_map(baseline);
    let actual = query_map(recovery);
    let missing_queries: Vec<String> = expected
        .keys()
        .filter(|query| !actual.contains_key(**query))
        .map(|query| (*query).to_owned())
        .collect();
    let unexpected_queries: Vec<String> = actual
        .keys()
        .filter(|query| !expected.contains_key(**query))
        .map(|query| (*query).to_owned())
        .collect();
    let mut queries = Vec::new();
    for (query_id, reference) in &expected {
        let Some(observed) = actual.get(query_id) else {
            continue;
        };
        ensure!(
            reference.config_fingerprint == observed.config_fingerprint,
            "query {query_id}: configuration/epoch fingerprints differ"
        );
        ensure!(
            reference.identity_contract == observed.identity_contract,
            "query {query_id}: identity contracts differ"
        );
        queries.push(QueryReport {
            query_id: (*query_id).to_owned(),
            delivery: compare_delivery(reference, observed)?,
            state: compare_state(reference.snapshot.as_deref(), observed.snapshot.as_deref()),
        });
    }
    let mut reasons = Vec::new();
    for (label, artifact) in [("baseline", baseline), ("recovery", recovery)] {
        if !artifact.capture.complete {
            reasons.push(format!(
                "{label} capture incomplete: {}",
                artifact.capture.evidence
            ));
        }
    }
    let verdict = if !reasons.is_empty() {
        Verdict::Inconclusive
    } else if !missing_queries.is_empty()
        || !unexpected_queries.is_empty()
        || queries.iter().any(|query| {
            query.delivery.verdict == Verdict::Failed || query.state.verdict == Verdict::Failed
        })
    {
        Verdict::Failed
    } else if queries.iter().any(|query| {
        query.delivery.verdict == Verdict::Inconclusive
            || query.state.verdict == Verdict::Inconclusive
    }) {
        Verdict::Inconclusive
    } else {
        Verdict::Passed
    };
    Ok(Report {
        schema_version: 1,
        verdict,
        workload_fingerprint: baseline.workload_fingerprint.clone(),
        reasons,
        missing_queries,
        unexpected_queries,
        queries,
    })
}

pub(crate) fn validate(artifact: &Artifact) -> Result<()> {
    ensure!(
        artifact.schema_version == 1,
        "unsupported artifact schema version"
    );
    ensure!(
        !artifact.workload_fingerprint.trim().is_empty(),
        "workload fingerprint must not be empty"
    );
    ensure!(
        !artifact.capture.evidence.trim().is_empty(),
        "capture evidence is required"
    );
    ensure!(
        !artifact.queries.is_empty(),
        "artifact must contain at least one query"
    );
    let mut ids = BTreeSet::new();
    for query in &artifact.queries {
        ensure!(
            !query.query_id.trim().is_empty(),
            "query ID must not be empty"
        );
        ensure!(
            ids.insert(&query.query_id),
            "duplicate query ID: {}",
            query.query_id
        );
        ensure!(
            !query.config_fingerprint.trim().is_empty(),
            "query {}: configuration fingerprint is required",
            query.query_id
        );
        if let Some(contract) = &query.identity_contract {
            ensure!(
                !contract.trim().is_empty(),
                "identity contract must not be empty"
            );
        }
        for event in &query.events {
            if let Some(identity) = &event.identity {
                ensure!(
                    !identity.trim().is_empty(),
                    "event identity must not be empty"
                );
            }
        }
    }
    Ok(())
}

fn query_map(artifact: &Artifact) -> BTreeMap<&str, &QueryArtifact> {
    artifact
        .queries
        .iter()
        .map(|query| (query.query_id.as_str(), query))
        .collect()
}

fn compare_delivery(
    expected: &QueryArtifact,
    actual: &QueryArtifact,
) -> Result<DeliveryReport> {
    let mut report = DeliveryReport {
        verdict: Verdict::Inconclusive,
        reason: None,
        expected_observations: expected.events.len(),
        actual_observations: actual.events.len(),
        missing: Vec::new(),
        unexpected: Vec::new(),
        duplicates: BTreeMap::new(),
        conflicting: Vec::new(),
        reordered: None,
    };
    if expected.identity_contract.is_none()
        || expected
            .events
            .iter()
            .chain(&actual.events)
            .any(|event| event.identity.is_none())
    {
        report.reason = Some("Stable cross-run producer identities are unavailable; receiver numbering and payload equality cannot establish redelivery.".to_owned());
        return Ok(report);
    }
    let mut reference = BTreeMap::new();
    for event in &expected.events {
        let identity = event.identity.as_deref().unwrap();
        if reference.insert(identity, &event.payload).is_some() {
            bail!(
                "query {}: baseline contains repeated identity {identity}",
                expected.query_id
            );
        }
    }
    let mut observed: BTreeMap<&str, Vec<&Value>> = BTreeMap::new();
    let mut actual_order = Vec::new();
    for event in &actual.events {
        let identity = event.identity.as_deref().unwrap();
        let payloads = observed.entry(identity).or_default();
        if payloads.is_empty() && reference.contains_key(identity) {
            actual_order.push(identity);
        }
        payloads.push(&event.payload);
    }
    report.missing = reference
        .keys()
        .filter(|identity| !observed.contains_key(**identity))
        .map(|identity| (*identity).to_owned())
        .collect();
    for (identity, payloads) in &observed {
        if payloads.len() > 1 {
            report
                .duplicates
                .insert((*identity).to_owned(), payloads.len() - 1);
        }
        if !reference.contains_key(identity) {
            report.unexpected.push((*identity).to_owned());
        }
        let expected_payload = reference.get(identity).copied().unwrap_or(payloads[0]);
        if payloads
            .iter()
            .any(|payload| **payload != *expected_payload)
        {
            report.conflicting.push((*identity).to_owned());
        }
    }
    let expected_order: Vec<&str> = expected
        .events
        .iter()
        .map(|event| event.identity.as_deref().unwrap())
        .filter(|identity| observed.contains_key(identity))
        .collect();
    let reordered = actual_order != expected_order;
    report.reordered = Some(reordered);
    report.verdict = if !report.missing.is_empty()
        || !report.unexpected.is_empty()
        || !report.conflicting.is_empty()
        || reordered
        || !report.duplicates.is_empty()
    {
        Verdict::Failed
    } else {
        Verdict::Passed
    };
    Ok(report)
}

fn compare_state(expected: Option<&[Value]>, actual: Option<&[Value]>) -> StateReport {
    let (Some(expected), Some(actual)) = (expected, actual) else {
        return StateReport {
            verdict: Verdict::Inconclusive,
            reason: Some("A final query snapshot is unavailable.".to_owned()),
            missing: Vec::new(),
            unexpected: Vec::new(),
        };
    };
    let expected = row_counts(expected);
    let actual = row_counts(actual);
    let missing = row_diff(&expected, &actual);
    let unexpected = row_diff(&actual, &expected);
    StateReport {
        verdict: if missing.is_empty() && unexpected.is_empty() {
            Verdict::Passed
        } else {
            Verdict::Failed
        },
        reason: None,
        missing,
        unexpected,
    }
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(fields) => {
            let sorted: BTreeMap<_, _> = fields
                .iter()
                .map(|(key, value)| (key.clone(), canonical(value)))
                .collect();
            serde_json::to_value(sorted).unwrap()
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical).collect()),
        value => value.clone(),
    }
}

fn row_counts(rows: &[Value]) -> BTreeMap<String, (Value, usize)> {
    let mut counts = BTreeMap::new();
    for row in rows {
        let canonical_row = canonical(row);
        let key = serde_json::to_string(&canonical_row).unwrap();
        counts.entry(key).or_insert((canonical_row, 0)).1 += 1;
    }
    counts
}

fn row_diff(
    expected: &BTreeMap<String, (Value, usize)>,
    actual: &BTreeMap<String, (Value, usize)>,
) -> Vec<RowDifference> {
    expected
        .iter()
        .filter_map(|(key, (row, count))| {
            let difference = count.saturating_sub(actual.get(key).map_or(0, |(_, count)| *count));
            (difference > 0).then(|| RowDifference {
                row: row.clone(),
                count: difference,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn artifact() -> Artifact {
        serde_json::from_value(json!({
            "schema_version": 1, "workload_fingerprint": "fixture-v1",
            "capture": {"complete": true, "evidence": "fixture producer finished, delivery boundary reached, snapshot taken"},
            "queries": [{"query_id": "items", "config_fingerprint": "items-v1",
                "identity_contract": "fixture-stable-sequence-v1",
                "events": [{"identity": "1:0", "payload": {"value": 1}},
                           {"identity": "2:0", "payload": {"value": 2}}],
                "snapshot": [{"value": 2}]}]
        })).unwrap()
    }

    fn duplicate(artifact: &mut Artifact, payload: Value) {
        artifact.queries[0].events.push(Event {
            identity: Some("1:0".to_owned()),
            payload,
        });
    }

    #[test]
    fn unchanged_passes() {
        assert_eq!(
            compare(&artifact(), &artifact())
                .unwrap()
                .verdict,
            Verdict::Passed
        );
    }

    #[test]
    fn missing_query_is_reported() {
        let mut baseline = artifact();
        let mut additional = artifact().queries.remove(0);
        additional.query_id = "missing".to_owned();
        baseline.queries.push(additional);
        let report = compare(&baseline, &artifact()).unwrap();
        assert_eq!(report.verdict, Verdict::Failed);
        assert_eq!(report.missing_queries, ["missing"]);
    }

    #[test]
    fn baseline_metadata_must_be_explicit_and_queries_unique() {
        let mut baseline = artifact();
        baseline.schema_version = 2;
        assert!(compare(&baseline, &artifact()).is_err());
        baseline = artifact();
        baseline.capture.evidence.clear();
        assert!(compare(&baseline, &artifact()).is_err());
        baseline = artifact();
        baseline.queries.push(artifact().queries.remove(0));
        assert!(compare(&baseline, &artifact()).is_err());
        baseline = artifact();
        baseline.queries[0].identity_contract = Some("different-contract".to_owned());
        assert!(compare(&baseline, &artifact()).is_err());
    }

    #[test]
    fn changed_payload_is_not_reordering() {
        let mut recovery = artifact();
        recovery.queries[0].events[0].payload = json!({"value": 3});
        let report = compare(&artifact(), &recovery).unwrap();
        assert_eq!(report.verdict, Verdict::Failed);
        assert_eq!(report.queries[0].delivery.conflicting, ["1:0"]);
        assert_eq!(report.queries[0].delivery.reordered, Some(false));
    }

    #[test]
    fn missing_cannot_be_hidden_by_duplicate() {
        let mut recovery = artifact();
        recovery.queries[0].events.pop();
        duplicate(&mut recovery, json!({"value": 1}));
        let report = compare(&artifact(), &recovery).unwrap();
        assert_eq!(report.verdict, Verdict::Failed);
        assert_eq!(report.queries[0].delivery.missing, ["2:0"]);
        assert_eq!(report.queries[0].delivery.duplicates["1:0"], 1);
    }

    #[test]
    fn duplicates_always_fail() {
        let mut recovery = artifact();
        duplicate(&mut recovery, json!({"value": 1}));
        assert_eq!(
            compare(&artifact(), &recovery).unwrap().verdict,
            Verdict::Failed
        );
    }

    #[test]
    fn conflicting_duplicates_fail() {
        let mut recovery = artifact();
        duplicate(&mut recovery, json!({"value": 999}));
        let report = compare(&artifact(), &recovery).unwrap();
        assert_eq!(report.verdict, Verdict::Failed);
        assert_eq!(report.queries[0].delivery.conflicting, ["1:0"]);
    }

    #[test]
    fn reordering_is_reported_separately() {
        let mut recovery = artifact();
        recovery.queries[0].events.reverse();
        let report = compare(&artifact(), &recovery).unwrap();
        assert_eq!(report.verdict, Verdict::Failed);
        assert_eq!(report.queries[0].delivery.reordered, Some(true));
    }

    #[test]
    fn snapshots_preserve_multiplicity_and_do_not_collapse_group_keys() {
        let mut baseline = artifact();
        baseline.queries[0].snapshot = Some(vec![
            json!({"floor": "A", "value": 1}),
            json!({"floor": "A", "value": 2}),
        ]);
        let mut recovery = artifact();
        recovery.queries[0].snapshot = Some(vec![json!({"floor": "A", "value": 2}); 2]);
        let report = compare(&baseline, &recovery).unwrap();
        assert_eq!(report.verdict, Verdict::Failed);
        assert_eq!(report.queries[0].delivery.verdict, Verdict::Passed);
        assert_eq!(
            report.queries[0].state.missing[0].row,
            json!({"floor": "A", "value": 1})
        );
        assert_eq!(report.queries[0].state.unexpected[0].count, 1);
    }

    #[test]
    fn snapshot_order_ignored_but_array_order_and_business_fields_retained() {
        let expected = vec![json!({"timestamp": 1, "list": [1, 2]}), json!({"value": 2})];
        let mut actual = expected.clone();
        actual.reverse();
        assert_eq!(
            compare_state(Some(&expected), Some(&actual)).verdict,
            Verdict::Passed
        );
        actual[1] = json!({"timestamp": 1, "list": [2, 1]});
        assert_eq!(
            compare_state(Some(&expected), Some(&actual)).verdict,
            Verdict::Failed
        );
    }

    #[test]
    fn missing_identity_is_inconclusive_even_with_matching_state() {
        let mut baseline = artifact();
        let mut recovery = artifact();
        baseline.queries[0].identity_contract = None;
        recovery.queries[0].identity_contract = None;
        let report = compare(&baseline, &recovery).unwrap();
        assert_eq!(report.verdict, Verdict::Inconclusive);
        assert_eq!(report.queries[0].state.verdict, Verdict::Passed);
        assert_eq!(report.queries[0].delivery.reordered, None);
    }

    #[test]
    fn incomplete_capture_never_passes() {
        let mut recovery = artifact();
        recovery.capture.complete = false;
        recovery.capture.evidence = "timed out".to_owned();
        assert_eq!(
            compare(&artifact(), &recovery).unwrap().verdict,
            Verdict::Inconclusive
        );
    }

    #[test]
    fn missing_snapshot_is_not_empty_snapshot() {
        let mut recovery = artifact();
        recovery.queries[0].snapshot = None;
        assert_eq!(
            compare(&artifact(), &recovery).unwrap().verdict,
            Verdict::Inconclusive
        );
        assert_eq!(compare_state(Some(&[]), Some(&[])).verdict, Verdict::Passed);
    }

    #[test]
    fn different_workload_or_epoch_is_invalid() {
        let mut recovery = artifact();
        recovery.workload_fingerprint = "different".to_owned();
        assert!(compare(&artifact(), &recovery).is_err());
        recovery = artifact();
        recovery.queries[0].config_fingerprint = "different".to_owned();
        assert!(compare(&artifact(), &recovery).is_err());
    }

    #[test]
    fn repeated_baseline_identity_is_invalid() {
        let mut baseline = artifact();
        duplicate(&mut baseline, json!({"value": 1}));
        assert!(compare(&baseline, &artifact()).is_err());
    }

    #[test]
    fn extra_event_and_query_are_reported() {
        let mut recovery = artifact();
        recovery.queries[0].events.push(Event {
            identity: Some("3:0".to_owned()),
            payload: json!({"value": 3}),
        });
        let mut extra = artifact().queries.remove(0);
        extra.query_id = "unexpected".to_owned();
        recovery.queries.push(extra);
        let report = compare(&artifact(), &recovery).unwrap();
        assert_eq!(report.verdict, Verdict::Failed);
        assert_eq!(report.unexpected_queries, ["unexpected"]);
        assert_eq!(report.queries[0].delivery.unexpected, ["3:0"]);
    }

    #[test]
    fn identical_payloads_with_distinct_ids_are_not_duplicates() {
        let mut baseline = artifact();
        baseline.queries[0].events[1].payload = json!({"value": 1});
        let report = compare(&baseline, &baseline).unwrap();
        assert_eq!(report.verdict, Verdict::Passed);
        assert!(report.queries[0].delivery.duplicates.is_empty());
    }
}
