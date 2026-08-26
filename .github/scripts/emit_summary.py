#!/usr/bin/env python3
"""Emit a run summary for drasi-project/test-results.

Reads the artifacts that run_test_ci.sh has already written to $ARTIFACTS_DIR
and produces a single summary record describing this job. The record is
published to drasi-project/test-results, which keeps a small, permanent
history of every scheduled run (GitHub artifacts expire after 90 days).

The schema and the validator that enforces it live in that repo:
    https://github.com/drasi-project/test-results

Usage:
    python3 .github/scripts/emit_summary.py \
        --artifacts "$ARTIFACTS_DIR" \
        --scenario building_comfort \
        --variant drasi_server_http \
        --job-status success \
        --output summary.json

Run metadata is taken from the standard GitHub Actions environment variables.
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Target and transport are derived from the variant, which is the CI directory
# name and the only place these are spelled out. Keep in step with the example
# folders under e2e-test-framework/examples/<scenario>/ci/.
VARIANT_FACETS = {
    "drasi_server_http": ("drasi_server", "http"),
    "drasi_server_grpc": ("drasi_server", "grpc"),
    "drasi_server_http_grpc_join": ("drasi_server", "http_grpc"),
    "drasi_lib": ("drasi_lib", "in_process"),
}

# drasi-server prints a startup banner containing its version. Recorded because
# the requested tag is normally the moving pointer "latest", which never
# changes and so cannot identify which build a number came from.
VERSION_RE = re.compile(r"^\s*Version:\s*(\S+)\s*$", re.MULTILINE)


def read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError):
        return None


def find_perf_metrics(artifacts):
    """Map bare reaction id -> PerformanceMetrics payload.

    PerformanceMetrics keys reactions by the dotted test_run_reaction_id
    (<repo>.<test>.<run>.<reaction>), while determinism_verdict.json and the
    reaction-state filenames use the bare id. Join on the last dotted segment.
    """
    metrics = {}
    for path in artifacts.rglob("performance_metrics/*.json"):
        payload = read_json(path)
        if not payload:
            continue
        dotted = payload.get("test_run_reaction_id") or ""
        bare = dotted.rsplit(".", 1)[-1] if dotted else ""
        if not bare:
            continue
        previous = metrics.get(bare)
        if previous is None or payload.get("timestamp", "") > previous.get("timestamp", ""):
            metrics[bare] = payload
    return metrics


def find_determinism(artifacts):
    """Per-reaction verdicts, or None when the scenario has no handler.

    None and {} mean different things: None means this scenario never runs a
    determinism check (stock_market), which must be reported as
    not_applicable rather than as a pass.
    """
    for path in sorted(artifacts.rglob("determinism_verdict.json")):
        payload = read_json(path)
        if payload and isinstance(payload.get("results"), dict):
            return payload["results"]
    return None


def find_server_version(artifacts):
    log = artifacts / "logs" / "drasi-server.log"
    if not log.exists():
        return None
    try:
        match = VERSION_RE.search(log.read_text(errors="replace"))
    except OSError:
        return None
    return match.group(1) if match else None


def build_reactions(artifacts, perf, verdicts):
    scenario_has_determinism = verdicts is not None
    reactions = []
    total_records = 0
    earliest_start = None
    latest_end = None

    for state_path in sorted(artifacts.glob("final_reaction_state__*.json")):
        reaction_id = state_path.name[len("final_reaction_state__"):-len(".json")]
        state = read_json(state_path) or {}
        observer = state.get("reaction_observer") or {}
        summary = observer.get("result_summary") or {}

        entry = {"reaction_id": reaction_id, "status": observer.get("status") or "Error"}

        records = summary.get("reaction_invocation_count")
        if isinstance(records, int):
            entry["records"] = records

        metrics = perf.get(reaction_id)
        if metrics:
            duration_ns = metrics.get("duration_ns")
            if isinstance(duration_ns, int) and duration_ns > 0:
                # Never parse result_summary.observer_runtime_s: it is a human
                # string such as "33.0 seconds".
                entry["duration_s"] = round(duration_ns / 1e9, 2)
            rate = metrics.get("records_per_second")
            if isinstance(rate, (int, float)):
                entry["records_per_sec"] = round(float(rate), 1)
            if isinstance(metrics.get("record_count"), int):
                entry["records"] = metrics["record_count"]
            start_ns, end_ns = metrics.get("start_time_ns"), metrics.get("end_time_ns")
            if isinstance(start_ns, int):
                earliest_start = start_ns if earliest_start is None else min(earliest_start, start_ns)
            if isinstance(end_ns, int):
                latest_end = end_ns if latest_end is None else max(latest_end, end_ns)

        if isinstance(entry.get("records"), int):
            total_records += entry["records"]

        for logger in observer.get("logger_results") or []:
            if logger.get("logger_name") == "DeterminismHash":
                sha = (logger.get("summary") or {}).get("sha256")
                if sha:
                    entry["sha256"] = sha

        if not scenario_has_determinism:
            entry["determinism"] = "not_applicable"
        else:
            verdict = verdicts.get(reaction_id)
            entry["determinism"] = "not_applicable" if verdict is None else (
                "pass" if verdict.get("passed") else "fail"
            )

        if observer.get("error_message"):
            entry["error_message"] = observer["error_message"]

        reactions.append(entry)

    totals = {}
    if total_records:
        totals["records"] = total_records
    if earliest_start is not None and latest_end is not None and latest_end > earliest_start:
        # Wall clock across all reactions, never a sum of per-reaction
        # durations, so the aggregate rate stays meaningful.
        wall = (latest_end - earliest_start) / 1e9
        totals["duration_s"] = round(wall, 2)
        if total_records:
            totals["records_per_sec"] = round(total_records / wall, 1)

    return reactions, totals


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--artifacts", required=True, help="$ARTIFACTS_DIR for this job")
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--variant", required=True)
    parser.add_argument("--job-status", default="success",
                        help="outcome of the test step: success, failure or cancelled")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    artifacts = Path(args.artifacts)
    if not artifacts.is_dir():
        print(f"error: artifacts directory not found: {artifacts}", file=sys.stderr)
        return 1

    perf = find_perf_metrics(artifacts)
    verdicts = find_determinism(artifacts)
    reactions, totals = build_reactions(artifacts, perf, verdicts)

    if verdicts is None:
        determinism = "not_applicable"
    elif any(r.get("determinism") == "fail" for r in reactions):
        determinism = "fail"
    elif any(r.get("determinism") == "pass" for r in reactions):
        determinism = "pass"
    else:
        determinism = "not_applicable"

    if args.job_status == "success":
        status = "success"
    elif args.job_status == "cancelled":
        status = "timeout"
    else:
        status = "failure"

    run_id = os.environ.get("GITHUB_RUN_ID", "")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "drasi-project/test-infra")

    record = {
        "schema_version": 1,
        "run": {
            "run_id": run_id,
            "run_attempt": int(os.environ.get("GITHUB_RUN_ATTEMPT", "1")),
            "workflow": os.environ.get("WORKFLOW_FILE", ""),
            "trigger": os.environ.get("GITHUB_EVENT_NAME", "schedule"),
            "runner": os.environ.get("RUNNER_LABEL", "ubuntu-latest"),
            "started_at": os.environ.get("RUN_STARTED_AT", ""),
            # This step runs immediately after the test step, so "now" is the
            # end of the test for practical purposes.
            "finished_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "url": f"{server}/{repo}/actions/runs/{run_id}",
        },
        "versions": {"test_infra_sha": os.environ.get("GITHUB_SHA", "")},
        "dimensions": {"scenario": args.scenario, "variant": args.variant},
        "status": status,
        "determinism": determinism,
        "reactions": reactions,
    }

    target, transport = VARIANT_FACETS.get(args.variant, (None, None))
    if target:
        record["dimensions"]["target"] = target
        record["dimensions"]["transport"] = transport

    version = find_server_version(artifacts)
    if version:
        record["versions"]["drasi_server_version"] = version
        record["versions"]["drasi_server_tag"] = os.environ.get("DRASI_SERVER_VERSION") or "latest"

    if totals:
        record["totals"] = totals

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2) + "\n")
    print(f"wrote {out}")
    print(json.dumps(record, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
