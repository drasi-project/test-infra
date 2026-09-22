#!/usr/bin/env python3
"""Compare scheduled results with rolling, like-for-like performance baselines."""

import argparse
import hashlib
import json
import math
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (AttributeError, TypeError, ValueError):
        return None


def metric_value(payload, path):
    for part in path.split("."):
        if not isinstance(payload, dict):
            return None
        payload = payload.get(part)
    if isinstance(payload, bool) or not isinstance(payload, (int, float)):
        return None
    return float(payload) if math.isfinite(payload) and payload >= 0 else None


def series(record):
    run = record.get("run", {})
    dimensions = record.get("dimensions", {})
    if not all(run.get(key) for key in ("runner", "workflow", "run_id")):
        return None
    if not all(dimensions.get(key) for key in ("scenario", "variant")):
        return None
    reactions = record.get("reactions", [])
    if not reactions or any(not reaction.get("reaction_id") for reaction in reactions):
        return None
    return {
        "schema_version": record.get("schema_version"),
        "dimensions": dimensions,
        "params": record.get("params", {}),
        "runner": run["runner"],
        "workflow": run["workflow"],
        "reactions": sorted(
            ({"reaction_id": reaction["reaction_id"], "records": reaction.get("records")}
             for reaction in reactions),
            key=lambda reaction: reaction["reaction_id"],
        ),
    }


def eligible(record):
    return (record.get("status") == "success"
            and record.get("run", {}).get("trigger") == "schedule")


def observations(record, metric):
    if metric["scope"] == "totals":
        return {"totals": metric_value(record.get("totals", {}), metric["path"])}
    return {reaction["reaction_id"]: metric_value(reaction, metric["path"])
            for reaction in record.get("reactions", [])}


def validate_policy(policy):
    baseline = policy["baseline"]
    for key in ("window", "min_samples", "max_age_days"):
        if type(baseline[key]) is not int or baseline[key] < 1:
            raise ValueError(f"baseline.{key} must be a positive integer")
    if baseline["min_samples"] > baseline["window"]:
        raise ValueError("min_samples must not exceed window")
    names = set()
    for metric in policy["metrics"]:
        if not metric.get("name") or metric["name"] in names:
            raise ValueError("metric names must be nonempty and unique")
        names.add(metric["name"])
        if metric["scope"] not in ("reactions", "totals"):
            raise ValueError("metric scope must be reactions or totals")
        if metric["direction"] not in ("higher_is_better", "lower_is_better"):
            raise ValueError("unsupported metric direction")
        if not metric["path"] or any(not part for part in metric["path"].split(".")):
            raise ValueError("metric path must be a dotted field path")
        for rule in [metric, *metric.get("overrides", [])]:
            threshold = rule["threshold_percent"]
            if (isinstance(threshold, bool) or not isinstance(threshold, (int, float))
                    or not math.isfinite(threshold) or threshold <= 0):
                raise ValueError("threshold_percent must be a positive finite number")
        for override in metric.get("overrides", []):
            if not isinstance(override.get("match"), dict) or not override["match"]:
                raise ValueError("threshold overrides require a nonempty match object")


def compare(current, history, policy):
    validate_policy(policy)
    baseline_policy = policy["baseline"]
    history_by_series = {}
    for record in history:
        identity = series(record)
        started = timestamp(record.get("run", {}).get("started_at"))
        if identity is None or started is None:
            continue
        by_run = history_by_series.setdefault(canonical(identity), {})
        run_id = record["run"]["run_id"]
        previous = by_run.get(run_id)
        if (previous is None or record["run"].get("run_attempt", 1)
                > previous["run"].get("run_attempt", 1)):
            by_run[run_id] = record

    checks = []
    for record in current:
        identity = series(record)
        run = record.get("run", {})
        started = timestamp(run.get("started_at"))
        if not eligible(record) or identity is None or started is None:
            checks.append({"status": "skipped", "run": run,
                           "reason": "Not a successful scheduled run with comparable metadata"})
            continue
        cutoff = started - timedelta(days=baseline_policy["max_age_days"])
        candidates = [
            previous for previous in history_by_series.get(canonical(identity), {}).values()
            if eligible(previous) and previous["run"]["run_id"] != run["run_id"]
            and cutoff <= timestamp(previous["run"]["started_at"]) < started
        ]
        candidates.sort(key=lambda previous: timestamp(previous["run"]["started_at"]), reverse=True)

        for metric in policy["metrics"]:
            for subject, value in observations(record, metric).items():
                context = {**identity["dimensions"], "runner": identity["runner"],
                           "workflow": identity["workflow"], "reaction_id": subject}
                threshold = metric["threshold_percent"]
                for override in metric.get("overrides", []):
                    if all(context.get(key) == expected for key, expected in override["match"].items()):
                        threshold = override["threshold_percent"]
                key = {"series": identity, "metric": metric["name"], "scope": metric["scope"],
                       "path": metric["path"], "direction": metric["direction"], "subject": subject}
                check = {
                    "key": hashlib.sha256(canonical(key).encode()).hexdigest(),
                    "series": identity, "run": run, "versions": record.get("versions", {}),
                    "metric": metric["name"], "subject": subject, "unit": metric.get("unit", ""),
                    "direction": metric["direction"], "threshold_percent": threshold,
                    "current": value, "status": "skipped", "baseline_samples": [],
                }
                checks.append(check)
                if value is None:
                    check["reason"] = "Metric absent or invalid"
                    continue
                samples = []
                for previous in candidates:
                    previous_value = observations(previous, metric).get(subject)
                    if previous_value is not None:
                        samples.append({"value": previous_value, "run": previous["run"]})
                    if len(samples) == baseline_policy["window"]:
                        break
                check["baseline_samples"] = samples
                if len(samples) < baseline_policy["min_samples"]:
                    check["reason"] = f"Insufficient baseline: {len(samples)}/{baseline_policy['min_samples']} samples"
                    continue
                baseline = statistics.median(sample["value"] for sample in samples)
                check["baseline"] = baseline
                if baseline <= 0:
                    check["reason"] = "Baseline is zero; percentage comparison unavailable"
                    continue
                change = (value - baseline) / baseline * 100
                degradation = -change if metric["direction"] == "higher_is_better" else change
                check["degradation_percent"] = degradation
                check["status"] = "regression" if degradation > threshold else "ok"
    return {"checks": checks, "regressions": [check for check in checks if check["status"] == "regression"]}


def read_records(directory):
    return [json.loads(path.read_text()) for path in sorted(Path(directory).rglob("*.json"))]


def markdown(report):
    lines = ["## Performance regression check", "",
             f"Detected {len(report['regressions'])} regression(s).", ""]
    for check in report["checks"]:
        identity = check.get("series")
        if identity:
            label = (f"{identity['dimensions']['scenario']} / {identity['dimensions']['variant']} / "
                     f"{identity['runner']} / {check['subject']} / {check['metric']}")
        else:
            label = f"Run {check['run'].get('run_id', 'unknown')}"
        if check["status"] == "skipped":
            detail = check["reason"]
        else:
            detail = (f"{check['current']:.2f} {check['unit']}; median {check['baseline']:.2f}; "
                      f"degradation {check['degradation_percent']:.1f}%; "
                      f"threshold >{check['threshold_percent']}%; "
                      f"{len(check['baseline_samples'])} baseline samples")
        lines.append(f"- **{check['status']}**: {label}: {detail}")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", required=True)
    parser.add_argument("--history", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--markdown", required=True)
    args = parser.parse_args()
    report = compare(read_records(args.current), read_records(args.history),
                     json.loads(Path(args.config).read_text()))
    Path(args.output).write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    rendered = markdown(report)
    Path(args.markdown).write_text(rendered)
    print(rendered)


if __name__ == "__main__":
    main()