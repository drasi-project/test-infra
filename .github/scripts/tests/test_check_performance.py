import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "check_performance.py"
SPEC = importlib.util.spec_from_file_location("check_performance", SCRIPT)
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)
POLICY = json.loads((SCRIPT.parent.parent / "performance-regression.json").read_text())
NOW = datetime(2026, 9, 22, tzinfo=timezone.utc)


def record(days_ago=0, value=100, **fields):
    result = {
        "schema_version": 1,
        "run": {"run_id": str(100 - days_ago), "run_attempt": 1,
                "workflow": "e2e-building-comfort.yml", "runner": "ubuntu-latest",
                "trigger": "schedule", "started_at": (NOW - timedelta(days=days_ago)).isoformat(),
                "url": f"https://github.com/drasi-project/test-infra/actions/runs/{100 - days_ago}"},
        "dimensions": {"scenario": "building_comfort", "variant": "drasi_lib",
                       "target": "drasi_lib", "transport": "in_process"},
        "status": "success", "determinism": "pass",
        "reactions": [{"reaction_id": "comfort", "records": 1000, "records_per_sec": value}],
        "totals": {"records_per_sec": value},
    }
    result.update(fields)
    return result


class PerformanceTests(unittest.TestCase):
    def setUp(self):
        self.policy = copy.deepcopy(POLICY)
        self.history = [record(days_ago) for days_ago in range(1, 8)]

    def check(self, current=None):
        return CHECKER.compare([current or record(value=70)], self.history, self.policy)["checks"][0]

    def test_throughput_regression(self):
        check = self.check()
        self.assertEqual(check["status"], "regression")
        self.assertEqual(check["baseline"], 100)
        self.assertEqual(check["degradation_percent"], 30)

    def test_threshold_boundary_and_improvement(self):
        for value in (80, 100, 120):
            with self.subTest(value=value):
                self.assertEqual(self.check(record(value=value))["status"], "ok")

    def test_rolling_median_ignores_outlier_and_older_samples(self):
        self.history[0]["reactions"][0]["records_per_sec"] = 10000
        self.history.extend(record(days_ago, value=10000) for days_ago in range(8, 25))
        check = self.check(record(value=90))
        self.assertEqual(check["baseline"], 100)
        self.assertEqual(len(check["baseline_samples"]), 7)
        self.assertEqual(check["status"], "ok")

    def test_requires_minimum_samples(self):
        self.history = self.history[:4]
        self.assertEqual(self.check()["status"], "skipped")
        self.assertIn("4/5", self.check()["reason"])

    def test_excludes_current_future_stale_manual_failed_runs(self):
        for previous in self.history:
            previous["status"] = "failure"
        self.history.extend([record(), record(-1), record(31)])
        manual = record(8)
        manual["run"]["trigger"] = "workflow_dispatch"
        self.history.append(manual)
        check = self.check()
        self.assertEqual(check["status"], "skipped")
        self.assertEqual(check["baseline_samples"], [])

    def test_duplicate_runs_count_once_and_latest_attempt_wins(self):
        previous = record(1)
        self.history = [copy.deepcopy(previous) for _ in range(7)]
        self.assertEqual(len(self.check()["baseline_samples"]), 1)
        self.history = [record(days_ago) for days_ago in range(1, 6)]
        previous["run"]["run_attempt"] = 2
        previous["status"] = "failure"
        self.history.append(previous)
        self.assertEqual(self.check()["status"], "skipped")

    def test_series_isolate_workload_and_hardware(self):
        changes = [
            ("run", "runner", "azure-ephemeral-Standard_D4s_v6-Premium_LRS-128gb"),
            ("run", "workflow", "building-comfort-azure.yml"),
            ("dimensions", "variant", "drasi_server_http"),
            ("dimensions", "scenario", "stock_market"),
            ("dimensions", "target", "drasi_server"),
        ]
        for section, key, value in changes:
            with self.subTest(key=key):
                current = record(value=70)
                current[section][key] = value
                self.assertEqual(self.check(current)["status"], "skipped")
        for current in (record(params={"bootstrap_size": "1m"}), record(schema_version=2)):
            self.assertEqual(self.check(current)["status"], "skipped")
        current = record()
        current["reactions"][0]["records"] = 2000
        self.assertEqual(self.check(current)["status"], "skipped")
        current = record()
        current["reactions"].append({"reaction_id": "another", "records": 1000})
        self.assertEqual(self.check(current)["status"], "skipped")

    def test_azure_profiles_are_independent(self):
        for previous in self.history:
            previous["run"]["runner"] = "azure-ephemeral-Standard_D4s_v6-Premium_LRS-128gb"
        current = record(value=70)
        current["run"]["runner"] = self.history[0]["run"]["runner"]
        self.assertEqual(self.check(current)["status"], "regression")
        current["run"]["runner"] = "azure-ephemeral-Standard_D4s_v6-Premium_LRS-256gb"
        self.assertEqual(self.check(current)["status"], "skipped")

    def test_sha_failure_is_not_an_alert_policy(self):
        self.assertEqual(self.check(record(determinism="fail"))["status"], "ok")
        for previous in self.history:
            previous["determinism"] = "fail"
        current = record(value=70, determinism="fail")
        current["reactions"][0]["determinism"] = "fail"
        self.assertEqual(self.check(current)["status"], "regression")

    def test_missing_invalid_metrics_are_not_zero(self):
        for value in (None, True, "70", -1, float("nan"), float("inf")):
            with self.subTest(value=value):
                self.assertEqual(self.check(record(value=value))["status"], "skipped")
        self.assertEqual(self.check(record(value=0))["status"], "regression")

    def test_missing_historical_metrics_are_skipped(self):
        self.history[0]["reactions"][0].pop("records_per_sec")
        self.assertEqual(len(self.check()["baseline_samples"]), 6)

    def test_missing_runner_and_unsuccessful_current_are_skipped(self):
        current = record(value=70)
        del current["run"]["runner"]
        self.assertEqual(self.check(current)["status"], "skipped")
        for status in ("failure", "timeout", "cancelled"):
            self.assertEqual(self.check(record(status=status))["status"], "skipped")

    def test_zero_baseline_has_no_percentage(self):
        self.history = [record(days_ago, value=0) for days_ago in range(1, 8)]
        self.assertEqual(self.check()["status"], "skipped")

    def test_future_lower_is_better_nested_metric(self):
        self.policy["metrics"] = [{"name": "p95_latency", "scope": "reactions", "path": "latency.p95_ms",
                                   "direction": "lower_is_better", "threshold_percent": 20}]
        for previous in self.history:
            previous["reactions"][0]["latency"] = {"p95_ms": 10}
        current = record()
        current["reactions"][0]["latency"] = {"p95_ms": 15}
        self.assertEqual(self.check(current)["status"], "regression")
        current["reactions"][0]["latency"]["p95_ms"] = 5
        self.assertEqual(self.check(current)["status"], "ok")

    def test_totals_scope_and_workload_override(self):
        metric = self.policy["metrics"][0]
        metric["scope"] = "totals"
        metric["overrides"] = [{"match": {"scenario": "building_comfort", "runner": "ubuntu-latest"},
                                "threshold_percent": 40}]
        check = self.check()
        self.assertEqual(check["subject"], "totals")
        self.assertEqual(check["status"], "ok")
        metric["overrides"][0]["match"]["runner"] = "other"
        self.assertEqual(self.check()["status"], "regression")

    def test_new_binary_version_still_compares(self):
        self.assertEqual(self.check(record(value=70, versions={"drasi_server_version": "new"}))["status"],
                         "regression")

    def test_policy_validation(self):
        self.policy["baseline"]["min_samples"] = 8
        with self.assertRaises(ValueError):
            self.check()

    def test_report_is_serializable_and_has_evidence(self):
        report = CHECKER.compare([record(value=70)], self.history, self.policy)
        json.dumps(report, allow_nan=False)
        self.assertEqual(len(report["regressions"]), 1)
        self.assertIn("median 100.00", CHECKER.markdown(report))

    def test_new_metric_warms_up_independently(self):
        self.policy["metrics"].append({"name": "memory", "scope": "totals", "path": "memory_mb",
                                       "direction": "lower_is_better", "threshold_percent": 20})
        current = record(value=70)
        current["totals"]["memory_mb"] = 200
        report = CHECKER.compare([current], self.history, self.policy)
        self.assertEqual([check["status"] for check in report["checks"]], ["regression", "skipped"])
        self.assertEqual(len(report["regressions"]), 1)

    def test_cli_reads_nested_history_excludes_self_and_exits_successfully_on_breach(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = root / "summaries"
            history = root / "results" / "ubuntu-latest" / "2026" / "09" / "22"
            current.mkdir()
            history.mkdir(parents=True)
            new_record = record(value=70)
            (current / "summary.json").write_text(json.dumps(new_record))
            for previous in [*self.history, new_record]:
                (history / f"{previous['run']['run_id']}.json").write_text(json.dumps(previous))
            output = root / "report.json"
            rendered = root / "report.md"
            result = subprocess.run([
                sys.executable, str(SCRIPT), "--current", str(current),
                "--history", str(root / "results"), "--config", str(SCRIPT.parent.parent / "performance-regression.json"),
                "--output", str(output), "--markdown", str(rendered),
            ], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(output.read_text())
            check = report["regressions"][0]
            self.assertEqual(len(check["baseline_samples"]), 7)
            self.assertNotIn("100", [sample["run"]["run_id"] for sample in check["baseline_samples"]])
            self.assertIn("Detected 1 regression(s)", rendered.read_text())


if __name__ == "__main__":
    unittest.main()