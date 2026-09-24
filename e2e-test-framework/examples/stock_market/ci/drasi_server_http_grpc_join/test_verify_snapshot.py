import argparse
import json
from pathlib import Path
import tempfile
import unittest
import os
import re
import subprocess
from unittest.mock import patch

import verify_snapshot as checker


def node(identifier, label, **properties):
    return {"id": identifier, "labels": [label], "properties": properties}


def event(source, sequence, operation, before, after):
    return {"op": operation, "payload": {"source": {"db": source, "table": "node", "lsn": sequence},
                                         "before": before, "after": after}}


class SnapshotTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.stock = node("MSFT", "Stock", symbol="MSFT", name="Microsoft", price=10.25, volume=100)
        updated = dict(self.stock, properties=dict(self.stock["properties"], price=11.5))
        self.stock_events = [event("stock-trades-db", 0, "i", None, self.stock),
                             event("stock-trades-db", 1, "u", self.stock, updated)]
        watch = node("watch-1", "WatchlistItem", symbol="MSFT")
        duplicate = dict(watch, id="watch-2")
        removed = dict(watch, id="watch-3")
        self.watch_events = [event("watchlist-db", 1, "i", None, watch),
                             event("watchlist-db", 2, "i", None, duplicate),
                             event("watchlist-db", 3, "i", None, removed),
                             event("watchlist-db", 4, "d", removed, None)]
        for source_id, events in (("stock-trades-db", self.stock_events), ("watchlist-db", self.watch_events)):
            folder = self.root / "run/sources" / source_id / "source_change_log"
            folder.mkdir(parents=True)
            (folder / "timestamp_00000.jsonl").write_text("".join(json.dumps(value) + "\n" for value in events))
        scripts = self.root / "repo/stock_market/sources/watchlist-db/source_change_scripts"
        scripts.mkdir(parents=True)
        (scripts / "source_change_scripts_00000.jsonl").write_text(
            json.dumps({"kind": "Header"}) + "\n" + "".join(json.dumps({
                "kind": "SourceChange", "source_change_event": value}) + "\n" for value in self.watch_events) +
            json.dumps({"kind": "Finish"}) + "\n")
        self.config = {"data_store": {"test_repos": [{"source_path": str(self.root / "repo"), "local_tests": [{
            "test_folder": "stock_market", "sources": [
                {"test_source_id": "stock-trades-db", "model_data_generator": {
                    "kind": "StockTrade", "change_count": 2, "stock_definitions": [{"symbol": "MSFT"}]}},
                {"test_source_id": "watchlist-db", "source_change_generator": {"script_file_folder": "source_change_scripts"}}
            ]}]}]}}

    def server(self):
        return {"queries": [{"id": checker.QUERY_ID, "query": checker.QUERY,
                             "sources": [{"sourceId": name} for name in checker.SOURCE_IDS],
                             "joins": [{"id": "WATCHES", "keys": [{"label": "WatchlistItem", "property": "symbol"},
                                                                    {"label": "Stock", "property": "symbol"}]}]}]}

    def test_changed_query_or_join_fails_closed(self):
        server = self.server()
        checker.validate_query(server)
        server["queries"][0]["query"] += " LIMIT 1"
        with self.assertRaisesRegex(ValueError, "Query changed"):
            checker.validate_query(server)
        server = self.server()
        server["queries"][0]["joins"] = []
        with self.assertRaisesRegex(ValueError, "join contract"):
            checker.validate_query(server)

    def test_committed_server_query_loads_through_yaml_parser(self):
        server = checker.load_server(Path(__file__).parent / "drasi_server_config.yaml")
        checker.validate_query(server)

    def test_both_variants_accept_all_persistence_profiles_without_changing_oracle(self):
        script = Path(__file__).parent / "run_test_ci.sh"
        original = json.loads((script.parent / "config.json").read_text())
        for variant in ("drasi_server_http_grpc_join", "drasi_server_http_grpc_join_adaptive"):
            for persist_index, state_store in ((False, False), (True, False), (False, True), (True, True)):
                work = self.root / f"{variant}-{persist_index}-{state_store}"
                env = dict(os.environ, WORK_DIR=str(work), ARTIFACTS_DIR=str(work / "artifacts"),
                           RENDER_CONFIG_ONLY="true", VARIANT=variant, BATCHING_SPEED="10000",
                           PERSIST_INDEX=str(persist_index).lower(), STATE_STORE=str(state_store).lower(),
                           WORKLOAD_SIZE="100000", DRASI_PLUGIN_TAG="", DRASI_PLUGIN_REGISTRY="")
                result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                server = checker.load_server(work / "drasi_server_config.ci.yaml")
                checker.validate_query(server)
                self.assertEqual(persist_index, server["persistIndex"])
                self.assertEqual(state_store, "stateStore" in server)
                for source in server["sources"]:
                    self.assertEqual(persist_index, "durability" in source)
                config = json.loads((work / "config.ci.json").read_text())
                test = config["data_store"]["test_repos"][0]["local_tests"][0]
                self.assertEqual([], test["reactions"][0]["stop_triggers"])
                self.assertEqual(original["data_store"]["test_repos"][0]["local_tests"][0]["reactions"][0]["output_handler"],
                                 test["reactions"][0]["output_handler"])
                metrics = next(logger for logger in config["test_run_host"]["test_runs"][0]["reactions"][0]["output_loggers"]
                               if logger["kind"] == "PerformanceMetrics")
                self.assertEqual(75000, metrics["measurement_record_count"])
                self.assertFalse(config["test_run_host"]["test_runs"][0]["reactions"][0]["start_immediately"])
                self.assertTrue(all(source["start_mode"] == "manual" for source in config["test_run_host"]["test_runs"][0]["sources"]))

    def test_explicit_startup_opens_receiver_before_sources_and_fails_on_error(self):
        script = (Path(__file__).parent / "run_test_ci.sh").read_text()
        function = re.search(r"^start_test_inputs\(\) \{\n.*?^\}\n", script, re.MULTILINE | re.DOTALL)[0]
        calls = self.root / "start-calls"
        stubs = """
set -euo pipefail
TEST_SERVICE_PORT=63123
TEST_RUN_ID=repo.test.run
curl() { printf '%s\\n' "${*: -1}" >> "$CALLS"; [[ "$*" != *"$FAIL_AT"* ]]; }
"""
        for fail_at, count, code in (("never", 3, 0), ("reactions/", 1, 1), ("stock-trades-db/", 2, 1)):
            calls.write_text("")
            result = subprocess.run(["bash"], input=stubs + function + "\nstart_test_inputs\n",
                                    text=True, capture_output=True, env=dict(os.environ, CALLS=str(calls), FAIL_AT=fail_at))
            self.assertEqual(code, result.returncode, result.stderr)
            urls = calls.read_text().splitlines()
            self.assertEqual(count, len(urls))
            self.assertTrue(urls[0].endswith("reactions/watchlist-prices/start"))

    def test_runner_exit_code_requires_snapshot_gate(self):
        script = (Path(__file__).parent / "run_test_ci.sh").read_text()
        main = script[script.index("\npoll_rc=0\n"):]
        stubs = """
set -euo pipefail
TEST_REACTION_IDS=watchlist-prices
fetch_final_reaction_state() { return 0; }
verify_final_snapshot() { printf 'snapshot checked\\n'; return "$CHECK_RC"; }
finish_test_run() { :; }
print_summary() { :; }
verify_test_run_status() { :; }
write_step_summary() { :; }
"""
        for exit_code in (0, 1):
            result = subprocess.run(["bash"], input=stubs + main, text=True, capture_output=True,
                                    env=dict(os.environ, CHECK_RC=str(exit_code)))
            self.assertEqual(exit_code, result.returncode, result.stderr)
            self.assertIn("snapshot checked", result.stdout)

    def test_stopped_observer_requires_finalized_metrics(self):
        script = (Path(__file__).parent / "run_test_ci.sh").read_text()
        function = re.search(r"^fetch_final_reaction_state\(\) \{\n.*?^\}\n", script, re.MULTILINE | re.DOTALL)[0]
        metrics = {"logger_name": "PerformanceMetrics", "has_output": True, "summary": {"record_count": 75000}}
        for loggers, expected in (([metrics], 0), ([], 1), ([metrics, metrics], 1),
                                 ([dict(metrics, summary={"record_count": 74999})], 1)):
            body = json.dumps({"reaction_observer": {"status": "Stopped", "logger_results": loggers}})
            result = subprocess.run(["bash"], text=True, capture_output=True,
                                    env=dict(os.environ, BODY=body, ARTIFACTS_DIR=str(self.root)),
                                    input="set -euo pipefail\nTEST_SERVICE_PORT=63123\nTEST_RUN_ID=repo.test.run\nREACTION_RECORD_COUNT=75000\n"
                                    "curl() { printf '%s' \"$BODY\"; }\nlog() { :; }\n" + function + "\nfetch_final_reaction_state watchlist-prices\n")
            self.assertEqual(expected, result.returncode, result.stderr)

    def test_wait_for_drain_does_not_poll_busy_sources_and_fails_on_timeout(self):
        logfile = self.root / "service.log"
        marker = "Source dispatchers drained for TestRunSource repo.test.run."
        args = argparse.Namespace(service_log=logfile, run_id="repo.test.run", service_url="http://service")
        logfile.write_text(marker + checker.SOURCE_IDS[0] + "\n")
        def finish_other_source(_seconds):
            with logfile.open("a") as stream:
                stream.write(marker + checker.SOURCE_IDS[1] + "\n")
        with patch.object(checker.time, "monotonic", side_effect=[0, 1]), \
             patch.object(checker.time, "sleep", side_effect=finish_other_source) as sleeping, \
             patch.object(checker, "validate_drain") as validate:
            checker.wait_for_drain(args, 2)
            sleeping.assert_called_once()
            validate.assert_called_once()
        logfile.write_text(marker + checker.SOURCE_IDS[0])
        with patch.object(checker.time, "monotonic", side_effect=[0, 3]), \
             patch.object(checker.time, "sleep"), patch.object(checker, "request") as request:
            with self.assertRaisesRegex(ValueError, "successful drain of both sources"):
                checker.wait_for_drain(args, 2)
            request.assert_not_called()

    def test_measurement_target_and_receiver_health_are_required(self):
        config = {"test_run_host": {"test_runs": [{"reactions": [{"test_reaction_id": "watchlist-prices", "output_loggers": [
            {"kind": "PerformanceMetrics", "measurement_record_count": 75000}]}]}]}}
        args = argparse.Namespace(service_url="http://service", run_id="repo.test.run")
        for count, expected in ((74999, False), (75000, True), (80000, True)):
            state = {"reaction_observer": {"status": "Running", "result_summary": {"reaction_invocation_count": count}}}
            with patch.object(checker, "request", return_value=state):
                self.assertEqual(expected, checker.measurement_received(config, args))
        with patch.object(checker, "request", return_value={"reaction_observer": {"status": "Error"}}):
            with self.assertRaisesRegex(ValueError, "remain running"):
                checker.measurement_received(config, args)
        with patch.object(checker.os, "kill", side_effect=ProcessLookupError):
            with self.assertRaisesRegex(ValueError, "exited"):
                checker.check_processes(argparse.Namespace(server_pid=123))

    def test_drain_requires_both_sources_and_rejects_source_or_run_errors(self):
        marker = "Source dispatchers drained for TestRunSource repo.test.run."
        log = "\n".join(marker + source_id for source_id in checker.SOURCE_IDS)
        finished = {"source_change_generator": {"status": "Finished"}}
        with patch.object(checker, "request", side_effect=[finished, finished, {"status": "Stopped"}]):
            checker.validate_drain(log, "repo.test.run", "http://service")
        for invalid in (marker + checker.SOURCE_IDS[0], log.replace("repo.test.run", "other.test.run"),
                        log + "\nSource dispatcher drain failed for TestRunSource repo.test.run.watchlist-db"):
            with patch.object(checker, "request", return_value=finished), self.assertRaises(ValueError):
                checker.validate_drain(invalid, "repo.test.run", "http://service")
        for states in ([{"source_change_generator": {"status": "Error"}}],
                       [finished, finished, {"status": "Error: source failure"}]):
            with patch.object(checker, "request", side_effect=states), self.assertRaises(ValueError):
                checker.validate_drain(log, "repo.test.run", "http://service")

    def test_live_gate_waits_for_snapshot_and_saves_differences_on_failure(self):
        config = self.root / "config.json"
        config.write_text(json.dumps({"data_store": {"data_store_path": str(self.root)}}))
        logfile = self.root / "test-service.log"
        logfile.write_text("")
        expected = {"snapshot": [{"Price": 1.25}], "sources": {}}
        args = argparse.Namespace(config=config, server_config=self.root / "server.yaml", service_log=logfile,
                                  run_id="repo.test.run", service_url="http://service", admin_url="http://server",
                                  artifacts=self.root / "artifacts", timeout=3)
        for matches in (False, True):
            with patch.object(checker.subprocess, "check_output", return_value=json.dumps(self.server())), \
                 patch.object(checker, "wait_for_drain"), \
                 patch.object(checker, "measurement_received", return_value=True), \
                 patch.object(checker, "expected_snapshot", return_value=expected), \
                 patch.object(checker.time, "monotonic", side_effect=[0, 0, 1, 4]), \
                 patch.object(checker.time, "sleep"), \
                 patch.object(checker, "request", side_effect=[{"success": True, "data": []},
                     {"success": True, "data": expected["snapshot"] if matches else [{"Price": 2}]}]), \
                 patch("builtins.print"):
                self.assertEqual(0 if matches else 1, checker.verify(args))
            verdict = json.loads((args.artifacts / "snapshot_verdict.json").read_text())
            self.assertEqual(matches, verdict["passed"])
            self.assertEqual("not_verified", verdict["delivery"])
            if not matches:
                self.assertEqual([{"row": {"Price": 1.25}, "count": 1}], verdict["missing"])
                self.assertEqual([{"row": {"Price": 2}, "count": 1}], verdict["unexpected"])

    def test_invalid_api_response_fails_and_writes_verdict(self):
        config = self.root / "config.json"
        config.write_text(json.dumps({"data_store": {"data_store_path": str(self.root)}}))
        logfile = self.root / "test-service.log"
        logfile.write_text("")
        args = argparse.Namespace(config=config, server_config=self.root / "server.yaml", service_log=logfile,
                                  run_id="repo.test.run", service_url="http://service", admin_url="http://server",
                                  artifacts=self.root / "artifacts", timeout=1)
        with patch.object(checker.subprocess, "check_output", return_value=json.dumps(self.server())), \
             patch.object(checker, "wait_for_drain"), \
             patch.object(checker, "expected_snapshot", return_value={"snapshot": [{"Price": 1}], "sources": {}}), \
             patch.object(checker, "request", return_value={"success": False, "data": []}), patch("builtins.print"):
            self.assertEqual(1, checker.verify(args))
        self.assertFalse(json.loads((args.artifacts / "snapshot_verdict.json").read_text())["passed"])
        self.assertIn("success:true", json.loads((args.artifacts / "snapshot_verdict.json").read_text())["error"])

    def test_input_replay_preserves_duplicate_join_rows_and_deletes(self):
        expected = checker.expected_snapshot(self.config, self.root / "run")
        self.assertEqual([{"Symbol": "MSFT", "Name": "Microsoft", "Price": 11.5, "Volume": 100}] * 2,
                         expected["snapshot"])
        self.assertEqual(2, expected["sources"]["stock-trades-db"]["events"])
        self.assertEqual(4, expected["sources"]["watchlist-db"]["events"])

    def test_comparison_ignores_order_but_checks_values_and_multiplicity(self):
        rows = [{"Price": 10}, {"Price": 11}]
        self.assertTrue(checker.compare_rows(list(reversed(rows)), rows)["passed"])
        for actual in (rows[:1], rows * 2, [{"Price": 12}, {"Price": 11}], [{"Price": 10, "extra": 1}, rows[1]]):
            self.assertFalse(checker.compare_rows(actual, rows)["passed"])
        with self.assertRaises(ValueError):
            checker.compare_rows(None, rows)

    def test_missing_truncated_reordered_and_wrong_source_inputs_fail(self):
        folder = self.root / "run/sources/stock-trades-db/source_change_log"
        filename = folder / "timestamp_00000.jsonl"
        for events in (self.stock_events[:1], list(reversed(self.stock_events)), self.watch_events):
            filename.write_text("".join(json.dumps(value) + "\n" for value in events))
            with self.assertRaises(ValueError):
                checker.expected_snapshot(self.config, self.root / "run")
        filename.unlink()
        with self.assertRaises(ValueError):
            checker.expected_snapshot(self.config, self.root / "run")

    def test_missing_chunks_and_modified_watchlist_fail(self):
        folder = self.root / "run/sources/watchlist-db/source_change_log"
        filename = folder / "timestamp_00000.jsonl"
        filename.rename(folder / "timestamp_00001.jsonl")
        with self.assertRaisesRegex(ValueError, "chunks"):
            checker.expected_snapshot(self.config, self.root / "run")
        (folder / "timestamp_00001.jsonl").rename(filename)
        changed = json.loads(json.dumps(self.watch_events))
        changed[0]["payload"]["after"]["properties"]["symbol"] = "OTHER"
        filename.write_text("".join(json.dumps(value) + "\n" for value in changed))
        with self.assertRaisesRegex(ValueError, "differs from its script"):
            checker.expected_snapshot(self.config, self.root / "run")


if __name__ == "__main__":
    unittest.main()