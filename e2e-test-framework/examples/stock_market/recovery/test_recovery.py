import argparse
import copy
import json
from pathlib import Path
import signal
import socket
import tempfile
import unittest
from unittest.mock import Mock, patch

import run_recovery as runner
from workload import RUN_ID, SOURCE_IDS


class RecoveryTest(unittest.TestCase):
    def test_failed_restart_reports_server_error_and_preserves_failed_verdict(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(mode="sigkill", variant="adaptive", admin_port=8091, service_port=63124,
                                      timeout=1)
            recovery = runner.Recovery(args, Path(directory))
            process = Mock(pid=7520)
            process.poll.return_value = 1
            recovery.processes = [process]
            logfile = Path(directory) / "drasi-server.log"
            detail = ("failed to deserialize durable outbox entry at sequence 2: "
                      "invalid type: integer `712048398700485272`, expected a sequence. "
                      "Refusing to start under Strict recovery policy.")
            logfile.write_text("older startup output\n" * 2000 + detail + "\n")
            recovery.process_logs[process] = logfile
            recovery.crash_injected = True
            probe = Mock()
            recovery.execute = lambda: recovery.wait("Drasi Server health", probe)
            with patch("builtins.print"):
                self.assertEqual(1, recovery.run())
            probe.assert_not_called()
            verdict = json.loads((Path(directory) / "verdict.json").read_text())
            self.assertFalse(verdict["passed"])
            self.assertTrue(verdict["crash_injected"])
            self.assertIn(detail, verdict["error"])
            self.assertIn("drasi-server (pid=7520)", verdict["error"])
            self.assertLess(len(verdict["error"]), 9000)
            self.assertEqual({"pid": 7520, "exit_code": 1, "log": "drasi-server.log",
                              "waiting_for": "Drasi Server health"}, verdict["failed_process"])

    def test_missing_process_log_does_not_hide_exit_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(mode="clean", variant="standard", admin_port=8091, service_port=63124)
            recovery = runner.Recovery(args, Path(directory))
            process = Mock(pid=456)
            process.poll.return_value = 101
            recovery.processes = [process]
            recovery.process_logs[process] = Path(directory) / "test-service.log"
            with self.assertRaisesRegex(RuntimeError, r"test-service \(pid=456\) exited with 101") as error:
                recovery.check_processes("test source APIs")
            self.assertIn("Could not read test-service.log", str(error.exception))
            process.poll.return_value = None
            recovery.check_processes("test source APIs")

    def test_control_request_supports_bounded_flush_timeout(self):
        with patch.object(runner.urllib.request, "urlopen") as urlopen:
            urlopen.return_value.__enter__.return_value.read.return_value = b"{}"
            runner.request("http://127.0.0.1/stop", "POST", timeout=60)
            self.assertEqual(60, urlopen.call_args.kwargs["timeout"])
            self.assertEqual("POST", urlopen.call_args.args[0].method)

    def test_drain_does_not_poll_busy_source_actors(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(mode="sigkill", variant="adaptive", admin_port=8091, service_port=63124)
            recovery = runner.Recovery(args, Path(directory))
            recovery.source_states = Mock(return_value={
                source_id: {"source_change_generator": {"status": "Finished"}} for source_id in SOURCE_IDS
            })
            logfile = Path(directory) / "test-service.log"
            logfile.write_text(f"Source dispatchers drained for TestRunSource {RUN_ID}.{SOURCE_IDS[0]}\n")
            self.assertFalse(recovery.drained())
            recovery.source_states.assert_not_called()
            with logfile.open("a") as stream:
                stream.write(f"Source dispatchers drained for TestRunSource {RUN_ID}.{SOURCE_IDS[1]}\n")
            self.assertTrue(recovery.drained())
            recovery.source_states.assert_called_once()

    def test_port_preflight_rejects_live_listener_and_duplicates(self):
        with self.assertRaisesRegex(RuntimeError, "distinct"):
            runner.check_ports([12345, 12345])
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            with self.assertRaises(OSError):
                runner.check_ports([listener.getsockname()[1]])

    def test_drain_requires_both_exact_source_ids_and_no_failure(self):
        marker = f"Source dispatchers drained for TestRunSource {RUN_ID}."
        self.assertFalse(runner.sources_drained("Script Finished"))
        self.assertFalse(runner.sources_drained(marker + SOURCE_IDS[0]))
        self.assertFalse(runner.sources_drained(marker + SOURCE_IDS[0] + "\n" + marker + SOURCE_IDS[1] + "-other"))
        complete = "\n".join(marker + source_id for source_id in SOURCE_IDS)
        self.assertTrue(runner.sources_drained(complete))
        with self.assertRaises(RuntimeError):
            runner.sources_drained(complete + f"\nSource dispatcher drain failed for TestRunSource {RUN_ID}.watchlist-db: send failed")

    def test_snapshot_is_full_row_multiset(self):
        self.assertTrue(runner.rows_equal([{"price": 1}, {"price": 2}], [{"price": 2}, {"price": 1}]))
        self.assertFalse(runner.rows_equal([{"price": 1}] * 2, [{"price": 1}]))
        self.assertFalse(runner.rows_equal([{"price": 1}], [{"price": 2}]))
        self.assertFalse(runner.rows_equal([{"price": 1, "extra": True}], [{"price": 1}]))
        with self.assertRaises(RuntimeError):
            runner.rows_equal(None, [])

    def test_report_requires_join_state_pass_even_when_overall_is_inconclusive(self):
        report = {"schema_version": 1, "missing_queries": [], "unexpected_queries": [],
                  "verdict": "inconclusive", "queries": [{"query_id": "watchlist-prices",
                  "state": {"verdict": "passed", "missing": [], "unexpected": []}}]}
        runner.verify_report(report)
        for state in ({}, {"verdict": "inconclusive", "missing": [], "unexpected": []},
                      {"verdict": "passed", "missing": [{"row": {}, "count": 1}], "unexpected": []}):
            invalid = copy.deepcopy(report)
            invalid["queries"][0]["state"] = state
            with self.assertRaises(RuntimeError):
                runner.verify_report(invalid)
        for invalid in ({}, dict(report, queries=[]), dict(report, missing_queries=["other"])):
            with self.assertRaises(RuntimeError):
                runner.verify_report(invalid)

    def test_crash_rejects_missed_window_and_requires_sigkill_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(mode="sigkill", variant="standard", admin_port=8091, service_port=63124)
            recovery = runner.Recovery(args, Path(directory))
            recovery.baseline = {"queries": [{"snapshot": [{"revision": 2}]}]}
            recovery.snapshot = Mock(return_value={"success": True, "data": [{"revision": 2}]})
            recovery.server = Mock(pid=123)
            recovery.server.poll.return_value = None
            recovery.server.wait.return_value = -signal.SIGKILL
            recovery.processes = [recovery.server]
            recovery.start_server = Mock()
            with self.assertRaisesRegex(RuntimeError, "window missed"):
                recovery.crash(Path("server"))
            recovery.server.kill.assert_not_called()
            recovery.snapshot.return_value = {"success": True, "data": [{"revision": 1}]}
            recovery.server.wait.return_value = 0
            with self.assertRaisesRegex(RuntimeError, "did not exit from SIGKILL"):
                recovery.crash(Path("server"))
            self.assertFalse(recovery.crash_injected)
            recovery.start_server.assert_not_called()
            recovery.server.wait.return_value = -signal.SIGKILL
            recovery.crash(Path("server"))
            self.assertTrue(recovery.crash_injected)
            recovery.start_server.assert_called_once_with(Path("server"))
            self.assertEqual("SIGKILL", json.loads((Path(directory) / "crash.json").read_text())["signal"])


if __name__ == "__main__":
    unittest.main()