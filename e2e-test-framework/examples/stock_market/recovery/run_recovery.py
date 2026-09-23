#!/usr/bin/env python3
"""Run a fixed stock-market join through a persisted SIGKILL restart."""

import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

from workload import QUERY_ID, RUN_ID, SOURCE_IDS, prepare, write_json


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def request(url, method="GET", timeout=5):
    with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=timeout) as response:
        body = response.read()
        return json.loads(body) if body else None


def rows_equal(actual, expected):
    require(isinstance(actual, list), "Query snapshot must be an array")
    def canonical(rows):
        return collections.Counter(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in rows)
    return canonical(actual) == canonical(expected)


def sources_drained(log):
    failure = f"Source dispatcher drain failed for TestRunSource {RUN_ID}."
    require(failure not in log, "Source dispatcher drain failed; refusing SIGKILL")
    return all(re.search(r"Source dispatchers drained for TestRunSource " +
                         re.escape(f"{RUN_ID}.{source_id}") + r"\s*$", log, re.MULTILINE)
               for source_id in SOURCE_IDS)


def verify_report(report):
    require(report.get("schema_version") == 1, "Invalid comparison schema")
    require(report.get("missing_queries") == [] and report.get("unexpected_queries") == [],
            "Comparison is missing queries or contains unexpected queries")
    queries = report.get("queries", [])
    require(len(queries) == 1 and queries[0].get("query_id") == QUERY_ID, "Missing join query comparison")
    state = queries[0].get("state", {})
    require(state.get("verdict") == "passed" and state.get("missing") == [] and state.get("unexpected") == [],
            "Golden snapshot comparison failed or is inconclusive")


def binary(value):
    resolved = Path(value).expanduser().resolve()
    require(resolved.is_file() and os.access(resolved, os.X_OK), f"Executable not found: {resolved}")
    return resolved


def binary_hash(filename):
    digest = hashlib.sha256()
    with filename.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(filename), "sha256": digest.hexdigest()}


def check_ports(ports):
    require(len(ports) == len(set(ports)), "Recovery ports must be distinct")
    for port in ports:
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", port))


class Recovery:
    def __init__(self, args, work):
        self.args, self.work = args, work
        self.processes, self.logs = [], []
        self.server = None
        self.crash_injected = False
        self.admin = f"http://127.0.0.1:{args.admin_port}"
        self.service = f"http://127.0.0.1:{args.service_port}/api/test_runs/{RUN_ID}"
        self.verdict = {"passed": False, "scope": "golden-query-snapshot", "mode": args.mode,
                        "variant": args.variant, "crash_injected": False, "delivery": "not_verified"}

    def launch(self, command, filename, log_filter):
        stream = (self.work / filename).open("ab")
        self.logs.append(stream)
        process = subprocess.Popen(command, cwd=self.work, env=dict(os.environ, RUST_LOG=log_filter),
                                   stdout=stream, stderr=subprocess.STDOUT)
        self.processes.append(process)
        return process

    def wait(self, description, probe):
        deadline = time.monotonic() + self.args.timeout
        last_error = None
        while time.monotonic() < deadline:
            for process in self.processes:
                require(process.poll() is None, f"Process {process.pid} exited with {process.returncode}")
            try:
                result = probe()
                if result:
                    return result
            except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                last_error = str(error)
            time.sleep(0.1)
        raise RuntimeError(f"Timed out waiting for {description}; last network error: {last_error}")

    def start_server(self, executable):
        self.server = self.launch([str(executable), "--config", str(self.work / "server.yaml")],
                                  "drasi-server.log", os.environ.get("DRASI_RUST_LOG", "info"))
        self.wait("Drasi Server health", lambda: request(self.admin + "/health"))

    def source_states(self):
        states = {source_id: request(self.service + f"/sources/{source_id}") for source_id in SOURCE_IDS}
        for source_id, source in states.items():
            generator = source["source_change_generator"]
            require(generator["status"] not in ("Error", "Stopped"), f"Source {source_id} failed: {generator}")
        return states

    def drained(self):
        if not sources_drained((self.work / "test-service.log").read_text(errors="replace")):
            return False
        states = self.source_states()
        if not all(source["source_change_generator"]["status"] == "Finished" for source in states.values()):
            return False
        write_json(self.work / "sources-drained.json", states)
        return True

    def snapshot(self):
        response = request(self.admin + f"/api/v1/queries/{QUERY_ID}/results")
        require(response.get("success") is True and isinstance(response.get("data"), list),
                f"Invalid query snapshot: {response}")
        return response

    def terminal_snapshot(self):
        self.source_states()
        response = self.snapshot()
        write_json(self.work / "snapshot-latest.json", response)
        return response if rows_equal(response["data"], self.baseline["queries"][0]["snapshot"]) else None

    def crash(self, server_binary):
        before = self.snapshot()
        write_json(self.work / "snapshot-before-crash.json", before)
        require(not rows_equal(before["data"], self.baseline["queries"][0]["snapshot"]),
                "Drain window missed: query already reached the terminal snapshot before SIGKILL")
        require(self.server.poll() is None, "Server exited before SIGKILL")
        started = time.monotonic()
        old_pid = self.server.pid
        print(f"SIGKILL server pid={old_pid}; both sources drained", flush=True)
        self.server.kill()
        require(self.server.wait(timeout=10) == -signal.SIGKILL, "Server did not exit from SIGKILL")
        self.processes.remove(self.server)
        self.crash_injected = True
        write_json(self.work / "crash.json", {"pid": old_pid, "signal": "SIGKILL", "sources_drained": list(SOURCE_IDS)})
        self.start_server(server_binary)
        self.verdict["restart_health_seconds"] = round(time.monotonic() - started, 3)
        self.verdict["restarted_pid"] = self.server.pid

    def compare_snapshot(self, comparator, response):
        actual = copy.deepcopy(self.baseline)
        actual["capture"] = {"complete": False,
                             "evidence": "Both sources drained and terminal revision rows observed; HTTP delivery completeness and identities are not verified."}
        actual["queries"][0]["snapshot"] = response["data"]
        write_json(self.work / "actual.json", actual)
        result = subprocess.run([str(comparator), str(self.work / "baseline.json"), str(self.work / "actual.json")],
                                text=True, capture_output=True)
        (self.work / "comparison.stderr.log").write_text(result.stderr)
        require(result.returncode in (0, 1, 2), f"Comparator execution failed: {result.stderr}")
        report = json.loads(result.stdout)
        write_json(self.work / "recovery_verdict.json", report)
        verify_report(report)

    def execute(self):
        args = self.args
        service_binary = binary(args.test_service_bin)
        server_binary = binary(args.drasi_server_bin)
        comparator = binary(args.recovery_compare_bin)
        ports = [args.admin_port, args.service_port, args.http_port, args.grpc_port, args.reaction_port]
        check_ports(ports)
        write_json(self.work / "binaries.json", {"server": binary_hash(server_binary),
                                               "test_service": binary_hash(service_binary),
                                               "comparator": binary_hash(comparator)})
        self.baseline = prepare(self.work, args)
        self.start_server(server_binary)
        self.launch([str(service_binary), "--config", str(self.work / "config.json"), "--port", str(args.service_port)],
                    "test-service.log", os.environ.get("TEST_SERVICE_RUST_LOG", "info"))
        self.wait("test source APIs", self.source_states)
        request(self.service + f"/reactions/{QUERY_ID}/start", "POST")
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda source_id: request(self.service + f"/sources/{source_id}/start", "POST"), SOURCE_IDS))
        self.wait("both successful dispatcher drains", self.drained)
        if args.mode == "sigkill":
            self.crash(server_binary)
        snapshot = self.wait("golden terminal revision snapshot", self.terminal_snapshot)
        write_json(self.work / "snapshot-final.json", snapshot)
        self.compare_snapshot(comparator, snapshot)
        request(self.service + "/stop", "POST", timeout=args.timeout)
        require(args.mode != "sigkill" or self.crash_injected, "SIGKILL was not injected")
        self.verdict["passed"] = True
        print(f"PASS: {args.mode}/{args.variant} golden query snapshot matched; delivery not verified", flush=True)

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
        for stream in self.logs:
            stream.close()

    def run(self):
        try:
            self.execute()
        except Exception as error:
            self.verdict["error"] = str(error)
            print(f"FAIL: {error}", flush=True)
        finally:
            self.verdict["crash_injected"] = self.crash_injected
            write_json(self.work / "verdict.json", self.verdict)
            self.close()
        return 0 if self.verdict["passed"] else 1


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("clean", "sigkill"), default="sigkill")
    parser.add_argument("--variant", choices=("standard", "adaptive"), default="standard")
    parser.add_argument("--batch-size", type=int, choices=(5000, 10000, 50000), default=10000)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--drasi-server-bin", default=os.environ.get("DRASI_SERVER_BIN", ""))
    parser.add_argument("--test-service-bin", default=os.environ.get("TEST_SERVICE_BIN", ""))
    parser.add_argument("--recovery-compare-bin", default=os.environ.get("RECOVERY_COMPARE_BIN", ""))
    parser.add_argument("--local-plugins", action="store_true",
                        help="Use trusted plugins already installed beside the server, without registry downloads")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--admin-port", type=int, default=8091)
    parser.add_argument("--service-port", type=int, default=63124)
    parser.add_argument("--http-port", type=int, default=9005)
    parser.add_argument("--grpc-port", type=int, default=50061)
    parser.add_argument("--reaction-port", type=int, default=9006)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    return args


def main():
    args = arguments()
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    print(f"Artifacts: {work}", flush=True)
    return Recovery(args, work).run()


if __name__ == "__main__":
    raise SystemExit(main())