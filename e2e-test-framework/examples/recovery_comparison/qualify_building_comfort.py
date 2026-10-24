#!/usr/bin/env python3
# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.

"""Run a completed reference or real Server SIGKILL recovery without settling/count shortcuts."""

import argparse
import copy
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener
import uuid

from completion_boundary import (
    Collector, IncompleteBoundary, INSTANCE, QUERIES, SOURCE,
    query_progress, recorded_output, write_json,
)

HERE = Path(__file__).resolve().parent
DYNAMIC = HERE.parent / "building_comfort" / "dynamic"
HTTP = build_opener(ProxyHandler({}))


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def fingerprint(value):
    return hashlib.sha256(
        (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
    ).hexdigest()


def request(url, body=None, method="GET"):
    data = None if body is None else json.dumps(body).encode()
    headers = {} if data is None else {"Content-Type": "application/json"}
    with HTTP.open(Request(url, data=data, headers=headers, method=method), timeout=10) as response:
        raw = response.read()
        if not raw and method == "POST":
            return None
        value = json.loads(raw)
    if isinstance(value, dict) and value.get("success") is False:
        raise ValueError(f"API rejected {method} {url}: {value}")
    return value


class Child:
    def __init__(self, command, cwd, log):
        self.log = Path(log)
        environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith("OTEL_EXPORTER_OTLP")
            and key.lower() not in ("http_proxy", "https_proxy", "all_proxy")
        }
        environment["RUST_LOG"] = "info,drasi_core=warn,test_run_host::reactions=warn"
        self.started_at_ns = time.time_ns()
        with self.log.open("ab") as output:
            self.process = subprocess.Popen(
                [str(value) for value in command], cwd=cwd, env=environment,
                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
            )
        self.pid = self.process.pid
        self.crashed = False

    def check(self):
        if self.process.poll() is not None:
            raise RuntimeError(f"Owned process {self.pid} exited; inspect {self.log}")

    def crash(self):
        self.check()
        self.process.kill()
        code = self.process.wait(timeout=10)
        if code != -signal.SIGKILL:
            raise RuntimeError(f"Expected SIGKILL exit, got {code}")
        self.crashed = True

    def close(self):
        if self.crashed:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
                raise RuntimeError(f"Owned process {self.pid} did not shut down gracefully")
        if self.process.returncode != 0:
            raise RuntimeError(f"Owned process {self.pid} exited with {self.process.returncode}")


def wait_until(label, operation, children, timeout):
    deadline = time.monotonic() + timeout
    last_report = 0
    reason = "not ready"
    while time.monotonic() < deadline:
        for child in children:
            child.check()
        try:
            return operation()
        except (IncompleteBoundary, URLError, ConnectionError) as error:
            reason = str(error)
        if time.monotonic() - last_report >= 10:
            print(f"{label}: {reason}", flush=True)
            last_report = time.monotonic()
        time.sleep(0.2)
    raise TimeoutError(f"{label} timed out: {reason}")


def ports(count):
    reserved = []
    try:
        for _ in range(count):
            bound = socket.socket()
            bound.bind(("127.0.0.1", 0))
            reserved.append(bound)
        return [bound.getsockname()[1] for bound in reserved]
    finally:
        for bound in reserved:
            bound.close()


def source_finished(log, run_id):
    text = Path(log).read_text(errors="replace")
    source = f"{run_id}.{SOURCE}"
    if f"Source dispatcher drain failed for TestRunSource {source}" in text:
        raise ValueError("Source dispatch failed; not a completed finite input")
    if f"Source dispatchers drained for TestRunSource {source}" not in text:
        raise IncompleteBoundary("Waiting for successful source dispatcher close")
    return True


def recovered_cursors(log, total):
    text = Path(log).read_text(errors="replace")
    cursors = {}
    for query in QUERIES:
        pattern = (
            rf"Query '{re.escape(query)}' subscribing to .* source '{re.escape(SOURCE)}' "
            rf"with WAL replay from seq (\d+)"
        )
        match = re.search(pattern, text)
        if match is None:
            raise IncompleteBoundary(f"No restored source checkpoint reported for {query}")
        cursors[query] = int(match[1])
    if any(cursor > total for cursor in cursors.values()):
        raise ValueError("Restored cursor exceeds the finite input")
    if all(cursor == total for cursor in cursors.values()):
        raise ValueError("Crash occurred after all input was processed; not a pending-input recovery test")
    return cursors


def source_state(directory, expected):
    nodes, relations = {}, {}
    count = 0
    files = sorted(Path(directory).glob("*.jsonl"))
    if not files:
        raise ValueError("Missing independent source-change capture")
    for path in files:
        with path.open() as source:
            for line in source:
                change = json.loads(line)
                payload = change["payload"]
                if payload["source"]["db"] != SOURCE:
                    raise ValueError("Source capture belongs to another source")
                if payload["source"]["lsn"] != count:
                    raise ValueError("Source capture has a repeated or missing logical input")
                count += 1
                table = payload["source"]["table"]
                if table not in ("node", "relation"):
                    raise ValueError(f"Unexpected source element type: {table}")
                target = nodes if table == "node" else relations
                if change["op"] in ("i", "u"):
                    after = payload["after"]
                    target[str(after["id"])] = after
                elif change["op"] == "d":
                    del target[str(payload["before"]["id"])]
                else:
                    raise ValueError(f"Unsupported finite workload operation: {change['op']}")
    if count != expected:
        raise ValueError(f"Source capture has {count} inputs, expected {expected}")
    return nodes, relations


def verify_expected_snapshots(snapshots, nodes, relations):
    """Independent model check; the shared comparator remains exact across runs."""
    rooms = {key: node["properties"] for key, node in nodes.items() if "Room" in node["labels"]}
    expected_rooms = [
        {"RoomId": key, "Temperature": value["temperature"],
         "Humidity": value["humidity"], "Co2": value["co2"]}
        for key, value in rooms.items()
    ]
    actual_rooms = snapshots[QUERIES[0]]["data"]
    if sorted(actual_rooms, key=lambda row: row["RoomId"]) != sorted(expected_rooms, key=lambda row: row["RoomId"]):
        raise ValueError("Room snapshot differs from the final source data")
    floors = {}
    for relation in relations.values():
        if "FLOOR_ROOM" in relation["labels"]:
            if "Floor" not in nodes[str(relation["start_id"])]["labels"]:
                raise ValueError("A floor-room relationship does not start at a floor")
            floors.setdefault(str(relation["start_id"]), []).append(rooms[str(relation["end_id"])])
    actual_floors = snapshots[QUERIES[1]]["data"]
    if len(actual_floors) != len(floors) or {row["FloorId"] for row in actual_floors} != set(floors):
        raise ValueError(
            f"Floor snapshot must have one row per floor ({len(floors)}), got {len(actual_floors)}"
        )
    for row in actual_floors:
        members = floors[row["FloorId"]]
        expected = {
            "RoomCount": len(members),
            "MaxCo2": max(room["co2"] for room in members),
            "MinHumidity": min(room["humidity"] for room in members),
        }
        if any(row[key] != value for key, value in expected.items()):
            raise ValueError(f"Incorrect floor aggregate: {row}")
        mean = math.fsum(room["temperature"] for room in members) / len(members)
        if not math.isclose(row["AvgTemperature"], mean, rel_tol=0, abs_tol=1e-7):
            raise ValueError(f"Incorrect floor average: {row['AvgTemperature']} versus {mean}")


def configuration(workload, output, network, collector, run_suffix):
    admin, source_port, service_port, room_port, floor_port = network
    server = {
        "apiVersion": "drasi.io/v1", "host": "127.0.0.1", "port": admin,
        "logLevel": "info", "persistConfig": True, "enableUi": False,
        "verifyPlugins": False, "autoInstallPlugins": False,
        "metrics": {"endpoint": collector.endpoint, "exportIntervalMs": 250, "exportTimeoutMs": 1000},
        "instances": [{
            "id": INSTANCE, "persistIndex": True,
            "stateStore": {"kind": "redb", "path": "./data/state.redb"},
            "sources": [], "queries": [], "reactions": [],
        }],
    }
    test = json.loads((DYNAMIC / "config.json").read_text())
    store = test["data_store"]
    store.update({"data_store_path": str(output / "cache"), "delete_on_start": False, "delete_on_stop": False})
    repo = store["test_repos"][0]
    repo["source_path"] = str(output / "repository")
    definition = repo["local_tests"][0]
    definition["completion_handlers"] = [{"kind": "Log", "log_level": "info"}]
    definition["sources"] = copy.deepcopy(workload["sources"])
    for dispatcher in definition["sources"][0]["source_change_dispatchers"]:
        if dispatcher["kind"] == "Grpc":
            dispatcher["host"] = "127.0.0.1"
            dispatcher["port"] = source_port
    for reaction, port in zip(definition["reactions"], (room_port, floor_port)):
        reaction["stop_triggers"] = []
        reaction["output_handler"].update({"host": "127.0.0.1", "port": port})
    run = test["test_run_host"]["test_runs"][0]
    run["test_run_id"] = run_suffix
    run_id = f"{run['test_repo_id']}.{run['test_id']}.{run_suffix}"
    source = json.loads((DYNAMIC / "components/server/source_grpc.json").read_text())
    source.update({"host": "127.0.0.1", "port": source_port,
                   "durability": {"enabled": True, "max_events": 500000}})
    queries = copy.deepcopy(workload["queries"])
    for query in queries:
        query.update({"priorityQueueCapacity": 10000, "dispatchBufferCapacity": 10000, "outboxCapacity": 100000})
    reactions = json.loads((DYNAMIC / "components/server/reactions_grpc.json").read_text())
    for reaction, port in zip(reactions, (room_port, floor_port)):
        reaction["endpoint"] = f"grpc://127.0.0.1:{port}"
    return server, test, source, queries, reactions, run_id, service_port


def qualify(args):
    output = args.output.resolve()
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    children = []
    collector = None
    result = None
    failures = []
    try:
        workload = json.loads(args.workload.read_text())
        canonical_queries = json.loads((DYNAMIC / "components/server/queries.json").read_text())
        if workload["queries"] != canonical_queries or len(workload["sources"]) != 1:
            raise ValueError("Only the finite, time-independent two-query Building Comfort profile is qualified")
        source_definition = workload["sources"][0]
        if source_definition["test_source_id"] != SOURCE or source_definition["kind"] != "Model":
            raise ValueError("This qualification requires the configured facilities model source")
        if sorted(item["kind"] for item in source_definition["source_change_dispatchers"]) != ["Grpc", "JsonlFile"]:
            raise ValueError("Only owned gRPC ingress and local source-file capture are supported")
        model = workload["sources"][0]["model_data_generator"]
        if model["kind"] != "BuildingHierarchy" or model["send_initial_inserts"] is not True:
            raise ValueError("The qualification requires the finite hierarchy with initial inserts")
        total = model["change_count"]
        if type(total) is not int or not 31 <= total <= 100000:
            raise ValueError("This bounded profile supports 31 to 100000 total inputs")
        write_json(output / "workload.json", workload)
        provenance = {
            "binaries": {name: digest(path) for name, path in {
                "server": args.server_bin, "test_service": args.test_service_bin, "comparator": args.compare_bin,
            }.items()},
            "plugins": {},
            "runner_sha256": digest(__file__),
            "boundary_validator_sha256": digest(HERE / "completion_boundary.py"),
            "workload_fingerprint": fingerprint(workload),
        }
        plugin_dir = output / "plugins"
        plugin_dir.mkdir()
        for library in ("libdrasi_source_grpc", "libdrasi_reaction_grpc"):
            matches = [path for path in args.plugins_dir.glob(library + ".*") if path.suffix in (".so", ".dylib")]
            if len(matches) != 1:
                raise ValueError(f"Require one rebuilt {library} shared library")
            path = matches[0]
            shutil.copy2(path, plugin_dir / path.name)
            provenance["plugins"][path.name] = digest(path)
        if args.build_metadata:
            provenance["build"] = json.loads(args.build_metadata.read_text())
        write_json(output / "provenance.json", provenance)
        baseline = None
        if args.baseline:
            baseline = args.baseline.resolve()
            previous = json.loads((baseline.parent / "provenance.json").read_text())
            for key in ("binaries", "plugins", "workload_fingerprint", "runner_sha256", "boundary_validator_sha256"):
                if previous[key] != provenance[key]:
                    raise ValueError(f"Recovery does not use the reference run's exact {key}")
        if args.crash and baseline is None:
            raise ValueError("A crash run requires a completed matching-build reference")
        collector = Collector(output / "otel")
        server_config, test_config, source, queries, reactions, run_id, service_port = configuration(
            workload, output, ports(5), collector, "run_" + uuid.uuid4().hex,
        )
        server_root = output / "server"
        (server_root / "data" / ("id-" + INSTANCE.encode().hex()) / "index").mkdir(parents=True)
        (output / "repository" / "building_comfort").mkdir(parents=True)
        config_path = output / "server.yaml"
        test_path = output / "framework.json"
        write_json(config_path, server_config)
        write_json(test_path, test_config)
        write_json(output / "effective-components.json", {"source": source, "queries": queries, "reactions": reactions})
        admin = f"http://127.0.0.1:{server_config['port']}"
        api = f"{admin}/api/v1/instances/{quote(INSTANCE, safe='')}"
        command = [args.server_bin.resolve(), "--config", config_path, "--plugins-dir", plugin_dir,
                   "--port", str(server_config["port"]), "--disable-ui"]
        server = Child(command, server_root, output / "server.log")
        children.append(server)
        wait_until("Server startup", lambda: request(admin + "/health"), [server], 120)
        for kind, values in (("sources", [source]), ("queries", queries), ("reactions", reactions)):
            for value in values:
                request(api + "/" + kind, value, "POST")
        framework = Child(
            [args.test_service_bin.resolve(), "--config", test_path, "--port", str(service_port)],
            output, output / "framework.log",
        )
        children.append(framework)
        service = f"http://127.0.0.1:{service_port}/api/test_runs/{quote(run_id, safe='')}"
        wait_until("Framework startup", lambda: request(service), [server, framework], 120)
        wait_until("Finite input", lambda: source_finished(framework.log, run_id), [server, framework], args.timeout)
        if args.crash:
            write_json(output / "pre-crash-metrics.json", collector.sample())
            server.crash()
            server = Child(command, server_root, output / "server-restarted.log")
            children.append(server)
            wait_until("Server reconstruction", lambda: request(admin + "/health"), [server, framework], 120)
            restored = request(api + "/queries")["data"]
            if {query["id"] for query in restored} != set(QUERIES):
                raise ValueError("Restart did not restore both queries; reapplying is forbidden")
            cursors = wait_until(
                "Actual restored input cursors", lambda: recovered_cursors(server.log, total),
                [server, framework], 120,
            )
            write_json(output / "recovered-input-cursors.json", cursors)

        def boundary():
            now = time.time_ns()
            sample = collector.sample()
            proof = query_progress(sample["body"], server.pid, server.started_at_ns, now, total)
            receivers = {query: request(service + "/reactions/" + quote(query, safe="")) for query in QUERIES}
            proof["receivers"] = recorded_output(proof, receivers, framework.started_at_ns, time.time_ns())
            proof["metrics_export"] = sample
            return proof

        proof = wait_until("Processing and recorded output", boundary, [server, framework], args.timeout)
        snapshots = {query: request(api + "/queries/" + quote(query, safe="") + "/results") for query in QUERIES}
        write_json(output / "observed-boundary.json", proof)
        for query, snapshot in snapshots.items():
            write_json(output / f"snapshot-{query}.json", snapshot)
        run_root = output / "cache" / "test_runs" / run_id
        nodes, relations = source_state(run_root / "sources" / SOURCE / "source_change_log", total)
        write_json(output / "final-source-state.json", {"nodes": nodes, "relations": relations})
        verify_expected_snapshots(snapshots, nodes, relations)
        final_receivers = {}
        for query in QUERIES:
            request(service + "/reactions/" + quote(query, safe="") + "/stop", method="POST")
            state = request(service + "/reactions/" + quote(query, safe=""))["reaction_observer"]
            if state["status"] != "Stopped" or state.get("error_message") is not None:
                raise ValueError("Receiver did not finalize its actual capture")
            final_receivers[query] = state
        proof.update({"test_run_id": run_id, "final_input_sequence": total, "recorded_at_ns": time.time_ns()})
        write_json(output / "completion-boundary.json", proof)
        evidence = (
            f"Finite input {total} committed by both queries; complete terminal producer batches "
            "recorded and logger finalization awaited. Independent source-model snapshots checked. "
            f"completion-boundary.json SHA-256 {digest(output / 'completion-boundary.json')}"
        )
        manifest = {"schema_version": 1, "workload_fingerprint": fingerprint(workload),
                    "capture": {"complete": True, "evidence": evidence}, "queries": []}
        for config in workload["queries"]:
            query = config["id"]
            snapshot = output / f"snapshot-{query}.json"
            write_json(snapshot, snapshots[query])
            loggers = [logger for logger in final_receivers[query]["logger_results"]
                       if logger["logger_name"] == "JsonlFile" and logger["has_output"]]
            if len(loggers) != 1:
                raise ValueError("Require one finalized JSONL logger per query")
            files = sorted(Path(loggers[0]["output_folder_path"]).glob("*.jsonl"))
            manifest["queries"].append({
                "query_id": query, "config_fingerprint": fingerprint(config),
                "identity_contract": "grpc-query-sequence-row-operation-v1",
                "identity_pointer": "/payload/headers/x-drasi-producer-key",
                "query_id_pointer": "/payload/request_body/query_id", "payload_pointer": "/payload/request_body/result",
                "event_files": [str(path) for path in files], "snapshot_file": str(snapshot),
            })
        capture = output / "capture.json"
        actual = output / "actual.json"
        write_json(capture, manifest)
        with actual.open("w") as destination:
            subprocess.run([args.compare_bin, "--import", capture], stdout=destination, check=True, timeout=args.timeout)
        if baseline and baseline.suffix == ".gz":
            unpacked = output / "baseline.json"
            with gzip.open(baseline, "rb") as source_file, unpacked.open("wb") as target:
                shutil.copyfileobj(source_file, target)
            baseline = unpacked
        with (output / "comparison.json").open("w") as destination:
            subprocess.run([args.compare_bin, baseline or actual, actual], stdout=destination, check=True, timeout=args.timeout)
        result = {"status": "passed", "crash_injected": args.crash, "test_run_id": run_id,
                  "workload_fingerprint": fingerprint(workload), "actual_sha256": digest(actual)}
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        failures.append(str(error))
    finally:
        for child in reversed(children):
            try:
                child.close()
            except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                failures.append(str(error))
        if collector:
            try:
                collector.close()
            except (OSError, RuntimeError) as error:
                failures.append(str(error))
    if failures:
        write_json(output / "result.json", {"status": "failed", "errors": failures})
        raise RuntimeError("; ".join(failures))
    write_json(output / "result.json", result)
    print(f"Verified completed {'recovery' if args.crash else 'reference'}: {output}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("server-bin", "test-service-bin", "compare-bin", "plugins-dir", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--workload", type=Path, default=HERE / "goldens/building-comfort-small-v1/workload.json")
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--build-metadata", type=Path)
    parser.add_argument("--crash", action="store_true")
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    if os.name != "posix" or not 1 <= args.timeout <= 3600:
        parser.error("This qualification requires POSIX processes and a timeout of 1 to 3600 seconds")
    try:
        qualify(args)
    except (ValueError, OSError, RuntimeError) as error:
        print(f"Qualification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
