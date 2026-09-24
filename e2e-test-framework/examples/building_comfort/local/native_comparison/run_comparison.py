#!/usr/bin/env python3
# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.

"""Compare the same Server workload over adapter-backed and native transports.

Only explicitly supplied local binaries are executed. This driver does not
download releases, change branches, or use the broad-kill local shell wrapper.
"""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import socket
import statistics
import subprocess
import time
import urllib.error
import urllib.request


DYNAMIC = Path(__file__).resolve().parents[2] / "dynamic"
ROOMS = "building-comfort"
FLOORS = "building-comfort-floor-agg"


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def verify_artifacts(binaries, plugin_directory, plugins):
    for name, artifact in binaries.items():
        if digest(artifact["path"]) != artifact["sha256"]:
            raise RuntimeError(f"binary changed during comparison: {name}")
    current = {
        path.name: digest(path)
        for path in plugin_directory.iterdir()
        if path.suffix in (".so", ".dylib", ".dll")
    }
    if current != plugins:
        raise RuntimeError("plugin directory changed during comparison")


def reserve_ports(count):
    sockets = []
    try:
        for _ in range(count):
            candidate = socket.socket()
            candidate.bind(("127.0.0.1", 0))
            sockets.append(candidate)
        return [candidate.getsockname()[1] for candidate in sockets]
    finally:
        for candidate in sockets:
            candidate.close()


def request(url, method="GET", body=None):
    encoded = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=encoded, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def wait_for(description, deadline, processes, probe):
    while True:
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(
                    f"{description}: process {process.pid} exited {process.returncode}"
                )
        try:
            result = probe()
        except urllib.error.HTTPError as error:
            if error.code not in (404, 503):
                raise
            result = None
        except (ConnectionError, TimeoutError, urllib.error.URLError):
            result = None
        if result:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(description)
        time.sleep(0.05)


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise RuntimeError(f"process {process.pid} did not shut down within 30 seconds")
    if process.returncode not in (0, -signal.SIGINT):
        raise RuntimeError(f"process {process.pid} exited {process.returncode}")


def prepare_workload(transport, selection, changes, output, ports, jsonl=False):
    if changes <= 31:
        raise ValueError("changes must exceed the 31 initial graph elements")
    if selection == "both" and changes != 100000:
        raise ValueError("the calibrated two-query profile requires exactly 100000 changes")
    config = read_json(DYNAMIC / ("config.http.json" if transport == "http" else "config.json"))
    config["data_store"].update(
        data_store_path=str(output / "data"), delete_on_start=False, delete_on_stop=False
    )
    repository = config["data_store"]["test_repos"][0]
    repository.pop("source_path", None)
    definition = repository["local_tests"][0]
    selected = {ROOMS, FLOORS} if selection == "both" else {ROOMS}
    expected = {ROOMS: changes - 19}
    if selection == "both":
        expected[FLOORS] = 49860
    definition["description"] = "Local native-versus-adapter Server comparison"
    # Compare each pair directly, not against hashes from a different release.
    # The original expected hashes are retained separately in the evidence.
    old_baselines = copy.deepcopy(definition.get("completion_handlers", []))
    definition["completion_handlers"] = [{"kind": "Log", "log_level": "info"}]
    source = definition["sources"][0]
    source["model_data_generator"]["change_count"] = changes
    source["subscribers"] = [
        subscriber for subscriber in source["subscribers"] if subscriber["query_id"] in selected
    ]
    source["source_change_dispatchers"] = [
        dispatcher
        for dispatcher in source["source_change_dispatchers"]
        if dispatcher["kind"] != "JsonlFile" or jsonl
    ]
    for dispatcher in source["source_change_dispatchers"]:
        if dispatcher["kind"] == "Grpc":
            dispatcher.update(host="127.0.0.1", port=ports["source"])
        elif dispatcher["kind"] == "Http":
            dispatcher.update(url="http://127.0.0.1", port=ports["source"])
    definition["reactions"] = [
        reaction for reaction in definition["reactions"] if reaction["test_reaction_id"] in selected
    ]
    for index, reaction in enumerate(definition["reactions"]):
        reaction["output_handler"]["port"] = ports["receivers"][index]
        reaction["stop_triggers"] = [
            {"kind": "RecordCount", "record_count": expected[reaction["test_reaction_id"]]}
        ]
    run = config["test_run_host"]["test_runs"][0]
    run["test_run_id"] = "native-comparison"
    run["sources"][0]["start_mode"] = "manual"
    run["reactions"] = [
        reaction for reaction in run["reactions"] if reaction["test_reaction_id"] in selected
    ]
    for reaction in run["reactions"]:
        reaction["start_immediately"] = True
        reaction["output_loggers"] = [
            {"kind": "PerformanceMetrics", "bootstrap_record_count": 12},
            {"kind": "DeterminismHash"},
        ]
        if jsonl:
            reaction["output_loggers"].append({"kind": "JsonlFile", "max_lines_per_file": 15000})
    return config, expected, old_baselines


def adapter_config(transport, selection, ports, capacity):
    component_dir = DYNAMIC / "components" / "server"
    source = read_json(component_dir / f"source_{transport}.json")
    reactions = read_json(component_dir / f"reactions_{transport}.json")
    queries = read_json(component_dir / "queries.json")
    selected = {ROOMS, FLOORS} if selection == "both" else {ROOMS}
    queries = [query for query in queries if query["id"] in selected]
    reactions = [reaction for reaction in reactions if reaction["queries"][0] in selected]
    source.update(host="127.0.0.1", port=ports["source"])
    for query in queries:
        query["enableBootstrap"] = False
        query["priorityQueueCapacity"] = capacity
        query["dispatchBufferCapacity"] = capacity
        query["outboxCapacity"] = capacity
    for index, reaction in enumerate(reactions):
        reaction["priorityQueueCapacity"] = capacity
        if transport == "grpc":
            reaction["endpoint"] = f"grpc://127.0.0.1:{ports['receivers'][index]}"
        else:
            reaction["baseUrl"] = f"http://127.0.0.1:{ports['receivers'][index]}"
    return {
        "apiVersion": "drasi.io/v1",
        "id": "performance",
        "host": "127.0.0.1",
        "port": ports["server"],
        "logLevel": "warn",
        "persistConfig": False,
        "persistIndex": False,
        "verifyPlugins": False,
        "autoInstallPlugins": False,
        "enableUi": False,
        "defaultPriorityQueueCapacity": capacity,
        "defaultDispatchBufferCapacity": capacity,
        "sources": [source],
        "queries": queries,
        "reactions": reactions,
    }


def observer_result(body, expected):
    observer = body["reaction_observer"]
    if observer["status"] == "Error":
        raise RuntimeError(f"reaction observer failed: {observer}")
    if observer["status"] != "Stopped":
        return None
    results = {logger["logger_name"]: logger["summary"] for logger in observer["logger_results"]}
    if not {"PerformanceMetrics", "DeterminismHash"} <= results.keys():
        return None
    actual = observer["result_summary"]["reaction_invocation_count"]
    metrics, hashed = results["PerformanceMetrics"], results["DeterminismHash"]
    if actual != expected or metrics["record_count"] != expected or hashed["record_count"] != expected:
        raise RuntimeError(f"record-count mismatch: expected={expected}, observer={actual}, metrics={metrics}, hash={hashed}")
    if len(hashed["sha256"]) != 64 or any(ch not in "0123456789abcdef" for ch in hashed["sha256"]):
        raise RuntimeError("invalid determinism hash")
    if metrics["duration_ns"] <= 0:
        raise RuntimeError("invalid measured duration")
    return {"record_count": actual, "sha256": hashed["sha256"], "metrics": metrics}


def source_completed(body, expected):
    generator = body["source_change_generator"]
    if generator["status"] == "Error":
        raise RuntimeError(f"source generator failed: {generator}")
    if generator["status"] != "Finished":
        return False
    state = generator["state"]
    stats = state["stats"]
    if state["error_messages"] or stats["num_source_change_events"] != expected or stats["num_skipped_source_change_events"]:
        raise RuntimeError(f"source did not dispatch the exact workload: {state}")
    return True


def run_api_id(workload):
    run = workload["test_run_host"]["test_runs"][0]
    return ".".join(run[field] for field in ["test_repo_id", "test_id", "test_run_id"])


def receivers_ready(run_url, reactions):
    for reaction_id in reactions:
        observer = request(run_url + f"/reactions/{reaction_id}")["reaction_observer"]
        if observer["status"] == "Error":
            raise RuntimeError(f"receiver startup failed: {observer}")
        if observer["status"] != "Running":
            return False
    return True


def run_case(args, transport, mode, ordinal, warmup):
    output = args.output / transport / f"{ordinal:02d}-{mode}"
    output.mkdir(parents=True)
    port_list = reserve_ports(5)
    ports = {"server": port_list[0], "service": port_list[1], "source": port_list[2], "receivers": port_list[3:]}
    workload, expected, baselines = prepare_workload(
        transport, args.queries, args.changes, output, ports, args.jsonl
    )
    baseline = adapter_config(transport, args.queries, ports, args.capacity)
    write_json(output / "workload.json", workload)
    write_json(output / "adapter-config.json", baseline)
    write_json(output / "published-baseline-handlers.json", baselines)
    if mode == "native":
        generated = subprocess.run(
            [str(args.config_generator), str(args.native_plugin), str(output / "adapter-config.json"), str(args.capacity)],
            capture_output=True, text=True,
        )
        (output / "config-generator.stdout").write_text(generated.stdout)
        (output / "config-generator.stderr").write_text(generated.stderr)
        if generated.returncode != 0:
            raise RuntimeError(f"native configuration generation failed: {generated.stderr.strip()}")
        config = json.loads(generated.stdout)
    else:
        config = baseline
    write_json(output / "server.json", config)
    service_command = [str(args.test_service), "--config", str(output / "workload.json"), "--port", str(ports["service"])]
    server_command = [str(args.server), "--config", str(output / "server.json"),
                      "--plugins-dir", str(args.plugins), "--skip-verification", "--disable-ui"]
    provenance = {
        "transport": transport, "mode": mode, "warmup": warmup, "ports": ports,
        "server_command": server_command, "test_service_command": service_command,
        "expected": expected, "workload": workload,
    }
    write_json(output / "provenance.json", provenance)
    processes = []
    with (output / "test-service.log").open("w") as service_log, (output / "server.log").open("w") as server_log:
        try:
            env = {**os.environ, "RUST_LOG": "warn"}
            service = subprocess.Popen(service_command, cwd=output, stdout=service_log, stderr=subprocess.STDOUT, env=env)
            processes.append(service)
            provenance["test_service_pid"] = service.pid
            write_json(output / "provenance.json", provenance)
            service_url = f"http://127.0.0.1:{ports['service']}"
            run_id = run_api_id(workload)
            run_url = service_url + f"/api/test_runs/{run_id}"
            wait_for("test-service readiness", time.monotonic() + 60, processes,
                     lambda: request(run_url).get("id") == run_id)
            wait_for("reaction receiver readiness", time.monotonic() + 60, processes,
                     lambda: receivers_ready(run_url, expected))
            server = subprocess.Popen(server_command, cwd=output, stdout=server_log, stderr=subprocess.STDOUT, env=env)
            processes.append(server)
            provenance["server_pid"] = server.pid
            write_json(output / "provenance.json", provenance)
            server_url = f"http://127.0.0.1:{ports['server']}"

            def ready():
                runtime = request(server_url + "/api/v1/instances/performance/runtime")
                if runtime.get("data") != {"instanceId": "performance", "runtime": "computationGraph", "running": True}:
                    return False
                if mode == "native":
                    graph = request(server_url + "/api/v1/instances/performance/computation/graphs/performance")
                    nodes = graph["data"]["components"]
                    if any(node["lifecycle"] == "Failed" or node["realization"] == "CreationFailed" for node in nodes):
                        raise RuntimeError(f"native graph startup failed: {graph}")
                    return nodes and all(node["lifecycle"] == "Running" for node in nodes)
                for kind, ids in [("sources", ["facilities-db"]), ("queries", list(expected)),
                                  ("reactions", [reaction["id"] for reaction in baseline["reactions"]])]:
                    for component_id in ids:
                        component = request(server_url + f"/api/v1/instances/performance/{kind}/{component_id}")
                        if component["data"]["status"] == "Error":
                            raise RuntimeError(f"adapter component startup failed: {component}")
                        if component["data"]["status"] != "Running":
                            return False
                return True

            wait_for("Drasi component readiness", time.monotonic() + 60, processes, ready)
            provenance["runtime"] = request(server_url + "/api/v1/instances/performance/runtime")
            provenance["loaded_plugins"] = request(server_url + "/api/v1/plugins")
            write_json(output / "provenance.json", provenance)
            started = time.monotonic()
            request(run_url + "/sources/facilities-db/start", method="POST")
            result = {}

            def completed():
                source = request(run_url + "/sources/facilities-db")
                write_json(output / "source.json", source)
                source_finished = source_completed(source, args.changes)
                for reaction_id, count in expected.items():
                    body = request(run_url + f"/reactions/{reaction_id}")
                    write_json(output / f"{reaction_id}.json", body)
                    outcome = observer_result(body, count)
                    if outcome is None:
                        return False
                    result[reaction_id] = outcome
                return source_finished

            wait_for("source completion and exact reaction results", started + args.timeout, processes, completed)
            record = {"transport": transport, "mode": mode, "warmup": warmup,
                      "results": result, "wall_seconds": time.monotonic() - started}
            write_json(output / "results.json", record)
            return record
        finally:
            failures = []
            for process in reversed(processes):
                try:
                    stop(process)
                except RuntimeError as error:
                    failures.append(str(error))
            if failures:
                raise RuntimeError("; ".join(failures))


def compare_pair(left, right):
    if left["transport"] != right["transport"] or left["results"].keys() != right["results"].keys():
        raise RuntimeError("pair does not cover the same transport and queries")
    for query, expected in left["results"].items():
        actual = right["results"][query]
        for field in ("record_count", "sha256"):
            if actual[field] != expected[field]:
                raise RuntimeError(f"{left['transport']}/{query}: {field} differs: adapter={expected[field]} native={actual[field]}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ["server", "test-service", "config-generator", "native-plugin", "plugins", "output"]:
        parser.add_argument("--" + flag, required=True, type=Path)
    parser.add_argument("--transport", choices=["http", "grpc", "both"], default="both")
    parser.add_argument("--queries", choices=["rooms", "both"], default="rooms")
    parser.add_argument("--changes", type=int, default=100000)
    parser.add_argument("--capacity", type=int, default=10000)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=1200)
    parser.add_argument("--jsonl", action="store_true", help="retain per-event diagnostics in both arms (changes timing)")
    args = parser.parse_args()
    for name in ["server", "test_service", "config_generator", "native_plugin", "plugins", "output"]:
        setattr(args, name, getattr(args, name).resolve())
    if args.runs < 1 or args.warmups < 0 or args.capacity < 1 or args.timeout <= 0:
        parser.error("runs/capacity/timeout must be positive and warmups nonnegative")
    if args.output.exists():
        parser.error("output directory must not already exist")
    for name in ["server", "test_service", "config_generator", "native_plugin"]:
        if not getattr(args, name).is_file():
            parser.error(f"{name} is not a file")
    if not args.plugins.is_dir():
        parser.error("plugins must be a directory containing matching local libraries")
    artifacts = sorted(path for path in args.plugins.iterdir() if path.suffix in (".so", ".dylib", ".dll"))
    native_hash = digest(args.native_plugin)
    plugin_hashes = {path.name: digest(path) for path in artifacts}
    if native_hash not in plugin_hashes.values():
        parser.error("the plugin directory must contain the exact native network library")
    binary_hashes = {
        name: {"path": str(getattr(args, name)), "sha256": digest(getattr(args, name))}
        for name in ["server", "test_service", "config_generator", "native_plugin"]
    }
    args.output.mkdir(parents=True)
    write_json(args.output / "binaries.json", {
        "platform": platform.platform(),
        "binaries": binary_hashes,
        "plugins": plugin_hashes,
        "arguments": {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
    })
    records = []
    transports = ["http", "grpc"] if args.transport == "both" else [args.transport]
    for transport in transports:
        for index in range(args.warmups + args.runs):
            order = ["adapters", "native"] if index % 2 == 0 else ["native", "adapters"]
            pair = {}
            for mode in order:
                print(f"{transport}: {'warmup' if index < args.warmups else 'measured'} {index + 1} {mode}", flush=True)
                record = run_case(args, transport, mode, index, index < args.warmups)
                records.append(record)
                pair[mode] = record
                write_json(args.output / "runs.json", records)
            compare_pair(pair["adapters"], pair["native"])
    verify_artifacts(binary_hashes, args.plugins, plugin_hashes)
    summary = []
    for transport in transports:
        selected = [record for record in records if record["transport"] == transport and not record["warmup"]]
        for query in selected[0]["results"]:
            medians = {
                mode: statistics.median(record["results"][query]["metrics"]["duration_ns"] for record in selected if record["mode"] == mode) / 1e9
                for mode in ["adapters", "native"]
            }
            summary.append({"transport": transport, "query": query, "median_seconds": medians,
                            "native_elapsed_change_percent": (medians["native"] / medians["adapters"] - 1) * 100})
    write_json(args.output / "summary.json", {"equivalent": True, "comparisons": summary})
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
