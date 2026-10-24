#!/usr/bin/env python3
# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.

"""Validate finite Building Comfort completion using actual OTLP and receiver evidence."""

import json
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

QUERIES = ("building-comfort", "building-comfort-floor-agg")
SOURCE = "facilities-db"
INSTANCE = "building-comfort-dynamic"


class IncompleteBoundary(ValueError):
    """A required participant has not yet supplied fresh completion evidence."""


def decimal(value, field, maximum=(1 << 63) - 1):
    if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]*", value):
        raise ValueError(f"{field} must be an exact unsigned decimal string")
    parsed = int(value)
    if parsed > maximum:
        raise ValueError(f"{field} exceeds the supported OTLP integer range")
    return parsed


def attributes(items):
    if not isinstance(items, list):
        raise ValueError("OTLP attributes must be an array")
    result = {}
    for item in items:
        key = item["key"]
        value = item["value"]
        if not isinstance(key, str) or key in result or not isinstance(value, dict):
            raise ValueError("Invalid or repeated OTLP attribute")
        if len(value) != 1:
            raise ValueError(f"Ambiguous OTLP attribute {key}")
        if "stringValue" in value and isinstance(value["stringValue"], str):
            result[key] = value["stringValue"]
        elif "intValue" in value:
            result[key] = decimal(value["intValue"], key)
        elif "boolValue" in value and type(value["boolValue"]) is bool:
            result[key] = value["boolValue"]
        else:
            raise ValueError(f"Unsupported OTLP attribute {key}")
    return result


def query_progress(body, pid, started_at_ns, now_ns, final_input, max_age_ns=15_000_000_000):
    """Use one fresh export, never a merge of values retained from older exports."""
    if type(final_input) is not int or final_input <= 0:
        raise ValueError("A positive final input boundary is required")
    selected = []
    for resource in body["resourceMetrics"]:
        identity = attributes(resource["resource"]["attributes"])
        if identity.get("process.pid") == pid:
            selected.append((identity, resource))
    if len(selected) != 1:
        raise IncompleteBoundary("No unique metrics resource for the current Server process")
    identity, resource = selected[0]
    if identity.get("service.name") != "drasi-server":
        raise ValueError("Progress came from a different service")
    incarnation = identity.get("service.instance.id")
    if not isinstance(incarnation, str) or not incarnation:
        raise ValueError("Server metrics have no process incarnation")
    result = {}
    for scope in resource["scopeMetrics"]:
        if scope["scope"]["name"] != "drasi.progress":
            continue
        for metric in scope["metrics"]:
            if metric["name"] != "drasi.query.progress.sequence":
                continue
            if "gauge" not in metric:
                raise ValueError("Query progress must be an observable gauge")
            for point in metric["gauge"]["dataPoints"]:
                tags = attributes(point["attributes"])
                if tags.get("otel.metric.overflow"):
                    raise ValueError("Metric cardinality overflow prevents complete evidence")
                if tags.get("drasi.instance.id") != INSTANCE:
                    continue
                query = tags.get("drasi.query.id")
                if query not in QUERIES:
                    raise ValueError(f"Unexpected query in the finite workload: {query}")
                timestamp = decimal(point["timeUnixNano"], "metric timestamp", (1 << 64) - 1)
                if timestamp > now_ns:
                    raise ValueError("Progress metric is future-dated")
                if timestamp < started_at_ns or now_ns - timestamp > max_age_ns:
                    raise IncompleteBoundary("Progress metric is stale or predates this restart")
                if tags.get("drasi.progress.persistent") is not True:
                    raise ValueError("Recovery requires actual persistent input and output progress")
                epoch = tuple(
                    decimal(tags[key], key, (1 << 64) - 1)
                    for key in (
                        "drasi.component.generation",
                        "drasi.component.operation",
                        "drasi.query.output.generation",
                        "drasi.query.config_hash",
                    )
                )
                query_state = result.setdefault(query, {"epoch": epoch})
                if query_state["epoch"] != epoch:
                    raise ValueError(f"Mixed component or output generations for {query}")
                if query_state.get("observed_at_ns", timestamp) != timestamp:
                    raise ValueError(f"Mixed collection times for {query}")
                stage = tags.get("drasi.progress.stage")
                if stage == "input.committed":
                    if tags.get("drasi.input.kind") != "source" or tags.get("drasi.input.id") != SOURCE:
                        raise ValueError("Unexpected input in the finite single-source workload")
                    key = "input"
                elif stage == "output.produced":
                    key = "output"
                else:
                    raise ValueError(f"Unsupported progress stage: {stage}")
                if key in query_state:
                    raise ValueError(f"Duplicate {key} progress for {query}")
                if "asInt" not in point or "asDouble" in point:
                    raise ValueError("Progress requires an exact integer data point")
                query_state[key] = decimal(point["asInt"], f"{query} {key} sequence")
                query_state["observed_at_ns"] = timestamp
    for query in QUERIES:
        progress = result.get(query)
        if not progress or "input" not in progress or "output" not in progress:
            raise IncompleteBoundary(f"Missing committed input or produced output for {query}")
        if progress["input"] > final_input:
            raise ValueError(f"{query} processed unexpected input beyond the finite workload")
        if progress["input"] < final_input:
            raise IncompleteBoundary(f"{query} has processed {progress['input']} of {final_input}")
    if len({progress["observed_at_ns"] for progress in result.values()}) != 1:
        raise ValueError("Required queries came from different metrics collections")
    return {"service_instance_id": incarnation, "pid": pid, "queries": result}


def recorded_output(proof, receivers, run_started_at_ns, now_ns):
    """A batch marker is meaningful only after every preceding row reached all loggers."""
    recorded = {}
    for query in QUERIES:
        state = receivers[query]["reaction_observer"]
        if state["status"] != "Running" or state.get("error_message") is not None:
            raise ValueError(f"{query} receiver failed or stopped before verified completion")
        expected = proof["queries"][query]["output"]
        batch = state["result_summary"]["recorded_batches"].get(query)
        if expected == 0 and batch is None:
            recorded[query] = None
            continue
        if batch is None:
            raise IncompleteBoundary(f"{query} has not recorded a complete producer batch")
        for field in ("sequence", "item_count", "recorded_at_ns"):
            if type(batch[field]) is not int or batch[field] < 0:
                raise ValueError(f"Invalid receiver batch {field}")
        if batch["item_count"] == 0:
            raise ValueError("Empty batches cannot confirm output delivery")
        if not run_started_at_ns <= batch["recorded_at_ns"] <= now_ns:
            raise ValueError("Receiver batch belongs to another run or is future-dated")
        if batch["sequence"] > expected:
            raise ValueError(f"{query} receiver advanced beyond the terminal query output")
        if batch["sequence"] < expected:
            raise IncompleteBoundary(f"{query} has not recorded terminal output {expected}")
        recorded[query] = batch
    return recorded


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


class Collector:
    """Bounded loopback-only OTLP fixture; does not install a global OTel provider."""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.lock = threading.Lock()
        self.latest = None
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                if self.path != "/v1/metrics":
                    self.send_error(404)
                    return
                self.connection.settimeout(5)
                try:
                    length = int(self.headers["Content-Length"])
                    if not 0 < length <= 4 * 1024 * 1024:
                        raise ValueError("OTLP request exceeds the fixture body limit")
                    if self.headers.get_content_type() != "application/json":
                        raise ValueError("The fixture requires OTLP/HTTP JSON")
                    raw = self.rfile.read(length)
                    if len(raw) != length:
                        raise ValueError("Incomplete OTLP body")
                    body = json.loads(raw)
                    if not isinstance(body.get("resourceMetrics"), list):
                        raise ValueError("Missing OTLP resourceMetrics")
                    record = {"received_at_ns": time.time_ns(), "body": body}
                    with owner.lock:
                        with (owner.directory / "exports.jsonl").open("a") as output:
                            output.write(json.dumps(record, separators=(",", ":")) + "\n")
                        owner.latest = record
                    response = b"{}"
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(response)))
                    self.end_headers()
                    self.wfile.write(response)
                except (KeyError, TypeError, ValueError, OSError) as error:
                    self.send_error(400, str(error))

            def log_message(self, message, *args):
                if len(args) < 2 or str(args[1]) != "200":
                    # HTTP diagnostics remain in the owning qualifier's log.
                    super().log_message(message, *args)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = False
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}/v1/metrics"

    def sample(self):
        with self.lock:
            if self.latest is None:
                raise IncompleteBoundary("No OTLP export received")
            return self.latest

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise RuntimeError("OTLP collector did not stop")
