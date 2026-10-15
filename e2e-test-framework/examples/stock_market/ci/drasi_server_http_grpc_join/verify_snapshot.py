#!/usr/bin/env python3
"""Verify the join snapshot against an independent oracle built from drained inputs."""

import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request


QUERY_ID = "watchlist-prices"
SOURCE_IDS = ("stock-trades-db", "watchlist-db")
QUERY = "MATCH (w:WatchlistItem)-[:WATCHES]->(s:Stock) RETURN w.symbol AS Symbol, s.name AS Name, s.price AS Price, s.volume AS Volume"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def write_json(filename, value):
    filename.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def event_content(event):
    payload = event["payload"]
    source = payload["source"]
    return {"op": event["op"], "before": payload["before"], "after": payload["after"],
            "source": {name: source[name] for name in ("db", "table", "lsn")}}


def fold_source(directory, source_id, expected_count, scripted=None):
    files = sorted(directory.glob("*.jsonl"))
    require(files, f"Missing input logs for {source_id}")
    prefix = None
    for index, filename in enumerate(files):
        match = re.fullmatch(r"(.+)_(\d{5,})\.jsonl", filename.name)
        require(match is not None, f"Invalid input log name: {filename.name}")
        prefix = prefix or match[1]
        require(match[1] == prefix and int(match[2]) == index,
                f"Missing or mixed input log chunks for {source_id}")
    state, count, previous = {}, 0, None
    digest = hashlib.sha256()
    for filename in files:
        with filename.open() as stream:
            for line in stream:
                event = json.loads(line)
                content = event_content(event)
                source = content["source"]
                require(source["db"] == source_id and source["table"] == "node",
                        f"Unexpected input source in {filename.name}")
                sequence = source["lsn"]
                require(type(sequence) is int and sequence >= 0, "Invalid input sequence")
                require(sequence == (previous + 1 if previous is not None else (0 if source_id == "stock-trades-db" else 1)),
                        f"Missing, repeated, or reordered input at {source_id} sequence {sequence}")
                previous = sequence
                if scripted is not None:
                    require(count < len(scripted) and content == event_content(scripted[count]),
                            f"Watchlist input differs from its script at event {count + 1}")
                digest.update((canonical(content) + "\n").encode())
                operation = event["op"]
                require(operation in ("i", "u", "d"), f"Unsupported input operation: {operation}")
                node = content["before"] if operation == "d" else content["after"]
                require(isinstance(node, dict) and isinstance(node.get("id"), str), "Invalid input node")
                require(isinstance(node.get("labels"), list) and isinstance(node.get("properties"), dict),
                        "Input node is missing labels or properties")
                if operation == "d":
                    require(node["id"] in state, f"Delete of unknown {source_id} node {node['id']}")
                    del state[node["id"]]
                else:
                    if operation == "i":
                        require(node["id"] not in state, f"Repeated insert for {source_id} node {node['id']}")
                    state[node["id"]] = node
                count += 1
    require(count == expected_count, f"Incomplete {source_id} inputs: expected {expected_count}, got {count}")
    return state, {"events": count, "sha256": digest.hexdigest(), "chunks": len(files)}


def expected_snapshot(config, run_storage):
    repo = config["data_store"]["test_repos"][0]
    test = repo["local_tests"][0]
    sources = {source["test_source_id"]: source for source in test["sources"]}
    require(set(sources) == set(SOURCE_IDS), "Snapshot oracle requires the two stock-market sources")
    model = sources["stock-trades-db"]["model_data_generator"]
    require(model["kind"] == "StockTrade", "Snapshot oracle requires StockTrade inputs")
    folder = sources["watchlist-db"]["source_change_generator"]["script_file_folder"]
    script_dir = Path(repo["source_path"]) / test["test_folder"] / "sources/watchlist-db" / folder
    scripts = sorted(script_dir.glob("*.jsonl"))
    require(scripts, "Missing watchlist script")
    watchlist_events = []
    finished = False
    for filename in scripts:
        with filename.open() as stream:
            for line in stream:
                record = json.loads(line)
                require(not finished, "Unexpected script records after Finish")
                require(record["kind"] in ("Header", "SourceChange", "Finish"), "Unsupported watchlist script command")
                finished = record["kind"] == "Finish"
                if record["kind"] == "SourceChange":
                    watchlist_events.append(record["source_change_event"])
    require(watchlist_events, "Empty watchlist script")
    stocks, stock_evidence = fold_source(run_storage / "sources/stock-trades-db/source_change_log",
                                         "stock-trades-db", model["change_count"])
    watches, watch_evidence = fold_source(run_storage / "sources/watchlist-db/source_change_log",
                                         "watchlist-db", len(watchlist_events), watchlist_events)
    require(set(stocks) == {stock["symbol"] for stock in model["stock_definitions"]},
            "Final stock input IDs do not match the configured stocks")
    rows = []
    for watch in watches.values():
        if "WatchlistItem" not in watch["labels"]:
            continue
        symbol = watch["properties"].get("symbol")
        for stock in stocks.values():
            if "Stock" in stock["labels"] and symbol is not None and stock["properties"].get("symbol") == symbol:
                properties = stock["properties"]
                rows.append({"Symbol": symbol, "Name": properties.get("name"),
                             "Price": properties.get("price"), "Volume": properties.get("volume")})
    require(rows, "The stock-market workload produced an empty expected join")
    return {"schema_version": 1, "oracle": "stock-watchlist-input-replay-v1", "query_id": QUERY_ID,
            "sources": {"stock-trades-db": stock_evidence, "watchlist-db": watch_evidence},
            "snapshot": sorted(rows, key=canonical)}


def compare_rows(actual, expected):
    require(isinstance(actual, list) and all(isinstance(row, dict) for row in actual),
            "Query snapshot must be an array of rows")
    actual_counts = collections.Counter(map(canonical, actual))
    expected_counts = collections.Counter(map(canonical, expected))
    def differences(counts):
        return [{"row": json.loads(row), "count": count} for row, count in sorted(counts.items())]
    missing = differences(expected_counts - actual_counts)
    unexpected = differences(actual_counts - expected_counts)
    return {"passed": not missing and not unexpected, "missing": missing, "unexpected": unexpected}


def request(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def validate_query(server):
    queries = server["queries"]
    require(len(queries) == 1 and queries[0]["id"] == QUERY_ID, "Unsupported query set for snapshot oracle")
    query = queries[0]
    require(" ".join(query["query"].split()) == QUERY, "Query changed; update the snapshot oracle explicitly")
    require(sorted(query["sources"], key=lambda source: source["sourceId"]) ==
            [{"sourceId": source_id} for source_id in SOURCE_IDS], "Query source configuration changed")
    require(query["joins"] == [{"id": "WATCHES", "keys": [
        {"label": "WatchlistItem", "property": "symbol"}, {"label": "Stock", "property": "symbol"}]}],
        "Query join contract changed")


def load_server(filename):
    return json.loads(subprocess.check_output([
        "ruby", "-ryaml", "-rjson", "-e", "puts JSON.generate(YAML.safe_load(File.read(ARGV.fetch(0))))",
        str(filename)], text=True))


def validate_drain(log, run_id, service):
    require(f"Source dispatcher drain failed for TestRunSource {run_id}." not in log, "Source dispatcher drain failed")
    for source_id in SOURCE_IDS:
        marker = r"Source dispatchers drained for TestRunSource " + re.escape(f"{run_id}.{source_id}") + r"\s*$"
        require(re.search(marker, log, re.MULTILINE), f"Missing successful drain for {source_id}; rebuild test-service if needed")
        state = request(f"{service}/api/test_runs/{run_id}/sources/{source_id}")
        require(state["source_change_generator"]["status"] == "Finished", f"Source {source_id} did not finish successfully")
    status = request(f"{service}/api/test_runs/{run_id}")
    require(status.get("status") in ("Stopped", "Running"), f"Test run is not successful: {status.get('status')}")


def check_processes(args):
    for name in ("server_pid", "service_pid"):
        process_id = getattr(args, name, None)
        if process_id is not None:
            require(process_id > 0, "Invalid monitored process ID")
            try:
                os.kill(process_id, 0)
            except ProcessLookupError as error:
                raise ValueError(f"{name} {process_id} exited during snapshot verification") from error


def wait_for_drain(args, deadline):
    while time.monotonic() < deadline:
        check_processes(args)
        log = args.service_log.read_text(errors="replace")
        require(f"Source dispatcher drain failed for TestRunSource {args.run_id}." not in log,
                "Source dispatcher drain failed")
        markers = [r"Source dispatchers drained for TestRunSource " +
                   re.escape(f"{args.run_id}.{source_id}") + r"\s*$" for source_id in SOURCE_IDS]
        if all(re.search(marker, log, re.MULTILINE) for marker in markers):
            validate_drain(log, args.run_id, args.service_url)
            return
        time.sleep(1)
    raise ValueError("Timed out waiting for successful drain of both sources; rebuild test-service if needed")


def measurement_received(config, args):
    reaction = next(reaction for reaction in config["test_run_host"]["test_runs"][0]["reactions"]
                    if reaction["test_reaction_id"] == QUERY_ID)
    logger = next(logger for logger in reaction["output_loggers"] if logger["kind"] == "PerformanceMetrics")
    target = logger["measurement_record_count"]
    require(type(target) is int and target > 0, "Missing positive performance measurement target")
    state = request(f"{args.service_url}/api/test_runs/{args.run_id}/reactions/{QUERY_ID}")
    observer = state["reaction_observer"]
    require(observer["status"] == "Running" and not observer.get("error_message"),
            "Reaction did not remain running through snapshot verification")
    return observer["result_summary"]["reaction_invocation_count"] >= target


def verify(args):
    report = {"schema_version": 1, "query_id": QUERY_ID, "passed": False,
              "scope": "input-derived-query-snapshot", "delivery": "not_verified"}
    artifacts = args.artifacts
    artifacts.mkdir(parents=True, exist_ok=True)
    try:
        require(args.timeout > 0, "Snapshot timeout must be positive")
        config = json.loads(args.config.read_text())
        server = load_server(args.server_config)
        validate_query(server)
        deadline = time.monotonic() + args.timeout
        wait_for_drain(args, deadline)
        run_storage = Path(config["data_store"]["data_store_path"]) / "test_runs" / args.run_id
        expected = expected_snapshot(config, run_storage)
        expected["run_id"] = args.run_id
        expected["query_sha256"] = hashlib.sha256(canonical(server["queries"]).encode()).hexdigest()
        report["sources"] = expected["sources"]
        report["expected_rows"] = len(expected["snapshot"])
        write_json(artifacts / "golden_snapshot.json", expected)
        last_error = None
        while time.monotonic() < deadline:
            check_processes(args)
            try:
                response = request(f"{args.admin_url}/api/v1/queries/{QUERY_ID}/results")
                require(isinstance(response, dict) and response.get("success") is True,
                        "Snapshot API did not return success:true")
                difference = compare_rows(response.get("data"), expected["snapshot"])
                write_json(artifacts / "actual_snapshot.json", response)
                report.update(difference, passed=False, actual_rows=len(response["data"]))
                last_error = None
                if difference["passed"] and measurement_received(config, args):
                    report["passed"] = True
                    break
            except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                last_error = str(error)
            time.sleep(1)
        if not report["passed"]:
            raise ValueError(f"Timed out waiting for the expected final query snapshot and measurement target; last network error: {last_error}")
    except (ValueError, KeyError, TypeError, StopIteration, OSError, subprocess.SubprocessError) as error:
        report.update(passed=False, error=str(error))
    write_json(artifacts / "snapshot_verdict.json", report)
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--server-config", type=Path, required=True)
    parser.add_argument("--service-log", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--service-url", required=True)
    parser.add_argument("--admin-url", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--service-pid", type=int)
    return verify(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())