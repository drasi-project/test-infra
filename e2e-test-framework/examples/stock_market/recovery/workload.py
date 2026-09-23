"""Fixed, finite cross-source recovery workload and analytical snapshot oracle."""

import hashlib
import json
import os
from pathlib import Path
import subprocess


HERE = Path(__file__).resolve().parent
REFERENCE = HERE.parent / "ci/drasi_server_http_grpc_join"
RUN_ID = "drasi_server_dev_repo.stock_market_recovery.test_run_001"
QUERY_ID = "watchlist-prices"
SOURCE_IDS = ("stock-trades-db", "watchlist-db")
STOCKS = (("MSFT", "Microsoft"), ("AAPL", "Apple"), ("NVDA", "NVIDIA"))
QUERY = """MATCH (w:WatchlistItem)-[:WATCHES]->(s:Stock)
RETURN w.id AS WatchlistId, w.symbol AS Symbol, s.name AS Name,
       s.price AS Price, s.volume AS Volume,
       s.revision AS StockRevision, w.revision AS WatchRevision"""


def write_json(filename, value):
    filename.write_text(json.dumps(value, indent=2) + "\n")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def node(symbol, name, index, revision, watch):
    identifier = f"watch-{symbol}" if watch else symbol
    properties = {"id": identifier, "symbol": symbol, "revision": revision}
    if not watch:
        properties.update(name=name, price=10000 + index * 100 + revision,
                          volume=1000 + index + revision)
    return {"id": identifier, "labels": ["WatchlistItem" if watch else "Stock"],
            "properties": properties}


def golden_snapshot(golden):
    return [dict(row, WatchlistId=f"{row['WatchlistId']}-{copy_index}")
            for row in golden["snapshot_templates"]
            for copy_index in range(golden["watchlist_copies"])]


def records(source_id, rounds, watchlist_copies):
    watch = source_id == "watchlist-db"
    sequence = 0
    revisions = (0, rounds) if watch else range(rounds + 1)
    for revision in revisions:
        for index, (symbol, name) in enumerate(STOCKS):
            for copy_index in range(watchlist_copies if watch else 1):
                sequence += 1
                after = node(symbol, name, index, revision, watch)
                before = node(symbol, name, index, 0 if watch else revision - 1, watch) if revision else None
                if watch:
                    for value in (before, after):
                        if value:
                            value["id"] += f"-{copy_index}"
                            value["properties"]["id"] = value["id"]
                yield change(source_id, sequence, "u" if revision else "i", before, after)
    if watch:
        for copy_index in range(watchlist_copies):
            sequence += 1
            before = node("NVDA", "NVIDIA", 2, rounds, True)
            before["id"] += f"-{copy_index}"
            before["properties"]["id"] = before["id"]
            yield change(source_id, sequence, "d", before, None)


def change(source_id, sequence, operation, before, after):
    return {
        "kind": "SourceChange", "offset_ns": sequence * 1_000_000,
        "source_change_event": {
            "op": operation, "reactivatorStart_ns": 0, "reactivatorEnd_ns": 0,
            "payload": {
                "source": {"db": source_id, "table": "node", "ts_ns": 0, "lsn": sequence},
                "before": before, "after": after,
            },
        },
    }


def prepare(work, args):
    golden = json.loads((HERE / "golden.json").read_text())
    config = json.loads((REFERENCE / "config.json").read_text())
    repo = config["data_store"]["test_repos"][0]
    repo["source_path"] = str(work / "dev_repo")
    config["data_store"].update(data_store_path=str(work / "cache"), delete_on_start=False, delete_on_stop=False)
    test = repo["local_tests"][0]
    test.update(test_id="stock_market_recovery", test_folder="stock_market_recovery",
                description=golden["workload_id"], completion_handlers=[])
    scripts = {}
    for source in test["sources"]:
        source_id = source["test_source_id"]
        source.pop("model_data_generator", None)
        source["kind"] = "Script"
        source["source_change_generator"] = {
            "kind": "Script", "script_file_folder": "source_change_scripts",
            "spacing_mode": "none", "time_mode": "recorded", "ignore_scripted_pause_commands": False,
        }
        dispatcher = source["source_change_dispatchers"][0]
        dispatcher.update(source_id=source_id, batch_events=True,
                          adaptive_enabled=args.variant == "adaptive",
                          batch_size=args.batch_size, batch_timeout_ms=50,
                          port=args.http_port if source_id == "stock-trades-db" else args.grpc_port)
        if source_id == "stock-trades-db":
            dispatcher["url"] = "http://127.0.0.1"
        else:
            dispatcher["host"] = "127.0.0.1"
        directory = work / "dev_repo/stock_market_recovery/sources" / source_id / "source_change_scripts"
        directory.mkdir(parents=True)
        script = directory / "source_change_scripts_00000.jsonl"
        count = 0
        with script.open("w") as stream:
            stream.write(json.dumps({"kind": "Header", "start_time": "2025-01-03T10:00:00Z"}) + "\n")
            for record in records(source_id, golden["rounds"], golden["watchlist_copies"]):
                stream.write(json.dumps(record, sort_keys=True) + "\n")
                count += 1
        scripts[source_id] = {"sha256": hashlib.sha256(script.read_bytes()).hexdigest(), "events": count}
    test["reactions"][0]["stop_triggers"] = []
    test["reactions"][0]["output_handler"]["port"] = args.reaction_port
    run = config["test_run_host"]["test_runs"][0]
    run["test_id"] = "stock_market_recovery"
    run["reactions"][0]["start_immediately"] = False
    for source in run["sources"]:
        source.update(start_mode="manual", test_run_overrides={})
    write_json(work / "config.json", config)

    env = dict(os.environ, PERSIST_INDEX="true", STATE_STORE="true", WAL_MAX_EVENTS="100000",
               DRASI_ADMIN_PORT=str(args.admin_port), QUERY_TUNING="10000")
    subprocess.run(["ruby", str(REFERENCE / "render_server_config.rb"),
                    str(REFERENCE / "drasi_server_config.yaml"), str(work / "server.yaml")],
                   env=env, check=True)
    server = json.loads(subprocess.check_output(
        ["ruby", "-ryaml", "-rjson", "-e", "puts JSON.generate(YAML.load_file(ARGV.fetch(0)))",
         str(work / "server.yaml")], text=True))
    server.update(id="stock-market-recovery", persistConfig=True, enableUi=False)
    if getattr(args, "local_plugins", False):
        server["autoInstallPlugins"] = False
    server["queries"][0].update(query=QUERY, outboxCapacity=500000)
    for source in server["sources"]:
        source["port"] = args.http_port if source["kind"] == "http" else args.grpc_port
        source["host"] = "127.0.0.1"
    server["reactions"][0].update(baseUrl=f"http://127.0.0.1:{args.reaction_port}", recoveryPolicy="strict")
    (work / "data").mkdir()
    write_json(work / "server.yaml", server)
    query = server["queries"][0]
    query_fingerprint = digest(query)
    manifest = {"workload_id": golden["workload_id"], "golden_sha256": digest(golden),
                "scripts": scripts, "query": query, "normalization": "full-row-multiset-v1"}
    fingerprint = digest(manifest)
    write_json(work / "workload.json", dict(manifest, workload_fingerprint=fingerprint))
    artifact = {
        "schema_version": 1, "workload_fingerprint": fingerprint,
        "capture": {"complete": False, "evidence": golden["provenance"]},
        "queries": [{"query_id": QUERY_ID, "config_fingerprint": query_fingerprint,
                     "identity_contract": None, "events": [], "snapshot": golden_snapshot(golden)}],
    }
    write_json(work / "baseline.json", artifact)
    return artifact