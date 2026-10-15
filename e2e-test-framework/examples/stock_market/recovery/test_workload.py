import argparse
import json
from pathlib import Path
import tempfile
import unittest

import workload


class WorkloadTest(unittest.TestCase):
    def test_scripted_terminal_state_matches_versioned_golden(self):
        golden = json.loads((workload.HERE / "golden.json").read_text())
        state = {}
        for source_id in workload.SOURCE_IDS:
            state[source_id] = {}
            previous = 0
            for record in workload.records(source_id, golden["rounds"], golden["watchlist_copies"]):
                event = record["source_change_event"]
                payload = event["payload"]
                self.assertEqual(previous + 1, payload["source"]["lsn"])
                previous += 1
                before, after = payload["before"], payload["after"]
                if before:
                    self.assertEqual(before, state[source_id][before["id"]])
                if after:
                    state[source_id][after["id"]] = after
                else:
                    del state[source_id][before["id"]]
        expected = []
        for watch in state["watchlist-db"].values():
            properties = watch["properties"]
            stock = state["stock-trades-db"][properties["symbol"]]["properties"]
            expected.append({"WatchlistId": properties["id"], "Symbol": properties["symbol"],
                             "Name": stock["name"], "Price": stock["price"], "Volume": stock["volume"],
                             "StockRevision": stock["revision"], "WatchRevision": properties["revision"]})
        self.assertCountEqual(expected, workload.golden_snapshot(golden))

    def test_variants_share_inputs_and_golden_without_performance_cutoff(self):
        artifacts = []
        for variant in ("standard", "adaptive"):
            with tempfile.TemporaryDirectory() as directory:
                work = Path(directory)
                args = argparse.Namespace(variant=variant, batch_size=10000, admin_port=8091,
                                          http_port=9005, grpc_port=50061, reaction_port=9006)
                artifacts.append(workload.prepare(work, args))
                config = json.loads((work / "config.json").read_text())
                test = config["data_store"]["test_repos"][0]["local_tests"][0]
                self.assertEqual([], test["reactions"][0]["stop_triggers"])
                self.assertFalse(config["test_run_host"]["test_runs"][0]["reactions"][0]["start_immediately"])
                self.assertTrue(all(source["start_mode"] == "manual" for source in config["test_run_host"]["test_runs"][0]["sources"]))
                for source in test["sources"]:
                    self.assertEqual("Script", source["kind"])
                    dispatcher = source["source_change_dispatchers"][0]
                    self.assertEqual(variant == "adaptive", dispatcher["adaptive_enabled"])
                    self.assertEqual(source["test_source_id"], dispatcher["source_id"])
                server = json.loads((work / "server.yaml").read_text())
                self.assertTrue(server["persistConfig"])
                self.assertTrue(server["persistIndex"])
                self.assertEqual("redb", server["stateStore"]["kind"])
                self.assertTrue(all(source["durability"]["enabled"] for source in server["sources"]))
                self.assertEqual("strict", server["reactions"][0]["recoveryPolicy"])
        self.assertEqual(artifacts[0], artifacts[1])


if __name__ == "__main__":
    unittest.main()