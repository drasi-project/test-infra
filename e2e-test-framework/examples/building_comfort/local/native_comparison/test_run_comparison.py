# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from run_comparison import (
    FLOORS, ROOMS, adapter_config, compare_pair, digest, observer_result, prepare_workload,
    receivers_ready, run_api_id, source_completed, verify_artifacts, native_artifacts,
    verify_native_catalogue
)


class ComparisonContractTests(unittest.TestCase):
    def setUp(self):
        self.ports = {"server": 61000, "service": 61001, "source": 61002, "receivers": [61003, 61004]}

    def test_workload_preserves_generator_and_protocol_for_both_transports(self):
        for transport in ["http", "grpc"]:
            config, expected, baselines = prepare_workload(
                transport, "both", 100000, Path("/evidence"), self.ports
            )
            definition = config["data_store"]["test_repos"][0]["local_tests"][0]
            source = definition["sources"][0]
            model = source["model_data_generator"]
            self.assertEqual(model["seed"], 123456789)
            self.assertEqual(model["change_count"], 100000)
            self.assertEqual(model["change_interval"], [2000000000, 500000000, 500000000, 4000000000])
            self.assertEqual(model["building_count"], [1, 0])
            self.assertEqual(model["floor_count"], [3, 0])
            self.assertEqual(model["room_count"], [4, 0])
            self.assertEqual(expected, {ROOMS: 99981, FLOORS: 49860})
            self.assertTrue(baselines[0]["expected"])
            self.assertFalse(config["data_store"]["delete_on_start"])
            self.assertFalse(config["data_store"]["delete_on_stop"])
            self.assertEqual(config["test_run_host"]["test_runs"][0]["sources"][0]["start_mode"], "manual")
            self.assertEqual(
                run_api_id(config),
                "drasi_server_dev_repo.building_comfort.native-comparison",
            )
            self.assertEqual(source["source_change_dispatchers"][0]["port"], 61002)
            self.assertEqual(len(source["source_change_dispatchers"]), 1)
            self.assertEqual([reaction["output_handler"]["port"] for reaction in definition["reactions"]], [61003, 61004])

    def test_rooms_only_has_exact_calibrated_count_and_matching_component_selection(self):
        for changes in [1000, 100000, 300000]:
            workload, expected, _ = prepare_workload("http", "rooms", changes, Path("/evidence"), self.ports)
            self.assertEqual(expected, {ROOMS: changes - 19})
            definition = workload["data_store"]["test_repos"][0]["local_tests"][0]
            self.assertEqual([item["test_reaction_id"] for item in definition["reactions"]], [ROOMS])
            self.assertEqual([item["query_id"] for item in definition["sources"][0]["subscribers"]], [ROOMS])
            config = adapter_config("http", "rooms", self.ports, 10000)
            self.assertEqual(len(config["instances"]), 1)
            instance = config["instances"][0]
            self.assertEqual(instance["id"], "performance")
            self.assertEqual([query["id"] for query in instance["queries"]], [ROOMS])
            self.assertEqual(len(instance["reactions"]), 1)
            self.assertFalse(instance["persistIndex"])
            self.assertFalse(config["autoInstallPlugins"])
            self.assertFalse(instance["queries"][0]["enableBootstrap"])
            self.assertEqual(instance["queries"][0]["priorityQueueCapacity"], 10000)
            self.assertTrue({"id", "queries", "reactions", "persistIndex"}.isdisjoint(config))

    def test_uncalibrated_aggregate_and_too_small_workloads_are_rejected(self):
        for selection, count in [("both", 1000), ("rooms", 31)]:
            with self.assertRaises(ValueError):
                prepare_workload("grpc", selection, count, Path("/evidence"), self.ports)

    def observer(self):
        return {"reaction_observer": {
            "status": "Stopped",
            "result_summary": {"reaction_invocation_count": 981},
            "logger_results": [
                {"logger_name": "PerformanceMetrics", "summary": {"record_count": 981, "duration_ns": 100}},
                {"logger_name": "DeterminismHash", "summary": {"record_count": 981, "sha256": "a" * 64}},
            ],
        }}

    def test_observer_requires_exact_counts_hash_and_valid_timing(self):
        body = self.observer()
        self.assertEqual(observer_result(body, 981)["sha256"], "a" * 64)
        for field in ["observer", "metrics", "hash", "duration"]:
            invalid = self.observer()
            if field == "observer":
                invalid["reaction_observer"]["result_summary"]["reaction_invocation_count"] = 980
            elif field == "metrics":
                invalid["reaction_observer"]["logger_results"][0]["summary"]["record_count"] = 980
            elif field == "hash":
                invalid["reaction_observer"]["logger_results"][1]["summary"]["sha256"] = "bad"
            else:
                invalid["reaction_observer"]["logger_results"][0]["summary"]["duration_ns"] = 0
            with self.assertRaises(RuntimeError):
                observer_result(invalid, 981)
        body["reaction_observer"]["status"] = "Running"
        self.assertIsNone(observer_result(body, 981))
        body["reaction_observer"]["status"] = "Error"
        with self.assertRaises(RuntimeError):
            observer_result(body, 981)

    def test_equivalence_failure_prevents_comparing_timings(self):
        left = {"transport": "grpc", "results": {ROOMS: observer_result(self.observer(), 981)}}
        right = copy.deepcopy(left)
        compare_pair(left, right)
        for change in ["count", "hash", "transport", "queries"]:
            invalid = copy.deepcopy(right)
            if change == "count":
                invalid["results"][ROOMS]["record_count"] -= 1
            elif change == "hash":
                invalid["results"][ROOMS]["sha256"] = "b" * 64
            elif change == "transport":
                invalid["transport"] = "http"
            else:
                invalid["results"].clear()
            with self.assertRaises(RuntimeError):
                compare_pair(left, invalid)

    def test_source_must_finish_exact_workload_without_skips(self):
        body = {"source_change_generator": {"status": "Finished", "state": {
            "error_messages": [], "stats": {"num_source_change_events": 1000, "num_skipped_source_change_events": 0}
        }}}
        self.assertTrue(source_completed(body, 1000))
        with self.assertRaises(RuntimeError):
            source_completed(body, 999)
        body["source_change_generator"]["state"]["stats"]["num_skipped_source_change_events"] = 1
        with self.assertRaises(RuntimeError):
            source_completed(body, 1000)
        body["source_change_generator"]["status"] = "Stopped"
        self.assertFalse(source_completed(body, 1000), "manual stop is not successful workload completion")

    def test_changed_binaries_and_plugin_set_invalidate_the_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "server"
            binary.write_bytes(b"same server")
            plugins = root / "plugins"
            plugins.mkdir()
            plugin = plugins / "network.so"
            plugin.write_bytes(b"same network plugin")
            binary_hashes = {"server": {"path": str(binary), "sha256": digest(binary)}}
            plugin_hashes = {plugin.name: digest(plugin)}
            verify_artifacts(binary_hashes, plugins, plugin_hashes)
            binary.write_bytes(b"rebuilt server")
            with self.assertRaisesRegex(RuntimeError, "binary changed"):
                verify_artifacts(binary_hashes, plugins, plugin_hashes)
            binary.write_bytes(b"same server")
            plugin.write_bytes(b"rebuilt plugin")
            with self.assertRaisesRegex(RuntimeError, "plugin directory changed"):
                verify_artifacts(binary_hashes, plugins, plugin_hashes)
            plugin.write_bytes(b"same network plugin")
            (plugins / "extra.so").write_bytes(b"newly discovered plugin")
            with self.assertRaisesRegex(RuntimeError, "plugin directory changed"):
                verify_artifacts(binary_hashes, plugins, plugin_hashes)

    def test_receiver_readiness_requires_running_observers(self):
        with patch("run_comparison.request") as request:
            for status, expected in [("Stopped", False), ("Running", True)]:
                request.return_value = {"reaction_observer": {"status": status}}
                self.assertEqual(receivers_ready("http://service/run", [ROOMS]), expected)
            request.assert_called_with(f"http://service/run/reactions/{ROOMS}")
            request.return_value = {"reaction_observer": {"status": "Error"}}
            with self.assertRaisesRegex(RuntimeError, "receiver startup failed"):
                receivers_ready("http://service/run", [ROOMS])

    def test_native_directory_requires_each_exact_family_library(self):
        with tempfile.TemporaryDirectory() as directory, patch("run_comparison.platform.system", return_value="Linux"):
            root = Path(directory)
            native, installed = root / "native", root / "installed"
            native.mkdir()
            installed.mkdir()
            with self.assertRaisesRegex(RuntimeError, "missing"):
                native_artifacts(native, installed, ["http"])
            for role in ("source", "sink"):
                name = f"libdrasi_computation_{role}_http.so"
                (native / name).write_bytes(role.encode())
                (installed / name).write_bytes(role.encode())
            self.assertEqual(len(native_artifacts(native, installed, ["http"])), 2)
            (installed / "libdrasi_computation_sink_http.so").write_bytes(b"stale")
            with self.assertRaisesRegex(RuntimeError, "differs"):
                native_artifacts(native, installed, ["http"])

    def test_live_native_identity_versions_and_factory_match_the_generated_components(self):
        plugins, components = [], []
        for role in ("source", "sink"):
            identity = {"id": f"drasi-computation-{role}-http", "version": "test"}
            implementation = {"name": f"drasi.network/http-{role}", "version": "1", "plugin": identity}
            metadata = {"implementation": implementation, "configuration_version": 1,
                        "ports": [], "completion": "Handled" if role == "sink" else None,
                        "role": role.title()}
            plugins.append({"plugin": identity, "abi_version": "1.0.0", "wire_version": 2,
                            "factories": [metadata]})
            specification = {**metadata, "descriptor": {"ports": []}}
            components.append({"construction": {"Factory": specification}})
        catalogue, definition = {"plugins": plugins}, {"components": components}
        verify_native_catalogue(catalogue, definition, "http")
        for change in ("missing", "wire", "factory", "identity"):
            altered = copy.deepcopy(catalogue)
            if change == "missing":
                altered["plugins"].pop()
            elif change == "wire":
                altered["plugins"][0]["wire_version"] = 1
            elif change == "factory":
                altered["plugins"][0]["factories"][0]["implementation"]["name"] = "unrelated"
            else:
                altered["plugins"][0]["factories"][0]["implementation"]["plugin"]["version"] = "other"
            with self.assertRaises(RuntimeError):
                verify_native_catalogue(altered, definition, "http")

    def test_local_profiles_use_only_native_network_factories_and_keep_their_protocol_settings(self):
        root = Path(__file__).resolve().parent.parent
        for profile in ("http", "http_adaptive", "grpc", "grpc_adaptive"):
            config = json.loads((root / f"drasi_server_{profile}" / "drasi_server_config.yaml").read_text())
            self.assertFalse(config["autoInstallPlugins"])
            instance = config["instances"][0]
            self.assertEqual(instance["sources"], [])
            self.assertEqual(instance["queries"], [])
            self.assertEqual(instance["reactions"], [])
            specifications = [node["construction"]["Factory"]
                              for node in instance["computation"]["definition"]["components"]]
            transport = profile.split("_")[0]
            self.assertEqual([spec["implementation"]["name"] for spec in specifications].count(
                f"drasi.network/{transport}-source"), 1)
            sinks = [spec for spec in specifications
                     if spec["implementation"]["name"] == f"drasi.network/{transport}-sink"]
            self.assertEqual(len(sinks), 2)
            for spec in sinks:
                settings = {name: value["Literal"] for name, value in spec["configuration"].items()}
                self.assertEqual(settings["failurePolicy"], "strict")
                if profile == "grpc_adaptive":
                    self.assertEqual(settings["batchSize"], 100)
                    self.assertEqual(settings["batchFlushTimeoutMs"], 50)
                elif transport == "http":
                    self.assertIn("outputTemplates", settings)


if __name__ == "__main__":
    unittest.main()
