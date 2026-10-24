# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.

import json
from pathlib import Path
import tempfile
import unittest

from completion_boundary import IncompleteBoundary, QUERIES
from qualify_building_comfort import recovered_cursors, source_state, verify_expected_snapshots


class QualificationTests(unittest.TestCase):
    def model(self):
        nodes = {
            "floor": {"id": "floor", "labels": ["Floor"], "properties": {}},
            "one": {"id": "one", "labels": ["Room"], "properties": {"temperature": 10, "humidity": 30, "co2": 100}},
            "two": {"id": "two", "labels": ["Room"], "properties": {"temperature": 20, "humidity": 40, "co2": 200}},
        }
        relations = {
            key: {"labels": ["FLOOR_ROOM"], "start_id": "floor", "end_id": key}
            for key in ("one", "two")
        }
        snapshots = {
            QUERIES[0]: {"data": [
                {"RoomId": "one", "Temperature": 10, "Humidity": 30, "Co2": 100},
                {"RoomId": "two", "Temperature": 20, "Humidity": 40, "Co2": 200},
            ]},
            QUERIES[1]: {"data": [
                {"FloorId": "floor", "AvgTemperature": 15, "MaxCo2": 200, "MinHumidity": 30, "RoomCount": 2},
            ]},
        }
        return snapshots, nodes, relations

    def test_independent_final_model_accepts_correct_results(self):
        verify_expected_snapshots(*self.model())

    def test_duplicate_floor_rows_are_not_blessed_as_a_reference(self):
        snapshots, nodes, relations = self.model()
        snapshots[QUERIES[1]]["data"] *= 2
        with self.assertRaises(ValueError):
            verify_expected_snapshots(snapshots, nodes, relations)

    def test_matching_row_counts_do_not_hide_wrong_aggregates(self):
        for field, value in (("RoomCount", 1), ("AvgTemperature", 20), ("MaxCo2", 100), ("MinHumidity", 40)):
            snapshots, nodes, relations = self.model()
            snapshots[QUERIES[1]]["data"][0][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                verify_expected_snapshots(snapshots, nodes, relations)

    def test_missing_source_records_cannot_prove_the_input_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            row = {"op": "i", "payload": {
                "source": {"db": "facilities-db", "table": "node", "lsn": 0},
                "after": {"id": "one", "labels": ["Room"], "properties": {}},
                "before": None,
            }}
            path = Path(directory) / "changes_00000.jsonl"
            path.write_text(json.dumps(row) + "\n")
            source_state(directory, 1)
            with self.assertRaises(ValueError):
                source_state(directory, 2)
            row["payload"]["source"]["lsn"] = 2
            path.write_text(json.dumps(row) + "\n")
            with self.assertRaises(ValueError):
                source_state(directory, 1)

    def test_reconstruction_must_report_actual_pending_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            lines = [
                f"Query '{query}' subscribing to gRPC source 'facilities-db' with WAL replay from seq {value}"
                for query, value in zip(QUERIES, (80, 100))
            ]
            path.write_text("\n".join(lines))
            self.assertEqual(recovered_cursors(path, 100)[QUERIES[0]], 80)
            path.write_text("\n".join(line.replace("seq 80", "seq 100") for line in lines))
            with self.assertRaises(ValueError):
                recovered_cursors(path, 100)
            path.write_text(lines[0])
            with self.assertRaises(IncompleteBoundary):
                recovered_cursors(path, 100)


if __name__ == "__main__":
    unittest.main()
