# Copyright 2026 The Drasi Authors.
# Licensed under the Apache License, Version 2.0.

import copy
import unittest

from completion_boundary import IncompleteBoundary, QUERIES, query_progress, recorded_output

NOW = 100_000_000_000
START = 90_000_000_000
PID = 123


def tags(values):
    result = []
    for key, value in values.items():
        kind = "boolValue" if type(value) is bool else "stringValue"
        result.append({"key": key, "value": {kind: value}})
    return result


def sample():
    points = []
    for query in QUERIES:
        common = {
            "drasi.instance.id": "building-comfort-dynamic",
            "drasi.query.id": query,
            "drasi.component.generation": "7",
            "drasi.component.operation": "3",
            "drasi.query.output.generation": "0",
            "drasi.query.config_hash": "890",
            "drasi.progress.persistent": True,
        }
        for stage, sequence in (("input.committed", 100000), ("output.produced", 99981)):
            attributes = dict(common, **{"drasi.progress.stage": stage})
            if stage == "input.committed":
                attributes.update({"drasi.input.kind": "source", "drasi.input.id": "facilities-db"})
            points.append({"asInt": str(sequence), "timeUnixNano": str(NOW), "attributes": tags(attributes)})
    return {"resourceMetrics": [{
        "resource": {"attributes": tags({
            "service.name": "drasi-server", "service.instance.id": "new-process",
        }) + [{"key": "process.pid", "value": {"intValue": str(PID)}}]},
        "scopeMetrics": [{"scope": {"name": "drasi.progress"}, "metrics": [{
            "name": "drasi.query.progress.sequence", "gauge": {"dataPoints": points},
        }]}],
    }]}


def data_points(body):
    return body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["gauge"]["dataPoints"]


def set_tag(point, key, value):
    for tag in point["attributes"]:
        if tag["key"] == key:
            tag["value"] = {"boolValue" if type(value) is bool else "stringValue": value}
            return
    raise AssertionError(key)


def receivers():
    return {
        query: {"reaction_observer": {
            "status": "Running",
            "error_message": None,
            "result_summary": {
                "reaction_invocation_count": 99981,
                "recorded_batches": {query: {
                    "sequence": 99981, "item_count": 1, "recorded_at_ns": NOW,
                }},
            },
        }}
        for query in QUERIES
    }


class CompletionBoundaryTests(unittest.TestCase):
    def progress(self, body):
        return query_progress(body, PID, START, NOW, 100000)

    def test_fresh_exact_owner_progress_and_complete_batches_pass(self):
        proof = self.progress(sample())
        self.assertEqual(proof["queries"][QUERIES[0]]["input"], 100000)
        self.assertEqual(len(recorded_output(proof, receivers(), START, NOW)), 2)

    def test_full_unsigned_configuration_hash_is_an_identity_not_a_gauge(self):
        body = sample()
        for point in data_points(body):
            set_tag(point, "drasi.query.config_hash", str((1 << 64) - 1))
        self.progress(body)

    def test_missing_or_delayed_query_progress_does_not_pass(self):
        body = sample()
        data_points(body).pop()
        with self.assertRaises(IncompleteBoundary):
            self.progress(body)
        body = sample()
        data_points(body)[0]["asInt"] = "99999"
        with self.assertRaises(IncompleteBoundary):
            self.progress(body)
        data_points(body)[0]["asInt"] = "100000"
        self.progress(body)

    def test_stale_and_previous_process_samples_do_not_pass(self):
        for timestamp in (START - 1, NOW - 16_000_000_000):
            body = sample()
            data_points(body)[0]["timeUnixNano"] = str(timestamp)
            with self.assertRaises(IncompleteBoundary):
                self.progress(body)
        with self.assertRaises(IncompleteBoundary):
            query_progress(sample(), PID + 1, START, NOW, 100000)

    def test_future_mixed_epoch_duplicate_or_volatile_samples_fail(self):
        invalid = []
        body = sample()
        data_points(body)[0]["timeUnixNano"] = str(NOW + 1)
        invalid.append(body)
        body = sample()
        set_tag(data_points(body)[0], "drasi.query.output.generation", "1")
        invalid.append(body)
        body = sample()
        data_points(body).append(copy.deepcopy(data_points(body)[0]))
        invalid.append(body)
        body = sample()
        set_tag(data_points(body)[0], "drasi.progress.persistent", False)
        invalid.append(body)
        body = sample()
        data_points(body)[0]["asInt"] = "100001"
        invalid.append(body)
        body = sample()
        data_points(body)[0]["timeUnixNano"] = str(NOW - 1)
        invalid.append(body)
        for body in invalid:
            with self.subTest(body=body), self.assertRaises(ValueError):
                self.progress(body)

    def test_rounded_or_invalid_sequences_fail(self):
        for value in ("100000.0", "-1", "1e5", True, 100000, str(1 << 63)):
            body = sample()
            data_points(body)[0]["asInt"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.progress(body)

    def test_receiver_counts_without_batch_evidence_are_not_completion(self):
        received = receivers()
        received[QUERIES[0]]["reaction_observer"]["result_summary"]["recorded_batches"] = {}
        with self.assertRaises(IncompleteBoundary):
            recorded_output(self.progress(sample()), received, START, NOW)

    def test_delayed_recording_must_reach_the_produced_output(self):
        received = receivers()
        batch = received[QUERIES[0]]["reaction_observer"]["result_summary"]["recorded_batches"][QUERIES[0]]
        batch["sequence"] -= 1
        with self.assertRaises(IncompleteBoundary):
            recorded_output(self.progress(sample()), received, START, NOW)
        batch["sequence"] += 1
        recorded_output(self.progress(sample()), received, START, NOW)

    def test_stopped_failed_stale_and_partial_receivers_fail(self):
        for field, value in (("status", "Stopped"), ("status", "Error"), ("error_message", "disk full")):
            received = receivers()
            received[QUERIES[0]]["reaction_observer"][field] = value
            with self.assertRaises(ValueError):
                recorded_output(self.progress(sample()), received, START, NOW)
        for field, value in (("recorded_at_ns", START - 1), ("sequence", 99982), ("item_count", 0)):
            received = receivers()
            received[QUERIES[0]]["reaction_observer"]["result_summary"]["recorded_batches"][QUERIES[0]][field] = value
            with self.assertRaises(ValueError):
                recorded_output(self.progress(sample()), received, START, NOW)


if __name__ == "__main__":
    unittest.main()
