"""Recorded durable contracts stay readable without granting current qualification."""

import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from infra.aws_benchmark import common, results
from infra.aws_benchmark.test_durable_worker import durable_bundle, direct_window, request
from infra.aws_benchmark.test_results import archive, binding


def replace_worker(root, policy, mutate):
    with zipfile.ZipFile(io.BytesIO((root / "results.zip").read_bytes())) as saved:
        entries = [(item.filename, saved.read(item)) for item in saved.infolist()]
    metadata = json.loads(next(body for name, body in entries if name == "worker.json"))
    mutate(metadata)
    data = archive([(name, results.encoded(metadata) if name == "worker.json" else body)
                    for name, body in entries])
    status = json.loads((root / "status.json").read_bytes())
    status["sha256"] = common.digest(data)
    receipt = json.loads((root / "controller.json").read_bytes())
    receipt.update(archive_sha256=common.digest(data), status_sha256=common.digest(results.encoded(status)))
    for name, content in (("results.zip", data), ("status.json", results.encoded(status)),
                          ("controller.json", results.encoded(receipt))):
        (root / name).write_bytes(content)
    policy["archive_sha256"] = common.digest(data)


class RecordedDurableResultTests(unittest.TestCase):
    def test_current_policy_changes_do_not_reinterpret_historical_bundles(self):
        mutations = {
            "DURABLE_PLAN": (dict(common.DURABLE_PLAN, version="bulk-metadata-plan-v2"), "not current"),
            "DURABLE_MAX_RUNTIME": (10000, "runtime does not fit"),
            "DURABLE_MAX_PROVISIONING": (60, "provisioning deadline"),
            "BINARIES": (common.BINARIES + ("candidate/future-tool",), "unexpected binary list"),
        }
        for name, (value, reason) in mutations.items():
            with self.subTest(policy=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, output = root / "source", root / "report"
                expected, policy = durable_bundle(source, direct=True)
                original = (source / "results.zip").read_bytes()
                before, raw, _ = results.inspect(source, expected, "success")
                self.assertEqual(before["errors"], [])
                with patch.object(common, name, value):
                    evidence = results.ingest(source, output, expected, "success")
                    self.assertEqual(evidence["errors"], [])
                    self.assertFalse(evidence["history_eligible"])
                    derived = json.loads((output / "derived.json").read_bytes())
                    self.assertEqual(derived["context"]["verification"], raw["context"]["verification"])
                    self.assertEqual(derived["trials"], raw["trials"])
                    self.assertTrue((output / "dashboard/index.html").is_file())
                    self.assertEqual((output / "raw/results.zip").read_bytes(), original)
                    with self.assertRaisesRegex(ValueError, reason):
                        results.validate_qualification(before, raw, policy)

    def test_recorded_contract_is_an_independent_versioned_snapshot(self):
        original = common.durable_plan_contract(common.DURABLE_PLAN, exact=False)
        changed = common.durable_plan_contract(common.DURABLE_PLAN, exact=False)
        changed["plan"]["work_seconds"] = 1
        changed["binaries"] = ()
        changed["max_runtime"] = 1
        self.assertEqual(common.durable_plan_contract(common.DURABLE_PLAN, exact=False), original)
        for plan in (dict(common.DURABLE_PLAN, version="unknown"),
                     dict(common.DURABLE_PLAN, work_seconds=3103),
                     dict(common.DURABLE_PLAN, version=[])):
            with self.subTest(plan=plan), self.assertRaisesRegex(ValueError, "unknown durable workload plan"):
                common.durable_plan_contract(plan, exact=False)

    def test_historical_manifest_and_clock_bindings_still_require_the_recorded_contract(self):
        expected = binding("bulk-1g-metadata")
        expected["candidate"] = expected["baseline"]
        window = direct_window()
        window.update({key: expected[key] for key in ("run", "baseline", "candidate", "suite")})
        window["attempt_id"] = window["run"] + "-1"
        receipt = dict(window=window, request=request(window))
        self.assertEqual(results.historical_inputs(receipt, expected), (window, receipt["request"]))
        mutations = (
            (lambda value: value["request"]["sha256"].pop("baseline/rcp"), "recorded durable binary manifest"),
            (lambda value: value["window"].update(outer_deadline=value["window"]["deadline"]), "reconciliation horizon"),
            (lambda value: value["window"]["plan"].update(work_seconds=3103), "unknown durable workload plan"),
        )
        for mutate, reason in mutations:
            with self.subTest(reason=reason):
                changed = copy.deepcopy(receipt)
                mutate(changed)
                with self.assertRaisesRegex(ValueError, reason):
                    results.historical_inputs(changed, expected)

    def test_historical_attempt_bound_is_independent_of_current_attempt_admission(self):
        window = direct_window()
        window["attempt_id"] = window["run"] + "-720"
        with patch.object(common, "DIRECT_MAX_ATTEMPTS", 1):
            self.assertEqual(common.validate_durable_window(window, now=common.utc(window["launch_at"]),
                                                           minimum=0, exact=False), common.utc(window["deadline"]))
            with self.assertRaisesRegex(ValueError, "invalid direct attempt identity"):
                common.validate_durable_window(window, now=common.utc(window["launch_at"]), minimum=0)
        for suffix in ("0", "01", "721", "1.0", "1x", "9" * 400):
            with self.subTest(suffix=suffix), self.assertRaisesRegex(ValueError, "invalid direct attempt identity"):
                common.validate_attempt_id(window["run"], window["run"] + "-" + suffix)

    def test_oversized_or_impossible_remaining_time_is_diagnostic_without_escaping_ingest(self):
        for remaining in (10**400, 10801, -1, True):
            with self.subTest(remaining=remaining), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, output = root / "source", root / "report"
                expected, policy = durable_bundle(source, direct=True)
                replace_worker(source, policy, lambda worker: worker["admission"].update(remaining_seconds=remaining))
                evidence = results.ingest(source, output, expected, "success", qualification=policy)
                self.assertFalse(evidence["qualification_accepted"])
                self.assertEqual(evidence["errors"], [
                    "qualification: durable admission does not prove the disclosed workload headroom"])
                self.assertFalse(evidence["history_eligible"])
                self.assertTrue((output / "dashboard/index.html").is_file())
                self.assertEqual((output / "raw/results.zip").read_bytes(), (source / "results.zip").read_bytes())

    def test_recorded_timing_text_is_copied_verbatim_and_missing_values_stay_unknown(self):
        for direct in (False, True):
            with self.subTest(direct=direct), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                expected, _ = durable_bundle(root, direct=direct)
                evidence, raw, _ = results.inspect(root, expected, "success")
                worker = evidence["worker"]
                worker.update(timing_basis="exact original supervisor wording", runtime_origin="recorded clock origin")
                timing = results.derived_record(evidence, raw)["context"]["aws_provenance"]["timing"]
                self.assertEqual(timing["basis"], worker["timing_basis"])
                self.assertEqual(timing["runtime_origin"], worker["runtime_origin"])
                worker.pop("timing_basis")
                worker.pop("runtime_origin")
                missing = results.derived_record(evidence, raw)["context"]["aws_provenance"]["timing"]
                self.assertIsNone(missing["basis"])
                self.assertIsNone(missing["runtime_origin"])


if __name__ == "__main__":
    unittest.main()
