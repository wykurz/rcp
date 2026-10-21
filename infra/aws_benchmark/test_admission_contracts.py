"""Suite-specific admission and frozen proof contracts, without AWS or large fixtures."""

import copy
from datetime import timedelta
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks import verification
from infra.aws_benchmark import common, controller, operator, results, worker
from infra.aws_benchmark.test_controller import controller_cloud, run_controller
from infra.aws_benchmark.test_results import binding, bundle, measurement
from infra.aws_benchmark.test_safety import FakeCloud, NOW, window
from infra.aws_benchmark.test_worker_review import frozen_worker_time, report, worker_fixture


def metadata_window():
    lease = window(NOW)
    lease.update(suite="bulk-1g-metadata")
    lease["deadline"] = common.timestamp(common.utc(lease["opens"]) + timedelta(seconds=3600))
    return lease


def remaining(cloud, clock, seconds):
    deadline = common.utc(json.loads(cloud.objects[common.WINDOW_KEY])["deadline"]).timestamp()
    elapsed = deadline - seconds - clock["wall"]
    clock["wall"] += elapsed
    clock["monotonic"] += elapsed


class SuiteAdmissionTests(unittest.TestCase):
    def test_metadata_floors_preserve_measured_work_upload_and_startup_headroom(self):
        contract = common.admission_contract("bulk-1g-metadata")
        self.assertEqual(contract, dict(window=3600, launch=3520, worker=3400))
        self.assertEqual(contract["worker"], 616 + 4 * 622 + common.RESERVE + 116)
        self.assertEqual(contract["launch"] - contract["worker"], 120)
        self.assertEqual(common.admission_contract("aa-smoke"), dict(
            window=common.MIN_WINDOW, launch=common.LAUNCH_RESERVE, worker=common.WORKER_START_RESERVE))

    def test_metadata_operator_requires_full_hour_before_any_cloud_operation(self):
        for duration in (1200, 3599, 3600, 3601):
            with self.subTest(duration=duration):
                lease, cloud = metadata_window(), FakeCloud()
                lease["deadline"] = common.timestamp(common.utc(lease["opens"]) + timedelta(seconds=duration))
                if duration == 3600:
                    operator.arm(cloud.api, "group", "bucket", lease, now=lambda: NOW)
                    self.assertEqual(cloud.events[-1], "open-123-1")
                else:
                    with self.assertRaisesRegex(ValueError, "window"):
                        operator.arm(cloud.api, "group", "bucket", lease, now=lambda: NOW)
                    self.assertTrue(all(not api.mock_calls for api in cloud.api.values()))

    def test_metadata_controller_exact_launch_floor_and_one_second_late(self):
        for seconds in (3520, 3519):
            with self.subTest(seconds=seconds), tempfile.TemporaryDirectory() as directory:
                cloud, identity = controller_cloud(metadata_window())
                def launched(**kwargs):
                    if kwargs["DesiredCapacity"] == 1:
                        cloud.objects[common.STATUS_KEY] = json.dumps(common.completed_status(identity["run"], True)).encode()
                cloud.api["autoscaling"].set_desired_capacity.side_effect = launched
                with frozen_worker_time(cloud) as clock:
                    remaining(cloud, clock, seconds)
                    if seconds == 3520:
                        run_controller(cloud, identity, Path(directory))
                    else:
                        with self.assertRaisesRegex(ValueError, "deadline"):
                            run_controller(cloud, identity, Path(directory))
                capacities = [call.kwargs["DesiredCapacity"] for call in
                              cloud.api["autoscaling"].set_desired_capacity.call_args_list]
                self.assertEqual(capacities, [1, 0] if seconds == 3520 else [])

    def test_controller_rechecks_after_upload_and_receipt_before_paid_capacity(self):
        for stage in ("upload", "receipt"):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                cloud, identity = controller_cloud(metadata_window())
                root = Path(directory)
                save = controller.Receipt.save
                with frozen_worker_time(cloud) as clock:
                    remaining(cloud, clock, 3600)
                    def put(**kwargs):
                        cloud.put(**kwargs)
                        if stage == "upload" and kwargs["Key"] == common.REQUEST_KEY:
                            remaining(cloud, clock, 3519)
                    def slow_save(receipt, required=False):
                        save(receipt, required)
                        if stage == "receipt" and required and receipt.value["cleanup"] == "unconfirmed":
                            remaining(cloud, clock, 3519)
                    cloud.api["s3"].put_object.side_effect = put
                    with patch.object(controller.Receipt, "save", slow_save), self.assertRaisesRegex(TimeoutError, "admission"):
                        run_controller(cloud, identity, root)
                cloud.api["autoscaling"].set_desired_capacity.assert_not_called()
                self.assertEqual(json.loads((root / "out/controller.json").read_text())["cleanup"], "not-requested")
                self.assertEqual(cloud.actions[0]["StartTime"], metadata_window()["deadline"])

    def test_worker_exact_storage_floor_and_one_second_late(self):
        manifest = Path(__file__).resolve().parents[2] / "benchmarks/large-cases.json"
        for seconds in (3400, 3399):
            with self.subTest(seconds=seconds), worker_fixture("bulk-1g-metadata") as (root, cloud, measure, stop), \
                 frozen_worker_time(cloud) as clock, patch.object(worker.large_storage, "prepare", return_value={}) as prepare:
                remaining(cloud, clock, seconds)
                (root / "benchmarks").mkdir()
                (root / "benchmarks/large-cases.json").write_bytes(manifest.read_bytes())
                worker.run(cloud.api["s3"], "bucket")
                metadata, _ = report(cloud)
                self.assertEqual(metadata["admission"]["required_seconds"], 3400)
                self.assertEqual(metadata["admission"]["remaining_seconds"], seconds)
                self.assertEqual(metadata["admission"]["accepted"], seconds == 3400)
                self.assertEqual(prepare.call_count, int(seconds == 3400))
                self.assertEqual(measure.call_count, int(seconds == 3400))
                self.assertEqual(metadata["status"]["success"], seconds == 3400)

    def test_worker_download_delay_rejected_immediately_before_storage(self):
        manifest = Path(__file__).resolve().parents[2] / "benchmarks/large-cases.json"
        with worker_fixture("bulk-1g-metadata") as (root, cloud, measure, stop), \
             frozen_worker_time(cloud) as clock, patch.object(worker.large_storage, "prepare") as prepare:
            remaining(cloud, clock, 3500)
            (root / "benchmarks").mkdir()
            (root / "benchmarks/large-cases.json").write_bytes(manifest.read_bytes())
            def get(**kwargs):
                response = cloud.get(**kwargs)
                if kwargs["Key"] == "input/" + common.BINARIES[-1]:
                    remaining(cloud, clock, 3399)
                return response
            cloud.api["s3"].get_object.side_effect = get
            worker.run(cloud.api["s3"], "bucket")
            prepare.assert_not_called()
            worker.prepare_storage.assert_not_called()
            measure.assert_not_called()
            metadata, _ = report(cloud)
            self.assertFalse(metadata["admission"]["accepted"])
            self.assertEqual(metadata["admission"]["remaining_seconds"], 3399)


class FrozenVerificationTests(unittest.TestCase):
    def test_cloud_profiles_do_not_follow_local_default_tuning_or_mutate_on_read(self):
        expected = {suite: common.verification_contracts(suite) for suite in common.LARGE_SUITES}
        with patch.object(verification, "DEFAULT_WORKERS", 1), \
             patch.object(verification, "DEFAULT_INLINE_BYTES", 7):
            self.assertEqual({suite: common.verification_contracts(suite) for suite in common.LARGE_SUITES}, expected)
        changed = common.verification_contract("bulk-1g-metadata")
        changed["workers"] = 32
        self.assertEqual(common.verification_contract("bulk-1g-metadata"), expected["bulk-1g-metadata"][-1])

    def test_historical_twenty_minute_metadata_window_remains_readable(self):
        expected = binding("bulk-1g-metadata")
        lease = metadata_window()
        lease["baseline"], lease["candidate"] = expected["baseline"], expected["candidate"]
        lease["deadline"] = common.timestamp(common.utc(lease["opens"]) + timedelta(seconds=1200))
        request = {key: lease[key] for key in ("run", "baseline", "candidate", "suite", "deadline")}
        request["sha256"] = dict.fromkeys(common.BINARIES, "d" * 64)
        self.assertEqual(results.historical_inputs(dict(window=lease, request=request), expected), (lease, request))
        with self.assertRaisesRegex(ValueError, "suite requires"):
            common.validate_admission(lease, window=True)

    def test_old_recorded_contract_is_inspectable_without_invented_fields_but_not_approvable(self):
        expected = binding("bulk-1g-metadata")
        for contract in common.verification_contracts(expected["suite"])[:-1]:
            with self.subTest(contract=contract), tempfile.TemporaryDirectory() as directory:
                raw = measurement(expected)
                raw["context"]["verification"] = contract
                if "inline_bytes" not in contract:
                    for trial in raw["trials"]:
                        trial["source_validation"]["metadata"]["checked_fields"] = ["mode", "uid", "gid", "mtime_ns"]
                root = Path(directory)
                _, policy = bundle(root, expected, raw)
                evidence, parsed, _ = results.inspect(root, expected, "success")
                self.assertEqual(evidence["errors"], [])
                self.assertEqual(evidence["expected_verification"], contract)
                self.assertEqual(results.derived_record(evidence, parsed)["context"]["aws_provenance"]["expected_verification"], contract)
                with self.assertRaisesRegex(ValueError, "verification contract"):
                    results.approve(evidence, parsed, policy)
                self.assertFalse(evidence["history_eligible"])

    def test_current_contract_with_old_source_proof_is_never_approved(self):
        expected = binding("bulk-1g-metadata")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, policy = bundle(root, expected)
            evidence, parsed, _ = results.inspect(root, expected, "success")
            self.assertEqual(evidence["errors"], [])
            changed = copy.deepcopy(parsed)
            for trial in changed["trials"]:
                trial["source_validation"]["metadata"]["checked_fields"] = ["mode", "uid", "gid", "mtime_ns"]
            with self.assertRaises(ValueError):
                results.approve(evidence, changed, policy)
            self.assertFalse(evidence["history_eligible"])


if __name__ == "__main__":
    unittest.main()
