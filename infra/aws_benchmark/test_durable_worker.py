"""Explicit guest clock origins and historical result boundaries, entirely offline."""

import copy
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import yaml

from infra.aws_benchmark import common, results, worker
from infra.aws_benchmark.test_results import archive, binding, bundle, measurement
from infra.aws_benchmark.test_safety import NOW, window
from infra.aws_benchmark.test_worker_review import frozen_worker_time, report, worker_fixture


def durable_window(launch=NOW):
    value = window(launch)
    value.update(suite="bulk-1g-metadata", protocol=common.DURABLE_PROTOCOL,
        instance_id="i-" + "1" * 17, launch_at=common.timestamp(launch),
        opens=common.timestamp(launch), runtime_seconds=10800,
        request_expires=common.timestamp(launch + timedelta(hours=12)),
        outer_deadline=common.timestamp(launch + timedelta(hours=15)),
        deadline=common.timestamp(launch + timedelta(seconds=10500)), plan=dict(common.DURABLE_PLAN))
    return value


def request(window):
    value = {key: window[key] for key in ("run", "baseline", "candidate", "suite", "deadline")}
    value["sha256"] = dict.fromkeys(common.BINARIES, common.digest(b"binary"))
    return value


@contextmanager
def durable_fixture():
    with worker_fixture("bulk-1g-metadata") as (root, cloud, measure, stop):
        lease = durable_window(datetime.now(timezone.utc) - timedelta(seconds=90))
        cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
        cloud.objects[common.REQUEST_KEY] = json.dumps(request(lease)).encode()
        (root / "benchmarks").mkdir()
        manifest = Path(__file__).resolve().parents[2] / "benchmarks/large-cases.json"
        (root / "benchmarks/large-cases.json").write_bytes(manifest.read_bytes())
        with frozen_worker_time(cloud) as clock, \
             patch.object(worker, "DURABLE_STATE", root / "persistent"), \
             patch.object(worker, "instance_identity", return_value=lease["instance_id"]) as identity, \
             patch.object(worker.large_storage, "prepare", return_value={}) as prepare:
            clock["wall"] = common.utc(lease["launch_at"]).timestamp() + 90
            yield root, cloud, measure, clock, identity, prepare


def advance(clock, seconds):
    clock["wall"] += seconds
    clock["monotonic"] += seconds


class WorkerProtocolTemplateTests(unittest.TestCase):
    def test_only_explicit_durable_selection_changes_timeout_and_worker_mode(self):
        template = yaml.safe_load((Path(__file__).with_name("stack.yaml")).read_text())
        parameter = template["Parameters"]["WorkerProtocol"]
        self.assertEqual(parameter["Default"], "operator-window")
        self.assertEqual(parameter["AllowedValues"], ["operator-window", "durable", "direct"])
        data = template["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]
        self.assertEqual(template["Conditions"]["DirectWorker"], {"Fn::Equals": [{"Ref": "WorkerProtocol"}, "direct"]})
        self.assertEqual(data["InstanceInitiatedShutdownBehavior"],
                         {"Fn::If": ["DirectWorker", "terminate", {"Ref": "AWS::NoValue"}]})
        user_data = data["UserData"]["Fn::Base64"]["Fn::Sub"]
        for protocol, limit in ((parameter["Default"], 3900), ("durable", 10800), ("direct", 10800)):
            with self.subTest(protocol=protocol):
                script = user_data.replace("${WorkerProtocol}", protocol).replace("${Artifacts}", "bucket").replace(
                    "${AWS::Region}", "us-east-1")
                stubs = "systemd-run() { printf '%s\\n' \"$@\"; }; systemctl() { exit 99; };\n"
                result = subprocess.run(["bash", "-c", stubs + script], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                argv = result.stdout.splitlines()
                self.assertIn(f"--property=RuntimeMaxSec={limit}", argv)
                self.assertEqual("--durable" in argv, protocol == "durable")
                self.assertEqual("--direct" in argv, protocol == "direct")
                self.assertIn("--property=FailureAction=poweroff", argv)
                self.assertIn("--property=SuccessAction=poweroff", argv)
                self.assertIn("--property=KillMode=control-group", argv)
                self.assertEqual(argv.count("--unit=rcp-supervisor"), 1)
        self.assertEqual(template["Resources"]["WorkerGroup"]["Properties"]["MinSize"], "0")
        self.assertEqual(template["Resources"]["WorkerGroup"]["Properties"]["DesiredCapacity"], "0")
        self.assertEqual(template["Resources"]["WorkerGroup"]["Properties"]["MaxSize"], "0")


class DurableWindowTests(unittest.TestCase):
    def test_projection_and_threefold_headroom_are_disclosed_separately_from_cap(self):
        lease = durable_window()
        self.assertEqual(sum(lease["plan"][key] for key in ("work_seconds", "setup_seconds", "report_seconds")), 3384)
        self.assertEqual(common.durable_worker_reserve(lease["plan"]), 9852)
        self.assertEqual(common.validate_window(lease, NOW), common.utc(lease["deadline"]))
        common.validate_admission(lease, window=True)
        self.assertEqual(lease["runtime_seconds"], 10800)

    def test_runtime_bounds_and_shutdown_reserve_do_not_extend_legacy_window(self):
        for duration in (10451, 10452, 10800, 10801, True):
            with self.subTest(duration=duration):
                lease = durable_window()
                lease.update(runtime_seconds=duration,
                    deadline=common.timestamp(NOW + timedelta(seconds=duration - 300)),
                    outer_deadline=common.timestamp(NOW + timedelta(hours=12, seconds=duration)))
                if duration in (10452, 10800):
                    common.validate_window(lease, NOW)
                else:
                    with self.assertRaisesRegex(ValueError, "runtime"):
                        common.validate_window(lease, NOW)
        legacy = {key: durable_window()[key] for key in ("run", "baseline", "candidate", "suite", "opens", "deadline")}
        with self.assertRaisesRegex(ValueError, "window must"):
            common.validate_window(legacy, NOW)

    def test_mutated_plan_schema_or_launch_binding_is_rejected(self):
        cases = {
            "extra": lambda value: value.update(extra=True),
            "protocol": lambda value: value.update(protocol="rcp-durable-v2"),
            "suite": lambda value: value.update(suite="bulk-1g"),
            "candidate": lambda value: value.update(candidate="b" * 40),
            "instance": lambda value: value.update(instance_id="i-123"),
            "plan": lambda value: value["plan"].update(work_seconds=3103),
            "plan boolean": lambda value: value["plan"].update(safety_factor=True),
            "launch": lambda value: value.update(launch_at=common.timestamp(NOW + timedelta(seconds=1))),
            "outer": lambda value: value.update(outer_deadline=common.timestamp(NOW + timedelta(hours=16))),
            "worker deadline": lambda value: value.update(deadline=common.timestamp(NOW + timedelta(hours=3))),
            "arrival after expiry": lambda value: value.update(request_expires=common.timestamp(NOW)),
            "too distant expiry": lambda value: value.update(request_expires=common.timestamp(NOW + timedelta(hours=13))),
            "missing plan": lambda value: value.pop("plan"),
        }
        for name, change in cases.items():
            with self.subTest(name=name):
                lease = durable_window()
                change(lease)
                with self.assertRaises(ValueError):
                    common.validate_window(lease, NOW)

    def test_future_launch_and_expired_worker_have_no_fresh_runtime(self):
        lease = durable_window()
        for now in (NOW - timedelta(seconds=1), common.utc(lease["deadline"]) + timedelta(seconds=1)):
            with self.subTest(now=now), self.assertRaises(ValueError):
                common.validate_window(lease, now, minimum=0)
        self.assertEqual(common.validate_window(lease, common.utc(lease["deadline"]), minimum=0),
                         common.utc(lease["deadline"]))

    def test_binary_equality_is_required_before_any_durable_work(self):
        lease = durable_window()
        value = request(lease)
        value["sha256"]["candidate/rcp"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "A/A binary"):
            common.validate_request(value, lease)

    def test_budget_uses_launch_deadline_and_monotonic_ceiling_after_clock_rollback(self):
        clock = {"wall": 1000, "monotonic": 100}
        with patch.object(common.time, "time", side_effect=lambda: clock["wall"]), \
             patch.object(common.time, "monotonic", side_effect=lambda: clock["monotonic"]):
            end = datetime.fromtimestamp(11000, timezone.utc)
            legacy = common.Budget(end)
            durable = common.Budget(end, ceiling_seconds=10800)
            self.assertEqual(legacy.left(), 3900)
            self.assertEqual(durable.left(), 10000)
            clock.update(wall=0, monotonic=200)
            self.assertEqual(durable.left(), 9900)
            clock["monotonic"] = 10100
            with self.assertRaises(TimeoutError):
                durable.left()
        for ceiling in (0, 10801, True, float("inf")):
            with self.subTest(ceiling=ceiling), self.assertRaises(ValueError):
                common.Budget(end, ceiling_seconds=ceiling)


class DurableGuestTests(unittest.TestCase):
    def test_complete_worker_discloses_binding_and_keeps_metadata_copy_contract(self):
        with durable_fixture() as (root, cloud, measure, clock, identity, prepare):
            worker.run(cloud.api["s3"], "bucket", durable=True)
            metadata, _ = report(cloud)
            self.assertTrue(metadata["status"]["success"])
            self.assertEqual(metadata["guest_instance_id"], metadata["window"]["instance_id"])
            self.assertEqual(metadata["admission"]["required_seconds"], 9852)
            self.assertEqual(metadata["admission"]["remaining_seconds"], 10410)
            self.assertIn("not a measured complete run", metadata["timing_basis"])
            prepare.assert_called_once()
            identity.assert_called_once()
            argv = measure.call_args.args[0]
            self.assertIn("--property=RuntimeMaxSec=10230", argv)
            self.assertEqual(argv[argv.index("--verification-mode") + 1], "metadata-size")
            self.assertEqual(argv[argv.index("--repetitions") + 1], "2")
            self.assertEqual(argv[argv.index("--cache") + 1], "linux-drop-caches")
            marker = json.loads((root / "persistent/durable-started.json").read_text())
            self.assertEqual(marker["deadline"], metadata["window"]["deadline"])

    def test_exact_derived_admission_floor_is_inclusive(self):
        for remaining in (9852, 9851):
            with self.subTest(remaining=remaining), durable_fixture() as (_, cloud, measure, clock, _, prepare):
                deadline = common.utc(json.loads(cloud.objects[common.WINDOW_KEY])["deadline"]).timestamp()
                advance(clock, deadline - remaining - clock["wall"])
                worker.run(cloud.api["s3"], "bucket", durable=True)
                metadata, _ = report(cloud)
                self.assertEqual(metadata["admission"]["accepted"], remaining == 9852)
                self.assertEqual(prepare.call_count, int(remaining == 9852))
                self.assertEqual(measure.call_count, int(remaining == 9852))
                self.assertEqual(metadata["status"]["success"], remaining == 9852)

    def test_slow_download_rechecks_derived_floor_before_formatting(self):
        with durable_fixture() as (_, cloud, measure, clock, _, prepare):
            def get(**kwargs):
                value = cloud.get(**kwargs)
                if kwargs["Key"] == "input/" + common.BINARIES[-1]:
                    advance(clock, 559)
                return value
            cloud.api["s3"].get_object.side_effect = get
            worker.run(cloud.api["s3"], "bucket", durable=True)
            metadata, _ = report(cloud)
            self.assertFalse(metadata["admission"]["accepted"])
            self.assertEqual(metadata["admission"]["remaining_seconds"], 9851)
            prepare.assert_not_called()
            measure.assert_not_called()

    def test_reboot_or_reentry_cannot_replay_a_consumed_assignment(self):
        with durable_fixture() as (root, cloud, measure, clock, _, prepare):
            worker.run(cloud.api["s3"], "bucket", durable=True)
            marker = (root / "persistent/durable-started.json").read_bytes()
            worker.run(cloud.api["s3"], "bucket", durable=True)
            metadata, _ = report(cloud)
            self.assertFalse(metadata["status"]["success"])
            self.assertEqual(metadata["status"]["error"]["type"], "FileExistsError")
            self.assertEqual((root / "persistent/durable-started.json").read_bytes(), marker)
            self.assertEqual(measure.call_count, 1)
            self.assertEqual(prepare.call_count, 1)

    def test_stale_other_instance_cannot_write_results_or_prepare_storage(self):
        with durable_fixture() as (root, cloud, measure, clock, identity, prepare):
            identity.return_value = "i-" + "2" * 17
            with patch.object(worker.time, "sleep", side_effect=lambda seconds: advance(clock, seconds)), \
                 self.assertRaises(TimeoutError):
                worker.run(cloud.api["s3"], "bucket", durable=True)
            self.assertFalse((root / "persistent").exists())
            self.assertNotIn(common.STATUS_KEY, cloud.objects)
            self.assertNotIn(common.RESULTS_KEY, cloud.objects)
            prepare.assert_not_called()
            measure.assert_not_called()
            self.assertEqual(worker.signal.setitimer.call_count, 1)
            self.assertEqual(worker.signal.setitimer.call_args.args, (signal.ITIMER_REAL, 120))

    def test_matching_assignment_can_arrive_after_legacy_bootstrap_minute(self):
        with durable_fixture() as (_, cloud, measure, clock, _, prepare):
            matching = cloud.objects[common.WINDOW_KEY]
            stale = json.loads(matching)
            stale["instance_id"] = "i-" + "2" * 17
            cloud.objects[common.WINDOW_KEY] = json.dumps(stale).encode()
            started = clock["wall"]
            def wait(seconds):
                advance(clock, seconds)
                if clock["wall"] - started > 60:
                    cloud.objects[common.WINDOW_KEY] = matching
            with patch.object(worker.time, "sleep", side_effect=wait):
                worker.run(cloud.api["s3"], "bucket", durable=True)
            metadata, _ = report(cloud)
            self.assertTrue(metadata["status"]["success"])
            self.assertEqual(metadata["admission"]["remaining_seconds"], 10347)
            self.assertEqual(worker.signal.setitimer.call_args_list[1].args, (signal.ITIMER_REAL, 10347))
            prepare.assert_called_once()
            measure.assert_called_once()

    def test_missing_assignment_is_polled_without_admitting_unbound_work(self):
        class Missing(Exception):
            response = {"Error": {"Code": "NoSuchKey"}}
        with durable_fixture() as (_, cloud, measure, clock, _, prepare):
            original = cloud.get
            calls = []
            def get(**kwargs):
                if kwargs["Key"] == common.WINDOW_KEY:
                    calls.append(kwargs["Key"])
                    if len(calls) == 1:
                        raise Missing()
                return original(**kwargs)
            cloud.api["s3"].get_object.side_effect = get
            with patch.object(worker.time, "sleep", side_effect=lambda seconds: advance(clock, seconds)):
                worker.run(cloud.api["s3"], "bucket", durable=True)
            self.assertEqual(len(calls), 2)
            self.assertTrue(report(cloud)[0]["status"]["success"])
            prepare.assert_called_once()
            measure.assert_called_once()

    def test_same_revision_different_bytes_never_creates_one_shot_marker(self):
        with durable_fixture() as (root, cloud, measure, clock, _, prepare):
            value = json.loads(cloud.objects[common.REQUEST_KEY])
            value["sha256"]["candidate/filegen"] = "e" * 64
            cloud.objects[common.REQUEST_KEY] = json.dumps(value).encode()
            worker.run(cloud.api["s3"], "bucket", durable=True)
            metadata, _ = report(cloud)
            self.assertFalse(metadata["status"]["success"])
            self.assertEqual(metadata["status"]["error"]["message"], "A/A binary digests differ")
            self.assertFalse((root / "persistent").exists())
            prepare.assert_not_called()
            measure.assert_not_called()

    def test_invalid_matching_assignment_never_extends_initial_alarm(self):
        with durable_fixture() as (_, cloud, measure, clock, _, prepare):
            value = json.loads(cloud.objects[common.WINDOW_KEY])
            value["runtime_seconds"] = 10801
            cloud.objects[common.WINDOW_KEY] = json.dumps(value).encode()
            with self.assertRaises(ValueError):
                worker.run(cloud.api["s3"], "bucket", durable=True)
            self.assertEqual(worker.signal.setitimer.call_count, 1)
            prepare.assert_not_called()
            measure.assert_not_called()
            self.assertNotIn(common.STATUS_KEY, cloud.objects)

    def test_durable_window_requires_explicit_launch_mode(self):
        with durable_fixture() as (_, cloud, measure, clock, _, prepare), self.assertRaisesRegex(ValueError, "protocol"):
            worker.run(cloud.api["s3"], "bucket")
        prepare.assert_not_called()
        measure.assert_not_called()

    def test_marker_refuses_a_symlink_or_public_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "target"
            target.mkdir(mode=0o700)
            link = root / "link"
            link.symlink_to(target, target_is_directory=True)
            with patch.object(worker, "DURABLE_STATE", link), self.assertRaises(OSError):
                worker.consume_durable_assignment(durable_window())
            target.chmod(0o755)
            with patch.object(worker, "DURABLE_STATE", target), self.assertRaisesRegex(ValueError, "private"):
                worker.consume_durable_assignment(durable_window())
            self.assertFalse((target / "durable-started.json").exists())


class ImdsIdentityTests(unittest.TestCase):
    def test_imdsv2_reads_only_instance_id_with_bounded_responses_and_no_proxy(self):
        opener = Mock()
        opener.open.side_effect = [io.BytesIO(b"opaque-token"), io.BytesIO(b"i-" + b"1" * 17)]
        budget = Mock()
        budget.left.return_value = 90
        with patch.object(worker, "build_opener", return_value=opener) as build:
            self.assertEqual(worker.instance_identity(budget), "i-" + "1" * 17)
        first, second = opener.open.call_args_list
        self.assertEqual(first.args[0].full_url, "http://169.254.169.254/latest/api/token")
        self.assertEqual(first.args[0].method, "PUT")
        self.assertEqual(second.args[0].full_url, "http://169.254.169.254/latest/meta-data/instance-id")
        self.assertEqual(second.args[0].get_header("X-aws-ec2-metadata-token"), "opaque-token")
        self.assertEqual(first.kwargs["timeout"], 2)
        self.assertEqual(build.call_args.args[0].proxies, {})
        self.assertIsNone(build.call_args.args[1].redirect_request(None, None, 302, "", {}, "http://elsewhere"))

    def test_bad_or_failed_token_never_falls_back_to_imdsv1(self):
        for token in (b"", b"x" * 4097, b"line\nnew", b"bad\x00"):
            with self.subTest(token=token[:10]), patch.object(worker, "build_opener") as build:
                build.return_value.open.return_value = io.BytesIO(token)
                with self.assertRaisesRegex(ValueError, "token"):
                    worker.instance_identity(Mock(left=lambda reserve: 30))
                self.assertEqual(build.return_value.open.call_count, 1)
        error = HTTPError("http://169.254.169.254", 403, "denied", {}, None)
        try:
            with patch.object(worker, "build_opener") as build:
                build.return_value.open.side_effect = error
                with self.assertRaises(HTTPError):
                    worker.instance_identity(Mock(left=lambda reserve: 30))
                self.assertEqual(build.return_value.open.call_count, 1)
        finally:
            error.close()


def durable_bundle(root, *, direct=False):
    expected = binding("bulk-1g-metadata")
    expected["candidate"] = expected["baseline"]
    _, policy = bundle(root, expected)
    lease = direct_window() if direct else durable_window()
    lease.update({key: expected[key] for key in ("run", "baseline", "candidate", "suite")})
    if direct:
        lease["attempt_id"] = lease["run"] + "-1"
    value = request(lease)
    value["sha256"] = dict.fromkeys(common.BINARIES, "d" * 64)
    record = json.loads((root / "controller.json").read_text())
    status = json.loads((root / "status.json").read_text())
    metadata = dict(window=lease, request=value,
        status={key: val for key, val in status.items() if key != "sha256"},
        harness_revision="f" * 40, admission=dict(accepted=True, required_seconds=9852, remaining_seconds=10410),
        guest_instance_id=lease["instance_id"], service_returncode=0)
    data = archive([("worker.json", results.encoded(metadata)),
                    (results.RESULT, results.encoded(measurement(expected)))])
    status["sha256"] = common.digest(data)
    record.update(window=lease, request=value, archive_sha256=common.digest(data),
                  status_sha256=common.digest(results.encoded(status)))
    for name, payload in (("results.zip", data), ("controller.json", results.encoded(record)),
                          ("status.json", results.encoded(status))):
        (root / name).write_bytes(payload)
    policy["archive_sha256"] = common.digest(data)
    return expected, policy


class DurableResultTests(unittest.TestCase):
    def test_durable_archive_preserves_frozen_timing_without_legacy_clock_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected, policy = durable_bundle(root)
            evidence, parsed, _ = results.inspect(root, expected, "success")
            self.assertEqual(evidence["errors"], [])
            results.approve(evidence, parsed, policy)
            self.assertTrue(evidence["history_eligible"])
            timing = results.derived_record(evidence, parsed)["context"]["aws_provenance"]["timing"]
            self.assertEqual(timing["runtime_seconds"], 10800)
            self.assertEqual(timing["plan"], common.DURABLE_PLAN)
            self.assertIn("not a measured complete run", timing["basis"])

    def test_durable_approval_requires_instance_binding_and_derived_admission(self):
        cases = {
            "wrong instance": lambda value: value["worker"].update(guest_instance_id="i-" + "2" * 17),
            "missing instance": lambda value: value["worker"].pop("guest_instance_id"),
            "old floor": lambda value: value["worker"]["admission"].update(required_seconds=3400),
            "too late": lambda value: value["worker"]["admission"].update(remaining_seconds=9851),
            "nonfinite": lambda value: value["worker"]["admission"].update(remaining_seconds=float("nan")),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected, policy = durable_bundle(root)
            evidence, parsed, _ = results.inspect(root, expected, "success")
            for name, change in cases.items():
                with self.subTest(name=name):
                    altered = copy.deepcopy(evidence)
                    change(altered)
                    with self.assertRaisesRegex(ValueError, "durable"):
                        results.approve(altered, parsed, policy)
                    self.assertFalse(altered["history_eligible"])



def direct_window(attempt=NOW, delay=60):
    value = durable_window(attempt)
    value.update(protocol=common.DIRECT_PROTOCOL, attempt_id=value["run"] + "-1",
        attempt_started_at=common.timestamp(attempt),
        launch_at=common.timestamp(attempt + timedelta(seconds=delay)))
    return value


@contextmanager
def direct_fixture():
    with durable_fixture() as values:
        root, cloud, measure, clock, identity, prepare = values
        old = json.loads(cloud.objects[common.WINDOW_KEY])
        value = direct_window(common.utc(old["launch_at"]))
        cloud.objects[common.WINDOW_KEY] = json.dumps(value).encode()
        cloud.objects[common.REQUEST_KEY] = json.dumps(request(value)).encode()
        yield values


class DirectWindowTests(unittest.TestCase):
    def test_attempt_clock_does_not_restart_when_instance_arrives(self):
        value = direct_window(delay=600)
        self.assertEqual(common.validate_window(value, NOW + timedelta(seconds=600)),
                         NOW + timedelta(seconds=10500))
        common.validate_admission(value, window=True)
        self.assertEqual((common.utc(value["deadline"]) - common.utc(value["launch_at"])).total_seconds(), 9900)
        value["deadline"] = common.timestamp(common.utc(value["launch_at"]) + timedelta(seconds=10500))
        with self.assertRaisesRegex(ValueError, "fixed attempt runtime"):
            common.validate_window(value, NOW + timedelta(seconds=600))

    def test_direct_schema_and_attempt_chronology_are_exact(self):
        cases = {
            "missing attempt": lambda value: value.pop("attempt_started_at"),
            "missing id": lambda value: value.pop("attempt_id"),
            "other run": lambda value: value.update(attempt_id="124-1-1"),
            "zero ordinal": lambda value: value.update(attempt_id=value["run"] + "-0"),
            "excess ordinal": lambda value: value.update(attempt_id=value["run"] + "-721"),
            "leading zero": lambda value: value.update(attempt_id=value["run"] + "-01"),
            "opens at launch": lambda value: value.update(opens=value["launch_at"]),
            "launch before intent": lambda value: value.update(launch_at=common.timestamp(NOW - timedelta(seconds=1))),
            "launch at expiry": lambda value: value.update(launch_at=value["request_expires"]),
            "attempt at expiry": lambda value: value.update(attempt_started_at=value["request_expires"]),
            "altered horizon": lambda value: value.update(outer_deadline=value["deadline"]),
            "future launch": lambda value: value.update(launch_at=common.timestamp(NOW + timedelta(seconds=91))),
        }
        for name, change in cases.items():
            with self.subTest(name=name):
                value = direct_window()
                change(value)
                with self.assertRaises(ValueError):
                    common.validate_window(value, NOW + timedelta(seconds=90))
        value = direct_window()
        value["attempt_id"] = value["run"] + "-720"
        common.validate_window(value, NOW + timedelta(seconds=90))

    def test_protocol_labels_never_reinterpret_historical_clock_origins(self):
        old, new = durable_window(), direct_window()
        common.validate_window(old, NOW)
        common.validate_window(new, NOW + timedelta(seconds=90))
        for value, protocol in ((old, common.DIRECT_PROTOCOL), (new, common.DURABLE_PROTOCOL)):
            value["protocol"] = protocol
            with self.assertRaisesRegex(ValueError, "fields"):
                common.validate_window(value, NOW + timedelta(seconds=90))


class DirectGuestTests(unittest.TestCase):
    def test_complete_worker_records_attempt_origin_and_preserves_copy_contract(self):
        with direct_fixture() as (root, cloud, measure, clock, identity, prepare):
            worker.run(cloud.api["s3"], "bucket", direct=True)
            metadata, _ = report(cloud)
            self.assertTrue(metadata["status"]["success"])
            self.assertEqual(metadata["runtime_origin"], "attempt_started_at")
            self.assertEqual(metadata["admission"]["remaining_seconds"], 10410)
            self.assertEqual(metadata["admission"]["required_seconds"], 9852)
            self.assertEqual(worker.signal.setitimer.call_args_list[1].args, (signal.ITIMER_REAL, 10410))
            marker = json.loads((root / "persistent/durable-started.json").read_text())
            self.assertEqual(marker["attempt_id"], metadata["window"]["attempt_id"])
            self.assertEqual(marker["attempt_started_at"], metadata["window"]["opens"])
            argv = measure.call_args.args[0]
            self.assertEqual(argv[argv.index("--verification-mode") + 1], "metadata-size")
            self.assertEqual(argv[argv.index("--cache") + 1], "linux-drop-caches")
            self.assertEqual(argv[argv.index("--repetitions") + 1], "2")
            self.assertEqual(argv[argv.index("--timeout") + 1], "1200")
            prepare.assert_called_once()
            identity.assert_called_once()

    def test_late_boot_cannot_recover_time_from_launch_or_control_horizon(self):
        with direct_fixture() as (_, cloud, measure, clock, _, prepare):
            value = json.loads(cloud.objects[common.WINDOW_KEY])
            attempt = common.utc(value["attempt_started_at"])
            value["launch_at"] = common.timestamp(attempt + timedelta(seconds=650))
            cloud.objects[common.WINDOW_KEY] = json.dumps(value).encode()
            advance(clock, 570)
            worker.run(cloud.api["s3"], "bucket", direct=True)
            metadata, _ = report(cloud)
            self.assertFalse(metadata["status"]["success"])
            self.assertFalse(metadata["admission"]["accepted"])
            self.assertEqual(metadata["admission"]["remaining_seconds"], 9840)
            prepare.assert_not_called()
            measure.assert_not_called()

    def test_reboot_cannot_reuse_direct_assignment(self):
        with direct_fixture() as (root, cloud, measure, clock, _, prepare):
            worker.run(cloud.api["s3"], "bucket", direct=True)
            marker = (root / "persistent/durable-started.json").read_bytes()
            worker.run(cloud.api["s3"], "bucket", direct=True)
            self.assertFalse(report(cloud)[0]["status"]["success"])
            self.assertEqual((root / "persistent/durable-started.json").read_bytes(), marker)
            self.assertEqual(measure.call_count, 1)

    def test_direct_launch_never_accepts_historical_launch_clock_assignment(self):
        with durable_fixture() as (root, cloud, measure, clock, _, prepare):
            with patch.object(worker.time, "sleep", side_effect=lambda seconds: advance(clock, seconds)), \
                 self.assertRaises(TimeoutError):
                worker.run(cloud.api["s3"], "bucket", direct=True)
            self.assertEqual(worker.signal.setitimer.call_count, 1)
            self.assertFalse((root / "persistent").exists())
            self.assertNotIn(common.STATUS_KEY, cloud.objects)
            prepare.assert_not_called()
            measure.assert_not_called()

    def test_wrong_direct_instance_cannot_prepare_or_report(self):
        with direct_fixture() as (root, cloud, measure, clock, identity, prepare):
            identity.return_value = "i-" + "2" * 17
            with patch.object(worker.time, "sleep", side_effect=lambda seconds: advance(clock, seconds)), \
                 self.assertRaises(TimeoutError):
                worker.run(cloud.api["s3"], "bucket", direct=True)
            self.assertFalse((root / "persistent").exists())
            self.assertNotIn(common.RESULTS_KEY, cloud.objects)
            prepare.assert_not_called()
            measure.assert_not_called()


class DirectResultTests(unittest.TestCase):
    def test_direct_report_preserves_attempt_clock_and_horizon_role(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected, policy = durable_bundle(root, direct=True)
            evidence, parsed, _ = results.inspect(root, expected, "success")
            self.assertEqual(evidence["errors"], [])
            results.approve(evidence, parsed, policy)
            timing = results.derived_record(evidence, parsed)["context"]["aws_provenance"]["timing"]
            self.assertEqual(timing["protocol"], common.DIRECT_PROTOCOL)
            self.assertEqual(timing["runtime_origin"], "attempt_started_at")
            self.assertEqual(timing["attempt_id"], expected["run"] + "-1")
            self.assertNotEqual(timing["attempt_started_at"], timing["launch_at"])
            self.assertEqual(timing["outer_deadline_role"], "control reconciliation horizon; not instance expiry")

    def test_direct_qualification_keeps_instance_and_workload_headroom_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected, policy = durable_bundle(root, direct=True)
            evidence, parsed, _ = results.inspect(root, expected, "success")
            evidence["worker"]["admission"]["remaining_seconds"] = 9851
            with self.assertRaisesRegex(ValueError, "durable admission"):
                results.approve(evidence, parsed, policy)


if __name__ == "__main__":
    unittest.main()
