"""Worker regressions for sandbox startup, bounded reports and failure deadlines."""

from contextlib import contextmanager, ExitStack
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import zipfile

from infra.aws_benchmark import common, controller, operator, results, worker
from infra.aws_benchmark.test_results import binding, measurement
from infra.aws_benchmark.test_safety import FakeCloud, closed, window


class Transient(Exception):
    response = {"Error": {"Code": "SlowDown"}, "ResponseMetadata": {"HTTPStatusCode": 503}}


@contextmanager
def worker_fixture(suite="aa-smoke"):
    with tempfile.TemporaryDirectory() as directory, ExitStack() as patches:
        root = Path(directory)
        cloud = FakeCloud()
        lease = window(datetime.now(timezone.utc))
        lease["suite"] = suite
        if suite in common.METADATA_SUITES:
            lease["opens"] = common.timestamp(datetime.now(timezone.utc) - timedelta(seconds=1))
            lease["deadline"] = common.timestamp(common.utc(lease["opens"]) + timedelta(seconds=3600))
        request = {k: lease[k] for k in ("run", "baseline", "candidate", "suite", "deadline")}
        request["sha256"] = {key: common.digest(b"binary") for key in common.BINARIES}
        cloud.objects.update({"input/" + key: b"binary" for key in common.BINARIES})
        cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
        cloud.objects[common.REQUEST_KEY] = json.dumps(request).encode()
        (root / "revision.txt").write_text("trusted-harness")
        (root / "data/output").mkdir(parents=True)
        for attr, value in (("ROOT", root), ("STATE", root / "state"), ("DATA", root / "data")):
            patches.enter_context(patch.object(worker, attr, value))
        patches.enter_context(patch.object(worker.pwd, "getpwnam", return_value=Mock(pw_uid=123, pw_gid=123)))
        patches.enter_context(patch.object(worker.signal, "signal"))
        patches.enter_context(patch.object(worker.signal, "setitimer"))
        patches.enter_context(patch.object(common.time, "sleep"))
        patches.enter_context(patch.object(worker, "check_quiet", return_value={}))
        patches.enter_context(patch.object(worker, "check_ssm_absent", return_value={}))
        patches.enter_context(patch.object(worker, "prepare_storage", return_value=["disk1", "disk2"]))
        patches.enter_context(patch.object(worker, "cache_server"))
        measure = patches.enter_context(patch.object(worker.subprocess, "run", return_value=Mock(returncode=0)))
        stop = patches.enter_context(patch.object(worker, "stop_unit"))
        yield root, cloud, measure, stop



@contextmanager
def frozen_worker_time(cloud):
    deadline = common.utc(json.loads(cloud.objects[common.WINDOW_KEY])["deadline"])
    clock = {"wall": (deadline - timedelta(minutes=20)).timestamp(), "monotonic": 100.0}
    class ClockDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromtimestamp(clock["wall"], tz)
    with patch.object(common.time, "time", side_effect=lambda: clock["wall"]), \
         patch.object(common.time, "monotonic", side_effect=lambda: clock["monotonic"]), \
         patch.object(common, "datetime", ClockDatetime), \
         patch.object(worker, "datetime", ClockDatetime), \
         patch.object(controller, "datetime", ClockDatetime):
        yield clock


def report(cloud):
    with zipfile.ZipFile(io.BytesIO(cloud.objects[common.RESULTS_KEY])) as archive:
        return json.loads(archive.read("worker.json")), archive.namelist()


class WorkerReviewTests(unittest.TestCase):
    def test_measurement_tmpfs_is_writable_by_unprivileged_user(self):
        self.assertIn("--property=TemporaryFileSystem=/tmp:size=64m,mode=1777 /var/tmp:size=64m,mode=1777",
                      worker.command(window(), 60))

    def test_expired_cache_server_exits_without_spinning_or_accepting(self):
        budget = Mock()
        budget.left.side_effect = TimeoutError("reserve exhausted")
        with tempfile.TemporaryDirectory() as directory, patch.object(worker, "SOCKET", str(Path(directory) / "cache.sock")):
            thread = worker.cache_server(budget, os.getuid())
            thread.join(timeout=1)
            self.assertFalse(thread.is_alive())
        self.assertEqual(budget.left.call_count, 1)

    def test_socket_timeout_continues_until_budget_expires(self):
        budget = Mock()
        budget.left.side_effect = [10, TimeoutError("reserve exhausted")]
        with tempfile.TemporaryDirectory() as directory, patch.object(worker, "SOCKET", str(Path(directory) / "cache.sock")):
            thread = worker.cache_server(budget, os.getuid())
            thread.join(timeout=3)
            self.assertFalse(thread.is_alive())
        self.assertEqual(budget.left.call_count, 2)

    def test_oversized_log_preserves_core_records_and_head_tail_with_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results/logs").mkdir(parents=True)
            (root / "results/results.json").write_text('{"status":"incomplete"}')
            (root / "results/summary.md").write_text("partial run")
            log = root / "results/logs/0.stdout.log"
            with log.open("wb") as stream:
                stream.write(b"HEAD")
                for _ in range(100):
                    stream.write(b"x" * 1024 * 1024)
                stream.write(b"TAIL")
            data = worker.collect(root, {"success": False}, Mock())
        self.assertLessEqual(len(data), common.MAX_RESULT)
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            self.assertEqual(json.loads(archive.read("benchmark/results/results.json"))["status"], "incomplete")
            self.assertEqual(archive.read("benchmark/results/summary.md"), b"partial run")
            payload = archive.read("benchmark/results/logs/0.stdout.log")
            self.assertEqual(len(payload), 1024 * 1024)
            self.assertTrue(payload.startswith(b"HEAD") and payload.endswith(b"TAIL"))
            record = json.loads(archive.read("worker.json"))
            self.assertEqual(record["omitted_or_truncated"], [dict(name="benchmark/results/logs/0.stdout.log",
                size=100 * 1024 * 1024 + 8, archived_bytes=1024 * 1024)])

    def test_aggregate_output_limit_keeps_core_records_and_trusted_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results").mkdir()
            (root / "results/results.json").write_text("{}")
            for n in range(10):
                (root / "results" / f"{n}.log").write_bytes(b"x" * 1024 * 1024)
            with patch.object(worker, "MAX_RESULT", 3 * 1024 * 1024):
                data = worker.collect(root, {}, Mock())
        self.assertLessEqual(len(data), 3 * 1024 * 1024)
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            self.assertEqual(archive.read("benchmark/results/results.json"), b"{}")
            self.assertGreater(json.loads(archive.read("worker.json"))["omitted_or_truncated_count"], 0)

    def test_failures_before_measurement_publish_specific_terminal_report(self):
        for phase in ("request", "revision", "state", "user"):
            with self.subTest(phase=phase), worker_fixture() as (root, cloud, measure, stop), ExitStack() as patches:
                if phase == "request":
                    cloud.objects[common.REQUEST_KEY] = b"invalid JSON"
                elif phase == "revision":
                    (root / "revision.txt").unlink()
                elif phase == "state":
                    (root / "state").mkdir()
                else:
                    patches.enter_context(patch.object(worker.pwd, "getpwnam", side_effect=KeyError("missing user")))
                worker.run(cloud.api["s3"], "bucket")
                status = json.loads(cloud.objects[common.STATUS_KEY])
                self.assertFalse(status["success"])
                self.assertEqual(status["state"], "completed")
                self.assertIn("error", status)
                metadata, _ = report(cloud)
                self.assertEqual(metadata["status"]["error"], status["error"])
                measure.assert_not_called()

    def test_fixed_key_transient_gets_and_puts_retry_without_extending_budget(self):
        for key in (common.WINDOW_KEY, common.REQUEST_KEY, "input/baseline/rcp", common.RESULTS_KEY, common.STATUS_KEY):
            with self.subTest(key=key), worker_fixture() as (_, cloud, measure, _):
                attempts = 0
                original = cloud.get if key.startswith(("control/", "input/")) else cloud.put
                def flaky(**kwargs):
                    nonlocal attempts
                    if kwargs["Key"] == key:
                        attempts += 1
                        if attempts == 1:
                            raise Transient()
                    return original(**kwargs)
                method = "get_object" if key.startswith(("control/", "input/")) else "put_object"
                getattr(cloud.api["s3"], method).side_effect = flaky
                worker.run(cloud.api["s3"], "bucket")
                self.assertEqual(attempts, 2)
                self.assertTrue(json.loads(cloud.objects[common.STATUS_KEY])["success"])

    def test_exhausted_final_status_write_propagates_to_poweroff_backstop(self):
        with worker_fixture() as (_, cloud, _, _):
            def unavailable(**kwargs):
                if kwargs["Key"] == common.STATUS_KEY:
                    raise Transient()
                return cloud.put(**kwargs)
            cloud.api["s3"].put_object.side_effect = unavailable
            with self.assertRaises(Transient):
                worker.run(cloud.api["s3"], "bucket")
            self.assertIn(common.RESULTS_KEY, cloud.objects)
            self.assertNotIn(common.STATUS_KEY, cloud.objects)

    def test_stop_failure_keeps_measurement_outcome_and_never_reads_candidate_paths(self):
        for outcome in (0, 3, subprocess.TimeoutExpired("systemd-run", 10)):
            with self.subTest(outcome=outcome), worker_fixture() as (root, cloud, measure, stop):
                if isinstance(outcome, Exception):
                    measure.side_effect = outcome
                else:
                    measure.return_value.returncode = outcome
                (root / "data/output/forbidden").symlink_to("/etc/passwd")
                stop.side_effect = ValueError("candidate cgroup still contains processes")
                worker.run(cloud.api["s3"], "bucket")
                metadata, names = report(cloud)
                self.assertEqual(names, ["worker.json"])
                self.assertFalse(metadata["status"]["success"])
                self.assertEqual(metadata["status"]["stop_error"]["message"], "candidate cgroup still contains processes")
                if isinstance(outcome, Exception):
                    self.assertEqual(metadata["status"]["error"]["type"], "TimeoutExpired")
                else:
                    self.assertEqual(metadata["service_returncode"], outcome)

    def test_collection_rejection_still_uploads_trusted_failure_metadata(self):
        with worker_fixture() as (root, cloud, _, _):
            collect = worker.collect
            def reject_candidate_output(output, metadata, budget):
                if output is not None:
                    raise ValueError("result entry limit exceeded")
                return collect(output, metadata, budget)
            with patch.object(worker, "collect", side_effect=reject_candidate_output):
                worker.run(cloud.api["s3"], "bucket")
            metadata, names = report(cloud)
            self.assertEqual(names, ["worker.json"])
            self.assertFalse(metadata["status"]["success"])
            self.assertIn("collection_error", metadata["status"])

    def test_real_deadline_signal_escapes_socket_and_sdk_exception_handlers(self):
        previous = signal.signal(signal.SIGALRM, worker.expired)
        start = time.monotonic()
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.05)
            with self.assertRaises(worker.DeadlineExpired):
                try:
                    try:
                        time.sleep(2)
                    except OSError:
                        self.fail("socket connection handler swallowed deadline")
                except Exception:
                    self.fail("SDK handler swallowed deadline")
            self.assertLess(time.monotonic() - start, 1)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)


    def test_bootstrap_alarm_is_replaced_by_remaining_absolute_window(self):
        with worker_fixture() as (_, cloud, _, _), frozen_worker_time(cloud) as clock:
            original = cloud.get
            def get(**kwargs):
                if kwargs["Key"] == common.WINDOW_KEY:
                    clock["wall"] += 7
                    clock["monotonic"] += 7
                return original(**kwargs)
            cloud.api["s3"].get_object.side_effect = get
            with patch.object(worker, "retry_io", wraps=common.retry_io) as retry:
                worker.run(cloud.api["s3"], "bucket")
            self.assertEqual(worker.signal.setitimer.call_args_list[0].args, (signal.ITIMER_REAL, 60))
            self.assertEqual(worker.signal.setitimer.call_args_list[1].args, (signal.ITIMER_REAL, 1193))
            self.assertEqual(retry.call_args_list[0].kwargs["reserve"], 5)
            worker.signal.signal.assert_called_once_with(signal.SIGALRM, worker.expired)

    def test_maintenance_runner_clamps_timeout_and_checks_actual_budget(self):
        for remaining in (common.RESERVE + 3, common.RESERVE):
            with self.subTest(remaining=remaining), worker_fixture() as (_, cloud, run, _), frozen_worker_time(cloud) as clock:
                deadline = common.utc(json.loads(cloud.objects[common.WINDOW_KEY])["deadline"]).timestamp()
                def quiet(runner):
                    elapsed = deadline - remaining - clock["wall"]
                    clock["wall"] += elapsed
                    clock["monotonic"] += elapsed
                    runner(["systemctl", "show", "apt-daily.timer"], timeout=10)
                    return {}
                with patch.object(worker, "check_quiet", side_effect=quiet):
                    worker.run(cloud.api["s3"], "bucket")
                status = json.loads(cloud.objects[common.STATUS_KEY])
                if remaining > common.RESERVE:
                    self.assertEqual(run.call_args_list[0].kwargs["timeout"], 3)
                else:
                    run.assert_not_called()
                    self.assertFalse(status["success"])
                    self.assertEqual(status["error"]["type"], "TimeoutError")

    def test_worker_admission_reserves_boot_preparation_and_reports_rejection(self):
        for remaining in (common.WORKER_START_RESERVE - 1, common.WORKER_START_RESERVE,
                          common.WORKER_START_RESERVE + 1):
            with self.subTest(remaining=remaining), worker_fixture() as (root, cloud, measure, _), frozen_worker_time(cloud) as clock:
                elapsed = 1200 - remaining
                clock["wall"] += elapsed
                clock["monotonic"] += elapsed
                worker.run(cloud.api["s3"], "bucket")
                metadata, _ = report(cloud)
                admission = metadata["admission"]
                accepted = remaining >= common.WORKER_START_RESERVE
                self.assertEqual(admission["accepted"], accepted)
                self.assertEqual(admission["remaining_seconds"], remaining)
                self.assertEqual(admission["required_seconds"], common.WORKER_START_RESERVE)
                self.assertEqual(metadata["status"]["success"], accepted)
                if not accepted:
                    measure.assert_not_called()
                    worker.prepare_storage.assert_not_called()
                    self.assertFalse((root / "state").exists())

    def test_unbounded_request_fields_are_rejected_before_retention_or_preparation(self):
        for placement in ("extra", "digest"):
            with self.subTest(placement=placement), worker_fixture() as (root, cloud, measure, _):
                request = json.loads(cloud.objects[common.REQUEST_KEY])
                nested = 0
                for _ in range(722):
                    nested = [nested]
                if placement == "extra":
                    request["extra"] = nested
                else:
                    request["sha256"]["baseline/rcp"] = nested
                cloud.objects[common.REQUEST_KEY] = json.dumps(request, separators=(",", ":")).encode()
                worker.run(cloud.api["s3"], "bucket")
                metadata, _ = report(cloud)
                self.assertNotIn("request", metadata)
                self.assertFalse(metadata["status"]["success"])
                self.assertIn("error", metadata["status"])
                self.assertIn(common.STATUS_KEY, cloud.objects)
                measure.assert_not_called()
                self.assertFalse((root / "state").exists())

    def test_both_archive_failures_still_publish_terminal_status(self):
        with worker_fixture() as (_, cloud, _, _):
            with patch.object(worker, "collect", side_effect=[
                    ValueError("worker metadata limit exceeded"), RecursionError("nested metadata")]) as collect:
                worker.run(cloud.api["s3"], "bucket")
            self.assertEqual(collect.call_count, 2)
            self.assertIsNone(collect.call_args.args[0])
            self.assertNotIn(common.RESULTS_KEY, cloud.objects)
            status = json.loads(cloud.objects[common.STATUS_KEY])
            self.assertEqual(status["state"], common.COMPLETED)
            self.assertFalse(status["success"])
            self.assertNotIn("sha256", status)
            self.assertEqual(status["collection_error"]["message"], "worker metadata limit exceeded")
            self.assertEqual(status["fallback_error"]["type"], "RecursionError")

    def test_report_deadline_still_escapes_without_uploading_after_expiry(self):
        with worker_fixture() as (_, cloud, _, _):
            with patch.object(worker, "collect", side_effect=worker.DeadlineExpired("deadline")):
                with self.assertRaises(worker.DeadlineExpired):
                    worker.run(cloud.api["s3"], "bucket")
            self.assertNotIn(common.RESULTS_KEY, cloud.objects)
            self.assertNotIn(common.STATUS_KEY, cloud.objects)

    def test_subprocess_and_sdk_details_distinguish_failures_without_exposing_payloads(self):
        failures = [
            (subprocess.CalledProcessError(1, ["/usr/bin/lsblk", "SECRET"], output="SECRET", stderr="SECRET"),
             {"type": "CalledProcessError", "command": "lsblk", "returncode": 1}),
            (subprocess.CalledProcessError(32, ["/usr/bin/mount", "SECRET"], stderr="SECRET"),
             {"type": "CalledProcessError", "command": "mount", "returncode": 32}),
            (subprocess.TimeoutExpired(["/usr/sbin/mkfs.ext4", "SECRET"], 60, output="SECRET", stderr="SECRET"),
             {"type": "TimeoutExpired", "command": "mkfs.ext4", "timeout": 60}),
        ]
        for error, expected in failures:
            with self.subTest(expected=expected):
                self.assertEqual(worker.error_detail(error), expected)
                self.assertNotIn("SECRET", json.dumps(worker.error_detail(error)))
        error = RuntimeError("SECRET")
        error.response = {"Error": {"Code": "ExpiredToken" + "x" * 200, "Message": "SECRET"},
                          "ResponseMetadata": {"RequestId": "SECRET"}}
        detail = worker.error_detail(error)
        self.assertEqual(detail, {"type": "RuntimeError", "code": ("ExpiredToken" + "x" * 200)[:128]})
        self.assertNotIn("SECRET", json.dumps(detail))

    def test_oversized_non_log_is_omitted_whole_and_core_records_survive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results").mkdir()
            (root / "results/results.json").write_text("{}")
            (root / "results/summary.md").write_text("summary")
            (root / "results/oversized.bin").write_bytes(b"x" * (2 * 1024 * 1024))
            with patch.object(worker, "MAX_RESULT", 2 * 1024 * 1024):
                data = worker.collect(root, {}, Mock())
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            self.assertEqual(set(archive.namelist()), {
                "benchmark/results/results.json", "benchmark/results/summary.md", "worker.json"})
            self.assertEqual(archive.read("benchmark/results/results.json"), b"{}")
            self.assertEqual(archive.read("benchmark/results/summary.md"), b"summary")
            metadata = json.loads(archive.read("worker.json"))
            self.assertEqual(metadata["omitted_or_truncated_count"], 1)
            self.assertEqual(metadata["omitted_or_truncated"], [
                {"name": "benchmark/results/oversized.bin", "size": 2 * 1024 * 1024, "archived_bytes": 0}])

    def run_pipeline(self, suite, failure=None):
        # only cloud/guest effects are faked; all producer and consumer contracts run
        with worker_fixture() as (root, cloud, measure, stop):
            expected = binding(suite)
            lease = {**json.loads(cloud.objects[common.WINDOW_KEY]),
                     **{key: expected[key] for key in ("run", "baseline", "candidate", "suite")}}
            cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
            baked = "f" * 40
            (root / "revision.txt").write_text(baked)
            raw = measurement(expected)
            for value in raw["tools"].values():
                value["sha256"] = common.digest(b"binary")
            def execute_measurement(*args, **kwargs):
                destination = root / "data/output/results/results.json"
                destination.parent.mkdir(parents=True)
                destination.write_bytes(results.encoded(raw))
                return Mock(returncode=1 if failure == "measurement" else 0)
            measure.side_effect = execute_measurement
            if failure == "stop":
                stop.side_effect = ValueError("candidate cgroup still contains processes")
            observed = dict(InstanceId="i-" + "1" * 17, ImageId="ami-" + "2" * 17,
                            InstanceType="c5d.24xlarge")
            with frozen_worker_time(cloud) as clock:
                cloud.objects.clear()
                operator.arm(cloud.api, "group", "bucket", lease,
                             now=lambda: datetime.fromtimestamp(clock["wall"], timezone.utc))
                opening = common.utc(lease["opens"]).timestamp() + 1
                clock["monotonic"] += opening - clock["wall"]
                clock["wall"] = opening
                asg = cloud.api["autoscaling"]
                asg.describe_auto_scaling_groups.return_value = {"AutoScalingGroups": [closed(1)]}
                def capacity(**kwargs):
                    if kwargs["DesiredCapacity"] == 1:
                        worker.run(cloud.api["s3"], "bucket")
                        cloud.api["ec2"].describe_instances.return_value = {
                            "Reservations": [{"Instances": [observed]}]}
                    else:
                        cloud.api["ec2"].describe_instances.return_value = {"Reservations": []}
                asg.set_desired_capacity.side_effect = capacity
                binaries = root / "controller-binaries"
                for key in common.BINARIES:
                    binary = binaries / key
                    binary.parent.mkdir(parents=True, exist_ok=True)
                    binary.write_bytes(b"binary")
                output = root / "controller-output"
                if failure:
                    with self.assertRaisesRegex(ValueError, "worker failed; see bounded partial results"):
                        controller.run(cloud.api, "group", "bucket", expected, binaries, output)
                else:
                    controller.run(cloud.api, "group", "bucket", expected, binaries, output)
            status = json.loads((output / "status.json").read_text())
            archive = (output / "results.zip").read_bytes()
            self.assertEqual(status["run"], lease["run"])
            self.assertEqual(status["success"], failure is None)
            self.assertEqual(status["sha256"], common.digest(archive))
            self.assertEqual([call.kwargs["DesiredCapacity"] for call in asg.set_desired_capacity.call_args_list], [1, 0])
            measure.assert_called_once()
            policy = dict(schema_version=1, identity=expected, archive_sha256=common.digest(archive),
                          ami_id=observed["ImageId"], harness_revision=baked)
            derived = root / "ingested"
            evidence = results.ingest(output, derived, expected, "failure" if failure else "success",
                                      policy if suite == "tiny" else None)
            for name in ("controller.json", "status.json", "results.zip"):
                self.assertEqual((derived / "raw" / name).read_bytes(), (output / name).read_bytes())
            self.assertEqual(evidence["controller"]["window"], lease)
            self.assertEqual(evidence["controller"]["cleanup"], "verified")
            self.assertEqual(evidence["controller"]["instance"]["image_id"], observed["ImageId"])
            self.assertEqual(evidence["worker"]["request"], evidence["controller"]["request"])
            self.assertEqual(evidence["worker"]["status"], {k: v for k, v in status.items() if k != "sha256"})
            self.assertEqual(evidence["controller"]["archive_sha256"], common.digest(archive))
            self.assertEqual(evidence["history_eligible"], suite == "tiny" and failure is None, evidence["errors"])
            if failure:
                self.assertIn("history approval: diagnostic run cannot enter performance history", evidence["errors"])
                self.assertEqual(evidence["controller"]["outcome"], "failure")
                if failure == "stop":
                    self.assertIn("harness result unavailable", evidence["errors"])
                    self.assertEqual(evidence["worker"]["status"]["stop_error"]["message"],
                                     "candidate cgroup still contains processes")
                else:
                    self.assertEqual(evidence["worker"]["service_returncode"], 1)
            else:
                self.assertEqual(evidence["errors"], [])
                self.assertEqual(evidence["controller"]["outcome"], "success")
            self.assertEqual((derived / "dashboard/index.html").exists(), failure != "stop")

    def test_real_smoke_pipeline_ingests_complete_diagnostic_evidence(self):
        self.run_pipeline("aa-smoke")

    def test_real_tiny_pipeline_is_eligible_under_exact_offline_policy(self):
        self.run_pipeline("tiny")

    def test_real_failed_measurement_pipeline_keeps_results_and_refuses_approval(self):
        self.run_pipeline("tiny", failure="measurement")

    def test_real_failed_stop_pipeline_keeps_supervisor_evidence_without_candidate_output(self):
        self.run_pipeline("tiny", failure="stop")


    def test_invalid_window_never_rearms_alarm_or_enters_reporting(self):
        for kind in ("extra", "too_long"):
            with self.subTest(kind=kind), worker_fixture() as (_, cloud, measure, _), frozen_worker_time(cloud):
                lease = json.loads(cloud.objects[common.WINDOW_KEY])
                if kind == "extra":
                    nested = 0
                    for _ in range(722):
                        nested = [nested]
                    lease["extra"] = nested
                else:
                    lease["deadline"] = common.timestamp(common.utc(lease["opens"]) + timedelta(seconds=common.MAX_WINDOW + 1))
                cloud.objects[common.WINDOW_KEY] = json.dumps(lease).encode()
                with patch.object(worker, "collect") as collect, self.assertRaises(ValueError):
                    worker.run(cloud.api["s3"], "bucket")
                self.assertEqual(worker.signal.setitimer.call_args_list[0].args, (signal.ITIMER_REAL, 60))
                self.assertEqual(worker.signal.setitimer.call_count, 1)
                measure.assert_not_called()
                collect.assert_not_called()
                self.assertNotIn(common.STATUS_KEY, cloud.objects)
                self.assertNotIn(common.RESULTS_KEY, cloud.objects)

    def test_late_valid_window_reports_admission_failure_within_remaining_time(self):
        with worker_fixture() as (_, cloud, measure, _), frozen_worker_time(cloud) as clock:
            clock["wall"] += 1100
            clock["monotonic"] += 1100
            worker.run(cloud.api["s3"], "bucket")
            status = json.loads(cloud.objects[common.STATUS_KEY])
            self.assertFalse(status["success"])
            self.assertEqual(status["state"], common.COMPLETED)
            self.assertEqual(status["error"]["type"], "TimeoutError")
            self.assertEqual(worker.signal.setitimer.call_args_list[1].args, (signal.ITIMER_REAL, 100))
            measure.assert_not_called()

    def test_guest_security_preflight_uses_budget_and_reports_failures_before_storage(self):
        with worker_fixture() as (_, cloud, measure, _), frozen_worker_time(cloud) as clock:
            deadline = common.utc(json.loads(cloud.objects[common.WINDOW_KEY])["deadline"]).timestamp()
            def security(*, timeout):
                elapsed = deadline - common.RESERVE - 3 - clock["wall"]
                clock["wall"] += elapsed
                clock["monotonic"] += elapsed
                self.assertEqual(timeout(30), 3)
                raise ValueError("SSM Agent is installed")
            with patch.object(worker, "check_ssm_absent", side_effect=security):
                worker.run(cloud.api["s3"], "bucket")
            metadata, _ = report(cloud)
            self.assertFalse(metadata["status"]["success"])
            self.assertEqual(metadata["status"]["error"]["message"], "SSM Agent is installed")
            worker.prepare_storage.assert_not_called()
            measure.assert_not_called()


if __name__ == "__main__":
    unittest.main()
