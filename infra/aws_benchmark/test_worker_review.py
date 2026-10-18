"""Worker regressions for sandbox startup, bounded reports and failure deadlines."""

from contextlib import contextmanager, ExitStack
from datetime import datetime, timezone
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

from infra.aws_benchmark import common, worker
from infra.aws_benchmark.test_safety import FakeCloud, window


class Transient(Exception):
    response = {"Error": {"Code": "SlowDown"}, "ResponseMetadata": {"HTTPStatusCode": 503}}


@contextmanager
def worker_fixture():
    with tempfile.TemporaryDirectory() as directory, ExitStack() as patches:
        root = Path(directory)
        cloud = FakeCloud()
        lease = window(datetime.now(timezone.utc))
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
        patches.enter_context(patch.object(worker, "prepare_storage", return_value=["disk1", "disk2"]))
        patches.enter_context(patch.object(worker, "cache_server"))
        measure = patches.enter_context(patch.object(worker.subprocess, "run", return_value=Mock(returncode=0)))
        stop = patches.enter_context(patch.object(worker, "stop_unit"))
        yield root, cloud, measure, stop


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
        for phase in ("request", "validation", "revision", "state", "user"):
            with self.subTest(phase=phase), worker_fixture() as (root, cloud, measure, stop), ExitStack() as patches:
                if phase == "request":
                    cloud.objects[common.REQUEST_KEY] = b"invalid JSON"
                elif phase == "validation":
                    patches.enter_context(patch.object(worker, "validate_window", side_effect=ValueError("window too short")))
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
            (root / "data/output/forbidden").symlink_to("/etc/passwd")
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


if __name__ == "__main__":
    unittest.main()
