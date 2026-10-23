"""Create-only worker delivery preserves uncertainty for independent collection."""

import base64
import json
import tempfile
import unittest
from unittest.mock import Mock, patch

from . import common, worker
from .production_mailbox import MailboxPaths
from .test_durable_coordinator import ApiError
from .test_production_collection import Error, Fixture


class WorkerUploadRecoveryTests(unittest.TestCase):
    def setUp(self):
        network = patch("socket.create_connection", side_effect=AssertionError("offline tests only"))
        network.start()
        self.addCleanup(network.stop)
        sleep = patch.object(common.time, "sleep")
        sleep.start()
        self.addCleanup(sleep.stop)
        self.budget = Mock()
        self.budget.left.return_value = 100

    def fixture(self, directory):
        fx = Fixture(directory)
        raw = next(iter(fx.s3.objects.pop(fx.paths.results).values()))
        fx.s3.objects.pop(fx.paths.status)
        return fx, raw

    def test_lost_archive_and_status_responses_collect_the_exact_successful_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            fx, raw = self.fixture(directory)
            fx.s3.lose_response = True
            status = common.completed_status("123-1", True)
            with patch.object(worker, "collect", return_value=raw):
                worker.publish_report(fx.s3, "bucket", None, {}, status, self.budget, paths=fx.paths)
            self.assertTrue(status["success"])
            self.assertEqual(status["sha256"], common.digest(raw))
            self.assertNotIn("upload_error", status)
            self.assertEqual(len(fx.s3.writes), 4)
            for write in fx.s3.writes:
                self.assertEqual(write["ChecksumSHA256"],
                    base64.b64encode(bytes.fromhex(common.digest(write["Body"]))).decode("ascii"))
            fx.s3.lose_response = False
            result = fx.collector.collect(retire=True)
            self.assertEqual(result["state"], "success", result)
            self.assertTrue(result["retired"])

    def test_initial_conflict_is_failure_without_overwriting_existing_bytes(self):
        for key_name in ("results", "status"):
            with self.subTest(key=key_name), tempfile.TemporaryDirectory() as directory:
                fx, raw = self.fixture(directory)
                key = getattr(fx.paths, key_name)
                fx.s3.seed(key, b"foreign bytes")
                status = common.completed_status("123-1", True)
                with patch.object(worker, "collect", return_value=raw):
                    if key_name == "status":
                        with self.assertRaises(Error):
                            worker.publish_report(fx.s3, "bucket", None, {}, status, self.budget, paths=fx.paths)
                    else:
                        worker.publish_report(fx.s3, "bucket", None, {}, status, self.budget, paths=fx.paths)
                        self.assertFalse(status["success"])
                        self.assertNotIn("sha256", status)
                        self.assertEqual(status["upload_error"]["code"], "PreconditionFailed")
                self.assertEqual(list(fx.s3.objects[key].values()), [b"foreign bytes"])

    def test_ambiguous_foreign_archive_cannot_be_certified_by_expected_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            fx, raw = self.fixture(directory)
            original = fx.s3.put_object
            def put(**parameters):
                if parameters["Key"] == fx.paths.results and parameters["Key"] not in fx.s3.objects:
                    fx.s3.seed(fx.paths.results, b"foreign archive bytes")
                    raise TimeoutError("transport outcome unknown")
                return original(**parameters)
            fx.s3.put_object = put
            status = common.completed_status("123-1", True)
            with patch.object(worker, "collect", return_value=raw):
                worker.publish_report(fx.s3, "bucket", None, {}, status, self.budget, paths=fx.paths)
            self.assertEqual(status["sha256"], common.digest(raw))
            result = fx.collector.collect(retire=True)
            self.assertNotEqual(result["state"], "success", result)
            reports = [json.loads(next(iter(versions.values()))) for key, versions in fx.s3.objects.items()
                       if key.endswith("/collection/report.json")]
            self.assertEqual(len(reports), 1)
            self.assertIn("digest", json.dumps(reports[0]["evidence"]["errors"]))
            self.assertEqual(list(fx.s3.objects[fx.paths.results].values()), [b"foreign archive bytes"])

    def test_ambiguous_foreign_status_is_not_overwritten_or_certified(self):
        with tempfile.TemporaryDirectory() as directory:
            fx, raw = self.fixture(directory)
            original = fx.s3.put_object
            foreign = json.dumps(common.completed_status("999-1", True)).encode()
            def put(**parameters):
                if parameters["Key"] == fx.paths.status and parameters["Key"] not in fx.s3.objects:
                    fx.s3.seed(fx.paths.status, foreign)
                    raise TimeoutError("transport outcome unknown")
                return original(**parameters)
            fx.s3.put_object = put
            with patch.object(worker, "collect", return_value=raw):
                worker.publish_report(fx.s3, "bucket", None, {}, common.completed_status("123-1", True),
                                      self.budget, paths=fx.paths)
            result = fx.collector.collect(retire=True)
            self.assertNotEqual(result["state"], "success", result)
            self.assertEqual(list(fx.s3.objects[fx.paths.status].values()), [foreign])

    def test_nonretryable_failure_does_not_make_a_conflict_recoverable(self):
        paths = MailboxPaths.for_instance("123456789012", "us-east-1", "i-" + "1" * 17)
        s3 = Mock()
        s3.put_object.side_effect = [ApiError("AccessDenied", status=403), ApiError("PreconditionFailed", status=412)]
        with self.assertRaises(ApiError):
            worker.put_report_object(s3, "bucket", paths.results, b"expected", "application/zip", self.budget, paths)
        self.assertEqual(s3.put_object.call_count, 1)

    def test_ambiguous_upload_cannot_extend_budget_or_hide_legacy_conflicts(self):
        paths = MailboxPaths.for_instance("123456789012", "us-east-1", "i-" + "1" * 17)
        s3 = Mock()
        s3.put_object.side_effect = TimeoutError("lost reply")
        self.budget.left.side_effect = [100, 100, TimeoutError("deadline exhausted")]
        with self.assertRaisesRegex(TimeoutError, "deadline exhausted"):
            worker.put_report_object(s3, "bucket", paths.results, b"expected", "application/zip", self.budget, paths)
        self.assertEqual(s3.put_object.call_count, 1)
        self.budget.left.side_effect = None
        s3.put_object.side_effect = [TimeoutError(), ApiError("PreconditionFailed", status=412)]
        with self.assertRaises(ApiError):
            worker.put_report_object(s3, "bucket", "output/results.zip", b"expected", "application/zip",
                                     self.budget, MailboxPaths.legacy())


if __name__ == "__main__":
    unittest.main()
