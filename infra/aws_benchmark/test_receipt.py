"""Controller receipts cannot infer an image, hide cleanup failure, or change evidence."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from infra.aws_benchmark import common, controller
from infra.aws_benchmark.test_controller import controller_cloud
from infra.aws_benchmark.test_results import binding


class ReceiptTests(unittest.TestCase):
    def execute(self, root, *, fail=False, unknown=False, cleanup_error=False, save_error=False):
        cloud, selected = controller_cloud()
        for key in common.BINARIES:
            path = root / key
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b"binary")
        expected = binding(selected["suite"])
        expected.update(selected)
        archive = b"opaque bytes never parsed on the credentialed controller"
        status = dict(common.completed_status(selected["run"], not fail), sha256=common.digest(archive))
        def launch(**kwargs):
            if kwargs["DesiredCapacity"] == 1:
                cloud.objects[common.STATUS_KEY] = json.dumps(status).encode()
                cloud.objects[common.RESULTS_KEY] = archive
                observed = [] if unknown else [dict(InstanceId="i-" + "1" * 17,
                    ImageId="ami-" + "2" * 17, InstanceType="c5d.24xlarge")]
                cloud.api["ec2"].describe_instances.return_value = {"Reservations": [{"Instances": observed}]}
                if save_error:
                    # simulate disk failure after the launch call, including final receipt writes
                    writer = patch.object(Path, "replace", side_effect=OSError("injected"))
                    writer.start()
                    self.addCleanup(writer.stop)
        cloud.api["autoscaling"].set_desired_capacity.side_effect = launch
        with patch.object(controller, "cleanup", side_effect=TimeoutError("unconfirmed") if cleanup_error else None) as cleanup:
            if fail or cleanup_error:
                with self.assertRaises(TimeoutError if cleanup_error else ValueError):
                    controller.run(cloud.api, "group", "bucket", selected, root, root / "output", provenance=expected)
            else:
                controller.run(cloud.api, "group", "bucket", selected, root, root / "output", provenance=expected)
            cleanup.assert_called_once_with(cloud.api, "group")
        return json.loads((root / "output/controller.json").read_text()), archive

    def test_success_receipt_separates_observed_image_and_bound_workflow_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt, archive = self.execute(root)
            self.assertEqual(receipt["outcome"], "success")
            self.assertEqual(receipt["cleanup"], "verified")
            self.assertEqual(receipt["instance"]["image_id"], "ami-" + "2" * 17)
            self.assertEqual(receipt["identity"]["controller"], "c" * 40)
            self.assertEqual(receipt["archive_sha256"], common.digest(archive))
            self.assertEqual(receipt["status_sha256"], common.digest((root / "output/status.json").read_bytes()))
            self.assertEqual((root / "output/results.zip").read_bytes(), archive)

    def test_failed_worker_unknown_image_and_failed_cleanup_are_truthful(self):
        for options in (dict(fail=True), dict(unknown=True), dict(cleanup_error=True)):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                receipt, _ = self.execute(Path(directory), **options)
                self.assertEqual(receipt["outcome"], "failure" if options.get("fail") or options.get("cleanup_error") else "success")
                self.assertEqual(receipt["cleanup"], "unconfirmed" if options.get("cleanup_error") else "verified")
                if options.get("unknown"):
                    self.assertIsNone(receipt["instance"])

    def test_receipt_write_failure_after_launch_never_prevents_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt, _ = self.execute(Path(directory), save_error=True)
            self.assertEqual(receipt["cleanup"], "unconfirmed")
            self.assertEqual(receipt["outcome"], "incomplete")

    def test_invalid_workflow_identity_never_calls_cloud(self):
        for mutation in (dict(ref="refs/heads/MAIN"), dict(ref="refs/tags/main"),
                         dict(workflow_sha="d" * 40), dict(event="pull_request"),
                         dict(repository="attacker/rcp"), dict(run_url="https://evil.test")):
            cloud, selected = controller_cloud()
            expected = {**binding(selected["suite"]), **selected, **mutation}
            with tempfile.TemporaryDirectory() as directory, self.assertRaises(ValueError):
                controller.run(cloud.api, "group", "bucket", selected, Path(directory), Path(directory) / "out", expected)
            self.assertTrue(all(not client.mock_calls for client in cloud.api.values()))


if __name__ == "__main__":
    unittest.main()
