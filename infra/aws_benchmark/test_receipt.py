"""Controller receipts cannot infer an image, hide cleanup failure, or change evidence."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from infra.aws_benchmark import common, controller, results
from infra.aws_benchmark.test_controller import controller_cloud
from infra.aws_benchmark.test_results import binding


class ReceiptTests(unittest.TestCase):
    def prepare(self, root):
        cloud, selected = controller_cloud()
        for key in common.BINARIES:
            path = root / key
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(b"binary")
        expected = binding(selected["suite"])
        expected.update(selected)
        return cloud, selected, expected

    def execute(self, root, *, fail=False, unknown=False, cleanup_error=False, save_error=False):
        cloud, selected, expected = self.prepare(root)
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

    def observation_run(self, responses, pending_polls=0, budget=None):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cloud, selected, expected = self.prepare(root)
            launched, polls = False, 0
            def launch(**kwargs):
                nonlocal launched
                launched = True
                cloud.api["ec2"].describe_instances.side_effect = responses
            def get(**kwargs):
                nonlocal polls
                if launched and kwargs["Key"] == common.STATUS_KEY:
                    polls += 1
                    if polls > pending_polls:
                        cloud.objects[common.STATUS_KEY] = json.dumps(common.completed_status(selected["run"], True)).encode()
                return cloud.get(**kwargs)
            cloud.api["autoscaling"].set_desired_capacity.side_effect = launch
            cloud.api["s3"].get_object.side_effect = get
            with patch.object(controller, "cleanup") as cleanup, patch.object(controller.time, "sleep") as sleep:
                if budget is None:
                    controller.run(cloud.api, "group", "bucket", selected, root, root / "out", expected)
                else:
                    with patch.object(controller, "Budget", return_value=budget), self.assertRaisesRegex(TimeoutError, "observation deadline"):
                        controller.run(cloud.api, "group", "bucket", selected, root, root / "out", expected)
            cleanup.assert_called_once_with(cloud.api, "group")
            self.assertEqual([call.kwargs["DesiredCapacity"] for call in
                              cloud.api["autoscaling"].set_desired_capacity.call_args_list], [1])
            receipt = json.loads((root / "out/controller.json").read_text())
            return receipt, cloud.api["ec2"].describe_instances.call_count - 1, sleep.call_count

    def test_delayed_instance_observation_survives_empty_first_three_polls(self):
        observed = dict(InstanceId="i-" + "1" * 17, ImageId="ami-" + "2" * 17, InstanceType="c5d.24xlarge")
        receipt, reads, sleeps = self.observation_run(
            [{"Reservations": []}] * 3 + [{"Reservations": [{"Instances": [observed]}]}], pending_polls=3)
        self.assertEqual(receipt["instance"], dict(instance_id=observed["InstanceId"],
            image_id=observed["ImageId"], instance_type=observed["InstanceType"]))
        self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("success", "verified"))
        self.assertEqual((reads, sleeps), (4, 3))

    def test_terminal_status_gets_a_final_instance_observation(self):
        observed = dict(InstanceId="i-" + "1" * 17, ImageId="ami-" + "2" * 17, InstanceType="c5d.24xlarge")
        receipt, reads, sleeps = self.observation_run(
            [{"Reservations": []}, {"Reservations": [{"Instances": [observed]}]}])
        self.assertEqual(receipt["instance"]["image_id"], observed["ImageId"])
        self.assertEqual((reads, sleeps), (2, 0))

    def test_observation_errors_are_isolated_and_capped_without_failing_run(self):
        receipt, reads, sleeps = self.observation_run(
            [RuntimeError("optional EC2 read failed")] * 3, pending_polls=5)
        self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("success", "verified"))
        self.assertIsNone(receipt["instance"])
        self.assertEqual((reads, sleeps), (3, 5))

    def test_empty_observations_do_not_consume_the_error_retry_limit(self):
        observed = dict(InstanceId="i-" + "1" * 17, ImageId="ami-" + "2" * 17, InstanceType="c5d.24xlarge")
        receipt, reads, sleeps = self.observation_run(
            [RuntimeError("transient EC2 read failure"), {"Reservations": []},
             RuntimeError("second EC2 read failure"), {"Reservations": []},
             {"Reservations": [{"Instances": [observed]}]}], pending_polls=4)
        self.assertEqual(receipt["instance"]["image_id"], observed["ImageId"])
        self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("success", "verified"))
        self.assertEqual((reads, sleeps), (5, 4))

    def test_observation_cannot_extend_deadline_or_bypass_cleanup(self):
        budget = Mock()
        budget.left.return_value = 1000
        def slow_read(**kwargs):
            budget.left.side_effect = TimeoutError("observation deadline")
            return {"Reservations": []}
        receipt, reads, sleeps = self.observation_run(slow_read, budget=budget)
        self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("failure", "verified"))
        self.assertIsNone(receipt["instance"])
        self.assertEqual((reads, sleeps), (1, 0))

    def test_failed_required_write_ahead_never_launches_and_final_receipt_is_truthful(self):
        for error in (OSError("injected write-ahead disk failure"), KeyboardInterrupt("cancelled write-ahead")):
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                cloud, selected, expected = self.prepare(root)
                original_replace, failures = Path.replace, 0
                def replace(path, target):
                    nonlocal failures
                    receipt = json.loads(path.read_text())
                    if receipt["cleanup"] == "unconfirmed" and failures == 0:
                        failures += 1
                        raise error
                    return original_replace(path, target)
                def launch(**kwargs):
                    cloud.objects[common.STATUS_KEY] = json.dumps(common.completed_status(selected["run"], True)).encode()
                cloud.api["autoscaling"].set_desired_capacity.side_effect = launch
                with patch.object(Path, "replace", replace), patch.object(controller, "cleanup") as cleanup, self.assertRaises(type(error)):
                    controller.run(cloud.api, "group", "bucket", selected, root, root / "out", expected)
                cloud.api["autoscaling"].set_desired_capacity.assert_not_called()
                cleanup.assert_not_called()
                self.assertEqual(failures, 1)
                receipt = json.loads((root / "out/controller.json").read_text())
                self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("failure", "not-requested"))
                self.assertIsNone(receipt["instance"])
                self.assertEqual(receipt["request"]["run"], selected["run"])

    def test_incomplete_controller_failure_bundles_retain_trusted_cleanup_and_image(self):
        for phase in ("worker-upload", "protocol", "status-timeout", "deadline"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                cloud, selected, expected = self.prepare(root)
                budget = Mock()
                budget.left.return_value = 1000
                observed = dict(InstanceId="i-" + "1" * 17, ImageId="ami-" + "2" * 17, InstanceType="c5d.24xlarge")
                launched = False
                def describe(**kwargs):
                    if phase == "deadline":
                        budget.left.side_effect = TimeoutError("benchmark deadline exhausted")
                    return {"Reservations": [{"Instances": [observed]}]}
                def launch(**kwargs):
                    nonlocal launched
                    launched = True
                    cloud.api["ec2"].describe_instances.side_effect = describe
                    status = common.completed_status(selected["run"], False)
                    if phase == "protocol":
                        status["state"] = "legacy-completed"
                    cloud.objects[common.STATUS_KEY] = json.dumps(status).encode()
                def get(**kwargs):
                    if launched and phase == "status-timeout" and kwargs["Key"] == common.STATUS_KEY:
                        raise TimeoutError("status unavailable")
                    return cloud.get(**kwargs)
                cloud.api["autoscaling"].set_desired_capacity.side_effect = launch
                cloud.api["s3"].get_object.side_effect = get
                error_type = TimeoutError if phase in ("status-timeout", "deadline") else ValueError
                with patch.object(controller, "Budget", return_value=budget), patch.object(controller, "cleanup") as cleanup, \
                     patch.object(controller.time, "sleep"), self.assertRaises(error_type):
                    controller.run(cloud.api, "group", "bucket", selected, root, root / "out", expected)
                cleanup.assert_called_once_with(cloud.api, "group")
                receipt = json.loads((root / "out/controller.json").read_text())
                self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("failure", "verified"))
                self.assertEqual(receipt["instance"]["image_id"], observed["ImageId"])
                self.assertFalse((root / "out/results.zip").exists())
                self.assertEqual((root / "out/status.json").exists(), phase in ("worker-upload", "protocol"))
                evidence = results.ingest(root / "out", root / "report", expected, "failure")
                self.assertEqual(evidence["controller"], receipt)
                self.assertFalse(evidence["history_eligible"])
                self.assertTrue(evidence["errors"])
                self.assertEqual(json.loads((root / "report/provenance.json").read_text())["controller"], receipt)
                self.assertIn(observed["ImageId"], (root / "report/summary.md").read_text())
                self.assertIn("<td><code>Controller cleanup</code></td><td><code>verified</code></td>",
                              (root / "report/summary.md").read_text())

    def test_controller_cli_uses_the_recorded_producer_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = binding()
            identity_path = root / "identity.json"
            identity_path.write_text(json.dumps(expected))
            args = ["controller", "--region", "us-east-1", "--asg", "group", "--bucket", "bucket",
                    "--role-arn", "runtime-role", "--identity", str(identity_path),
                    "--binaries", str(root / "binaries"), "--output", str(root / "out")]
            with patch("sys.argv", args), patch.object(controller, "oidc_clients", return_value={}) as clients, \
                 patch.object(controller, "run") as run, \
                 patch.object(controller.workflow_identity, "environment", side_effect=AssertionError("do not re-derive identity")):
                controller.main()
            clients.assert_called_once_with("us-east-1", "runtime-role")
            run.assert_called_once_with({}, "group", "bucket",
                {key: expected[key] for key in ("run", "baseline", "candidate", "suite")},
                root / "binaries", root / "out", provenance=expected)

    def test_controller_cli_rejects_invalid_identity_before_requesting_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity_path = root / "identity.json"
            identity_path.write_text(json.dumps({**binding(), "ref": "refs/heads/not-main"}))
            args = ["controller", "--region", "us-east-1", "--asg", "group", "--bucket", "bucket",
                    "--role-arn", "runtime-role", "--identity", str(identity_path),
                    "--binaries", str(root / "binaries"), "--output", str(root / "out")]
            with patch("sys.argv", args), patch.object(controller, "oidc_clients") as clients, \
                 patch.object(controller, "run") as run, self.assertRaisesRegex(ValueError, "exact approved main"):
                controller.main()
            clients.assert_not_called()
            run.assert_not_called()
            self.assertFalse((root / "out").exists())

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
