"""Offline submission, opaque collection and workflow credential-boundary tests."""

import copy
import base64
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

from infra.aws_benchmark import common, durable, durable_client as client, identity, results
from infra.aws_benchmark.durable_aws import RECEIPT, REQUEST
from infra.aws_benchmark.test_results import binding

NOW = datetime(2026, 10, 21, 12, tzinfo=timezone.utc)
MACHINE = "arn:aws:states:us-east-1:123456789012:stateMachine:benchmark"


class AwsError(Exception):
    def __init__(self, name):
        self.response = {"Error": {"Code": name}}


def deployment_pins(**overrides):
    return dict(dict(region="us-east-1", account="123456789012", namespace="benchmark",
                     bucket="bucket", lt_id="lt-" + "1" * 17, lt_version="9",
                     ami="ami-" + "2" * 17, source="a" * 40, subnet="subnet-" + "3" * 17,
                     security_group="sg-" + "4" * 17,
                     profile_arn="arn:aws:iam::123456789012:instance-profile/worker"), **overrides)


def fixture():
    provenance = dict(binding("bulk-1g-metadata"), candidate="a" * 40)
    pins = deployment_pins()
    request = durable.request(provenance, NOW, dict.fromkeys(common.BINARIES, common.digest(b"binary")), pins)
    return provenance, request


class Cloud:
    def __init__(self, request):
        self.api = {key: Mock() for key in ("s3", "stepfunctions")}
        self.objects = {REQUEST: client.encoded(request)}
        self.execution = None
        self.api["s3"].get_object.side_effect = self.get
        self.api["s3"].put_object.side_effect = self.put
        self.api["stepfunctions"].describe_execution.side_effect = self.describe
        self.api["stepfunctions"].start_execution.side_effect = self.start
    def get(self, **args):
        if args["Key"] not in self.objects:
            raise AwsError("NoSuchKey")
        data = self.objects[args["Key"]]
        return dict(Body=io.BytesIO(data), ContentLength=len(data))
    def put(self, **args):
        self.objects[args["Key"]] = args["Body"]
        return {}
    def describe(self, **args):
        if self.execution is None:
            raise AwsError("ExecutionDoesNotExist")
        return copy.deepcopy(self.execution)
    def start(self, **args):
        self.execution = dict(executionArn=client.execution_arn(args["stateMachineArn"], args["name"]),
                              stateMachineArn=args["stateMachineArn"], input=args["input"], status="RUNNING")
        return dict(executionArn=self.execution["executionArn"], startDate=NOW)


def binaries(root):
    for name in common.BINARIES:
        path = root / name
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(b"binary")


class SubmissionTests(unittest.TestCase):
    def test_submits_exact_identity_once_without_executing_artifacts(self):
        expected, request = fixture()
        cloud = Cloud(request)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries(root)
            result = client.submit(cloud.api, "bucket", MACHINE, expected, root, now=lambda: NOW)
        self.assertFalse(result["benchmark_success"])
        self.assertEqual(result["state"], "submitted")
        self.assertEqual(cloud.api["stepfunctions"].start_execution.call_args.kwargs,
                         dict(stateMachineArn=MACHINE, name="123-1", input='{"run":"123-1"}'))
        self.assertEqual({call.kwargs["Key"] for call in cloud.api["s3"].put_object.call_args_list},
                         {"input/" + name for name in common.BINARIES})

    def test_machine_account_and_region_must_match_immutable_pins(self):
        expected, request = fixture()
        for machine in (MACHINE.replace("us-east-1", "us-west-2"),
                        MACHINE.replace("123456789012", "111111111111")):
            with self.subTest(machine=machine):
                cloud = Cloud(request)
                with self.assertRaisesRegex(ValueError, "pinned account or region"):
                    client.submit(cloud.api, "bucket", machine, expected, Path("absent"), now=lambda: NOW)
                cloud.api["stepfunctions"].describe_execution.assert_not_called()
                cloud.api["s3"].put_object.assert_not_called()
                with self.assertRaisesRegex(ValueError, "pinned account or region"):
                    client.collect(cloud.api, "bucket", machine, "123-1", Path("absent"), now=lambda: NOW)
                cloud.api["stepfunctions"].describe_execution.assert_not_called()

    def test_repeat_submission_reads_execution_without_rewriting_running_inputs(self):
        expected, request = fixture()
        cloud = Cloud(request)
        cloud.start(stateMachineArn=MACHINE, name="123-1", input='{"run":"123-1"}')
        with patch.object(client, "read_binary", side_effect=AssertionError("must not read binaries")):
            result = client.submit(cloud.api, "bucket", MACHINE, expected, Path("absent"),
                                   now=lambda: NOW + timedelta(days=2))
        self.assertTrue(result["reused"])
        cloud.api["s3"].put_object.assert_not_called()
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_uncertain_start_is_reconciled_without_second_start(self):
        expected, request = fixture()
        cloud = Cloud(request)
        def response_lost(**args):
            cloud.start(**args)
            raise TimeoutError("lost")
        cloud.api["stepfunctions"].start_execution.side_effect = response_lost
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries(root)
            result = client.submit(cloud.api, "bucket", MACHINE, expected, root, now=lambda: NOW)
        self.assertTrue(result["reconciled"])
        self.assertEqual(cloud.api["stepfunctions"].start_execution.call_count, 1)

    def test_unknown_start_outcome_stays_uncertain_and_is_not_retried(self):
        expected, request = fixture()
        cloud = Cloud(request)
        cloud.api["stepfunctions"].start_execution.side_effect = TimeoutError("lost")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binaries(root)
            with self.assertRaisesRegex(ValueError, "outcome is uncertain"):
                client.submit(cloud.api, "bucket", MACHINE, expected, root, now=lambda: NOW)
        self.assertEqual(cloud.api["stepfunctions"].start_execution.call_count, 1)

    def test_workflow_rerun_cannot_take_over_an_earlier_attempt_request(self):
        expected, request = fixture()
        cloud = Cloud(request)
        rerun = dict(expected, run="123-2", run_url=expected["run_url"].replace("/attempts/1", "/attempts/2"))
        with self.assertRaisesRegex(ValueError, "operator request binding differs"):
            client.submit(cloud.api, "bucket", MACHINE, rerun, Path("absent"), now=lambda: NOW)
        cloud.api["s3"].put_object.assert_not_called()
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_expiry_prevents_upload_or_launch(self):
        expected, request = fixture()
        cloud = Cloud(request)
        with self.assertRaisesRegex(ValueError, "expired"):
            client.submit(cloud.api, "bucket", MACHINE, expected, Path("absent"),
                          now=lambda: common.utc(request["capacity_deadline"]))
        cloud.api["s3"].put_object.assert_not_called()
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_symlink_and_different_binary_cannot_be_submitted(self):
        expected, request = fixture()
        for symlink in (False, True):
            with self.subTest(symlink=symlink), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binaries(root)
                path = root / "baseline/rcp"
                if symlink:
                    path.unlink()
                    path.symlink_to(root / "candidate/rcp")
                else:
                    path.write_bytes(b"different")
                cloud = Cloud(request)
                with self.assertRaises((OSError, ValueError)):
                    client.submit(cloud.api, "bucket", MACHINE, expected, root, now=lambda: NOW)
                cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_other_producer_request_and_execution_are_rejected(self):
        expected, request = fixture()
        for altered in (dict(request, controller="d" * 40), dict(request, run="123-2")):
            cloud = Cloud(altered)
            with self.assertRaisesRegex(ValueError, "binding differs|workflow producer"):
                client.submit(cloud.api, "bucket", MACHINE, expected, Path("absent"), now=lambda: NOW)
            cloud.api["s3"].put_object.assert_not_called()
        cloud = Cloud(request)
        cloud.start(stateMachineArn=MACHINE, name="123-1", input='{"run":"123-2"}')
        with self.assertRaisesRegex(ValueError, "execution input differs"):
            client.submit(cloud.api, "bucket", MACHINE, expected, Path("absent"), now=lambda: NOW)

    def test_changed_or_expired_request_during_upload_cannot_start(self):
        expected, request = fixture()
        for change in (False, True):
            cloud = Cloud(request)
            count = 0
            def upload(**args):
                nonlocal count
                cloud.put(**args)
                count += 1
                if count == len(common.BINARIES) and change:
                    cloud.objects[REQUEST] = client.encoded(dict(request, controller="d" * 40))
            cloud.api["s3"].put_object.side_effect = upload
            def now():
                return NOW if count < len(common.BINARIES) or change else common.utc(request["capacity_deadline"])
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binaries(root)
                with self.assertRaises(ValueError):
                    client.submit(cloud.api, "bucket", MACHINE, expected, root, now=now)
            cloud.api["stepfunctions"].start_execution.assert_not_called()


class RetryTests(unittest.TestCase):
    def test_transient_upload_read_and_describe_errors_keep_the_original_attempt(self):
        expected, request = fixture()
        cloud = Cloud(request)
        counts = dict(read=0, upload=0, describe=0)
        def get(**args):
            counts["read"] += 1
            if counts["read"] == 1:
                raise AwsError("SlowDown")
            return cloud.get(**args)
        def put(**args):
            counts["upload"] += 1
            if counts["upload"] == 3:
                raise AwsError("SlowDown")
            return cloud.put(**args)
        def describe(**args):
            counts["describe"] += 1
            if counts["describe"] == 1:
                raise AwsError("ThrottlingException")
            return cloud.describe(**args)
        cloud.api["s3"].get_object.side_effect = get
        cloud.api["s3"].put_object.side_effect = put
        cloud.api["stepfunctions"].describe_execution.side_effect = describe
        with tempfile.TemporaryDirectory() as directory, patch.object(common.time, "sleep"):
            binaries(Path(directory))
            result = client.submit(cloud.api, "bucket", MACHINE, expected, directory, now=lambda: NOW)
        self.assertEqual(result["run"], "123-1")
        cloud.api["stepfunctions"].start_execution.assert_called_once()
        self.assertEqual(counts["upload"], len(common.BINARIES) + 1)
        for call in cloud.api["s3"].put_object.call_args_list:
            data = call.kwargs["Body"]
            self.assertEqual(call.kwargs["ChecksumSHA256"],
                             base64.b64encode(bytes.fromhex(common.digest(data))).decode())

    def test_upload_retries_are_bounded_and_permanent_denial_is_not_retried(self):
        for error, count in (("SlowDown", 3), ("AccessDenied", 1)):
            with self.subTest(error=error), tempfile.TemporaryDirectory() as directory, patch.object(common.time, "sleep"):
                expected, request = fixture()
                cloud = Cloud(request)
                cloud.api["s3"].put_object.side_effect = AwsError(error)
                binaries(Path(directory))
                with self.assertRaises(AwsError):
                    client.submit(cloud.api, "bucket", MACHINE, expected, directory, now=lambda: NOW)
                self.assertEqual(cloud.api["s3"].put_object.call_count, count)
                cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_final_request_read_exhausting_budget_cannot_start_execution(self):
        expected, request = fixture()
        cloud = Cloud(request)
        budget = Mock()
        budget.left.return_value = 100
        reads = 0
        def get(**args):
            nonlocal reads
            reads += 1
            result = cloud.get(**args)
            if reads == 2:
                budget.left.side_effect = TimeoutError("short job budget exhausted")
            return result
        cloud.api["s3"].get_object.side_effect = get
        with tempfile.TemporaryDirectory() as directory:
            binaries(Path(directory))
            with self.assertRaisesRegex(TimeoutError, "short job budget exhausted"):
                client.submit(cloud.api, "bucket", MACHINE, expected, directory, now=lambda: NOW, budget=budget)
        self.assertEqual(cloud.api["s3"].put_object.call_count, len(common.BINARIES))
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_short_oidc_clients_share_authentication_without_extending_the_session(self):
        budget, session, config = Mock(), Mock(), object()
        with patch.object(client, "oidc_session", return_value=(session, config)) as authenticate:
            result = client.oidc_clients("us-east-1", "role", budget=budget)
        authenticate.assert_called_once_with("us-east-1", "role", session_prefix="rcp-durable-",
                                             duration_seconds=900, budget=budget, reserve=30)
        self.assertEqual(set(result), {"s3", "stepfunctions"})
        self.assertEqual([call.args[0] for call in session.client.call_args_list], ["s3", "stepfunctions"])
        self.assertTrue(all(call.kwargs == {"config": config} for call in session.client.call_args_list))

    def test_timeout_with_none_response_keeps_original_error_classification(self):
        error = TimeoutError("synthetic")
        error.response = None
        self.assertIsNone(client.code(error))



class CollectionTests(unittest.TestCase):
    def setup_cloud(self, *, complete=True, status_success=True):
        expected, request = fixture()
        cloud = Cloud(request)
        cloud.start(stateMachineArn=MACHINE, name="123-1", input='{"run":"123-1"}')
        cloud.execution["status"] = "SUCCEEDED" if complete else "RUNNING"
        instance = dict(InstanceId="i-" + "3" * 17, LaunchTime=NOW + timedelta(seconds=60),
            ImageId=request["pins"]["ami"], InstanceType="c5d.24xlarge",
            LaunchTemplate={"LaunchTemplateId": request["pins"]["lt_id"], "Version": request["pins"]["lt_version"]})
        attempt = durable.attempt(request, 1, NOW + timedelta(seconds=30))
        instance.update(ClientToken=attempt["token"], SubnetId=request["pins"]["subnet"],
                        IamInstanceProfile={"Arn": request["pins"]["profile_arn"]},
                        SecurityGroups=[{"GroupId": request["pins"]["security_group"]}],
                        Tags=[dict(Key=key, Value=value) for key, value in durable.tags(request, attempt).items()])
        bound = durable.bind(request, instance, attempt)
        finish = common.utc(request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        observed = dict(protocol=durable.PROTOCOL, request=request, binding=bound,
            observed_at=common.timestamp(finish), execution_arn=cloud.execution["executionArn"],
            attempt=attempt, attempts=1, allocation_consumed=True, launch_outcome="accepted",
            known_instance_ids=[bound["instance_id"]], capacity_failures=[],
            phase="complete" if complete else "closing", runtime_verified=True, worker_published=True,
            cleanup=dict(live_instances=[], unresolved_allocation=False, allocation_consumed=True,
                         lease_released=False, verified_at=common.timestamp(finish),
                         deleted_roots=["vol-" + "4" * 17]) if complete else None,
            seen={bound["instance_id"]: dict(state="terminated", launch_at=bound["launch_at"],
                                             root_volumes=["vol-" + "4" * 17])})
        archive = b"opaque archive never parsed by credentialed collector"
        status = dict(common.completed_status("123-1", status_success), sha256=common.digest(archive))
        cloud.objects.update({RECEIPT: client.encoded(observed), common.STATUS_KEY: client.encoded(status),
                              common.RESULTS_KEY: archive})
        return expected, cloud, observed, finish

    def test_complete_collection_preserves_raw_bytes_without_claiming_performance_validity(self):
        expected, cloud, observed, now = self.setup_cloud()
        with tempfile.TemporaryDirectory() as directory, patch("zipfile.ZipFile", side_effect=AssertionError("must stay opaque")):
            output = Path(directory) / "raw"
            result = client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
            receipt = json.loads((output / "controller.json").read_text())
            self.assertEqual(receipt["identity"], expected)
            self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("success", "verified"))
            self.assertEqual((output / "results.zip").read_bytes(), cloud.objects[common.RESULTS_KEY])
            self.assertEqual(result["state"], "complete")
            self.assertFalse(result["benchmark_success"])
            self.assertTrue(result["requires_offline_validation"])

    def test_pending_cleanup_and_failed_worker_never_become_success(self):
        for complete, success in ((False, True), (True, False)):
            with self.subTest(complete=complete, success=success), tempfile.TemporaryDirectory() as directory:
                expected, cloud, observed, now = self.setup_cloud(complete=complete, status_success=success)
                output = Path(directory) / "raw"
                result = client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
                self.assertEqual(result["worker_reported_success"], success)
                self.assertFalse(result["benchmark_success"])
                self.assertEqual(result["conclusion"], "failure" if complete else "pending")

    def test_historical_cleanup_uses_its_recorded_delivery_drain(self):
        _, cloud, _, now = self.setup_cloud()
        with tempfile.TemporaryDirectory() as directory, patch.object(durable, "DELIVERY_DRAIN_SECONDS", 3600):
            output = Path(directory) / "raw"
            result = client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
        self.assertEqual((result["state"], result["conclusion"], result["cleanup"]),
                         ("complete", "success", "verified"))

    def test_missing_roots_and_early_cleanup_fail_with_diagnostic_receipt(self):
        for mutation in (lambda value: value["cleanup"].update(deleted_roots=[]),
                         lambda value: value["cleanup"].update(verified_at=common.timestamp(NOW))):
            expected, cloud, observed, now = self.setup_cloud()
            mutation(observed)
            cloud.objects[RECEIPT] = client.encoded(observed)
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "raw"
                with self.assertRaises(ValueError):
                    client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
                receipt = json.loads((output / "controller.json").read_text())
                self.assertNotEqual(receipt["outcome"], "success")

    def test_direct_cleanup_rejects_live_unresolved_or_inconsistent_inventory(self):
        changes = (
            lambda value: value["cleanup"].update(live_instances=["i-" + "3" * 17]),
            lambda value: value["cleanup"].update(unresolved_allocation=True),
            lambda value: value["cleanup"].update(allocation_consumed=False),
            lambda value: value["cleanup"].update(capacity=[0, 0, 0]),
            lambda value: value.update(known_instance_ids=[]),
        )
        for index, change in enumerate(changes):
            with self.subTest(change=index), tempfile.TemporaryDirectory() as directory:
                expected, cloud, observed, now = self.setup_cloud()
                change(observed)
                cloud.objects[RECEIPT] = client.encoded(observed)
                output = Path(directory) / "raw"
                with self.assertRaises(ValueError):
                    client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
                receipt = json.loads((output / "controller.json").read_text())
                self.assertEqual((receipt["cleanup"], receipt["outcome"]), ("unconfirmed", "failure"))

    def test_uncertain_allocation_is_not_cleanup_proof_from_an_empty_inventory(self):
        expected, cloud, observed, now = self.setup_cloud()
        observed.update(binding=None, seen={}, known_instance_ids=[], launch_outcome="uncertain")
        observed["cleanup"]["deleted_roots"] = []
        cloud.objects[RECEIPT] = client.encoded(observed)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "raw"
            with self.assertRaisesRegex(ValueError, "physical cleanup proof"):
                client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
            receipt = json.loads((output / "controller.json").read_text())
            self.assertEqual(receipt["cleanup"], "unconfirmed")

    def test_failed_coordinator_preserves_verified_physical_cleanup(self):
        for status in ("FAILED", "TIMED_OUT", "ABORTED"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                expected, cloud, observed, now = self.setup_cloud()
                cloud.execution["status"] = status
                output = Path(directory) / "raw"
                result = client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
                receipt = json.loads((output / "controller.json").read_text())
                self.assertEqual((receipt["cleanup"], receipt["outcome"]), ("verified", "failure"))
                self.assertEqual((result["state"], result["conclusion"]), ("complete", "failure"))
                self.assertFalse(result["benchmark_success"])

    def test_terminal_never_launched_execution_exports_cleanup_without_qualification(self):
        for status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                expected, cloud, observed, now = self.setup_cloud()
                cloud.execution["status"] = status
                observed.update(execution_arn=None, attempt=None, attempts=0, allocation_consumed=False,
                                launch_outcome="none", known_instance_ids=[], binding=None, seen={},
                                runtime_verified=False, worker_published=False)
                observed["cleanup"].update(deleted_roots=[], allocation_consumed=False)
                cloud.objects[RECEIPT] = client.encoded(observed)
                cloud.objects[common.STATUS_KEY] = client.encoded(common.pending_status("123-1"))
                output = Path(directory) / "raw"
                result = client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
                receipt = json.loads((output / "controller.json").read_text())
                self.assertEqual((receipt["cleanup"], receipt["outcome"]), ("verified", "failure"))
                self.assertEqual(result["execution_binding"], "not-launched")
                self.assertEqual((result["state"], result["conclusion"]), ("complete", "failure"))
                self.assertFalse(result["benchmark_success"])
                self.assertEqual(json.loads((output / "durable.json").read_text()), observed)

    def test_unbound_execution_cannot_hide_launch_or_request_mismatch(self):
        changes = [
            lambda value, cloud: value.update(execution_arn=MACHINE + ":other"),
            lambda value, cloud: value.update(attempt={"started_at": common.timestamp(NOW)}),
            lambda value, cloud: value.update(attempts=1),
            lambda value, cloud: value.update(allocation_consumed=True),
            lambda value, cloud: value.update(launch_outcome="uncertain"),
            lambda value, cloud: value.update(known_instance_ids=["i-" + "3" * 17]),
            lambda value, cloud: value.update(binding={"instance_id": "i-" + "3" * 17}),
            lambda value, cloud: value.update(seen={"i-" + "3" * 17: {}}),
            lambda value, cloud: value.update(runtime_verified=True),
            lambda value, cloud: value.update(worker_published=True),
            lambda value, cloud: value.update(phase="requested"),
            lambda value, cloud: value.pop("attempt"),
            lambda value, cloud: (value.update(phase="closing"), cloud.execution.update(status="SUCCEEDED")),
            lambda value, cloud: value["request"].update(controller="e" * 40),
        ]
        for index, change in enumerate(changes):
            with self.subTest(change=index), tempfile.TemporaryDirectory() as directory:
                expected, cloud, observed, now = self.setup_cloud()
                cloud.execution["status"] = "FAILED"
                observed.update(execution_arn=None, attempt=None, attempts=0, allocation_consumed=False,
                                launch_outcome="none", known_instance_ids=[], binding=None, seen={},
                                runtime_verified=False, worker_published=False)
                observed["cleanup"].update(deleted_roots=[], allocation_consumed=False)
                change(observed, cloud)
                cloud.objects[RECEIPT] = client.encoded(observed)
                output = Path(directory) / "raw"
                with self.assertRaisesRegex(ValueError, "watchdog receipt structure and request binding"):
                    client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
                self.assertEqual(json.loads((output / "controller.json").read_text())["outcome"], "failure")
                self.assertEqual((output / "durable.json").read_bytes(), cloud.objects[RECEIPT])

    def test_missing_admission_proof_preserves_cleanup_but_fails_qualification(self):
        expected, cloud, observed, now = self.setup_cloud()
        observed["runtime_verified"] = False
        cloud.objects[RECEIPT] = client.encoded(observed)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "raw"
            result = client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
            receipt = json.loads((output / "controller.json").read_text())
            self.assertEqual((receipt["cleanup"], receipt["outcome"]), ("verified", "failure"))
            self.assertEqual(result["conclusion"], "failure")

    def test_artifact_failures_preserve_independent_cleanup_and_partial_diagnostics(self):
        for problem in ("malformed-status", "missing-archive", "wrong-digest"):
            with self.subTest(problem=problem), tempfile.TemporaryDirectory() as directory:
                expected, cloud, observed, now = self.setup_cloud()
                if problem == "malformed-status":
                    cloud.objects[common.STATUS_KEY] = b"{not json"
                elif problem == "missing-archive":
                    cloud.objects.pop(common.RESULTS_KEY)
                else:
                    cloud.objects[common.RESULTS_KEY] = b"mismatched archive"
                output = Path(directory) / "raw"
                with self.assertRaises(Exception):
                    client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
                receipt = json.loads((output / "controller.json").read_text())
                self.assertEqual((receipt["cleanup"], receipt["outcome"]), ("verified", "failure"))
                self.assertIn("collection_error", receipt)
                self.assertEqual((output / "status.json").read_bytes(), cloud.objects[common.STATUS_KEY])
                self.assertEqual(json.loads((output / "collection.json").read_text())["conclusion"], "failure")

    def test_replacement_cleanup_is_preserved_without_qualification(self):
        expected, cloud, observed, now = self.setup_cloud()
        observed["seen"]["i-" + "5" * 17] = dict(state="terminated", launch_at=common.timestamp(NOW),
                                                 root_volumes=["vol-" + "6" * 17])
        observed["cleanup"]["deleted_roots"].append("vol-" + "6" * 17)
        observed["known_instance_ids"].append("i-" + "5" * 17)
        cloud.objects[RECEIPT] = client.encoded(observed)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "raw"
            result = client.collect(cloud.api, "bucket", MACHINE, "123-1", output, now=lambda: now)
            receipt = json.loads((output / "controller.json").read_text())
        self.assertEqual((receipt["cleanup"], receipt["outcome"]), ("verified", "failure"))
        self.assertEqual(result["conclusion"], "failure")

    def test_wrong_run_and_archive_digest_fail(self):
        expected, cloud, observed, now = self.setup_cloud()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "raw"
            with self.assertRaisesRegex(ValueError, "binding differs"):
                client.collect(cloud.api, "bucket", MACHINE, "123-2", output, now=lambda: now)
            self.assertFalse(output.exists())
        cloud.objects[common.RESULTS_KEY] = b"corrupted"
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "worker archive"):
                client.collect(cloud.api, "bucket", MACHINE, "123-1", Path(directory) / "raw", now=lambda: now)

    def test_capacity_expiry_without_worker_is_complete_failure(self):
        expected, cloud, observed, now = self.setup_cloud()
        observed.update(binding=None, seen={}, known_instance_ids=[], allocation_consumed=False,
                        launch_outcome="capacity-rejected", runtime_verified=False, worker_published=False)
        observed["cleanup"].update(deleted_roots=[], allocation_consumed=False)
        cloud.objects[RECEIPT] = client.encoded(observed)
        cloud.objects[common.STATUS_KEY] = client.encoded(common.pending_status("123-1"))
        with tempfile.TemporaryDirectory() as directory:
            result = client.collect(cloud.api, "bucket", MACHINE, "123-1", Path(directory) / "raw", now=lambda: now)
        self.assertEqual(result["state"], "complete")
        self.assertEqual(result["conclusion"], "failure")

    def test_matching_unlaunched_running_receipt_is_pending_without_claiming_cleanup(self):
        expected, cloud, observed, now = self.setup_cloud(complete=False)
        observed.update(execution_arn=None, attempt=None, attempts=0, allocation_consumed=False,
                        launch_outcome="none", known_instance_ids=[], binding=None, seen={},
                        runtime_verified=False, worker_published=False, phase="armed")
        cloud.objects[RECEIPT] = client.encoded(observed)
        cloud.objects[common.STATUS_KEY] = client.encoded(common.pending_status("123-1"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = client.collect(cloud.api, "bucket", MACHINE, "123-1", root / "raw", now=lambda: now)
            self.assertEqual((result["state"], result["conclusion"], result["cleanup"]),
                             ("pending", "pending", "unconfirmed"))
            evidence, verdict = client.validate_offline(root / "raw", root / "report")
            self.assertFalse(verdict["accepted"])
            self.assertEqual(evidence["producer_conclusion"], "pending")
            self.assertIn("still pending", (root / "report/summary.md").read_text())

    def test_missing_first_watchdog_receipt_is_pending_online_and_offline(self):
        _, request = fixture()
        cloud = Cloud(request)
        cloud.start(stateMachineArn=MACHINE, name="123-1", input='{"run":"123-1"}')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = client.collect(cloud.api, "bucket", MACHINE, "123-1", root / "raw", now=lambda: NOW)
            self.assertEqual((result["state"], result["conclusion"], result["cleanup"]),
                             ("pending", "pending", "unconfirmed"))
            self.assertEqual(result["execution_binding"], "unconfirmed")
            self.assertFalse(result["worker_reported_success"])
            self.assertFalse(result["benchmark_success"])
            receipt = json.loads((root / "raw/controller.json").read_text())
            self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("incomplete", "unconfirmed"))
            self.assertNotIn("collection_error", receipt)
            self.assertEqual({p.name for p in (root / "raw").iterdir()},
                             {"identity.json", "controller.json", "collection.json"})
            evidence, verdict = client.validate_offline(root / "raw", root / "report")
            self.assertEqual((evidence["producer_conclusion"], verdict["state"]), ("pending", "pending"))
            self.assertEqual(evidence["errors"], [])
            self.assertFalse(evidence["history_eligible"])
            self.assertFalse(verdict["accepted"])
            self.assertFalse(verdict["history_published"])
            self.assertIn("still pending", (root / "report/summary.md").read_text())
        self.assertEqual([call.kwargs["Key"] for call in cloud.api["s3"].get_object.call_args_list],
                         [REQUEST, RECEIPT])
        cloud.api["s3"].put_object.assert_not_called()
        cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_terminal_execution_missing_watchdog_receipt_is_failure(self):
        for status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                _, cloud, _, now = self.setup_cloud()
                cloud.execution["status"] = status
                cloud.objects.pop(RECEIPT)
                root = Path(directory)
                with self.assertRaisesRegex(ValueError, "watchdog receipt read"):
                    client.collect(cloud.api, "bucket", MACHINE, "123-1", root / "raw", now=lambda: now)
                result = json.loads((root / "raw/collection.json").read_text())
                self.assertEqual((result["conclusion"], result["cleanup"]), ("failure", "unconfirmed"))
                self.assertFalse(result["benchmark_success"])
                evidence, verdict = client.validate_offline(root / "raw", root / "report")
                self.assertEqual((evidence["producer_conclusion"], verdict["state"]), ("failure", "failure"))
                self.assertFalse(verdict["accepted"])

    def test_running_receipt_read_errors_are_not_treated_as_missing(self):
        for error in (AwsError("AccessDenied"), AwsError("NoSuchBucket"), AwsError("404"),
                      TimeoutError("receipt read outcome unknown")):
            with self.subTest(error=client.code(error)), tempfile.TemporaryDirectory() as directory, patch.object(common.time, "sleep"):
                _, cloud, _, now = self.setup_cloud(complete=False)
                def get(**args):
                    if args["Key"] == RECEIPT:
                        raise error
                    return cloud.get(**args)
                cloud.api["s3"].get_object.side_effect = get
                root = Path(directory)
                with self.assertRaisesRegex(ValueError, "watchdog receipt read") as caught:
                    client.collect(cloud.api, "bucket", MACHINE, "123-1", root / "raw", now=lambda: now)
                self.assertIs(caught.exception.__cause__, error)
                receipt = json.loads((root / "raw/controller.json").read_text())
                self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("failure", "unconfirmed"))
                self.assertEqual(json.loads((root / "raw/collection.json").read_text())["conclusion"], "failure")

    def test_running_missing_worker_evidence_is_failure_after_watchdog_receipt(self):
        for key in (common.STATUS_KEY, common.RESULTS_KEY):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                _, cloud, _, now = self.setup_cloud(complete=False)
                cloud.objects.pop(key)
                root = Path(directory)
                with self.assertRaisesRegex(ValueError, "worker status|worker archive"):
                    client.collect(cloud.api, "bucket", MACHINE, "123-1", root / "raw", now=lambda: now)
                self.assertTrue((root / "raw/durable.json").exists())
                receipt = json.loads((root / "raw/controller.json").read_text())
                self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("failure", "unconfirmed"))

    def test_malformed_watchdog_receipt_keeps_bounded_raw_evidence_and_trusted_diagnostic(self):
        _, request = fixture()
        wrong_request = copy.deepcopy(request)
        wrong_request["controller"] = "e" * 40
        payloads = (b"[]", b"{not-json", b'{"protocol":"incorrect"}',
                    client.encoded(dict(protocol="incorrect", request=request)),
                    client.encoded(dict(protocol=durable.PROTOCOL, request=wrong_request)))
        for payload in payloads:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as directory:
                expected, cloud, observed, now = self.setup_cloud(complete=False)
                cloud.objects[RECEIPT] = payload
                root = Path(directory)
                with self.assertRaisesRegex(ValueError, "watchdog receipt structure and request binding"):
                    client.collect(cloud.api, "bucket", MACHINE, "123-1", root / "raw", now=lambda: now)
                receipt = json.loads((root / "raw/controller.json").read_text())
                self.assertEqual((receipt["outcome"], receipt["cleanup"]), ("failure", "unconfirmed"))
                self.assertEqual((root / "raw/durable.json").read_bytes(), payload)
                evidence, verdict = client.validate_offline(root / "raw", root / "report")
                self.assertFalse(verdict["accepted"])
                self.assertIn("diagnostic", (root / "report/summary.md").read_text())

    def test_historical_missing_observed_image_stays_unknown(self):
        expected, cloud, observed, now = self.setup_cloud()
        observed["binding"].pop("image_id")
        observed["binding"].pop("instance_type")
        cloud.objects[RECEIPT] = client.encoded(observed)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client.collect(cloud.api, "bucket", MACHINE, "123-1", root / "raw", now=lambda: now)
            instance = json.loads((root / "raw/controller.json").read_text())["instance"]
            self.assertIsNone(instance["image_id"])
            self.assertIsNone(instance["instance_type"])


class QualificationTests(unittest.TestCase):
    def complete_files(self, source):
        from infra.aws_benchmark.test_durable_worker import durable_bundle
        expected, policy = durable_bundle(source, direct=True)
        receipt = json.loads((source / "controller.json").read_text())
        window = receipt["window"]
        pins = deployment_pins(ami=policy["ami_id"], source=policy["harness_revision"])
        request = durable.request(expected, common.utc(window["request_expires"]) - timedelta(hours=12),
                                  receipt["request"]["sha256"], pins)
        bound = dict(instance_id=window["instance_id"], launch_at=window["launch_at"], deadline=window["deadline"],
                     attempt_id=window["attempt_id"], attempt_started_at=window["attempt_started_at"],
                     close_at=common.timestamp(common.utc(window["attempt_started_at"]) + timedelta(seconds=10800)))
        observed = dict(protocol=durable.PROTOCOL, request=request, binding=bound, phase="complete")
        collection = dict(run=expected["run"], state="complete", conclusion="success", execution_status="SUCCEEDED")
        for name, value in (("identity.json", expected), ("durable.json", observed), ("collection.json", collection)):
            (source / name).write_bytes(client.encoded(value))
        return expected, policy

    def collect_fixture(self, root, *, execution_status="SUCCEEDED", cleanup=True, observed_identity=True):
        seed, output = root / "seed", root / "raw"
        expected, _ = self.complete_files(seed)
        observed = json.loads((seed / "durable.json").read_text())
        bound = observed["binding"]
        if observed_identity:
            bound.update({k: v for k, v in json.loads((seed / "controller.json").read_text())["instance"].items()
                          if k in ("image_id", "instance_type")})
        cloud = Cloud(observed["request"])
        cloud.start(stateMachineArn=MACHINE, name=expected["run"], input=json.dumps({"run": expected["run"]}))
        cloud.execution["status"] = execution_status
        finish = common.utc(observed["request"]["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        root_volume = "vol-" + "4" * 17
        observed.update(observed_at=common.timestamp(finish), execution_arn=cloud.execution["executionArn"],
            phase="complete" if cleanup else "closing", close_reason="manual reconciliation required" if not cleanup else None,
            allocation_consumed=True, known_instance_ids=[bound["instance_id"]], runtime_verified=True, worker_published=True,
            seen={bound["instance_id"]: dict(state="terminated", launch_at=bound["launch_at"], root_volumes=[root_volume])},
            cleanup=dict(live_instances=[], unresolved_allocation=False, allocation_consumed=True, lease_released=False,
                         verified_at=common.timestamp(finish), deleted_roots=[root_volume]) if cleanup else None)
        cloud.objects.update({RECEIPT: client.encoded(observed), common.STATUS_KEY: (seed / "status.json").read_bytes(),
                              common.RESULTS_KEY: (seed / "results.zip").read_bytes()})
        result = client.collect(cloud.api, "bucket", MACHINE, expected["run"], output, now=lambda: finish)
        return result

    def test_terminal_cleanup_hold_retains_measurements_in_failure_report(self):
        for status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                collection = self.collect_fixture(root, execution_status=status, cleanup=False)
                self.assertEqual((collection["state"], collection["conclusion"], collection["cleanup"]),
                                 ("held", "failure", "unconfirmed"))
                self.assertTrue(collection["worker_reported_success"])
                self.assertFalse(collection["benchmark_success"])
                evidence, verdict = client.validate_offline(root / "raw", root / "report")
                self.assertFalse(verdict["accepted"])
                self.assertEqual(evidence["producer_conclusion"], "failure")
                self.assertFalse(evidence["history_eligible"])
                self.assertTrue((root / "report/derived.json").exists())
                self.assertTrue((root / "report/index.html").exists())
                self.assertEqual((root / "report/raw/results.zip").read_bytes(), (root / "raw/results.zip").read_bytes())

    def test_unknown_historical_instance_fields_keep_measurements_without_qualification(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            collection = self.collect_fixture(root, observed_identity=False)
            self.assertEqual((collection["conclusion"], collection["cleanup"]), ("success", "verified"))
            receipt = json.loads((root / "raw/controller.json").read_text())
            self.assertIsNone(receipt["instance"]["image_id"])
            self.assertIsNone(receipt["instance"]["instance_type"])
            evidence, verdict = client.validate_offline(root / "raw", root / "report")
            self.assertFalse(verdict["accepted"])
            self.assertFalse(evidence["history_eligible"])
            self.assertTrue((root / "report/derived.json").exists())
            self.assertTrue((root / "report/index.html").exists())
            self.assertEqual(json.loads((root / "report/raw/controller.json").read_text())["instance"], receipt["instance"])

    def test_complete_qualification_does_not_promote_or_publish_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / "raw", root / "report"
            self.complete_files(source)
            before = {p.name: p.read_bytes() for p in source.iterdir()}
            with patch.object(results, "stage_history", side_effect=AssertionError("must not publish")):
                evidence, verdict = client.validate_offline(source, output)
            self.assertEqual(evidence["errors"], [])
            self.assertTrue(verdict["accepted"])
            self.assertFalse(evidence["history_eligible"])
            self.assertFalse(verdict["history_published"])
            self.assertNotIn("policy_sha256", evidence)
            self.assertIn("qualification_contract_sha256", evidence)
            self.assertIn("does not establish file-content integrity", verdict["validation"])
            self.assertEqual(before, {p.name: p.read_bytes() for p in source.iterdir()})

    def test_full_qualification_rejects_baked_source_or_collection_failure(self):
        for changed in ("source", "failure"):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, output = root / "raw", root / "report"
                self.complete_files(source)
                if changed == "source":
                    observed = json.loads((source / "durable.json").read_text())
                    observed["request"]["pins"]["source"] = "e" * 40
                    (source / "durable.json").write_bytes(client.encoded(observed))
                else:
                    collection = json.loads((source / "collection.json").read_text())
                    collection.update(conclusion="failure", execution_status="FAILED")
                    (source / "collection.json").write_bytes(client.encoded(collection))
                evidence, verdict = client.validate_offline(source, output)
                self.assertFalse(verdict["accepted"])
                self.assertFalse(evidence["history_eligible"])

    def test_qualification_helper_has_no_history_eligibility_side_effect(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected, policy = self.complete_files(root)
            evidence, parsed, _ = results.inspect(root, expected, "success")
            results.validate_qualification(evidence, parsed, policy)
            self.assertFalse(evidence["history_eligible"])
            self.assertNotIn("policy_sha256", evidence)
            results.approve(evidence, parsed, policy)
            self.assertTrue(evidence["history_eligible"])



class WorkflowTests(unittest.TestCase):
    def load(self, name):
        root = Path(__file__).resolve().parents[2]
        return yaml.load((root / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader)

    def test_submission_is_short_manual_and_separate_from_legacy_worker(self):
        workflow = self.load("aws-benchmark.yml")
        self.assertEqual(set(workflow["on"]), {"workflow_dispatch"})
        self.assertEqual(workflow["on"]["workflow_dispatch"]["inputs"]["execution"]["default"], "operator-window")
        jobs = workflow["jobs"]
        self.assertEqual(jobs["benchmark"]["if"], "inputs.execution != 'durable-submit'")
        job = jobs["durable-submit"]
        self.assertIn("RCP_AWS_DURABLE_ENABLED == 'true'", job["if"])
        self.assertEqual(job["permissions"], {"contents": "read", "id-token": "write"})
        self.assertEqual(job["environment"], "aws-benchmark-durable")
        self.assertLessEqual(int(job["timeout-minutes"]), 15)
        self.assertIn('test "$GITHUB_REF"', job["steps"][0]["run"])
        self.assertEqual(jobs["build"]["permissions"], {"contents": "read"})
        self.assertNotIn("environment", jobs["build"])
        self.assertNotIn("id-token", jobs["build"]["permissions"])

    def test_collection_parses_archives_only_without_cloud_credentials(self):
        workflow = self.load("aws-benchmark-durable-collect.yml")
        self.assertEqual(set(workflow["on"]), {"workflow_dispatch"})
        collect, report = workflow["jobs"]["collect"], workflow["jobs"]["report"]
        self.assertIn('test "$GITHUB_REF"', collect["steps"][0]["run"])
        self.assertEqual(collect["permissions"], {"contents": "read", "id-token": "write"})
        self.assertEqual(report["permissions"], {"contents": "read"})
        self.assertNotIn("environment", report)
        self.assertNotIn("validate_offline", json.dumps(collect))
        self.assertIn("validate_offline", json.dumps(report))
        self.assertNotIn("durable_client collect", json.dumps(report))
        downloads = [s for s in report["steps"] if s.get("uses", "").startswith("actions/download-artifact@")]
        self.assertEqual(downloads[0]["with"]["artifact-ids"], "${{ needs.collect.outputs.artifact-id }}")
        for job in workflow["jobs"].values():
            self.assertLessEqual(int(job["timeout-minutes"]), 15)
            for step in job["steps"]:
                if "uses" in step:
                    self.assertRegex(step["uses"], r"@[0-9a-f]{40}$")


if __name__ == "__main__":
    unittest.main()
