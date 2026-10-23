"""Offline collection, immutable evidence and retirement failure boundaries."""

import base64
import copy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import zipfile
from unittest.mock import Mock, patch

from . import common, durable, durable_client, production_collection as collection, results
from .production_mailbox import MailboxPaths
from .production_runtime import ProductionService
from .production_store import RunRecord
from .test_results import archive, binding, measurement

NOW = datetime(2026, 10, 22, 1, tzinfo=timezone.utc)
MACHINE = "arn:aws:states:us-east-1:123456789012:stateMachine:production"
INSTANCE = "i-" + "3" * 17
ROOT = "vol-" + "4" * 17


class Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class Store:
    def __init__(self, state, producer):
        self.run_key, self.epoch = "repo-123-run-123", "e" * 64
        initial = dict(state=durable.initial_lease(state["request"]), producer=producer)
        self.record = RunRecord(self.run_key, self.epoch, "f" * 64,
            dict(initial_payload=initial), state, 7, False, True, None)
        self.retire_calls = []
    def read(self):
        return copy.deepcopy(self.record)
    def retire(self, proof, revision):
        common.require(revision == self.record.revision and not self.record.closed, "fenced")
        self.retire_calls.append((proof, revision))
        self.record = replace(self.record, closed=True, retirement_sha256=proof, revision=revision + 1)
        return self.read()


class S3:
    def __init__(self):
        self.objects, self.writes = {}, []
        self.lose_response = False
        self.reject_write = False
        self.bad_checksum = False
    def seed(self, key, raw):
        version = str(len(self.objects.get(key, {})) + 1)
        self.objects.setdefault(key, {})[version] = raw
        return version
    def list_objects_v2(self, **args):
        assert args["MaxKeys"] == 1
        matches = sorted(key for key in self.objects if key.startswith(args["Prefix"]))
        return dict(Contents=[dict(Key=key) for key in matches[:1]], KeyCount=min(1, len(matches)),
                    IsTruncated=len(matches) > 1)
    def get_object(self, **args):
        versions = self.objects.get(args["Key"])
        if not versions:
            raise Error("NoSuchKey")
        version = args.get("VersionId", next(reversed(versions)))
        if version not in versions:
            raise Error("NoSuchVersion")
        raw = versions[version]
        return dict(Body=io.BytesIO(raw), ContentLength=len(raw), VersionId=version,
            ServerSideEncryption="AES256", ChecksumSHA256="wrong" if self.bad_checksum else
            base64.b64encode(bytes.fromhex(common.digest(raw))).decode(), ChecksumType="FULL_OBJECT")
    def put_object(self, **args):
        self.writes.append(args)
        assert args["IfNoneMatch"] == "*" and args["ServerSideEncryption"] == "AES256"
        if args["Key"] in self.objects:
            raise Error("PreconditionFailed")
        if self.reject_write:
            raise TimeoutError("opaque SDK text must not be exposed")
        version = self.seed(args["Key"], args["Body"])
        if self.lose_response:
            raise TimeoutError("lost successful response")
        return dict(VersionId=version)


class Fixture:
    def __init__(self, temp_root):
        expected = dict(binding("bulk-1g-metadata"), candidate="a" * 40,
            workflow="wykurz/rcp/.github/workflows/aws-benchmark-production.yml@refs/heads/main")
        pins = dict(region="us-east-1", account="123456789012", namespace="production", bucket="bucket",
            lt_id="lt-" + "1" * 17, lt_version="9", ami="ami-" + "2" * 17, source="f" * 40,
            subnet="subnet-" + "3" * 17, security_group="sg-" + "4" * 17,
            profile_arn="arn:aws:iam::123456789012:instance-profile/worker")
        request = durable.request(expected, NOW, dict.fromkeys(common.BINARIES, "d" * 64), pins)
        attempt = durable.attempt(request, 1, NOW)
        self.instance = dict(InstanceId=INSTANCE, State=dict(Name="terminated", Code=48),
            LaunchTime=NOW + timedelta(seconds=60), ImageId=pins["ami"], InstanceType="c5d.24xlarge",
            ClientToken=attempt["token"], SubnetId=pins["subnet"], SecurityGroups=[dict(GroupId=pins["security_group"])],
            IamInstanceProfile=dict(Arn=pins["profile_arn"]),
            Tags=[dict(Key=k, Value=v) for k, v in durable.tags(request, attempt).items()],
            LaunchTemplate=dict(LaunchTemplateId=pins["lt_id"], Version=pins["lt_version"]))
        self.instance["State"] = dict(Name="running", Code=16)
        bound = durable.bind(request, self.instance, attempt)
        self.instance["State"] = dict(Name="terminated", Code=48)
        self.finish = common.utc(request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        self.execution_arn = durable_client.execution_arn(MACHINE, request["run"])
        state = durable.initial_lease(request)
        state.update(phase="complete", execution_arn=self.execution_arn, submission_arn=self.execution_arn,
            attempt=attempt, attempts=1, binding=bound, allocation_consumed=True, launch_outcome="accepted",
            known_instance_ids=[INSTANCE], runtime_verified=True, worker_published=True,
            cleanup=dict(verified_at=common.timestamp(self.finish), live_instances=[], deleted_roots=[ROOT],
                unresolved_allocation=False, allocation_consumed=True, lease_released=False),
            seen={INSTANCE: dict(state="terminated", launch_at=bound["launch_at"], root_volumes=[ROOT])})
        producer = dict(repository="wykurz/rcp", repository_id="123", repository_owner_id="44",
            ref=expected["ref"], workflow_ref=expected["workflow"], workflow_sha=expected["controller"],
            sha=expected["controller"], run_id="123", run_attempt="1", actor_id="44",
            event_name="workflow_dispatch", created_at=common.timestamp(NOW))
        self.store = Store(state, producer)
        self.s3, self.ec2, self.sfn, self.scheduler = S3(), Mock(), Mock(), Mock()
        self.instance_error, self.root_present, self.live, self.orphan, self.pages = None, False, False, False, False
        self.ec2.describe_instances.side_effect = self.instances
        self.ec2.describe_volumes.side_effect = self.volumes
        self.config = SimpleNamespace(**pins, table="runs", runtime_group="expiry",
            termination_role="arn:aws:iam::123456789012:role/termination")
        self.api = dict(s3=self.s3, ec2=self.ec2, stepfunctions=self.sfn, scheduler=self.scheduler)
        self.service = ProductionService(self.api, self.config, self.store.run_key, self.store.epoch,
            store=self.store, now=lambda: self.finish)
        self.execution = dict(executionArn=self.execution_arn, stateMachineArn=MACHINE, name=request["run"], status="SUCCEEDED",
            input=collection.encoded(dict(run_key=self.store.run_key, epoch=self.store.epoch, run=request["run"])).decode())
        self.sfn.describe_state_machine.return_value = dict(stateMachineArn=MACHINE, type="STANDARD")
        self.sfn.describe_execution.side_effect = lambda **kw: copy.deepcopy(self.execution)
        self.scheduler.get_schedule.side_effect = lambda **kw: self.service.schedule(binding=bound)
        run = dict(id=123, run_attempt=1, repository=dict(id=123, full_name="wykurz/rcp", owner=dict(id=44)),
            head_sha=producer["sha"], head_branch="main", path=".github/workflows/aws-benchmark-production.yml",
            event="workflow_dispatch", actor=dict(id=44), status="completed", conclusion="success")
        job = dict(id=1, run_id=123, run_attempt=1, head_sha=producer["sha"], status="completed",
            conclusion="success", completed_at=common.timestamp(NOW + timedelta(hours=2)))
        self.documents = dict(attempt=copy.deepcopy(run), current=copy.deepcopy(run),
            attempt_jobs=dict(total_count=1, jobs=[copy.deepcopy(job)]),
            current_jobs=dict(total_count=1, jobs=[copy.deepcopy(job)]), pending_deployments=[])
        self.reader = Mock()
        self.reader.read.side_effect = lambda producer: copy.deepcopy(self.documents)
        self.collector = collection.Collector(self.service, MACHINE, self.reader, temp_root=temp_root, now=lambda: self.finish)
        self.paths = MailboxPaths.for_instance(pins["account"], pins["region"], INSTANCE)
        status = common.completed_status(request["run"], True)
        window, command = durable.window(request, bound), durable.worker_command(request, bound)
        worker = dict(window=window, request=command, status=status, harness_revision=pins["source"],
            admission=dict(accepted=True, required_seconds=9852, remaining_seconds=10410),
            guest_instance_id=INSTANCE, service_returncode=0,
            timing_basis="projected workload with explicit headroom; not a measured complete run",
            runtime_origin="attempt_started_at")
        raw = archive([("worker.json", results.encoded(worker)), (results.RESULT, results.encoded(measurement(expected)))])
        self.s3.seed(self.paths.results, raw)
        self.s3.seed(self.paths.status, collection.encoded(dict(status, sha256=common.digest(raw))))
        self.publish()
    def publish(self):
        self.s3.seed("production/runs/" + self.store.run_key + "/durable-receipt.json",
                     self.service.receipt_bytes(self.store.record.state))
    def instances(self, **args):
        if "InstanceIds" in args:
            if self.instance_error == "empty":
                return dict(Reservations=[])
            if self.instance_error:
                raise Error(self.instance_error)
            return dict(Reservations=[dict(Instances=[copy.deepcopy(self.instance)])])
        if self.live:
            live = dict(self.instance, State=dict(Name="running"))
            return dict(Reservations=[dict(Instances=[live])])
        return dict(Reservations=[])
    def volumes(self, **args):
        if "VolumeIds" in args:
            if self.root_present:
                return dict(Volumes=[dict(VolumeId=ROOT)])
            raise Error("InvalidVolume.NotFound")
        if self.orphan:
            return dict(Volumes=[dict(VolumeId="vol-" + "5" * 17)])
        if self.pages:
            return dict(Volumes=[], NextToken="same")
        return dict(Volumes=[])


class CollectionTests(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.create_connection", side_effect=AssertionError("offline tests only"))
        self.network.start()
        self.addCleanup(self.network.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.fx = Fixture(self.directory.name)
    def assertHeld(self):
        result = self.fx.collector.collect(retire=True)
        self.assertEqual(result["state"], "held", result)
        self.assertFalse(result["retired"])
        self.assertEqual(self.fx.store.retire_calls, [])
        return result
    def test_complete_metadata_run_retains_exact_versions_and_closes_once(self):
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        self.assertTrue(value["retired"])
        self.assertEqual(len(self.fx.store.retire_calls), 1)
        ref = value["retention"]
        manifest = json.loads(self.fx.s3.objects[ref["key"]][ref["version"]])
        self.assertEqual(manifest["identity"]["workflow"], self.fx.store.record.admission["initial_payload"]["producer"]["workflow_ref"])
        self.assertEqual(manifest["schema"], collection.SCHEMA)
        self.assertTrue(all(len(v) == 1 for k, v in self.fx.s3.objects.items() if "/collection/" in k))
        writes = len(self.fx.s3.writes)
        again = self.fx.collector.collect(retire=True)
        self.assertEqual(again, value)
        self.assertEqual(len(self.fx.s3.writes), writes)
    def test_production_submission_proof_does_not_broaden_legacy_cli_conclusions(self):
        self.assertEqual(results.CONCLUSIONS, ("success", "failure", "cancelled", "skipped"))
        self.assertIs(collection.CONCLUSIONS, results.SUBMISSION_CONCLUSIONS)

    def test_cancelled_or_failed_submission_does_not_invalidate_successful_worker(self):
        for conclusion in sorted(results.SUBMISSION_CONCLUSIONS - {"success"}):
            with self.subTest(conclusion=conclusion):
                fx = Fixture(self.directory.name)
                for kind in ("attempt", "current"):
                    fx.documents[kind]["conclusion"] = conclusion
                    fx.documents[kind + "_jobs"]["jobs"][0]["conclusion"] = conclusion
                value = fx.collector.collect(retire=True)
                self.assertEqual(value["state"], "success", value)
                report = json.loads(fx.s3.objects[fx.collector.prefix + "report.json"]["1"])
                self.assertEqual(report["evidence"]["producer_conclusion"], conclusion)
                fx.reader.read.assert_called_once()
                evidence = report["evidence"]
                with self.assertRaisesRegex(ValueError, "diagnostic run"):
                    results.validate_qualification(evidence, {}, dict(schema_version=1,
                        identity=evidence["identity"], archive_sha256=evidence["files"]["results.zip"],
                        ami_id=fx.config.ami, harness_revision=fx.config.source))

    def commit_outputs(self, fx):
        fx.store.record.state["worker_output"] = {
            "status": fx.collector.objects.read(fx.paths.status, collection.MAX_DOCUMENT)[1],
            "results": fx.collector.objects.read(fx.paths.results, common.MAX_RESULT)[1]}
        fx.publish()

    def test_committed_versions_survive_new_current_versions_and_shutdown_reason(self):
        self.commit_outputs(self.fx)
        self.fx.store.record.state["close_reason"] = "bound instance stopped or terminated"
        self.fx.publish()
        self.fx.s3.seed(self.fx.paths.status, b"unrelated later status")
        self.fx.s3.seed(self.fx.paths.results, b"unrelated later result")
        value = self.fx.collector.collect(retire=True)
        self.assertEqual((value["state"], value["retired"]), ("success", True), value)
        manifest = json.loads(self.fx.s3.objects[value["retention"]["key"]]["1"])
        self.assertEqual(manifest["sources"]["status.json"], self.fx.store.record.state["worker_output"]["status"])
        self.assertEqual(manifest["sources"]["results.zip"], self.fx.store.record.state["worker_output"]["results"])

    def test_committed_missing_evidence_has_bounded_unknown_outcome_not_failed_workload(self):
        self.commit_outputs(self.fx)
        del self.fx.s3.objects[self.fx.paths.status]
        del self.fx.s3.objects[self.fx.paths.results]
        self.assertIn("recovery grace", self.assertHeld()["reason"])
        self.fx.finish = common.utc(self.fx.store.record.state["binding"]["deadline"]) + timedelta(
            seconds=collection.EVIDENCE_RECOVERY_SECONDS)
        value = self.fx.collector.collect(retire=True)
        self.assertEqual((value["state"], value["retired"], value["qualification_outcome"]),
                         ("unavailable", True, "evidence-unavailable"), value)
        self.assertFalse(value["benchmark_valid"])
        report = json.loads(self.fx.s3.objects[self.fx.collector.prefix + "report.json"]["1"])
        self.assertEqual(report["evidence_availability"]["missing"], ["results.zip", "status.json"])
        self.assertIn("unknown", report["public_projection"]["note"])
        with patch.object(results, "inspect", side_effect=AssertionError("historical parser")), \
                patch.object(results, "validate_qualification", side_effect=AssertionError("historical admission")), \
                patch.object(collection.projection, "freeze", side_effect=AssertionError("historical projection")), \
                patch.object(collection, "EVIDENCE_RECOVERY_SECONDS", 1):
            self.assertEqual(self.fx.collector.collect(retire=True), value)

    def test_uncommitted_expired_outputs_do_not_turn_success_unknown_into_failure(self):
        self.fx.store.record.state["close_reason"] = "bound instance stopped or terminated"
        self.fx.publish()
        del self.fx.s3.objects[self.fx.paths.status]
        del self.fx.s3.objects[self.fx.paths.results]
        self.fx.finish = NOW + timedelta(seconds=collection.MAILBOX_EXPIRY_SECONDS)
        self.assertHeld()
        self.fx.finish = common.utc(self.fx.store.record.state["binding"]["deadline"]) + timedelta(
            seconds=collection.EVIDENCE_RECOVERY_SECONDS)
        value = self.fx.collector.collect(retire=True)
        self.assertEqual((value["state"], value["retired"]), ("unavailable", True), value)

    def test_failed_archive_capture_then_expiry_is_unknown_not_failed_workload(self):
        with patch.object(self.fx.s3, "head_object", create=True, side_effect=Error("AccessDenied")):
            with self.assertRaises(Error):
                self.fx.service.read_worker_status(self.fx.store.record.state)
        self.assertNotIn("worker_output", self.fx.store.record.state)
        del self.fx.s3.objects[self.fx.paths.status]
        del self.fx.s3.objects[self.fx.paths.results]
        self.fx.finish = common.utc(self.fx.store.record.state["binding"]["deadline"]) + timedelta(
            seconds=collection.EVIDENCE_RECOVERY_SECONDS - 1)
        self.assertHeld()
        self.fx.finish += timedelta(seconds=1)
        value = self.fx.collector.collect(retire=True)
        self.assertEqual((value["state"], value["retired"]), ("unavailable", True), value)

    def test_expired_missing_indicated_archive_is_explicitly_unavailable(self):
        del self.fx.s3.objects[self.fx.paths.results]
        self.fx.finish = common.utc(self.fx.store.record.state["binding"]["deadline"]) + timedelta(
            seconds=collection.EVIDENCE_RECOVERY_SECONDS)
        value = self.fx.collector.collect(retire=True)
        self.assertEqual((value["state"], value["retired"]), ("unavailable", True), value)
        report = json.loads(self.fx.s3.objects[self.fx.collector.prefix + "report.json"]["1"])
        self.assertEqual(report["evidence_availability"]["missing"], ["results.zip"])

    def test_aged_inaccessible_or_corrupt_committed_evidence_still_holds(self):
        self.commit_outputs(self.fx)
        self.fx.finish = NOW + timedelta(days=30)
        original = self.fx.s3.get_object
        for error in (Error("AccessDenied"), TimeoutError(), Error("InternalError")):
            with self.subTest(error=type(error).__name__):
                def inaccessible(**args):
                    if args["Key"] == self.fx.paths.status:
                        raise error
                    return original(**args)
                with patch.object(self.fx.s3, "get_object", side_effect=inaccessible):
                    self.assertHeld()
        self.fx.s3.objects[self.fx.paths.status]["1"] = b"corrupt"
        self.assertHeld()

    def test_terminal_coordinator_failure_does_not_invalidate_bound_successful_worker(self):
        for status in ("FAILED", "TIMED_OUT", "ABORTED"):
            with self.subTest(status=status):
                fx = Fixture(self.directory.name)
                fx.execution["status"] = status
                value = fx.collector.collect(retire=True)
                self.assertEqual((value["state"], value["retired"]), ("success", True), value)
                report = json.loads(fx.s3.objects[fx.collector.prefix + "report.json"]["1"])
                self.assertEqual(report["execution_proof"]["status"], status)
                self.assertIn(status, report["public_projection"]["note"])
                self.assertEqual(fx.collector.collect(retire=True), value)

    def test_terminal_failed_execution_does_not_bypass_invalid_worker_evidence(self):
        self.fx.execution["status"] = "TIMED_OUT"
        self.fx.s3.seed(self.fx.paths.results, b"invalid worker archive")
        self.fx.s3.seed(self.fx.paths.status, collection.encoded(dict(common.completed_status("123-1", True),
            sha256=common.digest(b"invalid worker archive"))))
        value = self.fx.collector.collect(retire=True)
        self.assertEqual((value["state"], value["retired"]), ("failed", True), value)
        self.assertFalse(value["benchmark_valid"])

    def test_missing_checksum_and_trailing_stream_bytes_hold(self):
        original = self.fx.s3.get_object
        def missing(**kwargs):
            response = original(**kwargs)
            del response["ChecksumSHA256"]
            return response
        self.fx.s3.get_object = missing
        self.assertHeld()
        class Extra(io.BytesIO):
            def __init__(self, raw):
                super().__init__(raw)
                self.first = True
            def read(self, size=-1):
                if self.first:
                    self.first = False
                    return super().read(size)
                return b"x"
        def trailing(**kwargs):
            response = original(**kwargs)
            response["Body"] = Extra(response["Body"].read())
            return response
        self.fx.s3.get_object = trailing
        self.assertHeld()

    def never_started(self):
        self.never_submitted()
        state = self.fx.store.record.state
        state["submission_arn"] = self.fx.execution_arn
        state["sealed_inputs"] = {name: dict(key="production/staging/" + self.fx.store.run_key + "/" + name,
            version="1", sha256="d" * 64, bytes=1) for name in common.BINARIES}
        self.fx.publish()

    def test_sealed_never_started_retires_after_full_horizon_without_refund(self):
        self.never_started()
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "failed", value)
        self.assertTrue(value["retired"])
        saved = json.loads(self.fx.s3.objects[value["retention"]["key"]]["1"])
        self.assertEqual(saved["execution"]["status"], "NEVER_STARTED")
        self.assertEqual(self.fx.collector.collect(retire=True), value)
        self.fx.sfn.start_execution.assert_not_called()

    def test_never_started_allocation_intent_or_unknown_absence_holds(self):
        self.never_started()
        original = copy.deepcopy(self.fx.store.record.state)
        for field, value in dict(attempts=1, attempt={}, allocation_consumed=True,
                launch_outcome="uncertain", execution_arn=self.fx.execution_arn,
                known_instance_ids=[INSTANCE], seen={INSTANCE: {}}, worker_published=True).items():
            with self.subTest(field=field):
                self.fx.store.record = replace(self.fx.store.record, state=copy.deepcopy(original))
                self.fx.store.record.state[field] = value
                self.fx.publish()
                self.assertHeld()
        self.fx.store.record = replace(self.fx.store.record, state=copy.deepcopy(original))
        self.fx.publish()
        for error in (Error("AccessDenied"), TimeoutError()):
            self.fx.sfn.describe_execution.side_effect = error
            self.assertHeld()
        self.fx.sfn.describe_execution.side_effect = Error("ExecutionDoesNotExist")
        self.fx.finish -= timedelta(seconds=1)
        self.assertHeld()

    def test_retained_open_verdict_change_holds_until_compatible_validator_restored(self):
        self.assertEqual(self.fx.collector.collect()["state"], "success")
        with patch.object(results, "validate_qualification", side_effect=ValueError("new contract")):
            value = self.assertHeld()
        self.assertIn("retained qualification verdict differs", value["reason"])
        self.assertEqual(self.fx.collector.collect(retire=True)["state"], "success")

    def test_retained_source_version_metadata_rejects_control_and_oversized_values(self):
        self.fx.collector.collect()
        key = self.fx.collector.prefix + "manifest.json"
        original = self.fx.s3.objects[key]["1"]
        for version in ("bad\nversion", "x" * 1025):
            with self.subTest(version_length=len(version)):
                manifest = json.loads(original)
                manifest["sources"]["durable.json"]["version"] = version
                self.fx.s3.objects[key]["1"] = collection.encoded(manifest)
                self.assertHeld()

    def test_late_runtime_change_after_retention_prevents_release(self):
        for change in (lambda fx: setattr(fx, "live", True), lambda fx: setattr(fx, "root_present", True),
                       lambda fx: fx.execution.update(status="RUNNING")):
            with self.subTest(change=change):
                fx = Fixture(self.directory.name)
                original = fx.collector.retained
                def retain(*args):
                    value = original(*args)
                    change(fx)
                    return value
                fx.collector.retained = retain
                result = fx.collector.collect(retire=True)
                self.assertEqual(result["state"], "held", result)
                self.assertFalse(result["retired"])
                self.assertEqual(fx.store.retire_calls, [])

    def test_later_rerun_cannot_reopen_completed_run_after_terminal_observation(self):
        original = self.fx.collector.retained
        def retain(*args):
            value = original(*args)
            self.fx.documents["current"].update(run_attempt=2, status="queued", conclusion=None)
            return value
        self.fx.collector.retained = retain
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        self.assertTrue(value["retired"])
        self.fx.reader.read.assert_called_once()
        self.assertEqual(self.fx.sfn.describe_execution.call_count, 2)
        self.assertGreaterEqual(self.fx.ec2.describe_instances.call_count, 4)

    def test_collection_without_retirement_keeps_active_ownership(self):
        value = self.fx.collector.collect()
        self.assertEqual(value["state"], "success", value)
        self.assertFalse(value["retired"])
        self.assertEqual(self.fx.store.retire_calls, [])
    def test_pending_state_has_no_side_effects(self):
        self.fx.store.record = replace(self.fx.store.record, terminal=False)
        self.assertEqual(self.fx.collector.collect(retire=True)["state"], "pending")
        self.assertEqual(self.fx.s3.writes, [])
    def test_failed_worker_can_retire_after_all_physical_proofs(self):
        self.fx.s3.seed(self.fx.paths.results, b"invalid archive retained as diagnostics")
        self.fx.s3.seed(self.fx.paths.status, collection.encoded(dict(common.completed_status("123-1", False),
            sha256=common.digest(b"invalid archive retained as diagnostics"))))
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "failed", value)
        self.assertTrue(value["retired"])
    def test_missing_worker_output_is_failed_not_success(self):
        del self.fx.s3.objects[self.fx.paths.results]
        del self.fx.s3.objects[self.fx.paths.status]
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "failed", value)
        self.assertTrue(value["retired"])
    def test_missing_indicated_archive_holds(self):
        del self.fx.s3.objects[self.fx.paths.results]
        self.assertHeld()
    def test_physical_failures_hold(self):
        for name in ("root_present", "live", "orphan", "pages"):
            with self.subTest(name=name):
                setattr(self.fx, name, True)
                self.assertHeld()
                setattr(self.fx, name, False)
    def test_only_exact_expired_instance_notfound_is_accepted(self):
        for error in ("empty", "AccessDenied", "InvalidInstanceID.Malformed"):
            with self.subTest(error=error):
                self.fx.instance_error = error
                self.assertHeld()
        self.fx.instance_error = "InvalidInstanceID.NotFound"
        result = self.fx.collector.collect(retire=True)
        self.assertEqual(result["state"], "success", result)
    def test_notfound_still_requires_committed_termination_and_fresh_root_absence(self):
        self.fx.instance_error = "InvalidInstanceID.NotFound"
        self.fx.root_present = True
        self.assertHeld()
        self.fx.root_present = False
        self.fx.store.record.state["seen"][INSTANCE]["state"] = "running"
        self.fx.publish()
        self.assertHeld()
    def test_changed_immutable_instance_fields_hold(self):
        for name, value in (("ClientToken", "bad"), ("ImageId", "ami-" + "7" * 17), ("InstanceId", "i-" + "7" * 17)):
            with self.subTest(name=name):
                old = self.fx.instance[name]
                self.fx.instance[name] = value
                self.assertHeld()
                self.fx.instance[name] = old
    def test_incomplete_cleanup_horizon_uncertainty_and_unpublished_proof_hold(self):
        original = copy.deepcopy(self.fx.store.record.state)
        changes = [lambda s: s["cleanup"].update(verified_at=common.timestamp(self.fx.finish - timedelta(seconds=1))),
            lambda s: s.update(launch_outcome="uncertain"), lambda s: s["cleanup"].update(unresolved_allocation=True),
            lambda s: s.update(known_instance_ids=[])]
        for change in changes:
            self.fx.store.record = replace(self.fx.store.record, state=copy.deepcopy(original))
            change(self.fx.store.record.state)
            self.fx.publish()
            self.assertHeld()
        self.fx.store.record = replace(self.fx.store.record, state=original)
        self.assertHeld()  # latest published receipt still differs
    def test_execution_identity_standard_type_and_terminal_status_are_required(self):
        for key, value in (("status", "RUNNING"), ("name", "other"), ("input", "{}"), ("stateMachineArn", MACHINE + "x")):
            original = self.fx.execution[key]
            self.fx.execution[key] = value
            self.assertHeld()
            self.fx.execution[key] = original
        self.fx.sfn.describe_state_machine.return_value["type"] = "EXPRESS"
        self.assertHeld()
    def test_producer_current_rerun_and_jobs_must_be_terminal_and_complete(self):
        original = copy.deepcopy(self.fx.documents)
        changes = [lambda d: d["current"].update(status="in_progress"),
            lambda d: d["attempt"].update(head_sha="a" * 40),
            lambda d: d["attempt"].update(actor=dict(id=999)),
            lambda d: d["current_jobs"].update(total_count=2),
            lambda d: d["current_jobs"]["jobs"][0].update(status="queued"),
            lambda d: d.update(pending_deployments=[{}]),
            lambda d: d["current"].update(run_attempt=2)]
        for change in changes:
            self.fx.documents = copy.deepcopy(original)
            change(self.fx.documents)
            self.assertHeld()
        self.fx.documents = copy.deepcopy(original)
        self.fx.documents["current"]["run_attempt"] = 2
        self.fx.documents["current_jobs"]["jobs"][0]["run_attempt"] = 2
        self.assertEqual(self.fx.collector.collect(retire=True)["state"], "success")
    def test_rate_limit_and_changed_schedule_hold(self):
        self.fx.reader.read.side_effect = RuntimeError("rate limited")
        self.assertHeld()
        self.fx.reader.read.side_effect = lambda p: copy.deepcopy(self.fx.documents)
        self.fx.scheduler.get_schedule.return_value = {}
        self.fx.scheduler.get_schedule.side_effect = None
        self.assertHeld()
    def test_lost_s3_write_response_reconciles_without_replay(self):
        self.fx.s3.lose_response = True
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        self.assertEqual(len(self.fx.s3.writes), len({call["Key"] for call in self.fx.s3.writes}))
    def test_unknown_write_holds_and_does_not_replay_in_same_call(self):
        self.fx.s3.reject_write = True
        value = self.assertHeld()
        self.assertEqual(len(self.fx.s3.writes), 1)
        self.assertNotIn("opaque SDK", value["reason"])
    def test_checksum_different_retained_bytes_and_deleted_retained_version_hold(self):
        self.fx.s3.bad_checksum = True
        self.assertHeld()
        self.fx.s3.bad_checksum = False
        value = self.fx.collector.collect()
        self.assertEqual(value["state"], "success", value)
        key = self.fx.collector.prefix + "results.zip"
        self.fx.s3.objects[key]["1"] = b"altered retained archive"
        self.assertHeld()
        del self.fx.s3.objects[key]
        self.assertHeld()
    def test_revision_change_after_retention_prevents_retirement(self):
        original = self.fx.collector.retained
        def changed(*args):
            value = original(*args)
            self.fx.store.record = replace(self.fx.store.record, revision=8)
            return value
        with patch.object(self.fx.collector, "retained", side_effect=changed):
            self.assertHeld()
    def test_retirement_lost_response_cannot_reopen_or_duplicate(self):
        retire = self.fx.store.retire
        def lost(*args):
            retire(*args)
            raise TimeoutError()
        with patch.object(self.fx.store, "retire", side_effect=lost):
            value = self.fx.collector.collect(retire=True)
            self.assertEqual(value["state"], "held")
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        self.assertTrue(value["retired"])
        self.assertEqual(len(self.fx.store.retire_calls), 1)
    def test_tombstone_digest_and_producer_provenance_cannot_be_substituted(self):
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        self.fx.store.record = replace(self.fx.store.record, retirement_sha256="0" * 64)
        self.assertEqual(self.fx.collector.collect()["state"], "held")
    def test_closed_run_reads_retention_without_touching_next_active_owner(self):
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        self.fx.live = True
        self.fx.reader.read.side_effect = AssertionError("next attempt need not be read")
        self.fx.sfn.describe_execution.side_effect = AssertionError("retained closed proof only")
        self.assertEqual(self.fx.collector.collect(retire=True), value)
        self.assertEqual(len(self.fx.store.retire_calls), 1)
    def test_closed_replay_does_not_render_or_revalidate_historical_measurements(self):
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        writes = len(self.fx.s3.writes)
        with patch.object(results, "summary", side_effect=AssertionError("new renderer is irrelevant")), \
             patch.object(results, "inspect", side_effect=AssertionError("new parser is irrelevant")), \
             patch.object(results, "validate_qualification", side_effect=AssertionError("historical verdict is retained")), \
             patch.object(self.fx.service, "receipt_bytes", side_effect=AssertionError("historical receipt stays unchanged")):
            self.assertEqual(self.fx.collector.collect(retire=True), value)
        self.assertEqual(len(self.fx.s3.writes), writes)
    def test_closed_missing_or_corrupt_retention_preserves_known_retirement(self):
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        key = self.fx.collector.prefix + "results.zip"
        del self.fx.s3.objects[key]
        held = self.fx.collector.collect(retire=True)
        self.assertEqual(held["state"], "held", held)
        self.assertTrue(held["retired"])
        self.assertEqual(len(self.fx.store.retire_calls), 1)
    def test_closed_replay_checks_manifest_and_raw_provenance(self):
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
        self.fx.store.record.state["binding"]["instance_id"] = "i-" + "7" * 17
        held = self.fx.collector.collect()
        self.assertEqual(held["state"], "held", held)
        self.assertTrue(held["retired"])
    def test_report_projection_bounds_long_archive_member_diagnostics(self):
        with zipfile.ZipFile(io.BytesIO(self.fx.s3.objects[self.fx.paths.results]["1"])) as original:
            worker = original.read("worker.json")
        raw = archive([("worker.json", worker)] +
            [("benchmark/" + str(i).zfill(4) + "-" + "x" * 2027, b"") for i in range(1000)])
        self.assertGreater(len(raw), 4_000_000)
        self.assertLess(len(raw), common.MAX_RESULT)
        self.fx.s3.seed(self.fx.paths.results, raw)
        self.fx.s3.seed(self.fx.paths.status, collection.encoded(dict(common.completed_status("123-1", True),
            sha256=common.digest(raw))))
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "failed", value)
        self.assertTrue(value["retired"])
        manifest = json.loads(self.fx.s3.objects[value["retention"]["key"]][value["retention"]["version"]])
        report_ref = manifest["retained"]["report.json"]
        report_raw = self.fx.s3.objects[report_ref["key"]][report_ref["version"]]
        self.assertLessEqual(len(report_raw), collection.MAX_REPORT)
        report = json.loads(report_raw)
        self.assertEqual(report["presentation"]["report"], "bounded-diagnostic")
        self.assertTrue(report["evidence"]["diagnostic_projection"])
        self.assertLessEqual(len(report["evidence"]["errors"]), 16)
        self.assertTrue(all(len(error) <= 256 for error in report["evidence"]["errors"]))
        raw_ref = manifest["retained"]["results.zip"]
        self.assertEqual(self.fx.s3.objects[raw_ref["key"]][raw_ref["version"]], raw)
        self.assertEqual(raw_ref["sha256"], common.digest(raw))
        self.assertEqual(self.fx.collector.collect(retire=True), value)
    def test_summary_rendering_overflow_or_error_gets_bounded_fallback(self):
        for rendered in ("x" * (collection.MAX_REPORT + 1), ValueError("renderer failure")):
            with self.subTest(rendered=type(rendered).__name__):
                fx = Fixture(self.directory.name)
                options = {"side_effect": rendered} if isinstance(rendered, Exception) else {"return_value": rendered}
                with patch.object(results, "summary", **options):
                    value = fx.collector.collect(retire=True)
                self.assertEqual(value["state"], "success", value)
                report = json.loads(fx.s3.objects[fx.collector.prefix + "report.json"]["1"])
                self.assertEqual(report["presentation"]["summary"], "bounded-fallback")
                self.assertLess(len(fx.s3.objects[fx.collector.prefix + "summary.html"]["1"]), 2048)
                self.assertEqual(fx.collector.collect(retire=True), value)
    def test_empty_current_jobs_allowed_only_for_later_cancelled_or_startup_failure(self):
        original = copy.deepcopy(self.fx.documents)
        for conclusion in ("cancelled", "startup_failure"):
            with self.subTest(conclusion=conclusion):
                fx = Fixture(self.directory.name)
                fx.documents["current"].update(run_attempt=2, conclusion=conclusion)
                fx.documents["current_jobs"] = dict(total_count=0, jobs=[])
                value = fx.collector.collect(retire=True)
                self.assertEqual(value["state"], "success", value)
                self.assertTrue(value["retired"])
        changes = [lambda d: d["current"].update(run_attempt=1),
            lambda d: d["current"].update(status="queued"),
            lambda d: d["current"].update(status="in_progress"),
            lambda d: d["current"].update(conclusion="success"),
            lambda d: d["current"].update(conclusion="failure"),
            lambda d: d.update(attempt_jobs=dict(total_count=0, jobs=[])),
            lambda d: d.update(pending_deployments=[{}]),
            lambda d: d["current_jobs"].update(total_count=1),
            lambda d: d["current_jobs"].update(total_count=False),
            lambda d: d["current_jobs"].update(next_page=2)]
        for change in changes:
            self.fx.documents = copy.deepcopy(original)
            self.fx.documents["current"].update(run_attempt=2, conclusion="cancelled")
            self.fx.documents["current_jobs"] = dict(total_count=0, jobs=[])
            change(self.fx.documents)
            self.assertHeld()
    def test_maximum_production_receipt_does_not_change_legacy_bound(self):
        from .durable_aws import MAX_RECEIPT as LEGACY_LIMIT
        state = self.fx.store.record.state
        state["capacity_failures"] = [dict(attempt=i, at=common.timestamp(NOW), request_id="a" * 64)
                                      for i in range(1, 721)]
        state["sealed_inputs"] = {name: dict(key="production/staging/" + self.fx.store.run_key + "/" + name,
            version=chr(92) * 1024, sha256="d" * 64, bytes=1) for name in common.BINARIES}
        raw = self.fx.service.receipt_bytes(state)
        self.assertGreater(len(raw), LEGACY_LIMIT)
        self.assertEqual(LEGACY_LIMIT, 100_000)
        self.assertEqual(collection.MAX_RECEIPT, 128 * 1024)
        self.assertLess(len(raw), collection.MAX_RECEIPT)
        self.fx.publish()
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "success", value)
    def never_submitted(self):
        state = self.fx.store.record.state
        state.update(sealed_inputs=None, submission_arn=None, execution_arn=None, binding=None, attempt=None,
            attempts=0, allocation_consumed=False, launch_outcome="none", known_instance_ids=[], seen={},
            runtime_verified=False, worker_published=False,
            cleanup=dict(state["cleanup"], allocation_consumed=False, deleted_roots=[]))
        self.fx.sfn.describe_execution.side_effect = Error("ExecutionDoesNotExist")
        self.fx.publish()
    def test_never_submitted_has_distinct_absence_proof_and_never_succeeds(self):
        self.never_submitted()
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "failed", value)
        self.assertTrue(value["retired"])
        manifest = json.loads(self.fx.s3.objects[value["retention"]["key"]]["1"])
        self.assertEqual(manifest["execution"]["status"], "NEVER_SUBMITTED")
        self.assertEqual(manifest["execution"]["proof"], "never-submitted")
        self.assertFalse(manifest["benchmark_valid"])
        self.assertEqual(self.fx.collector.collect(retire=True), value)
    def test_never_submitted_requires_every_no_intent_field_and_full_horizon(self):
        self.never_submitted()
        original = copy.deepcopy(self.fx.store.record.state)
        values = dict(sealed_inputs={}, submission_arn=self.fx.execution_arn, execution_arn=self.fx.execution_arn,
            binding={}, attempt={}, attempts=1, allocation_consumed=True, launch_outcome="capacity-rejected",
            known_instance_ids=[INSTANCE], seen={INSTANCE: {}}, runtime_verified=True, worker_published=True)
        for field, value in values.items():
            with self.subTest(field=field):
                self.fx.store.record = replace(self.fx.store.record, state=copy.deepcopy(original))
                self.fx.store.record.state[field] = value
                self.fx.publish()
                self.assertHeld()
        self.fx.store.record = replace(self.fx.store.record, state=copy.deepcopy(original))
        self.fx.store.record.state["cleanup"]["verified_at"] = common.timestamp(self.fx.finish - timedelta(seconds=1))
        self.fx.publish()
        self.assertHeld()
    def test_never_submitted_existing_unknown_or_mismatched_execution_holds(self):
        self.never_submitted()
        for error in (Error("AccessDenied"), TimeoutError()):
            self.fx.sfn.describe_execution.side_effect = error
            self.assertHeld()
        for response in ({}, self.fx.execution, dict(self.fx.execution, executionArn="wrong")):
            self.fx.sfn.describe_execution.side_effect = None
            self.fx.sfn.describe_execution.return_value = response
            self.assertHeld()
    def test_never_submitted_requires_empty_fresh_namespace(self):
        self.never_submitted()
        self.fx.ec2.describe_instances.side_effect = lambda **kw: dict(Reservations=[dict(Instances=[self.fx.instance])])
        self.assertHeld()
    def test_terminal_store_fence_rejects_late_seal_before_or_after_retirement(self):
        from .test_production_store import DynamoTransactions, decision
        from .production_store import RunStore
        self.never_submitted()
        cloud = DynamoTransactions()
        payload = copy.deepcopy(self.fx.store.record.admission["initial_payload"])
        payload["state"].update(phase="awaiting-inputs", sealed_inputs=None, submission_arn=None)
        approved = decision(run_key=self.fx.store.run_key, epoch=self.fx.store.epoch, initial_payload=payload)
        owner = RunStore(cloud, "runs", approved.run_key, approved.epoch)
        owner.admit(approved)
        stale, revision = owner.load()
        stale.update(phase="armed", sealed_inputs={"untrusted": "late"}, submission_arn=self.fx.execution_arn)
        owner.save(copy.deepcopy(self.fx.store.record.state), revision)
        with self.assertRaisesRegex(ValueError, "terminal"):
            owner.save(stale, revision)
        self.fx.store = owner
        self.fx.service.store = owner
        self.fx.collector.store = owner
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "failed", value)
        self.assertTrue(value["retired"])
        with self.assertRaisesRegex(ValueError, "active slot"):
            owner.save(stale, revision)
        self.assertNotIn("active", cloud.items)
        self.assertTrue(owner.read().closed)
    def test_optional_absence_requires_complete_listing_not_a_get_denial(self):
        self.fx.s3.list_objects_v2 = Mock(return_value=dict(Contents=[], KeyCount=0, IsTruncated=True))
        self.assertHeld()
        self.fx.s3.list_objects_v2 = Mock(side_effect=Error("AccessDenied"))
        self.assertHeld()
        self.fx.s3.list_objects_v2 = lambda **kw: S3.list_objects_v2(self.fx.s3, **kw)
        original = self.fx.s3.get_object
        def denied(**kw):
            if kw["Key"] == self.fx.paths.status:
                raise Error("AccessDenied")
            return original(**kw)
        self.fx.s3.get_object = denied
        self.assertHeld()
    def test_no_allocation_failure_retires_only_with_complete_empty_evidence(self):
        state = self.fx.store.record.state
        state.update(binding=None, attempt=None, attempts=0, execution_arn=None,
            allocation_consumed=False, launch_outcome="none", known_instance_ids=[], seen={}, runtime_verified=False,
            worker_published=False, cleanup=dict(state["cleanup"], allocation_consumed=False, deleted_roots=[]))
        self.fx.publish()
        value = self.fx.collector.collect(retire=True)
        self.assertEqual(value["state"], "failed", value)
        self.assertTrue(value["retired"])


if __name__ == "__main__":
    unittest.main()
