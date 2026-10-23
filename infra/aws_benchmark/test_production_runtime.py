"""Production allocation, mailbox and cleanup integration with fake AWS services."""

import base64
import copy
from datetime import timedelta
import io
import json
import os
import unittest
from unittest.mock import Mock, patch

from infra.aws_benchmark import common, durable, production_runtime
from infra.aws_benchmark.durable_aws import Service
from infra.aws_benchmark.production_mailbox import MailboxPaths
from infra.aws_benchmark.production_runtime import ProductionService
from infra.aws_benchmark.production_store import AdmissionDecision, ReconciliationRequired, RunStore
from infra.aws_benchmark.production_startup import reviewed_startup
from infra.aws_benchmark.test_durable_coordinator import ApiError, Cloud, INSTANCE, NOW
from infra.aws_benchmark.test_production_store import DynamoTransactions

RUN_KEY = "repo-777-run-123"
EPOCH = "e" * 64


class ProductionCloud(Cloud):
    def __init__(self, *, awaiting=False):
        self.owner = None
        super().__init__()
        script = reviewed_startup(account=self.config.account, bucket=self.config.bucket, region=self.config.region)
        template = self.api["ec2"].describe_launch_template_versions.return_value["LaunchTemplateVersions"][0]
        template["LaunchTemplateData"]["UserData"] = base64.b64encode(script.encode()).decode("ascii")
        self.database = DynamoTransactions()
        ddb = Mock(wraps=self.database)
        ddb.get_item.side_effect = self.production_get
        ddb.put_item = Mock(side_effect=self.put_item)
        self.api["dynamodb"] = ddb
        self.objects = {}
        self.object_versions, self.versions, self.copies = {}, {}, []
        self.copy_hook = None
        sealed, manifest = {}, {}
        for index, binary in enumerate(common.BINARIES):
            key, version = "production/staging/" + RUN_KEY + "/" + binary, "source-version-" + str(index)
            self.objects[key] = self.binary
            self.object_versions[key] = version
            self.versions[(key, version)] = self.binary
            manifest[binary] = dict(sha256=common.digest(self.binary), bytes=len(self.binary))
            sealed[binary] = dict(key=key, version=version, **manifest[binary])
        initial = copy.deepcopy(self._state)
        initial.update(phase="awaiting-inputs" if awaiting else "armed", sealed_inputs=None if awaiting else sealed,
                       submission_arn=None if awaiting else self.event["execution_arn"])
        decision = AdmissionDecision(run_key=RUN_KEY, epoch=EPOCH, fingerprint="f" * 64, policy_version="v1",
            reserve_microusd=20_000_000, total_cap_microusd=350_000_000, monthly_cap_microusd=80_000_000,
            daily_run_limit=2, month="2026-10", day="2026-10-21",
            initial_payload=dict(producer=dict(run_id="123", run_attempt="1"),
                manifest=dict(binaries=manifest), upload_deadline=common.timestamp(NOW + timedelta(seconds=600)),
                state=initial))
        self.owner = RunStore(ddb, self.config.table, RUN_KEY, EPOCH)
        self.owner.admit(decision)
        self.api["s3"].copy_object.side_effect = self.copy_object
        self.api["s3"].list_objects_v2.side_effect = self.list_objects_v2
        self.service = ProductionService(self.api, self.config, RUN_KEY, EPOCH, store=self.owner,
                                         now=lambda: self.now, sleep=self.sleeps.append)
        self.event.update(run_key=RUN_KEY, epoch=EPOCH)

    @property
    def state(self):
        return self.owner.read().state if self.owner is not None else self._state

    @state.setter
    def state(self, value):
        self._state = value

    def production_get(self, **parameters):
        if parameters["Key"]["pk"]["S"] == "guard":
            return {} if self.guard is None else {"Item": {"payload": {"S": json.dumps(self.guard)}}}
        return self.database.get_item(**parameters)

    def head_object(self, **parameters):
        key, version = parameters["Key"], parameters.get("VersionId")
        try:
            data = self.versions[(key, version)] if version is not None else self.objects[key]
        except KeyError:
            raise ApiError("NoSuchKey", status=404)
        return dict(ContentLength=len(data), ChecksumType="FULL_OBJECT", ServerSideEncryption="AES256",
                    VersionId=version or self.object_versions[key],
                    ChecksumSHA256=base64.b64encode(bytes.fromhex(common.digest(data))).decode("ascii"))

    def list_objects_v2(self, **parameters):
        keys = sorted(key for key in self.objects if key.startswith(parameters["Prefix"]))
        return dict(IsTruncated=len(keys) > parameters["MaxKeys"], KeyCount=min(len(keys), parameters["MaxKeys"]),
                    Contents=[dict(Key=key) for key in keys[:parameters["MaxKeys"]]])

    def get_object(self, **parameters):
        key, version = parameters["Key"], parameters.get("VersionId")
        try:
            data = self.versions[(key, version)] if version is not None else self.objects[key]
        except KeyError:
            raise ApiError("NoSuchVersion" if version else "NoSuchKey", status=404)
        version = version or self.object_versions.get(key, "test-version-" + common.digest(data))
        self.versions[(key, version)] = data
        return dict(ContentLength=len(data), Body=io.BytesIO(data), VersionId=version,
                    ServerSideEncryption="AES256", ChecksumType="FULL_OBJECT",
                    ChecksumSHA256=base64.b64encode(bytes.fromhex(common.digest(data))).decode("ascii"))

    def put_object(self, **parameters):
        key = parameters["Key"]
        if parameters.get("IfNoneMatch") == "*" and key in self.objects:
            raise ApiError("PreconditionFailed", status=412)
        self.objects[key] = parameters["Body"]
        self.object_versions[key] = "put-version-" + str(len(self.events))
        self.events.append(("object", key))
        return dict(VersionId=self.object_versions[key])

    def copy_object(self, **parameters):
        if parameters.get("IfNoneMatch") == "*" and parameters["Key"] in self.objects:
            raise ApiError("PreconditionFailed", status=412)
        source = parameters["CopySource"]
        data = self.versions[(source["Key"], source["VersionId"])]
        self.copies.append(copy.deepcopy(parameters))
        key = parameters["Key"]
        self.objects[key] = data
        self.object_versions[key] = "copy-version-" + str(len(self.copies))
        self.events.append(("copy", key))
        if self.copy_hook:
            self.copy_hook()
        return dict(VersionId=self.object_versions[key])

    def paths(self):
        return MailboxPaths.for_instance(self.config.account, self.config.region, INSTANCE)

    def handler(self, name, *, context=None, event=None):
        def service(api, config, run_key, epoch):
            self.handler_service = ProductionService(api, config, run_key, epoch,
                now=lambda: self.now, sleep=self.sleeps.append)
            return self.handler_service
        with patch.object(production_runtime.Configuration, "environment", return_value=self.config), \
             patch.object(production_runtime, "clients", return_value=self.api), \
             patch.object(production_runtime, "Service", return_value=Service(self.api, self.config, now=lambda: self.now)), \
             patch.object(production_runtime, "ProductionService", side_effect=service):
            message = self.event if name == "coordinator" else {}
            return getattr(production_runtime, name)(message if event is None else event, context)

    def complete(self):
        self.instances[0]["State"]["Name"] = "terminated"
        self.now = common.utc(self.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS)
        self.service.watchdog()


class ProductionLaunchTests(unittest.TestCase):
    def test_unreviewed_startup_cannot_persist_intent_or_call_ec2(self):
        changes = (None, "", "***", "legacy", "account", "bucket", "region", "runtime", "poweroff")
        for change in changes:
            with self.subTest(change=change):
                cloud = ProductionCloud()
                template = cloud.api["ec2"].describe_launch_template_versions.return_value["LaunchTemplateVersions"][0]
                data = template["LaunchTemplateData"]
                script = base64.b64decode(data["UserData"]).decode()
                if change is None:
                    del data["UserData"]
                elif change in ("", "***"):
                    data["UserData"] = change
                else:
                    replacements = {
                        "legacy": ("--production-account '123456789012' ", ""),
                        "account": ("123456789012", "123456789013"),
                        "bucket": ("--bucket 'bucket'", "--bucket 'another-bucket'"),
                        "region": ("us-east-1", "eu-west-1"),
                        "runtime": ("RuntimeMaxSec=10800", "RuntimeMaxSec=10801"),
                        "poweroff": ("FailureAction=poweroff", "FailureAction=none"),
                    }
                    before, after = replacements[change]
                    data["UserData"] = base64.b64encode(script.replace(before, after).encode()).decode("ascii")
                writes = len(cloud.database.writes)
                with self.assertRaises(ValueError):
                    cloud.coordinator()
                self.assertEqual(len(cloud.database.writes), writes)
                self.assertEqual(cloud.state["attempts"], 0)
                self.assertEqual(cloud.state["phase"], "armed")
                cloud.api["ec2"].run_instances.assert_not_called()

    def test_checked_numeric_version_is_the_version_used_to_launch(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.api["ec2"].describe_launch_template_versions.assert_called_once_with(
            LaunchTemplateId=cloud.config.lt_id, Versions=[cloud.config.lt_version])
        self.assertEqual(cloud.launches[0]["LaunchTemplate"],
                         dict(LaunchTemplateId=cloud.config.lt_id, Version=cloud.config.lt_version))
        self.assertNotIn("UserData", cloud.launches[0])

    def test_single_allocation_uses_sealed_versions_and_transactional_launch_intent(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        self.assertEqual(len(cloud.launches), 1)
        self.assertEqual(cloud.state["phase"], "requested")
        self.assertTrue(cloud.state["allocation_consumed"])
        heads = cloud.api["s3"].head_object.call_args_list
        self.assertEqual(len(heads), len(common.BINARIES))
        self.assertTrue(all("VersionId" in call.kwargs for call in heads))
        launch_transaction = cloud.database.writes[1]["TransactItems"]
        keys = [next(iter(item.values())).get("Key", {}).get("pk", {}).get("S") for item in launch_transaction]
        self.assertIn("policy", keys)
        self.assertIn("active", keys)
        cloud.coordinator()
        self.assertEqual(len(cloud.launches), 1)

    def test_foreign_event_owner_execution_name_and_extra_fields_cannot_launch(self):
        for change in (dict(run_key="repo-777-run-124"), dict(epoch="0" * 64), dict(run="124-1"),
                       dict(execution_name="123-2"), dict(execution_arn="arn:other"), dict(extra=True)):
            with self.subTest(change=change):
                cloud = ProductionCloud()
                with self.assertRaises(ValueError):
                    cloud.service.coordinator({**cloud.event, **change})
                self.assertEqual(cloud.launches, [])

    def test_changed_deployment_pins_are_rejected_before_launch(self):
        cloud = ProductionCloud()
        object.__setattr__(cloud.config, "ami", "ami-" + "9" * 17)
        with self.assertRaisesRegex(ValueError, "pins changed"):
            cloud.coordinator()
        self.assertEqual(cloud.launches, [])

    def test_policy_switch_during_artifact_reads_prevents_new_intent(self):
        cloud = ProductionCloud()
        original = cloud.head_object
        def disable(**parameters):
            cloud.database.items["policy"]["enabled"] = {"BOOL": False}
            return original(**parameters)
        cloud.api["s3"].head_object.side_effect = disable
        with self.assertRaises(ReconciliationRequired):
            cloud.coordinator()
        self.assertEqual(cloud.launches, [])
        self.assertEqual(cloud.state["phase"], "armed")
        self.assertFalse(cloud.state["allocation_consumed"])

    def test_changed_current_staging_version_does_not_replace_sealed_binary(self):
        cloud = ProductionCloud()
        key = "production/staging/" + RUN_KEY + "/candidate/rcp"
        cloud.objects[key] = b"changed current version"
        cloud.object_versions[key] = "untrusted-later-version"
        cloud.coordinator()
        cloud.service.watchdog()
        self.assertEqual(cloud.objects[cloud.paths().binary("candidate/rcp")], cloud.binary)
        self.assertTrue(cloud.state["worker_published"])

    def test_missing_or_changed_source_version_blocks_before_allocation(self):
        for corrupt in (False, True):
            with self.subTest(corrupt=corrupt):
                cloud = ProductionCloud()
                entry = cloud.state["sealed_inputs"]["candidate/rcp"]
                if corrupt:
                    cloud.versions[(entry["key"], entry["version"])] = b"wrong"
                else:
                    del cloud.versions[(entry["key"], entry["version"])]
                with self.assertRaises((ValueError, ApiError)):
                    cloud.coordinator()
                self.assertEqual(cloud.launches, [])

    def test_malformed_seals_and_unversioned_artifacts_are_rejected(self):
        cloud = ProductionCloud()
        state, _ = cloud.service.load()
        for change in (dict(key="production/staging/foreign/candidate/rcp"), dict(version="null"),
                       dict(version="bad\nversion"), dict(bytes=True), dict(bytes=0), dict(sha256="0" * 64)):
            with self.subTest(change=change):
                mutated = copy.deepcopy(state)
                mutated["sealed_inputs"]["candidate/rcp"].update(change)
                with self.assertRaises(ValueError):
                    cloud.service.validate_inputs(mutated)
        entry = state["sealed_inputs"]["candidate/rcp"]
        head = cloud.head_object(Key=entry["key"], VersionId=entry["version"])
        for change in (dict(VersionId="null"), dict(VersionId="different"), dict(ServerSideEncryption=None),
                       dict(ChecksumType="COMPOSITE"), dict(ContentLength=True)):
            with self.subTest(change=change), self.assertRaises(ValueError):
                cloud.service.verify_head({**head, **change}, entry, version=entry["version"])
        self.assertEqual(cloud.launches, [])

    def test_maximum_capacity_history_and_version_pins_fit_production_only_bound(self):
        cloud = ProductionCloud()
        state, revision = cloud.service.load()
        state["capacity_failures"] = [dict(attempt=i, at=common.timestamp(NOW), request_id="a" * 64)
                                     for i in range(1, durable.MAX_ATTEMPTS + 1)]
        state["attempt"] = durable.attempt(cloud.request, 720, NOW)
        state["attempt"]["parameters"] = cloud.service.launch_parameters(cloud.request, state["attempt"])
        for entry in state["sealed_inputs"].values():
            # escaped printable ASCII is the longest JSON representation accepted for a source version
            entry["version"] = chr(92) * 1024
        payload = cloud.service.receipt_bytes(state)
        self.assertGreater(len(payload), 100_000)
        self.assertLess(len(payload), 128 * 1024)
        self.assertEqual(cloud.service.receipt_limit, 128 * 1024)
        cloud.service.save(state, revision)
        self.assertEqual(cloud.owner.load()[0], state)
        with self.assertRaisesRegex(ValueError, "receipt exceeds"):
            Service(cloud.api, cloud.config, now=lambda: cloud.now).receipt_bytes(state)
        state["unexpected_padding"] = "x" * (128 * 1024)
        with self.assertRaisesRegex(ValueError, "receipt exceeds"):
            cloud.service.receipt_bytes(state)

    def test_uncertain_launch_never_gets_another_intent_or_deadline(self):
        cloud = ProductionCloud()
        cloud.launch_error = TimeoutError()
        cloud.coordinator()
        original = copy.deepcopy(cloud.state["attempt"])
        self.assertTrue(cloud.state["allocation_consumed"])
        cloud.now += timedelta(hours=1)
        cloud.coordinator()
        self.assertEqual(cloud.state["attempt"], original)
        self.assertTrue(all(value == cloud.launches[0] for value in cloud.launches))
        self.assertEqual(cloud.state["attempts"], 1)


class ProductionWatchdogTests(unittest.TestCase):
    def test_schedule_and_all_six_copies_precede_immutable_assignment(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        paths = cloud.paths()
        self.assertTrue(cloud.state["worker_published"])
        self.assertEqual(len(cloud.copies), 6)
        self.assertTrue(all(call["CopySource"]["VersionId"].startswith("source-version-") for call in cloud.copies))
        self.assertTrue(all(call["ServerSideEncryption"] == "AES256" for call in cloud.copies))
        self.assertTrue(all(call["IfNoneMatch"] == "*" for call in cloud.copies))
        positions = {entry: i for i, entry in enumerate(cloud.events)}
        command_position = positions[("object", paths.request)]
        window_position = positions[("object", paths.window)]
        self.assertLess(command_position, window_position)
        self.assertTrue(all(positions[("copy", paths.binary(name))] < command_position for name in common.BINARIES))
        self.assertLess(next(i for i, entry in enumerate(cloud.events) if entry[0] == "schedule"), command_position)
        puts = [call.kwargs for call in cloud.api["s3"].put_object.call_args_list
                if call.kwargs["Key"] in (paths.request, paths.window)]
        self.assertTrue(all(call["IfNoneMatch"] == "*" for call in puts))
        self.assertNotIn(paths.status, cloud.objects)
        self.assertEqual(cloud.state["phase"], "requested")
        self.assertIn("production/runs/" + RUN_KEY + "/durable-receipt.json", cloud.objects)
        self.assertNotIn("output/durable-receipt.json", cloud.objects)

    def test_replayed_publication_accepts_only_identical_assignment(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        state, _ = cloud.owner.load()
        original = {key: cloud.objects[key] for key in (cloud.paths().request, cloud.paths().window)}
        cloud.service.publish_worker(state)
        self.assertEqual(len(cloud.copies), 6)
        self.assertEqual({key: cloud.objects[key] for key in original}, original)
        cloud.objects[cloud.paths().window] = b'{"different":true}'
        with self.assertRaisesRegex(ValueError, "existing production assignment differs"):
            cloud.service.publish_worker(state)
        self.assertEqual(cloud.objects[cloud.paths().window], b'{"different":true}')

    def test_changed_destination_binary_is_not_overwritten(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        path = cloud.paths().binary("baseline/rcp")
        cloud.objects[path], cloud.object_versions[path] = b"wrong destination", "prior-version"
        with self.assertRaisesRegex(ValueError, "binary bytes"):
            cloud.service.watchdog()
        self.assertEqual(cloud.objects[path], b"wrong destination")
        self.assertNotIn(cloud.paths().window, cloud.objects)
        self.assertIn(("terminate", [INSTANCE]), cloud.events)

    def test_closure_during_copy_blocks_later_assignment_publication(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        def close():
            cloud.copy_hook = None
            state, revision = cloud.owner.load()
            state.update(phase="closing", close_reason="manual closure")
            cloud.owner.save(state, revision)
        cloud.copy_hook = close
        with self.assertRaises((ValueError, ReconciliationRequired)):
            cloud.service.watchdog()
        self.assertEqual(len(cloud.copies), 1)
        self.assertNotIn(cloud.paths().request, cloud.objects)
        self.assertNotIn(cloud.paths().window, cloud.objects)
        self.assertIn(("terminate", [INSTANCE]), cloud.events)

    def test_expiry_scan_precedes_unreadable_active_record(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.now += timedelta(hours=3, seconds=1)
        with patch.object(cloud.owner, "load", side_effect=RuntimeError("unreadable active record")), \
             self.assertRaisesRegex(RuntimeError, "unreadable active"):
            cloud.service.watchdog()
        self.assertIn(("terminate", [INSTANCE]), cloud.events)

    def test_disabled_policy_closes_and_terminates_known_live_allocation(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        original = copy.deepcopy(cloud.state["attempt"])
        cloud.database.items["policy"]["enabled"] = {"BOOL": False}
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "closing")
        self.assertIn("kill switch", cloud.state["close_reason"])
        self.assertEqual(cloud.state["attempt"], original)
        self.assertIn(("terminate", [INSTANCE]), cloud.events)
        self.assertNotIn(cloud.paths().window, cloud.objects)

    def test_policy_read_failure_cannot_skip_expiry_or_failure_cleanup(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        def get(**parameters):
            if parameters["Key"]["pk"]["S"] == "policy":
                raise ApiError("AccessDenied", status=403)
            return cloud.production_get(**parameters)
        cloud.api["dynamodb"].get_item.side_effect = get
        with self.assertRaises(ApiError):
            cloud.service.watchdog()
        self.assertIn(("terminate", [INSTANCE]), cloud.events)
        self.assertEqual(cloud.state["phase"], "closing")

    def test_input_timeout_closes_without_launch_and_keeps_full_reconciliation_horizon(self):
        cloud = ProductionCloud(awaiting=True)
        cloud.now += timedelta(seconds=599)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "awaiting-inputs")
        cloud.now += timedelta(seconds=1)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "closing")
        self.assertIn("upload expired", cloud.state["close_reason"])
        self.assertEqual(cloud.launches, [])
        cloud.now = common.utc(cloud.request["outer_deadline"]) + timedelta(seconds=durable.DELIVERY_DRAIN_SECONDS - 1)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "closing")
        cloud.now += timedelta(seconds=1)
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "complete")
        self.assertEqual(cloud.state["cleanup"]["deleted_roots"], [])
        self.assertFalse(cloud.state["cleanup"]["allocation_consumed"])

    def test_missing_status_stays_pending_but_invalid_status_closes(self):
        for value in ([], {}, dict(run="other", state="completed", success=False),
                      dict(run="123-1", state="completed", success=1),
                      dict(run="123-1", state="completed", success=True),
                      dict(run="123-1", state="pending")):
            with self.subTest(value=value):
                cloud = ProductionCloud()
                cloud.coordinator()
                cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "requested")
                cloud.objects[cloud.paths().status] = json.dumps(value).encode()
                with self.assertRaises(ValueError):
                    cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "closing")
                self.assertIn(("terminate", [INSTANCE]), cloud.events)

    def test_status_listing_is_exact_and_access_errors_do_not_look_pending(self):
        for variant in ("list-denied", "get-denied", "wrong-key", "truncated", "missing-after-list"):
            with self.subTest(variant=variant):
                cloud = ProductionCloud()
                cloud.coordinator()
                cloud.service.watchdog()
                paths = cloud.paths()
                cloud.objects[paths.status] = json.dumps(common.completed_status("123-1", False)).encode()
                if variant == "list-denied":
                    cloud.api["s3"].list_objects_v2.side_effect = ApiError("AccessDenied", status=403)
                elif variant == "get-denied":
                    cloud.api["s3"].get_object.side_effect = ApiError("AccessDenied", status=403)
                elif variant in ("wrong-key", "truncated"):
                    cloud.api["s3"].list_objects_v2.side_effect = None
                    cloud.api["s3"].list_objects_v2.return_value = dict(IsTruncated=variant == "truncated",
                        Contents=[dict(Key=paths.status if variant == "truncated" else paths.status + ".foreign")])
                else:
                    cloud.api["s3"].get_object.side_effect = ApiError("NoSuchKey", status=404)
                with self.assertRaises((ValueError, ApiError)):
                    cloud.service.watchdog()
                self.assertEqual(cloud.state["phase"], "closing")
                self.assertIn(("terminate", [INSTANCE]), cloud.events)
                call = cloud.api["s3"].list_objects_v2.call_args.kwargs
                self.assertEqual(call, dict(Bucket=cloud.config.bucket, Prefix=paths.status, MaxKeys=1))

    def test_copy_requires_annotation_support_and_status_parameters_match_sdk(self):
        import boto3
        from botocore.stub import Stubber
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        entry = cloud.state["sealed_inputs"]["candidate/rcp"]
        paths = cloud.paths()
        s3 = boto3.client("s3", region_name=cloud.config.region, aws_access_key_id="fake", aws_secret_access_key="fake")
        cloud.service.api = {**cloud.api, "s3": s3}
        expected = dict(Bucket=cloud.config.bucket, Key=paths.binary("candidate/rcp"),
            CopySource=dict(Bucket=cloud.config.bucket, Key=entry["key"], VersionId=entry["version"]),
            ChecksumAlgorithm="SHA256", ServerSideEncryption="AES256", MetadataDirective="REPLACE", AnnotationDirective="EXCLUDE",
            ContentType="application/octet-stream", IfNoneMatch="*")
        operation = s3.meta.service_model.operation_model("CopyObject")
        if "AnnotationDirective" not in operation.input_shape.members:
            # older optional development SDKs must reject the unchanged request, never omit the guard
            from botocore.exceptions import ParamValidationError
            with patch.object(s3._endpoint, "make_request", side_effect=AssertionError("network must not be reached")) as send:
                with self.assertRaisesRegex(ParamValidationError, "AnnotationDirective"):
                    cloud.service.copy_binary(paths.binary("candidate/rcp"), entry)
                send.assert_not_called()
        else:
            wire_parameters = dict(expected, CopySource="/bucket/source?versionId=source-version-3")
            wire = s3._serializer.serialize_to_request(wire_parameters, operation)
            self.assertEqual(wire["headers"]["x-amz-object-annotation-directive"], "EXCLUDE")
            with Stubber(s3) as stub:
                stub.add_response("copy_object", dict(VersionId="sdk-version"), expected)
                head = cloud.head_object(Key=paths.binary("candidate/rcp"))
                stub.add_response("head_object", head,
                                  dict(Bucket=cloud.config.bucket, Key=paths.binary("candidate/rcp"), ChecksumMode="ENABLED"))
                cloud.service.copy_binary(paths.binary("candidate/rcp"), entry)
                stub.assert_no_pending_responses()
        with Stubber(s3) as stub:
            stub.add_response("list_objects_v2", dict(IsTruncated=False, KeyCount=0, Contents=[]),
                              dict(Bucket=cloud.config.bucket, Prefix=paths.status, MaxKeys=1))
            self.assertEqual(cloud.service.read_worker_status(cloud.state), common.pending_status("123-1"))
            stub.assert_no_pending_responses()

    def test_terminal_failed_status_closes_without_claiming_benchmark_success(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        cloud.objects[cloud.paths().status] = json.dumps(common.completed_status("123-1", False)).encode()
        cloud.service.watchdog()
        self.assertEqual(cloud.state["phase"], "closing")
        self.assertIn(("terminate", [INSTANCE]), cloud.events)
        self.assertIsNone(cloud.state["cleanup"])

    def test_complete_and_retired_records_cannot_be_revived_by_late_launch_response(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        cloud.service.watchdog()
        allocation, response = copy.deepcopy(cloud.state["attempt"]), {"Instances": copy.deepcopy(cloud.instances)}
        cloud.complete()
        self.assertEqual(cloud.state["phase"], "complete")
        before = copy.deepcopy(cloud.state)
        with self.assertRaises((ValueError, ReconciliationRequired)):
            cloud.service.finish_launch("123-1", allocation, response=response)
        self.assertEqual(cloud.state, before)
        record = cloud.owner.read()
        cloud.owner.retire("a" * 64, record.revision)
        with self.assertRaises(ValueError):
            cloud.service.finish_launch("123-1", allocation, response=response)
        self.assertEqual(cloud.state, before)
        self.assertTrue(cloud.owner.read().closed)


class ProductionEntrypointTests(unittest.TestCase):
    def test_coordinator_requires_literal_enablement_and_exact_context_identity(self):
        for enabled in ("false", "True", "1", ""):
            with self.subTest(enabled=enabled), patch.dict(os.environ, RCP_PRODUCTION_ENABLED=enabled):
                cloud = ProductionCloud()
                with self.assertRaisesRegex(ValueError, "runtime is disabled"):
                    cloud.handler("coordinator")
                self.assertEqual(cloud.launches, [])
        cloud = ProductionCloud()
        context = Mock()
        context.get_remaining_time_in_millis.return_value = 10_000
        with patch.dict(os.environ, RCP_PRODUCTION_ENABLED="true", RCP_DURABLE_ENABLED="false"):
            response = cloud.handler("coordinator", context=context)
            self.assertEqual(response["state"], "wait")
            self.assertEqual(len(cloud.launches), 1)
            self.assertLessEqual(cloud.handler_service.retry_until - cloud.handler_service.monotonic(), 5)
            with self.assertRaisesRegex(ValueError, "execution fields"):
                cloud.handler("coordinator", event={**cloud.event, "extra": "forbidden"})

    def test_expired_instances_are_scanned_before_missing_malformed_or_denied_pointer(self):
        for variant in ("absent", "malformed", "denied", "bad-identity"):
            with self.subTest(variant=variant):
                cloud = ProductionCloud()
                cloud.coordinator()
                cloud.now += timedelta(hours=3, seconds=1)
                if variant == "absent":
                    del cloud.database.items["active"]
                elif variant == "malformed":
                    cloud.database.items["active"]["unexpected"] = {"S": "field"}
                elif variant == "bad-identity":
                    cloud.database.items["active"]["run_key"] = {"S": "../foreign"}
                original = cloud.production_get
                def get(**parameters):
                    if parameters["Key"]["pk"]["S"] == "active":
                        self.assertIn(("terminate", [INSTANCE]), cloud.events)
                        if variant == "denied":
                            raise ApiError("AccessDenied", status=403)
                    return original(**parameters)
                cloud.api["dynamodb"].get_item.side_effect = get
                with patch.dict(os.environ, RCP_PRODUCTION_ENABLED="false", RCP_DURABLE_ENABLED="false"):
                    if variant == "absent":
                        self.assertEqual(cloud.handler("watchdog"), dict(state="idle"))
                    else:
                        with self.assertRaises((ValueError, ApiError)):
                            cloud.handler("watchdog")
                self.assertIn(("terminate", [INSTANCE]), cloud.events)
                self.assertEqual(len(cloud.launches), 1)

    def test_watchdog_uses_one_scan_and_exact_active_owner_while_coordinator_is_disabled(self):
        cloud = ProductionCloud()
        cloud.coordinator()
        before = cloud.api["ec2"].describe_instances.call_count
        with patch.dict(os.environ, RCP_PRODUCTION_ENABLED="false", RCP_DURABLE_ENABLED="false"):
            result = cloud.handler("watchdog")
        self.assertEqual(result["state"], "requested")
        self.assertEqual(cloud.api["ec2"].describe_instances.call_count - before, 1)
        self.assertEqual((cloud.handler_service.run_key, cloud.handler_service.epoch), (RUN_KEY, EPOCH))
        self.assertTrue(cloud.state["worker_published"])


if __name__ == "__main__":
    unittest.main()
