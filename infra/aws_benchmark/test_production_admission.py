"""Submission integration tests use only in-memory transactions and fixed binary heads."""

import base64
import copy
from datetime import timedelta
import json
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from . import production_client
from .common import BINARIES
from .durable_aws import Configuration
from .production_admission import Admission, UPLOAD_SECONDS, staged_key
from .production_store import ReconciliationRequired
from .test_production_store import DynamoTransactions
from .test_production_policy import NOW, identity, inputs, policy, run_json
from .production_policy import WorkflowRun
from .test_durable_contracts import request as legacy_request


class ApiError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class Cloud:
    def __init__(self):
        self.now = NOW
        self.who = identity()
        self.policy = policy()
        self.ddb = DynamoTransactions()
        self.ddb.items["policy"]["version"]["S"] = self.policy.version
        self.ddb.items["budget#total"] = dict(pk={"S": "budget#total"}, reserved={"N": "64000000"})
        self.config = Configuration(**legacy_request()["pins"], table="production-table",
                                    runtime_group="runtime", termination_role="arn:aws:iam::123456789012:role/stop")
        self.machine = "arn:aws:states:us-east-1:123456789012:stateMachine:production"
        self.api = {"dynamodb": self.ddb, "s3": Mock(), "stepfunctions": Mock()}
        self.api["s3"].generate_presigned_url.side_effect = lambda method, **kw: "https://example.invalid/exact-upload"
        self.objects = {}
        self.api["s3"].head_object.side_effect = lambda **kw: copy.deepcopy(self.objects[kw["Key"]])
        self.execution = None
        self.lost_response = False
        self.api["stepfunctions"].describe_execution.side_effect = self.describe
        self.api["stepfunctions"].start_execution.side_effect = self.start
        self.verifier = Mock()
        self.verifier.verify.side_effect = lambda token: self.who
        self.reader = Mock()
        self.reader.read.side_effect = lambda who: WorkflowRun.verify(run_json(who), who, self.now)
        self.service = Admission(self.api, self.config, self.machine, lambda: self.policy,
                                 self.verifier, self.reader, now=lambda: self.now)

    def describe(self, **kw):
        if self.execution is None:
            raise ApiError("ExecutionDoesNotExist")
        return copy.deepcopy(self.execution)

    def start(self, **kw):
        self.execution = dict(executionArn=self.machine.replace(":stateMachine:", ":execution:") + ":" + kw["name"],
                              stateMachineArn=self.machine, name=kw["name"], input=kw["input"], status="RUNNING")
        if self.lost_response:
            raise TimeoutError("lost response")
        return dict(executionArn=self.execution["executionArn"])

    def upload(self):
        for name, entry in inputs()["binaries"].items():
            self.objects[staged_key("repo-123-run-100", name)] = dict(ContentLength=entry["bytes"],
                ChecksumSHA256=base64.b64encode(bytes.fromhex(entry["sha256"])).decode(),
                ChecksumType="FULL_OBJECT", ServerSideEncryption="AES256", VersionId="sealed-version-" + name)

    def begin(self):
        return self.service.begin("not-a-real-token", inputs())

    def seal(self):
        return self.service.seal_and_start("not-a-real-token")


class AdmissionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.cloud = Cloud()

    def test_reserves_once_before_upload_without_starting_execution(self):
        response = self.cloud.begin()
        self.assertEqual(response["state"], "awaiting-inputs")
        self.assertEqual(set(response["uploads"]), set(BINARIES))
        self.assertEqual(self.cloud.ddb.items["budget#total"]["reserved"]["N"], "80052800")
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()
        self.cloud.begin()
        self.assertEqual(self.cloud.ddb.items["budget#total"]["reserved"]["N"], "80052800")

    def test_every_upload_capability_pins_key_size_checksum_and_create_only(self):
        self.cloud.begin()
        for call in self.cloud.api["s3"].generate_presigned_url.call_args_list:
            self.assertEqual(call.args, ("put_object",))
            value = call.kwargs
            self.assertEqual(value["HttpMethod"], "PUT")
            self.assertEqual(value["ExpiresIn"], UPLOAD_SECONDS)
            parameters = value["Params"]
            self.assertEqual(parameters["IfNoneMatch"], "*")
            self.assertEqual(parameters["ContentLength"], 100)
            self.assertEqual(parameters["ServerSideEncryption"], "AES256")
            self.assertTrue(parameters["Key"].startswith("production/staging/repo-123-run-100/"))
            self.assertEqual(parameters["ChecksumSHA256"], base64.b64encode(bytes.fromhex("b" * 64)).decode())

    def test_workflow_rerun_returns_existing_admission_without_new_upload_or_charge(self):
        self.cloud.begin()
        self.cloud.api["s3"].generate_presigned_url.reset_mock()
        self.cloud.who = identity(run_attempt="2")
        response = self.cloud.begin()
        self.assertNotIn("uploads", response)
        self.cloud.api["s3"].generate_presigned_url.assert_not_called()
        with self.assertRaisesRegex(ValueError, "reruns"):
            self.cloud.seal()
        self.assertEqual(self.cloud.ddb.items["rate#day#2026-10-22"]["runs"]["N"], "1")

    def test_missing_or_modified_input_never_starts_execution(self):
        self.cloud.begin()
        with self.assertRaises(KeyError):
            self.cloud.seal()
        self.cloud.upload()
        self.cloud.objects[staged_key("repo-123-run-100", "candidate/rcp")]["ChecksumSHA256"] = "wrong"
        with self.assertRaisesRegex(ValueError, "bytes"):
            self.cloud.seal()
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_unversioned_or_wrong_size_input_is_rejected(self):
        self.cloud.begin()
        self.cloud.upload()
        key = staged_key("repo-123-run-100", "candidate/rcp")
        original = copy.deepcopy(self.cloud.objects[key])
        for update in (dict(VersionId="null"), dict(VersionId=""), dict(ContentLength=True),
                       dict(ContentLength=101), dict(ServerSideEncryption="none"), dict(ChecksumType="COMPOSITE")):
            self.cloud.objects[key] = original | update
            with self.subTest(update=update), self.assertRaises(ValueError):
                self.cloud.seal()
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_seals_exact_versions_before_one_idempotent_execution(self):
        self.cloud.begin()
        self.cloud.upload()
        response = self.cloud.seal()
        self.assertEqual(response["state"], "armed")
        record = self.cloud.service.store(self.cloud.who).read()
        self.assertEqual(record.state["phase"], "armed")
        self.assertEqual(record.state["sealed_inputs"]["candidate/rcp"]["version"], "sealed-version-candidate/rcp")
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)
        again = self.cloud.seal()
        self.assertEqual(again["execution_status"], "RUNNING")
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)

    def test_progressed_execution_readback_never_restarts_it(self):
        self.cloud.begin()
        self.cloud.upload()
        self.cloud.seal()
        store = self.cloud.service.store(self.cloud.who)
        for phase in ("launching", "uncertain", "requested"):
            with self.subTest(phase=phase):
                state, revision = store.load()
                state["phase"] = phase
                if phase == "launching":
                    store.save_launch_intent(state, revision)
                else:
                    store.save(state, revision)
                value = self.cloud.seal()
                self.assertEqual(value["state"], phase)
                self.assertEqual(value["execution_status"], "RUNNING")
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)
        self.cloud.execution["input"] = "{}"
        with self.assertRaisesRegex(ValueError, "immutable submission"):
            self.cloud.seal()
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)

    def test_client_server_reconciles_fast_progress_and_lost_success_reply(self):
        real_start = self.cloud.start
        def start(**kwargs):
            response = real_start(**kwargs)
            store = self.cloud.service.store(self.cloud.who)
            state, revision = store.load()
            state.update(phase="launching", execution_arn=response["executionArn"])
            store.save_launch_intent(state, revision)
            return response
        self.cloud.api["stepfunctions"].start_execution.side_effect = start
        def invoke(**kwargs):
            value = json.loads(kwargs["Payload"])
            result = self.cloud.service.begin(value["token"], value["manifest"]) if value["action"] == "begin" else \
                self.cloud.service.seal_and_start(value["token"])
            return dict(StatusCode=200, Payload=io.BytesIO(json.dumps(dict(ok=True, result=result)).encode()))
        api = Mock()
        api.invoke.side_effect = invoke
        function = "arn:aws:lambda:us-east-1:123456789012:function:admission"
        with tempfile.TemporaryDirectory() as directory:
            for _ in range(2):
                value = production_client.submit(api, function, inputs(), dict.fromkeys(BINARIES, b"test"),
                    Path(directory) / "receipt.json", audience="audience", bucket=self.cloud.config.bucket,
                    region=self.cloud.config.region, token_reader=lambda _: "verified-by-test", expected_run="100-1",
                    uploader=lambda *args, **kwargs: self.cloud.upload(), wall_clock=lambda: self.cloud.now.timestamp())
                self.assertEqual(value["state"], "launching")
                self.assertIn(value["execution_status"], ("submitted", "RUNNING"))
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)
        self.assertEqual(self.cloud.ddb.items["rate#day#2026-10-22"]["runs"]["N"], "1")

    def test_lost_start_response_reconciles_only_exact_execution(self):
        self.cloud.begin()
        self.cloud.upload()
        self.cloud.lost_response = True
        result = self.cloud.seal()
        self.assertEqual(result["execution_status"], "RUNNING")
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)
        self.assertIn("active", self.cloud.ddb.items)

    def test_uncertain_start_without_execution_retains_owner(self):
        self.cloud.begin()
        self.cloud.upload()
        self.cloud.api["stepfunctions"].start_execution.side_effect = TimeoutError("uncertain")
        with self.assertRaises(TimeoutError):
            self.cloud.seal()
        self.assertIn("active", self.cloud.ddb.items)
        self.assertEqual(self.cloud.service.store(self.cloud.who).read().state["phase"], "armed")
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)

    def test_existing_execution_must_match_entire_input_and_identity(self):
        self.cloud.begin()
        self.cloud.upload()
        self.cloud.seal()
        self.cloud.execution["input"] = '{}'
        with self.assertRaisesRegex(ValueError, "immutable"):
            self.cloud.seal()
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)

    def test_policy_kill_switch_blocks_sealing_but_does_not_release_owner(self):
        self.cloud.begin()
        self.cloud.upload()
        self.cloud.policy = policy(enabled=False)
        with self.assertRaisesRegex(ValueError, "disabled"):
            self.cloud.seal()
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()
        self.assertIn("active", self.cloud.ddb.items)

    def test_upload_timeout_and_sealing_timeout_do_not_extend_request(self):
        self.cloud.begin()
        self.cloud.upload()
        request = copy.deepcopy(self.cloud.service.store(self.cloud.who).read().state["request"])
        self.cloud.now += timedelta(seconds=UPLOAD_SECONDS)
        with self.assertRaisesRegex(ValueError, "expired"):
            self.cloud.seal()
        self.assertEqual(self.cloud.service.store(self.cloud.who).read().state["request"], request)
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_short_capacity_expires_before_seal_without_armed_intent(self):
        self.cloud.policy = policy(capacity_seconds=60)
        response = self.cloud.begin()
        self.assertTrue(all(call.kwargs["ExpiresIn"] == 60 for call in
                            self.cloud.api["s3"].generate_presigned_url.call_args_list))
        self.cloud.upload()
        self.cloud.now += timedelta(seconds=60)
        with self.assertRaisesRegex(ValueError, "capacity period expired"):
            self.cloud.seal()
        state = self.cloud.service.store(self.cloud.who).read().state
        self.assertEqual(state["phase"], "awaiting-inputs")
        self.assertIsNone(state["submission_arn"])
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_capacity_expiring_during_heads_never_persists_seal(self):
        self.cloud.policy = policy(capacity_seconds=60)
        self.cloud.begin()
        self.cloud.upload()
        def head(**kwargs):
            self.cloud.now += timedelta(seconds=10)
            return copy.deepcopy(self.cloud.objects[kwargs["Key"]])
        self.cloud.api["s3"].head_object.side_effect = head
        with self.assertRaisesRegex(ValueError, "exceeded upload or capacity"):
            self.cloud.seal()
        state = self.cloud.service.store(self.cloud.who).read().state
        self.assertEqual(state["phase"], "awaiting-inputs")
        self.assertIsNone(state["sealed_inputs"])
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_first_admission_from_rerun_cannot_spend_original_actors_allowance(self):
        self.cloud.who = identity(run_attempt="2")
        with self.assertRaisesRegex(ValueError, "reruns cannot create"):
            self.cloud.begin()
        self.assertEqual(self.cloud.ddb.writes, [])

    def test_near_stale_run_is_rejected_before_reserving_upload_budget(self):
        self.cloud.now += timedelta(seconds=4800)
        with self.assertRaisesRegex(ValueError, "insufficient time"):
            self.cloud.begin()
        self.assertEqual(self.cloud.ddb.writes, [])

    def test_invalid_token_never_touches_admission(self):
        self.cloud.verifier.verify.side_effect = ValueError("invalid signed identity")
        with self.assertRaises(ValueError):
            self.cloud.begin()
        self.assertEqual(self.cloud.ddb.writes, [])
        self.cloud.reader.read.assert_not_called()

    def test_upload_keys_reject_another_namespace_or_unknown_binary(self):
        for key, binary in (("../run", "candidate/rcp"), ("repo-123-run-100", "../rcp"),
                            ("repo-123-run-100/other", "candidate/rcp")):
            with self.subTest(key=key, binary=binary), self.assertRaises(ValueError):
                staged_key(key, binary)

    def test_real_sdk_presigner_binds_upload_conditions_without_network(self):
        import boto3
        from botocore.config import Config
        from urllib.parse import parse_qs, urlsplit
        self.cloud.api["s3"] = boto3.client("s3", region_name="us-east-1", aws_access_key_id="fake-test-id",
            aws_secret_access_key="fake-test-secret", config=Config(signature_version="s3v4"))
        response = self.cloud.begin()
        for upload in response["uploads"].values():
            signed = parse_qs(urlsplit(upload["url"]).query)["X-Amz-SignedHeaders"][0].split(";")
            self.assertTrue({"content-length", "if-none-match", "x-amz-checksum-sha256",
                             "x-amz-server-side-encryption", "content-type"} <= set(signed))


if __name__ == "__main__":
    unittest.main()
