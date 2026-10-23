"""Offline tests for the submit Lambda trust boundary and server policy envelope."""

from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict
import base64
import hashlib
import io
import json
import os
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from . import production_handler as handler
from . import production_client
from .production_auth import GitHubIdentityVerifier
from .production_policy import canonical
from .test_production_admission import Cloud
from .test_production_policy import NOW, inputs, policy


TOKEN = "test-only-bearer-do-not-log"


def policy_item(value=None):
    value = value or policy()
    return dict(pk={"S": "policy"}, version={"S": value.version}, enabled={"BOOL": value.enabled},
                payload={"S": canonical(asdict(value)).decode()})


def environment(cloud=None):
    cloud = cloud or Cloud()
    result = {"RCP_DURABLE_" + key.upper(): value for key, value in asdict(cloud.config).items()}
    result.update(RCP_PRODUCTION_ENABLED="true", RCP_PRODUCTION_ADMISSION_ENABLED="true",
        RCP_PRODUCTION_REPOSITORY="wykurz/rcp", RCP_PRODUCTION_REPOSITORY_ID="123",
        RCP_PRODUCTION_OWNER_ID="483465", RCP_PRODUCTION_AUDIENCE="test-admission-audience",
        RCP_PRODUCTION_SUBMIT_WORKFLOW="wykurz/rcp/.github/workflows/aws-benchmark-production.yml@refs/heads/main",
        RCP_PRODUCTION_MACHINE_ARN=cloud.machine)
    return result


def request(action="begin"):
    result = dict(action=action, token=TOKEN)
    if action == "begin":
        result["manifest"] = inputs()
    return result


def machine_metadata(machine):
    return dict(stateMachineArn=machine, name=machine.rsplit(":", 1)[1], type="STANDARD", status="ACTIVE")


class PolicyReaderTests(unittest.TestCase):
    def setUp(self):
        self.item = policy_item()
        self.api = Mock()
        self.api.get_item.side_effect = lambda **kwargs: {"Item": self.item}

    def read(self):
        return handler.read_policy(self.api, "production-table", "483465")

    def test_canonical_policy_matches_transaction_fence_without_type_coercion(self):
        result = self.read()
        self.assertEqual(result, policy())
        self.api.get_item.assert_called_once_with(TableName="production-table",
            Key={"pk": {"S": "policy"}}, ConsistentRead=True)

    def test_lists_are_converted_only_after_validating_the_wire_schema(self):
        value = policy(manual_actor_ids=("77",), schedule_slots=((3, 19, 30),), scheduled_baseline="a" * 40)
        self.item = policy_item(value)
        self.assertEqual(self.read(), value)
        self.assertIsInstance(self.read().schedule_slots[0], tuple)

    def test_disabled_policy_remains_disabled(self):
        self.item = policy_item(policy(enabled=False))
        self.assertFalse(self.read().enabled)

    def test_missing_or_wrong_policy_item_is_rejected(self):
        for item in (None, {}, [], {"pk": {"S": "other"}}, policy_item() | {"extra": {"S": "x"}}):
            with self.subTest(kind=type(item)), self.assertRaisesRegex(ValueError, "policy unavailable"):
                self.item = item
                self.read()

    def test_envelope_type_version_enabled_and_owner_drift_are_rejected(self):
        changes = [dict(version={"S": "other"}), dict(version={"N": "1"}),
            dict(version={"S": "policy-1", "N": "1"}), dict(enabled={"BOOL": False}),
            dict(enabled={"BOOL": 1}), dict(enabled={"S": "true"}),
            dict(enabled={"BOOL": True, "S": "true"}), dict(pk={"S": "other"}),
            dict(payload={"S": 123}), dict(payload={"B": b"bytes"})]
        for change in changes:
            with self.subTest(fields=list(change)), self.assertRaises(ValueError):
                self.item = policy_item() | change
                self.read()
        self.item = policy_item(policy(owner_id="999"))
        with self.assertRaises(ValueError):
            self.read()

    def test_noncanonical_duplicate_nonfinite_and_oversize_payloads_are_rejected(self):
        original = policy_item()["payload"]["S"]
        values = [original + " ", '{"enabled":true,' + original[1:], "[]", "null", "{broken}",
                  original[:-1] + ',"extra":NaN}', "x" * (handler.MAX_POLICY_BYTES + 1)]
        for value in values:
            with self.subTest(size=len(value)), self.assertRaises(ValueError):
                self.item = policy_item()
                self.item["payload"]["S"] = value
                self.read()

    def test_payload_requires_every_policy_field_and_valid_array_shapes(self):
        original = json.loads(policy_item()["payload"]["S"])
        missing = dict(original)
        del missing["daily_run_limit"]
        cases = [missing, original | {"extra": 1}, original | {"manual_actor_ids": "77"},
            original | {"manual_actor_ids": ["77"] * 21}, original | {"schedule_slots": "slot"},
            original | {"schedule_slots": [[3, 19]]}, original | {"enabled": 1},
            original | {"runtime_seconds": 1}, original | {"total_cap_microusd": True}]
        for value in cases:
            with self.subTest(fields=len(value)), self.assertRaises(ValueError):
                self.item = policy_item()
                self.item["payload"]["S"] = canonical(value).decode()
                self.read()

    def test_read_error_does_not_expose_response_details(self):
        self.api.get_item.side_effect = RuntimeError(TOKEN)
        with self.assertRaises(ValueError) as caught:
            self.read()
        self.assertNotIn(TOKEN, str(caught.exception))


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.network = patch("socket.create_connection", side_effect=AssertionError("unexpected network"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def test_missing_or_nonliteral_enable_flags_are_inert_before_configuration_or_client_creation(self):
        for values in ({}, {"RCP_PRODUCTION_ENABLED": "true"},
                       {"RCP_PRODUCTION_ADMISSION_ENABLED": "true"},
                       {"RCP_PRODUCTION_ENABLED": "True", "RCP_PRODUCTION_ADMISSION_ENABLED": "true"},
                       {"RCP_PRODUCTION_ENABLED": "true", "RCP_PRODUCTION_ADMISSION_ENABLED": "false"}):
            with self.subTest(values=values), patch.dict(os.environ, values, clear=True), \
                    patch.object(handler, "build_admission") as build:
                self.assertEqual(handler.lambda_handler(request(), None),
                                 {"ok": False, "error": "admission_disabled"})
                build.assert_not_called()

    def test_begin_and_seal_route_exact_token_and_validated_inputs(self):
        service = Mock()
        service.begin.return_value = {"state": "awaiting-inputs"}
        service.seal_and_start.return_value = {"state": "armed"}
        with patch.dict(os.environ, environment(), clear=True), \
                patch.object(handler, "build_admission", return_value=service):
            self.assertEqual(handler.lambda_handler(request(), None),
                             {"ok": True, "result": {"state": "awaiting-inputs"}})
            self.assertEqual(handler.lambda_handler(request("seal"), None),
                             {"ok": True, "result": {"state": "armed"}})
        service.begin.assert_called_once_with(TOKEN, inputs())
        service.seal_and_start.assert_called_once_with(TOKEN)

    def test_unknown_routes_fields_wrong_types_and_oversized_tokens_fail_before_service_build(self):
        bad = [None, [], "raw body", {}, dict(action="other", token=TOKEN),
            dict(action=[], token=TOKEN), request() | {"machine_arn": "caller-machine"},
            request("seal") | {"manifest": inputs()}, dict(action="begin", token=TOKEN),
            dict(action="seal", token=b"bytes"), dict(action="seal", token=""),
            dict(action="seal", token="x" * (handler.MAX_TOKEN_BYTES + 1)),
            request() | {"manifest": inputs() | {"quote": 0}}, request() | {"token": "\u00e9"}]
        for event in bad:
            with self.subTest(kind=type(event)), patch.dict(os.environ, environment(), clear=True), \
                    patch.object(handler, "build_admission") as build:
                self.assertEqual(handler.lambda_handler(event, None),
                                 {"ok": False, "error": "invalid_request"})
                build.assert_not_called()

    def test_errors_log_only_safe_categories_without_token_exception_text_or_retry(self):
        for action in ("begin", "seal"):
            service = Mock()
            service.begin.side_effect = RuntimeError(TOKEN)
            service.seal_and_start.side_effect = RuntimeError(TOKEN)
            out, err = io.StringIO(), io.StringIO()
            with patch.dict(os.environ, environment(), clear=True), \
                    patch.object(handler, "build_admission", return_value=service), \
                    redirect_stdout(out), redirect_stderr(err):
                result = handler.lambda_handler(request(action), None)
            self.assertEqual(result, {"ok": False, "error": "admission_unavailable"})
            self.assertNotIn(TOKEN, json.dumps(result) + out.getvalue() + err.getvalue())
            self.assertEqual(err.getvalue(), "")
            self.assertEqual(json.loads(out.getvalue()), dict(event="admission_failure", action=action,
                category="validation_or_service", retryable=False))
            route = service.begin if action == "begin" else service.seal_and_start
            route.assert_called_once()

    def test_transient_failure_has_typed_delay_and_safe_request_id(self):
        for error, delay, category in ((TimeoutError(TOKEN), 5, "transient_service"),
                (handler.GitHubRateLimitError(120), 120, "github_rate_limit")):
            service = Mock()
            service.begin.side_effect = error
            out = io.StringIO()
            with patch.dict(os.environ, environment(), clear=True), \
                    patch.object(handler, "build_admission", return_value=service), redirect_stdout(out):
                result = handler.lambda_handler(request(), Mock(aws_request_id="safe-request-123"))
            self.assertEqual(result, dict(ok=False, error="admission_retryable", retry_after_seconds=delay))
            self.assertEqual(json.loads(out.getvalue()), dict(event="admission_failure", action="begin",
                category=category, retryable=True, request_id="safe-request-123"))
            self.assertNotIn(TOKEN, out.getvalue())
            service.begin.assert_called_once()

    def test_signing_key_cache_reused_only_for_identical_immutable_trust_config(self):
        handler._cached_verifier.cache_clear()
        self.addCleanup(handler._cached_verifier.cache_clear)
        with patch.dict(os.environ, environment(), clear=True), patch("boto3.client") as client:
            first, second = handler.build_admission(), handler.build_admission()
            self.assertIs(first.verifier.verifier, second.verifier.verifier)
            self.assertIsNot(first, second)
            self.assertIsNot(first.run_reader, second.run_reader)
            os.environ["RCP_PRODUCTION_AUDIENCE"] = "different-admission-audience"
            third = handler.build_admission()
            self.assertIsNot(first.verifier.verifier, third.verifier.verifier)
            client.assert_not_called()

    def test_configuration_failure_is_sanitized(self):
        with patch.dict(os.environ, environment(), clear=True), \
                patch.object(handler, "build_admission", side_effect=ValueError(TOKEN)):
            self.assertEqual(handler.lambda_handler(request(), None),
                             {"ok": False, "error": "admission_unavailable"})

    def test_bounded_clients_are_lazy_no_retry_and_use_fixed_regional_endpoints(self):
        with patch("boto3.client") as client:
            clients = handler.SubmissionClients("us-east-1")
            client.assert_not_called()
            for name, service in (("dynamodb", "dynamodb"), ("s3", "s3"), ("stepfunctions", "states")):
                result = clients[name]
                self.assertIs(result, clients[name])
                call = client.call_args
                self.assertEqual(call.args, (name,))
                self.assertEqual(call.kwargs["region_name"], "us-east-1")
                self.assertEqual(call.kwargs["endpoint_url"], f"https://{service}.us-east-1.amazonaws.com")
                config = call.kwargs["config"]
                self.assertEqual((config.connect_timeout, config.read_timeout), (3, 5))
                self.assertEqual(config.retries, {"total_max_attempts": 1})
                self.assertEqual(config.proxies, {})
                self.assertTrue(config.ignore_configured_endpoint_urls)
                self.assertEqual(config.signature_version, "s3v4" if name == "s3" else "v4")
                self.assertEqual(config.s3, {"addressing_style": "virtual"} if name == "s3" else None)
            self.assertEqual(client.call_count, 3)
            with self.assertRaises(KeyError):
                clients["ec2"]
            self.assertEqual(client.call_count, 3)

    def test_real_sdk_presigned_host_matches_strict_upload_client_without_network(self):
        credentials = {"AWS_ACCESS_KEY_ID": "testing-only", "AWS_SECRET_ACCESS_KEY": "testing-only",
                       "AWS_SESSION_TOKEN": "testing-only", "AWS_EC2_METADATA_DISABLED": "true"}
        data = b"test-only-binary"
        checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
        headers = {"Content-Length": str(len(data)), "x-amz-checksum-sha256": checksum,
                   "If-None-Match": "*", "Content-Type": "application/octet-stream",
                   "x-amz-server-side-encryption": "AES256"}
        with patch.dict(os.environ, credentials, clear=True), patch("boto3.DEFAULT_SESSION", None), \
                patch("botocore.httpsession.URLLib3Session.send", side_effect=AssertionError("unexpected SDK request")):
            for region, bucket in (("us-east-1", "production-artifacts"), ("eu-west-1", "a" * 63)):
                client = handler.SubmissionClients(region)["s3"]
                url = client.generate_presigned_url("put_object", Params={"Bucket": bucket,
                    "Key": "production/staging/repo-655073755-run-1234/baseline/rcp",
                    "ContentLength": len(data), "ChecksumSHA256": checksum, "IfNoneMatch": "*",
                    "ContentType": "application/octet-stream", "ServerSideEncryption": "AES256"},
                    ExpiresIn=600, HttpMethod="PUT")
                target = urlsplit(url)
                self.assertEqual(target.hostname, bucket + ".s3." + region + ".amazonaws.com")
                self.assertTrue(target.path.startswith("/production/staging/"))
                with patch.object(production_client, "opener") as opener:
                    response = opener.return_value.open.return_value.__enter__.return_value
                    response.status = 200
                    response.read.return_value = b""
                    production_client.upload({"url": url, "headers": headers}, data, bucket=bucket, region=region)
                    self.assertEqual(opener.return_value.open.call_count, 1)

    def test_build_has_no_sdk_calls_and_restricts_workflow_and_machine_scope(self):
        with patch.dict(os.environ, environment(), clear=True), patch("boto3.client") as client:
            service = handler.build_admission(clock=lambda: NOW.timestamp())
            self.assertEqual(service.machine, environment()["RCP_PRODUCTION_MACHINE_ARN"])
            self.assertEqual(service.verifier.verifier._workflow_refs,
                             {environment()["RCP_PRODUCTION_SUBMIT_WORKFLOW"]})
            client.assert_not_called()
        for key, value in (("RCP_PRODUCTION_SUBMIT_WORKFLOW", "wykurz/rcp/.github/workflows/aws-benchmark-collect.yml@refs/heads/main"),
            ("RCP_PRODUCTION_SUBMIT_WORKFLOW", environment()["RCP_PRODUCTION_SUBMIT_WORKFLOW"].replace("main", "feature")),
            ("RCP_PRODUCTION_MACHINE_ARN", "arn:aws:states:us-west-2:123456789012:stateMachine:production"),
            ("RCP_PRODUCTION_MACHINE_ARN", "arn:aws:states:us-east-1:999999999999:stateMachine:production"),
            ("RCP_PRODUCTION_MACHINE_ARN", environment()["RCP_PRODUCTION_MACHINE_ARN"] + ":1"),
            ("RCP_PRODUCTION_AUDIENCE", "sts.amazonaws.com"), ("RCP_PRODUCTION_REPOSITORY_ID", "0"),
            ("RCP_DURABLE_AMI", "caller-image"), ("RCP_DURABLE_TABLE", "../table"),
            ("RCP_DURABLE_BUCKET", "production.artifacts"), ("RCP_DURABLE_BUCKET", "ProductionArtifacts"),
            ("RCP_DURABLE_BUCKET", "ab"), ("RCP_DURABLE_BUCKET", "a" * 64)):
            with self.subTest(key=key), patch.dict(os.environ, environment() | {key: value}, clear=True), \
                    patch("boto3.client") as client:
                with self.assertRaises(ValueError):
                    handler.build_admission()
                client.assert_not_called()

    def test_collect_signed_token_is_rejected_before_any_machine_read(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key())) | {"kid": "test-key", "use": "sig"}
        fetch = Mock(return_value=canonical({"keys": [jwk]}))
        def verifier(**kwargs):
            return GitHubIdentityVerifier(**kwargs, jwks_fetcher=fetch)
        api = {"dynamodb": Mock(), "s3": Mock(), "stepfunctions": Mock()}
        with patch.dict(os.environ, environment(), clear=True), \
                patch.object(handler, "GitHubIdentityVerifier", side_effect=verifier):
            service = handler.build_admission(api=api, clock=lambda: NOW.timestamp())
        claims = dict(iss="https://token.actions.githubusercontent.com", aud="test-admission-audience",
            sub="repo:wykurz/rcp:ref:refs/heads/main", exp=int(NOW.timestamp()) + 300,
            iat=int(NOW.timestamp()), nbf=int(NOW.timestamp()), jti="test-only", repository="wykurz/rcp",
            repository_id="123", repository_owner="wykurz", repository_owner_id="483465", ref="refs/heads/main",
            workflow_ref="wykurz/rcp/.github/workflows/aws-benchmark-collect.yml@refs/heads/main",
            workflow_sha="a" * 40, sha="a" * 40, run_id="100", run_attempt="1", actor="wykurz",
            actor_id="483465", event_name="workflow_dispatch")
        token = jwt.encode(claims, key, algorithm="RS256", headers={"kid": "test-key"})
        with self.assertRaises(ValueError):
            service.begin(token, inputs())
        for client in api.values():
            self.assertEqual(client.mock_calls, [])


class HandlerAdmissionIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.cloud = Cloud()
        self.cloud.ddb.items["policy"] = policy_item(self.cloud.policy)
        self.cloud.service.policy_reader = lambda: handler.read_policy(self.cloud.ddb,
            self.cloud.config.table, self.cloud.policy.owner_id)
        self.cloud.service.verifier = handler._StandardMachineVerifier(
            self.cloud.verifier, self.cloud.api, self.cloud.machine)
        self.cloud.api["stepfunctions"].describe_state_machine.return_value = machine_metadata(self.cloud.machine)

    def invoke(self, action="begin"):
        with patch.dict(os.environ, environment(self.cloud), clear=True), \
                patch.object(handler, "build_admission", return_value=self.cloud.service):
            return handler.lambda_handler(request(action), None)

    def test_valid_begin_then_seal_uses_original_token_and_checks_standard_before_effects(self):
        self.assertTrue(self.invoke()["ok"])
        self.cloud.upload()
        self.assertTrue(self.invoke("seal")["ok"])
        self.assertEqual(self.cloud.api["stepfunctions"].describe_state_machine.call_count, 2)
        self.cloud.api["stepfunctions"].describe_state_machine.assert_called_with(stateMachineArn=self.cloud.machine)
        self.assertEqual(self.cloud.api["stepfunctions"].start_execution.call_count, 1)
        self.assertTrue(all(call.args == (TOKEN,) for call in self.cloud.verifier.verify.call_args_list))

    def test_wrong_machine_metadata_and_express_never_reserve_or_issue_uploads(self):
        for update in (dict(type="EXPRESS"), dict(type=None), dict(status="DELETING"),
                       dict(stateMachineArn=self.cloud.machine + "other"), dict(name="other")):
            with self.subTest(update=update):
                self.cloud.api["stepfunctions"].describe_state_machine.return_value = machine_metadata(self.cloud.machine) | update
                self.assertEqual(self.invoke(), {"ok": False, "error": "admission_unavailable"})
                self.assertEqual(self.cloud.ddb.writes, [])
                self.cloud.api["s3"].generate_presigned_url.assert_not_called()
                self.cloud.api["stepfunctions"].start_execution.assert_not_called()

    def test_machine_lookup_error_and_invalid_token_do_not_touch_state(self):
        self.cloud.api["stepfunctions"].describe_state_machine.side_effect = TimeoutError(TOKEN)
        self.assertFalse(self.invoke()["ok"])
        self.assertEqual(self.cloud.ddb.writes, [])
        self.cloud.api["stepfunctions"].describe_state_machine.reset_mock()
        self.cloud.verifier.verify.side_effect = ValueError(TOKEN)
        self.assertFalse(self.invoke()["ok"])
        self.cloud.api["stepfunctions"].describe_state_machine.assert_not_called()
        self.assertEqual(self.cloud.ddb.writes, [])

    def test_missing_corrupt_or_disabled_server_policy_cannot_admit(self):
        for item in (None, policy_item(policy(enabled=False)), policy_item() | {"version": {"S": "drift"}}):
            with self.subTest(kind=type(item)):
                if item is None:
                    self.cloud.ddb.items.pop("policy", None)
                else:
                    self.cloud.ddb.items["policy"] = item
                self.assertFalse(self.invoke()["ok"])
                self.assertEqual(self.cloud.ddb.writes, [])
                self.cloud.api["s3"].generate_presigned_url.assert_not_called()

    def test_express_or_disabled_policy_at_seal_preserves_existing_hold(self):
        self.assertTrue(self.invoke()["ok"])
        self.cloud.upload()
        self.cloud.api["stepfunctions"].describe_state_machine.return_value["type"] = "EXPRESS"
        self.assertFalse(self.invoke("seal")["ok"])
        self.assertIn("active", self.cloud.ddb.items)
        self.cloud.api["stepfunctions"].describe_state_machine.return_value["type"] = "STANDARD"
        self.cloud.ddb.items["policy"] = policy_item(policy(enabled=False))
        self.assertFalse(self.invoke("seal")["ok"])
        self.assertIn("active", self.cloud.ddb.items)
        self.cloud.api["stepfunctions"].start_execution.assert_not_called()


if __name__ == "__main__":
    unittest.main()
