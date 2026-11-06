"""Offline publisher-reader, identity and actual SDK transport contracts; never live proof."""

import copy
from datetime import datetime, timezone
import hashlib
import io
import json
import logging
import os
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs
from xml.sax.saxutils import escape

from botocore import UNSIGNED
from botocore.awsrequest import AWSResponse
from botocore.configloader import raw_config_parse
from botocore.exceptions import ClientError
from botocore.session import Session
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt

from . import acceptance_publication as probe
from .acceptance_oidc_workflow import Interrupted

NOW = 1800000000
SHA = "a" * 40
SECRET = "TEST-ONLY-NEVER-LOG"
RAW = b'{"schema":"TEST-ONLY-nonsecret-manifest"}\n'
SIGNER = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def approval(**updates):
    value = dict(schema=probe.SCHEMA, source_commit=SHA, approval_nonce="d" * 32,
        role_arn=probe.ROLE, role_id=probe.ROLE_ID, bucket=probe.BUCKET, table=probe.TABLE, table_key_arn=probe.TABLE_KEY_ARN,
        manifest_key=probe.MANIFEST, manifest_version=probe.VERSION, manifest_bytes=probe.MANIFEST_BYTES,
        manifest_sha256=probe.MANIFEST_SHA256, negative_key=probe.FORBIDDEN, negative_version="TEST-ONLY-version",
        fixture_capture_sha256="e" * 64, not_before=NOW - 10, expires_at=NOW + 300,
        approval_capture_sha256="f" * 64, call_plan=copy.deepcopy(probe.CALL_PLAN))
    value.update(updates)
    return value


def bindings(**updates):
    return {**approval(), "run_id": "10001", "run_attempt": "1", **updates}


def claims(**updates):
    value = dict(iss=probe.oidc.auth.ISSUER, aud="sts.amazonaws.com", sub=probe.SUBJECT,
        repository="wykurz/rcp", repository_id="655073755", repository_owner="wykurz",
        repository_owner_id="483465", ref="refs/heads/main", workflow_ref=probe.WORKFLOW,
        workflow=probe.WORKFLOW_NAME, workflow_sha=SHA, sha=SHA, run_id="10001", run_attempt="1",
        actor="wykurz", actor_id="483465", event_name="workflow_dispatch", runner_environment="github-hosted",
        iat=NOW - 10, nbf=NOW - 10, exp=NOW + 300)
    value.update(updates)
    return value


def token(**updates):
    return jwt.encode(claims(**updates), SIGNER, algorithm="RS256", headers={"kid": "TEST-ONLY"})


def error(code, status):
    return ClientError({"Error": {"Code": code, "Message": SECRET},
        "ResponseMetadata": {"HTTPStatusCode": status}}, "TEST-ONLY")


class PublicationProbeTests(unittest.TestCase):
    def setUp(self):
        for name, value in (("MANIFEST_BYTES", len(RAW)), ("MANIFEST_SHA256", hashlib.sha256(RAW).hexdigest())):
            change = patch.object(probe, name, value)
            change.start()
            self.addCleanup(change.stop)
        network = patch("socket.create_connection", side_effect=AssertionError("unexpected network"))
        network.start()
        self.addCleanup(network.stop)
        self.now, self.elapsed = NOW, 0
        self.name = "rcp-publication-test-10001-1"
        self.arn = "arn:aws:sts::381183037896:assumed-role/" + probe.ROLE.rsplit("/", 1)[1] + "/" + self.name
        self.uid = probe.ROLE_ID + ":" + self.name
        self.credentials = dict(AccessKeyId="ASIA" + "B" * 16, SecretAccessKey=SECRET,
            SessionToken=SECRET + "-session", Expiration=datetime.fromtimestamp(NOW + 900, timezone.utc))
        self.unsigned, self.signed, self.db, self.s3 = Mock(), Mock(), Mock(), Mock()
        self.unsigned.assume_role_with_web_identity.return_value = dict(Provider=probe.oidc.auth.ISSUER,
            Audience="sts.amazonaws.com", SubjectFromWebIdentityToken=probe.SUBJECT,
            AssumedRoleUser=dict(Arn=self.arn, AssumedRoleId=self.uid), Credentials=self.credentials)
        self.signed.get_caller_identity.return_value = dict(Account=probe.ACCOUNT, Arn=self.arn, UserId=self.uid)
        self.db.get_item.side_effect = [{"Item": {"pk": {"S": "run#" + probe.RUN}}}, error("AccessDeniedException", 400)]
        self.body = io.BytesIO(RAW)
        self.s3.get_object.side_effect = [dict(Body=self.body, VersionId=probe.VERSION, ContentLength=len(RAW)),
            error("AccessDenied", 403)]
        self.factory = Mock(side_effect=[self.unsigned, self.signed, self.db, self.s3])
        self.reader = Mock(return_value=token())

    def run_probe(self, config=None, **kwargs):
        options = dict(factory=self.factory, clock=lambda: self.now, monotonic=lambda: self.elapsed)
        options.update(kwargs)
        return probe.run_probe(bindings() if config is None else config, self.reader, **options)

    def assert_hold(self, result, stage=None):
        self.assertFalse(result["ok"], result)
        self.assertFalse(result["live_accepted"])
        self.assertEqual(result["unproven"], probe.UNPROVEN)
        if stage:
            self.assertEqual(result["diagnostics"]["stage"], stage)
        for secret in (SECRET, self.credentials["AccessKeyId"], self.reader.return_value):
            self.assertNotIn(secret, json.dumps(result))

    def test_exact_one_session_and_fixed_reads_produce_limited_sanitized_evidence(self):
        result = self.run_probe()
        self.assertTrue(result["ok"], result)
        self.assertFalse(result["live_accepted"])
        self.assertEqual(result["diagnostics"], {"stage": "complete"})
        self.assertEqual(result["run_id"], "10001")
        self.assertEqual(result["table_key_arn"], probe.TABLE_KEY_ARN)
        self.assertEqual(result["session_policy_sha256"], hashlib.sha256(probe.SESSION_POLICY.encode("ascii")).hexdigest())
        self.reader.assert_called_once_with()
        self.unsigned.assume_role_with_web_identity.assert_called_once_with(RoleArn=probe.ROLE,
            RoleSessionName=self.name, WebIdentityToken=token(), DurationSeconds=900, Policy=probe.SESSION_POLICY)
        self.signed.get_caller_identity.assert_called_once_with()
        self.assertEqual([c.args[0] for c in self.factory.call_args_list], ["sts", "sts", "dynamodb", "s3"])
        for call in self.factory.call_args_list[1:]:
            self.assertEqual(call.kwargs, {"credentials": self.credentials})
        self.assertEqual([c.kwargs for c in self.db.get_item.call_args_list], [dict(TableName=probe.TABLE,
            Key={"pk": {"S": key}}, ConsistentRead=True, ProjectionExpression="#pk",
            ExpressionAttributeNames={"#pk": "pk"}, ReturnConsumedCapacity="NONE") for key in ("run#" + probe.RUN, "policy")])
        self.assertEqual([c.kwargs for c in self.s3.get_object.call_args_list], [dict(Bucket=probe.BUCKET,
            Key=key, VersionId=version, ExpectedBucketOwner=probe.ACCOUNT) for key, version in
            ((probe.MANIFEST, probe.VERSION), (probe.FORBIDDEN, "TEST-ONLY-version"))])
        for client, operations in ((self.unsigned, ["assume_role_with_web_identity", "close"]),
                (self.signed, ["get_caller_identity", "close"]), (self.db, ["get_item", "get_item", "close"]),
                (self.s3, ["get_object", "get_object", "close"])):
            self.assertEqual([c[0] for c in client.method_calls], operations)
        self.assertTrue(self.body.closed)
        for value in (SECRET, token(), self.credentials["AccessKeyId"], RAW.decode()):
            self.assertNotIn(value, json.dumps(result))

    def test_invalid_approval_or_runtime_bindings_precede_token_and_clients(self):
        changes = [{key: "wrong"} for key in bindings() if key != "negative_version"]
        changes += [dict(not_before=NOW + 1), dict(expires_at=NOW + probe.SECONDS),
            dict(expires_at=NOW + 3601), dict(not_before=True), dict(manifest_bytes=True),
            dict(call_plan=probe.CALL_PLAN + ["lambda:InvokeFunction"]), dict(extra="unknown"),
            dict(schema="rcp-publication-read-approval-v1"), dict(negative_version="null"), dict(negative_version="bad?"), dict(negative_version="x" * 1025)]
        for change in changes:
            with self.subTest(change=change):
                self.assert_hold(self.run_probe({**bindings(), **change}), "configuration")
        self.reader.assert_not_called()
        self.factory.assert_not_called()

    def test_missing_key_binding_rejects_both_approval_versions_before_credentials(self):
        for schema in (probe.SCHEMA, "rcp-publication-read-approval-v1"):
            with self.subTest(schema=schema):
                config = bindings(schema=schema)
                del config["table_key_arn"]
                self.assert_hold(self.run_probe(config), "configuration")
        self.reader.assert_not_called()
        self.factory.assert_not_called()

    def test_direct_kms_client_is_rejected_before_sdk_or_credential_construction(self):
        with patch("botocore.session.Session", side_effect=AssertionError("must not construct SDK")) as sdk:
            with self.assertRaises(Exception):
                probe.sdk_client("kms", credentials=self.credentials)
            sdk.assert_not_called()

    def test_sts_denial_transient_or_malformed_result_holds_without_signed_calls(self):
        for failure in (error("AccessDenied", 403), error("ServiceUnavailable", 503), ValueError(SECRET)):
            self.factory.reset_mock()
            self.factory.side_effect = [self.unsigned]
            self.unsigned.assume_role_with_web_identity.side_effect = failure
            self.assert_hold(self.run_probe(), "sts-exchange")
            self.assertEqual(self.factory.call_count, 1)
        self.signed.get_caller_identity.assert_not_called()

    def test_every_authenticated_claim_is_exact_and_checked_after_only_one_sts(self):
        for key in claims():
            with self.subTest(claim=key):
                self.factory.reset_mock()
                self.factory.side_effect = [self.unsigned]
                self.reader.return_value = token(**{key: "wrong"})
                self.assert_hold(self.run_probe(), "signed-claims")
                self.assertEqual(self.factory.call_count, 1)
        self.db.get_item.assert_not_called()

    def test_stale_or_future_token_and_incorrect_role_or_credentials_hold(self):
        for change in (dict(exp=NOW), dict(iat=NOW + 61), dict(nbf=NOW + 61),
                dict(iat=NOW - 601), dict(exp=NOW + 601), dict(iat=True)):
            self.factory.side_effect = [self.unsigned]
            self.reader.return_value = token(**change)
            self.assert_hold(self.run_probe(), "signed-claims")
        self.reader.return_value = token()
        response = self.unsigned.assume_role_with_web_identity.return_value
        for path, value in (("Provider", "https://evil.invalid"), ("Audience", "wrong"),
                ("SubjectFromWebIdentityToken", "wrong"), ("AssumedRoleUser", {"Arn": self.arn, "AssumedRoleId": "wrong"}),
                ("Credentials", {**self.credentials, "Expiration": datetime.fromtimestamp(NOW - 1, timezone.utc)}),
                ("Credentials", {**self.credentials, "Expiration": datetime.fromtimestamp(NOW + 961, timezone.utc)}),
                ("Credentials", {**self.credentials, "SessionToken": ""})):
            self.factory.side_effect = [self.unsigned]
            self.unsigned.assume_role_with_web_identity.return_value = {**response, path: value}
            self.assert_hold(self.run_probe())
        self.signed.get_caller_identity.assert_not_called()

    def test_missing_positive_item_or_wrong_identity_holds_before_object_reads(self):
        self.signed.get_caller_identity.return_value["UserId"] = "wrong"
        self.assert_hold(self.run_probe(), "caller-identity")
        self.db.get_item.assert_not_called()
        self.factory.side_effect = [self.unsigned, self.signed, self.db, self.s3]
        self.signed.get_caller_identity.return_value["UserId"] = self.uid
        self.db.get_item.side_effect = [{}]
        self.assert_hold(self.run_probe(), "positive-item")
        self.s3.get_object.assert_not_called()

    def test_positive_manifest_requires_exact_version_size_and_bytes_and_closes_stream(self):
        for change in (dict(VersionId="other"), dict(ContentLength=len(RAW) + 1), dict(Body=io.BytesIO(b"wrong")),
                dict(Body=io.BytesIO(RAW + b"x"))):
            self.factory.side_effect = [self.unsigned, self.signed, self.db, self.s3]
            self.db.get_item.side_effect = [{"Item": {"pk": {"S": "run#" + probe.RUN}}}]
            body = io.BytesIO(RAW)
            response = dict(Body=body, VersionId=probe.VERSION, ContentLength=len(RAW))
            response.update(change)
            self.s3.get_object.side_effect = [response]
            self.assert_hold(self.run_probe(), "positive-object")
            self.assertTrue(response["Body"].closed)

    def test_negative_item_accepts_only_access_denied_never_missing_or_transient_or_allow(self):
        for result in ({}, {"Item": {"pk": {"S": "policy"}}}, error("ResourceNotFoundException", 400),
                error("AccessDeniedException", 403), error("InternalServerError", 500), ValueError(SECRET)):
            self.factory.side_effect = [self.unsigned, self.signed, self.db, self.s3]
            self.db.get_item.side_effect = [{"Item": {"pk": {"S": "run#" + probe.RUN}}}, result]
            self.s3.get_object.side_effect = [dict(Body=io.BytesIO(RAW), VersionId=probe.VERSION, ContentLength=len(RAW))]
            self.assert_hold(self.run_probe(), "negative-item")
        self.assertEqual(self.s3.get_object.call_count, 6)

    def test_negative_object_accepts_only_access_denied_never_missing_or_transient_or_allow(self):
        unexpected = Mock()
        for result in (dict(Body=unexpected), error("NoSuchKey", 404), error("NoSuchVersion", 404),
                error("AccessDenied", 400), error("SlowDown", 503), ValueError(SECRET)):
            self.factory.side_effect = [self.unsigned, self.signed, self.db, self.s3]
            self.db.get_item.side_effect = [{"Item": {"pk": {"S": "run#" + probe.RUN}}}, error("AccessDeniedException", 400)]
            self.s3.get_object.side_effect = [dict(Body=io.BytesIO(RAW), VersionId=probe.VERSION, ContentLength=len(RAW)), result]
            self.assert_hold(self.run_probe(), "negative-object")
        unexpected.read.assert_not_called()
        unexpected.close.assert_called_once_with()

    def test_expiry_timeout_and_cancellation_close_clients_and_body(self):
        def expired(**kwargs):
            self.now = NOW + 301
            return self.unsigned.assume_role_with_web_identity.return_value
        self.unsigned.assume_role_with_web_identity.side_effect = expired
        self.assert_hold(self.run_probe(), "sts-exchange")
        self.unsigned.close.assert_called_once_with()
        self.factory.side_effect = [self.unsigned, self.signed, self.db, self.s3]
        self.unsigned.assume_role_with_web_identity.side_effect = None
        self.now = NOW
        body = Mock()
        body.read.side_effect = Interrupted()
        self.s3.get_object.side_effect = [dict(Body=body, VersionId=probe.VERSION, ContentLength=len(RAW))]
        with self.assertRaises(Interrupted):
            self.run_probe()
        body.close.assert_called_once_with()
        for client in (self.signed, self.db, self.s3):
            client.close.assert_called_once_with()

    def test_inline_policy_does_not_synthesize_tested_resource_denials(self):
        policy = json.loads(probe.SESSION_POLICY)
        reads = ["dynamodb:GetItem", "s3:GetObjectVersion"]
        self.assertEqual(policy["Statement"][:2], [dict(Effect="Deny", NotAction=reads + ["kms:Decrypt"], Resource="*"),
            dict(Effect="Allow", Action=reads, Resource="*")])
        self.assertIn("base-policy-write-launch-invoke-and-secret-denials", probe.UNPROVEN)

    def test_sdk_ignores_ambient_credentials_config_proxies_and_endpoints(self):
        hostile = dict(AWS_PROFILE="must-not-load", AWS_CONFIG_FILE="/must-not-read-config",
            AWS_SHARED_CREDENTIALS_FILE="/must-not-read-credentials", AWS_CA_BUNDLE="/must-not-read-ca",
            AWS_DATA_PATH="/must-not-read-models", AWS_ENDPOINT_URL="http://evil.invalid",
            HTTPS_PROXY="http://evil.invalid", AWS_CSM_ENABLED="true")
        clients = []
        with patch.dict(os.environ, hostile), patch.object(Session, "get_credentials", side_effect=AssertionError("credentials")), \
                patch("botocore.configloader.raw_config_parse", wraps=raw_config_parse) as reads:
            clients = [probe.sdk_client("sts"), *[probe.sdk_client(service, credentials=self.credentials)
                for service in ("sts", "dynamodb", "s3")]]
        for client in clients:
            self.addCleanup(client.close)
            self.assertEqual(client.meta.config.proxies, {})
            self.assertEqual(client.meta.config.retries["total_max_attempts"], 1)
            self.assertEqual(client.meta.config.connect_timeout, 1)
            self.assertEqual(client.meta.config.read_timeout, 3)
        self.assertTrue(reads.call_args_list)
        self.assertTrue(all(call.args[0] == os.devnull for call in reads.call_args_list))
        self.assertEqual(clients[0].meta.config.signature_version, UNSIGNED)
        self.assertIsNone(clients[0]._request_signer._credentials)
        for client, service in zip(clients[1:], ("sts", "dynamodb", "s3")):
            self.assertEqual(client.meta.endpoint_url, "https://" + service + ".us-east-1.amazonaws.com")
            self.assertEqual(client._request_signer._credentials.access_key, self.credentials["AccessKeyId"])

    def test_actual_sdk_s3_errors_redirects_and_transients_send_only_once(self):
        for status, code, headers in ((403, "AccessDenied", {}), (404, "NoSuchVersion", {}),
                (503, "SlowDown", {}), (301, "PermanentRedirect", {"x-amz-bucket-region": "eu-west-1"}),
                (301, "PermanentRedirect", {})):
            with self.subTest(code=code, headers=headers):
                client = probe.sdk_client("s3", credentials=self.credentials)
                self.addCleanup(client.close)
                sent = []
                def send(request):
                    sent.append(request)
                    raw = Mock()
                    raw.stream.return_value = [("<Error><Code>" + code + "</Code><Message>" + SECRET + "</Message></Error>").encode()]
                    return AWSResponse(request.url, status, {"content-type": "application/xml", **headers}, raw)
                client._endpoint.http_session.send = send
                with self.assertRaises(Exception):
                    client.get_object(Bucket=probe.BUCKET, Key=probe.FORBIDDEN,
                        VersionId="TEST-ONLY-version", ExpectedBucketOwner=probe.ACCOUNT)
                self.assertEqual(len(sent), 1)
                self.assertTrue(sent[0].url.startswith("https://s3.us-east-1.amazonaws.com/" + probe.BUCKET + "/"))
                self.assertIn(self.credentials["AccessKeyId"].encode(), sent[0].headers["Authorization"])

    def test_actual_sdk_full_wire_round_trip_authenticates_fixture_and_sends_only_fixed_calls(self):
        sent = []
        def xml(action, fields):
            return ("<" + action + 'Response xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><' + action +
                "Result>" + fields + "</" + action + "Result><ResponseMetadata><RequestId>TEST-ONLY</RequestId>" +
                "</ResponseMetadata></" + action + "Response>").encode()
        def send(request):
            sent.append(request)
            raw = Mock()
            if request.url.startswith("https://sts."):
                form = parse_qs(request.body)
                action = form["Action"][0]
                if action == "AssumeRoleWithWebIdentity":
                    self.assertNotIn("Authorization", request.headers)
                    self.assertEqual(form["RoleArn"], [probe.ROLE])
                    self.assertEqual(form["RoleSessionName"], [self.name])
                    self.assertEqual(form["DurationSeconds"], ["900"])
                    self.assertEqual(form["Policy"], [probe.SESSION_POLICY])
                    # only the simulated STS verifies the locally signed token in this fixture
                    jwt.decode(form["WebIdentityToken"][0], SIGNER.public_key(), algorithms=["RS256"],
                        issuer=probe.oidc.auth.ISSUER, audience="sts.amazonaws.com",
                        options={"verify_exp": False, "verify_iat": False, "verify_nbf": False})
                    credentials = dict(self.credentials, Expiration=self.credentials["Expiration"].isoformat())
                    fields = "<Provider>" + probe.oidc.auth.ISSUER + "</Provider><Audience>sts.amazonaws.com</Audience>"
                    fields += "<SubjectFromWebIdentityToken>" + probe.SUBJECT + "</SubjectFromWebIdentityToken>"
                    fields += "<AssumedRoleUser><Arn>" + self.arn + "</Arn><AssumedRoleId>" + self.uid + "</AssumedRoleId></AssumedRoleUser>"
                    fields += "<Credentials>" + "".join("<" + key + ">" + escape(value) + "</" + key + ">"
                        for key, value in credentials.items()) + "</Credentials>"
                else:
                    self.assertEqual(action, "GetCallerIdentity")
                    fields = "<Account>381183037896</Account><Arn>" + self.arn + "</Arn><UserId>" + self.uid + "</UserId>"
                raw.stream.return_value = [xml(action, fields)]
                return AWSResponse(request.url, 200, {"content-type": "text/xml"}, raw)
            self.assertIn(self.credentials["AccessKeyId"].encode(), request.headers["Authorization"])
            self.assertEqual(request.headers["X-Amz-Security-Token"], self.credentials["SessionToken"].encode())
            if request.url == "https://dynamodb.us-east-1.amazonaws.com/":
                payload = json.loads(request.body)
                self.assertEqual(payload["TableName"], probe.TABLE)
                self.assertEqual(payload["ProjectionExpression"], "#pk")
                self.assertEqual(payload["ExpressionAttributeNames"], {"#pk": "pk"})
                self.assertTrue(payload["ConsistentRead"])
                self.assertEqual(request.headers["X-Amz-Target"], b"DynamoDB_20120810.GetItem")
                key = payload["Key"]["pk"]["S"]
                self.assertIn(key, {"run#" + probe.RUN, "policy"})
                response = {"Item": {"pk": {"S": key}}} if key != "policy" else {"__type": "AccessDeniedException", "message": SECRET}
                raw.stream.return_value = [json.dumps(response).encode()]
                return AWSResponse(request.url, 200 if key != "policy" else 400,
                    {"content-type": "application/x-amz-json-1.0"}, raw)
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.headers["x-amz-expected-bucket-owner"], probe.ACCOUNT.encode())
            if "/collection/manifest.json?" in request.url:
                self.assertEqual(request.url, "https://s3.us-east-1.amazonaws.com/" + probe.BUCKET + "/" + probe.MANIFEST + "?versionId=" + probe.VERSION)
                raw.read.return_value = RAW
                return AWSResponse(request.url, 200, {"content-length": str(len(RAW)), "x-amz-version-id": probe.VERSION}, raw)
            self.assertEqual(request.url, "https://s3.us-east-1.amazonaws.com/" + probe.BUCKET + "/" + probe.FORBIDDEN + "?versionId=TEST-ONLY-version")
            raw.stream.return_value = [("<Error><Code>AccessDenied</Code><Message>" + SECRET + "</Message></Error>").encode()]
            return AWSResponse(request.url, 403, {"content-type": "application/xml"}, raw)
        def factory(service, **kwargs):
            client = probe.sdk_client(service, **kwargs)
            client._endpoint.http_session.send = send
            return client
        result = self.run_probe(factory=factory)
        self.assertTrue(result["ok"], result)
        self.assertEqual(len(sent), 6)
        self.reader.assert_called_once_with()
        for value in (SECRET, token(), self.credentials["AccessKeyId"]):
            self.assertNotIn(value, json.dumps(result))

    def test_actual_sdk_forbids_unapproved_operations_before_transport(self):
        for service, operation in (("s3", lambda c: c.head_bucket(Bucket=probe.BUCKET)),
                ("dynamodb", lambda c: c.scan(TableName=probe.TABLE)),
                ("sts", lambda c: c.assume_role(RoleArn=probe.ROLE, RoleSessionName="TEST-ONLY"))):
            client = probe.sdk_client(service, credentials=self.credentials)
            self.addCleanup(client.close)
            sent = Mock()
            client._endpoint.http_session.send = sent
            with self.assertRaises(Exception):
                operation(client)
            sent.assert_not_called()

    def test_actual_sdk_wire_logs_never_contain_credentials_tokens_or_response_errors(self):
        stream = io.StringIO()
        logger = logging.getLogger("botocore")
        old = logger.level
        handler = logging.StreamHandler(stream)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        try:
            client = probe.sdk_client("dynamodb", credentials=self.credentials)
            self.addCleanup(client.close)
            raw = Mock()
            raw.stream.return_value = [json.dumps({"__type": "AccessDeniedException", "message": SECRET}).encode()]
            client._endpoint.http_session.send = lambda req: AWSResponse(req.url, 400,
                {"content-type": "application/x-amz-json-1.0"}, raw)
            probe.denied(lambda: probe.get_item(client, "policy"), "AccessDeniedException")
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old)
        for value in (SECRET, self.credentials["AccessKeyId"], token()):
            self.assertNotIn(value, stream.getvalue())


if __name__ == "__main__":
    unittest.main()
