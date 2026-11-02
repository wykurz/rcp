"""Offline signed fixtures and STS wire contracts; never evidence of live trust policy."""

import copy
from datetime import datetime, timezone
import io
import json
import logging
import os
import stat
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs
from xml.sax.saxutils import escape

from botocore import UNSIGNED
from botocore.awsrequest import AWSResponse
from botocore.configloader import raw_config_parse
from botocore.exceptions import ClientError
from botocore.session import Session
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from infra.aws_benchmark import acceptance_oidc as probe
from infra.aws_benchmark import production_auth as auth
from infra.aws_benchmark.test_production_auth import fixture_claims, fixture_jwk

NOW = 1800000000
ROLE = "arn:aws:iam::381183037896:role/path/TEST-ONLY-oidc"
ROLE_ID = "AROA" + "A" * 17
WORKFLOW = "wykurz/rcp/.github/workflows/TEST-ONLY-existing-lane.yml@refs/heads/main"
AUDIENCE = "rcp-benchmark-production-admission"
SUBJECT = "repo:wykurz/rcp:ref:refs/heads/main"
SESSION = "rcp-acceptance-10001-1"
ARN = "arn:aws:sts::381183037896:assumed-role/TEST-ONLY-oidc/" + SESSION
USER_ID = ROLE_ID + ":" + SESSION
SENTINEL = "TEST-ONLY-DO-NOT-LOG"
CREDENTIALS = dict(AccessKeyId="ASIA" + "B" * 16, SecretAccessKey=SENTINEL,
                   SessionToken=SENTINEL + "-session",
                   Expiration=datetime.fromtimestamp(NOW + 900, timezone.utc))


def config(**updates):
    value = dict(schema=probe.SCHEMA, account="381183037896", region="us-east-1",
        source_commit="a" * 40, role_arn=ROLE, role_id=ROLE_ID, admission_audience=AUDIENCE,
        workflow_ref=WORKFLOW, run_id="10001", run_attempt="1", actor="test-owner",
        actor_id="9876", event_name="workflow_dispatch", not_before=NOW - 10,
        expires_at=NOW + 900, approval_capture_sha256="c" * 64)
    value.update(updates)
    return value


def claims(**updates):
    value = fixture_claims(repository="wykurz/rcp", repository_id="655073755",
        repository_owner="wykurz", repository_owner_id="483465", sub=SUBJECT,
        workflow_ref=WORKFLOW)
    value.update(updates)
    return value


def sts_response():
    return dict(Provider=auth.ISSUER, Audience="sts.amazonaws.com",
        SubjectFromWebIdentityToken=SUBJECT, Credentials=copy.deepcopy(CREDENTIALS),
        AssumedRoleUser=dict(Arn=ARN, AssumedRoleId=USER_ID))


class AcceptanceOidcTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.signer = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.other_signer = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.jwks = json.dumps({"keys": [fixture_jwk(cls.signer)]}).encode()

    def setUp(self):
        self.now = NOW
        self.elapsed = 0
        self.fetch = Mock(return_value=self.jwks)
        self.unsigned = Mock()
        self.unsigned.assume_role_with_web_identity.return_value = sts_response()
        self.signed = Mock()
        self.signed.get_caller_identity.return_value = dict(Account="381183037896", Arn=ARN, UserId=USER_ID)
        self.factory = Mock(side_effect=[self.unsigned, self.signed])
        self.tokens = dict(admission_token=self.token(), sts_token=self.token(aud="sts.amazonaws.com"))
        self.reader = Mock(side_effect=lambda: dict(self.tokens))
        network = patch("socket.create_connection", side_effect=AssertionError("unexpected network"))
        network.start()
        self.addCleanup(network.stop)

    def token(self, *, signer=None, **updates):
        return jwt.encode(claims(**updates), signer or self.signer,
                          algorithm="RS256", headers={"kid": "test-key"})

    def run_probe(self, configuration=None, **updates):
        arguments = dict(factory=self.factory, jwks_fetcher=self.fetch,
                         clock=lambda: self.now, monotonic=lambda: self.elapsed)
        arguments.update(updates)
        return probe.run_probe(config() if configuration is None else configuration, self.reader, **arguments)

    def assert_hold(self, result):
        self.assertFalse(result["ok"])
        self.assertFalse(result["live_accepted"])
        self.assertEqual(result, probe._hold())
        encoded = json.dumps(result)
        for secret in [SENTINEL, *self.tokens.values(), CREDENTIALS["AccessKeyId"]]:
            self.assertNotIn(secret, encoded)

    def test_verified_main_identity_is_bound_to_two_distinct_audiences_and_exact_role(self):
        result = self.run_probe()
        self.assertTrue(result["ok"])
        self.assertFalse(result["live_accepted"])
        self.assertEqual(result["unproven"], probe.UNPROVEN)
        self.assertEqual(result["identity"], {name: claims()[name] for name in probe.IDENTITY_FIELDS})
        self.assertEqual(set(result["cases"]), {"signed_admission_main", "sts_same_run_main", "exact_role_identity"})
        self.unsigned.assume_role_with_web_identity.assert_called_once_with(RoleArn=ROLE,
            RoleSessionName=SESSION, WebIdentityToken=self.tokens["sts_token"],
            DurationSeconds=900, Policy=probe.SESSION_POLICY)
        self.assertEqual(json.loads(probe.SESSION_POLICY)["Statement"],
                         [{"Effect": "Deny", "Action": "*", "Resource": "*"}])
        self.factory.assert_any_call(credentials=CREDENTIALS)
        self.signed.get_caller_identity.assert_called_once_with()
        self.unsigned.close.assert_called_once_with()
        self.signed.close.assert_called_once_with()
        self.assertNotIn("jti", result["identity"])
        for token in self.tokens.values():
            self.assertNotIn(token, json.dumps(result))
        self.assertNotIn(SENTINEL, json.dumps(result))

    def test_invalid_or_expired_bindings_fail_before_reading_tokens_or_creating_clients(self):
        invalid = [dict(account="000000000000"), dict(region="us-west-2"), dict(source_commit="main"),
            dict(role_arn=ROLE.replace("381183037896", "000000000000")), dict(role_id="name"),
            dict(admission_audience="sts.amazonaws.com"), dict(workflow_ref=WORKFLOW.replace("main", "feature")),
            dict(run_id="0"), dict(run_attempt="2"), dict(actor=""), dict(actor_id=9876),
            dict(event_name="pull_request"), dict(not_before=NOW + 1), dict(expires_at=NOW + 45),
            dict(expires_at=NOW + 3601), dict(not_before=True), dict(approval_capture_sha256=""),
            dict(extra="field")]
        for update in invalid:
            with self.subTest(field=list(update)):
                self.assert_hold(self.run_probe(config(**update)))
        self.reader.assert_not_called()
        self.factory.assert_not_called()
        self.fetch.assert_not_called()

    def test_bad_signed_admission_claims_and_forged_signature_never_reach_sts(self):
        invalid = [dict(ref="refs/heads/feature"), dict(ref="refs/pull/1/merge"),
            dict(repository="other/repo"), dict(repository_id="2"), dict(repository_owner_id="2"),
            dict(workflow_ref=WORKFLOW.replace("existing", "other")), dict(workflow_sha="b" * 40),
            dict(sha="b" * 40, workflow_sha="b" * 40), dict(run_id="10002"), dict(run_attempt="2"),
            dict(actor="other"), dict(actor_id="2"), dict(event_name="schedule"), dict(sub="other"),
            dict(exp=NOW), dict(aud="sts.amazonaws.com"), dict(signer=self.other_signer)]
        for update in invalid:
            with self.subTest(field=list(update)):
                self.tokens["admission_token"] = self.token(**update)
                self.assert_hold(self.run_probe())
        self.factory.assert_not_called()

    def test_sts_claims_are_not_parsed_when_exchange_fails_and_denial_is_not_acceptance(self):
        for code in ("AccessDenied", "InvalidIdentityToken", "ExpiredToken", "IDPCommunicationError"):
            self.factory = Mock(return_value=self.unsigned)
            self.unsigned.assume_role_with_web_identity.side_effect = ClientError(
                {"Error": {"Code": code, "Message": SENTINEL + self.tokens["sts_token"]},
                 "ResponseMetadata": {"HTTPStatusCode": 403}}, "AssumeRoleWithWebIdentity")
            with patch.object(probe, "_authenticated_claims", wraps=probe._authenticated_claims) as parse:
                self.assert_hold(self.run_probe())
                self.assertEqual(parse.call_count, 1)
                self.assertEqual(parse.call_args.args[0], self.tokens["admission_token"])
            self.assertEqual(self.factory.call_count, 1)
        self.assertEqual(self.unsigned.assume_role_with_web_identity.call_count, 4)
        self.signed.get_caller_identity.assert_not_called()

    def test_mismatched_sts_claims_stop_before_using_returned_credentials(self):
        invalid = [dict(repository="other/repo"), dict(repository_id="1"), dict(repository_owner="other"),
            dict(repository_owner_id="1"), dict(ref="refs/heads/feature"), dict(workflow_ref=WORKFLOW + "x"),
            dict(workflow_sha="b" * 40), dict(sha="b" * 40), dict(run_id="10002"), dict(run_attempt="2"),
            dict(actor="other"), dict(actor_id="2"), dict(event_name="schedule"), dict(iss="https://other.invalid"),
            dict(aud=AUDIENCE), dict(sub="other"), dict(exp=NOW), dict(iat=NOW + 61),
            dict(nbf=NOW + 61), dict(exp=NOW + 601), dict(exp=True)]
        for update in invalid:
            with self.subTest(field=list(update)):
                self.factory = Mock(return_value=self.unsigned)
                self.tokens["sts_token"] = self.token(**dict({"aud": "sts.amazonaws.com", "jti": "second-token"}, **update))
                self.assert_hold(self.run_probe())
                self.assertEqual(self.factory.call_count, 1)
        self.signed.get_caller_identity.assert_not_called()

    def test_provider_subject_role_and_credential_readbacks_are_exact(self):
        responses = [dict(Provider="https://other.invalid"), dict(Audience="other"),
            dict(SubjectFromWebIdentityToken="other"),
            dict(AssumedRoleUser=dict(Arn=ARN, AssumedRoleId="AROA" + "B" * 17 + ":" + SESSION)),
            dict(AssumedRoleUser=dict(Arn=ARN + "other", AssumedRoleId=USER_ID))]
        for field, bad in (("AccessKeyId", ""), ("SessionToken", None),
                           ("Expiration", datetime.fromtimestamp(NOW + 3600, timezone.utc)),
                           ("Expiration", datetime.fromtimestamp(NOW - 1, timezone.utc)),
                           ("Expiration", datetime.fromtimestamp(NOW + 900))):
            responses.append(dict(Credentials=dict(CREDENTIALS, **{field: bad})))
        for update in responses:
            with self.subTest(field=list(update)):
                self.factory = Mock(return_value=self.unsigned)
                self.unsigned.assume_role_with_web_identity.return_value = dict(sts_response(), **update)
                self.assert_hold(self.run_probe())
                self.assertEqual(self.factory.call_count, 1)
        self.signed.get_caller_identity.assert_not_called()

    def test_caller_identity_must_match_account_role_id_and_exact_session(self):
        for update in (dict(Account="000000000000"), dict(Arn=ARN + "other"), dict(UserId=USER_ID + "other")):
            self.factory = Mock(side_effect=[self.unsigned, self.signed])
            self.signed.get_caller_identity.return_value = dict(dict(Account="381183037896", Arn=ARN, UserId=USER_ID), **update)
            self.assert_hold(self.run_probe())
        self.assertEqual(self.signed.get_caller_identity.call_count, 3)

    def test_late_key_response_or_sts_response_stops_following_operations(self):
        def late_jwks():
            self.elapsed = 45
            return self.jwks
        self.assert_hold(self.run_probe(jwks_fetcher=late_jwks))
        self.factory.assert_not_called()
        self.elapsed = 0
        def late_sts(**kwargs):
            self.elapsed = 45
            return sts_response()
        self.unsigned.assume_role_with_web_identity.side_effect = late_sts
        self.assert_hold(self.run_probe())
        self.assertEqual(self.factory.call_count, 1)
        self.unsigned.close.assert_called_once_with()
        self.signed.get_caller_identity.assert_not_called()

    def test_token_expiry_during_sts_call_stops_credential_use(self):
        def expiring_sts(**kwargs):
            self.now = NOW + 300
            return sts_response()
        self.unsigned.assume_role_with_web_identity.side_effect = expiring_sts
        self.assert_hold(self.run_probe())
        self.assertEqual(self.factory.call_count, 1)
        self.signed.get_caller_identity.assert_not_called()

    def test_input_and_adapter_exceptions_never_echo_secrets(self):
        for tokens in ({}, {"admission_token": "x" * (auth.MAX_TOKEN_BYTES + 1), "sts_token": "x"},
                       {"admission_token": "same", "sts_token": "same"}):
            self.reader.side_effect = None
            self.reader.return_value = tokens
            self.assert_hold(self.run_probe())
        self.reader.side_effect = RuntimeError(SENTINEL)
        self.assert_hold(self.run_probe())
        self.factory.assert_not_called()

    def test_bounded_stdin_accepts_only_an_ephemeral_envelope_and_no_arguments(self):
        envelope = dict(config=config(), **self.tokens)
        def runner(configuration, token_reader):
            self.assertEqual(configuration, config())
            self.assertEqual(token_reader(), self.tokens)
            return dict(ok=True, live_accepted=False)
        output = io.StringIO()
        self.assertEqual(probe.main([], stream=io.BytesIO(json.dumps(envelope).encode()),
                                    output=output, runner=runner), 0)
        self.assertNotIn(SENTINEL, output.getvalue())
        for raw in (b"", b"{" + SENTINEL.encode(), b"x" * (probe.MAX_INPUT + 1),
                    b'{"config":{},"config":{}}', b"[]",
                    json.dumps(dict(envelope, extra="field")).encode()):
            output = io.StringIO()
            unused = Mock()
            self.assertEqual(probe.main([], stream=io.BytesIO(raw), output=output, runner=unused), 1)
            self.assertEqual(json.loads(output.getvalue()), probe._hold())
            unused.assert_not_called()
        unused = Mock()
        stream = Mock()
        self.assertEqual(probe.main([SENTINEL], stream=stream, output=io.StringIO(), runner=unused), 1)
        stream.read.assert_not_called()
        unused.assert_not_called()
        stream.isatty.return_value = True
        self.assertEqual(probe.main([], stream=stream, output=io.StringIO(), runner=unused), 1)
        stream.read.assert_not_called()

    def test_live_entrypoint_rejects_redirected_files_before_reading_tokens(self):
        stdin = Mock()
        stdin.fileno.return_value = 0
        output = io.StringIO()
        runner = Mock()
        with patch.object(probe.sys, "stdin", stdin), patch.object(probe.os, "fstat", return_value=Mock(st_mode=stat.S_IFREG)):
            self.assertEqual(probe.main([], output=output, runner=runner), 1)
        self.assertEqual(json.loads(output.getvalue()), probe._hold())
        stdin.buffer.read.assert_not_called()
        runner.assert_not_called()

    def test_sdk_clients_ignore_credentials_profiles_proxies_and_endpoints(self):
        env = {"AWS_PROFILE": "must-not-load", "AWS_CONFIG_FILE": "/must-not-read-config",
            "AWS_SHARED_CREDENTIALS_FILE": "/must-not-read-credentials", "AWS_CA_BUNDLE": "/must-not-read-ca",
            "AWS_DATA_PATH": "/must-not-read-models",
            "AWS_ENDPOINT_URL": "http://attacker.invalid", "AWS_ENDPOINT_URL_STS": "http://attacker.invalid",
            "HTTPS_PROXY": "http://attacker.invalid", "AWS_CSM_ENABLED": "true"}
        with patch.dict(os.environ, env), patch.object(Session, "get_credentials", side_effect=AssertionError("credential lookup")), \
                patch("botocore.configloader.raw_config_parse", wraps=raw_config_parse) as config_reads:
            unsigned = probe.sdk_sts()
            signed = probe.sdk_sts(credentials=CREDENTIALS)
        self.addCleanup(unsigned.close)
        self.addCleanup(signed.close)
        self.assertTrue(config_reads.call_args_list)
        self.assertTrue(all(call.args[0] == os.devnull for call in config_reads.call_args_list))
        self.assertEqual(unsigned.meta.config.signature_version, UNSIGNED)
        self.assertIsNone(unsigned._request_signer._credentials)
        self.assertEqual(signed._request_signer._credentials.access_key, CREDENTIALS["AccessKeyId"])
        for client in (unsigned, signed):
            self.assertEqual(client.meta.endpoint_url, "https://sts.us-east-1.amazonaws.com")
            self.assertEqual(client.meta.region_name, "us-east-1")
            self.assertEqual(client.meta.config.retries, {"total_max_attempts": 1, "mode": "legacy"})
            self.assertEqual(client.meta.config.proxies, {})
            self.assertEqual(client.meta.config.connect_timeout, 1)
            self.assertEqual(client.meta.config.read_timeout, 3)

    def test_real_sdk_wire_round_trip_is_unsigned_then_session_signed_and_redacted(self):
        sent = []
        def response(action, fields):
            body = f'<{action}Response xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
            body += f'<{action}Result>{fields}</{action}Result>'
            body += '<ResponseMetadata><RequestId>test-request</RequestId></ResponseMetadata>'
            return (body + f'</{action}Response>').encode()
        def send(request):
            sent.append(request)
            self.assertEqual(request.url, "https://sts.us-east-1.amazonaws.com/")
            args = parse_qs(request.body)
            action = args["Action"][0]
            if action == "AssumeRoleWithWebIdentity":
                self.assertNotIn("Authorization", request.headers)
                self.assertNotIn("X-Amz-Security-Token", request.headers)
                self.assertEqual(args["RoleArn"], [ROLE])
                self.assertEqual(args["DurationSeconds"], ["900"])
                self.assertEqual(args["Policy"], [probe.SESSION_POLICY])
                token = args["WebIdentityToken"][0]
                self.assertEqual(token, self.tokens["sts_token"])
                # the simulated provider verifies this locally signed fixture before success
                jwt.decode(token, self.signer.public_key(), algorithms=["RS256"], issuer=auth.ISSUER,
                    audience="sts.amazonaws.com", options={"verify_exp": False, "verify_iat": False, "verify_nbf": False})
                fields = '<Provider>' + auth.ISSUER + '</Provider><Audience>sts.amazonaws.com</Audience>'
                fields += '<SubjectFromWebIdentityToken>' + SUBJECT + '</SubjectFromWebIdentityToken>'
                fields += '<AssumedRoleUser><Arn>' + ARN + '</Arn><AssumedRoleId>' + USER_ID + '</AssumedRoleId></AssumedRoleUser>'
                fields += '<Credentials>' + ''.join('<' + key + '>' + escape(value) + '</' + key + '>'
                    for key, value in dict(CREDENTIALS, Expiration=CREDENTIALS["Expiration"].isoformat()).items()) + '</Credentials>'
            else:
                self.assertEqual(action, "GetCallerIdentity")
                self.assertIn(CREDENTIALS["AccessKeyId"].encode(), request.headers["Authorization"])
                self.assertEqual(request.headers["X-Amz-Security-Token"], CREDENTIALS["SessionToken"].encode())
                fields = '<Account>381183037896</Account><Arn>' + ARN + '</Arn><UserId>' + USER_ID + '</UserId>'
            raw = Mock()
            raw.stream.return_value = [response(action, fields)]
            return AWSResponse(request.url, 200, {"content-type": "text/xml"}, raw)
        def factory(**kwargs):
            client = probe.sdk_sts(**kwargs)
            client._endpoint.http_session.send = send
            return client
        logs = io.StringIO()
        handler = logging.StreamHandler(logs)
        logger = logging.getLogger("botocore")
        old_level = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        try:
            result = self.run_probe(factory=factory)
        finally:
            logger.removeHandler(handler)
            logger.setLevel(old_level)
        self.assertTrue(result["ok"], result)
        self.assertEqual(len(sent), 2)
        captured = logs.getvalue() + json.dumps(result)
        for secret in (SENTINEL, *self.tokens.values(), CREDENTIALS["AccessKeyId"]):
            self.assertNotIn(secret, captured)


    def test_sdk_wire_failures_are_not_retried_or_treated_as_negative_evidence(self):
        for status, code in ((403, "AccessDenied"), (503, "ServiceUnavailable")):
            clients = []
            sends = []
            def send(request):
                sends.append(request)
                raw = Mock()
                raw.stream.return_value = [('<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
                    '<Error><Type>Sender</Type><Code>' + code + '</Code><Message>' +
                    escape(SENTINEL + self.tokens["sts_token"]) + '</Message></Error>'
                    '<RequestId>test-request</RequestId></ErrorResponse>').encode()]
                return AWSResponse(request.url, status, {"content-type": "text/xml"}, raw)
            def factory(**kwargs):
                client = probe.sdk_sts(**kwargs)
                clients.append(client)
                client._endpoint.http_session.send = send
                return client
            logs = io.StringIO()
            handler = logging.StreamHandler(logs)
            logger = logging.getLogger("botocore")
            old_level = logger.level
            logger.setLevel(logging.DEBUG)
            logger.addHandler(handler)
            try:
                result = self.run_probe(factory=factory)
            finally:
                logger.removeHandler(handler)
                logger.setLevel(old_level)
            self.assert_hold(result)
            self.assertEqual(len(clients), 1)
            self.assertEqual(len(sends), 1)
            for secret in (SENTINEL, *self.tokens.values()):
                self.assertNotIn(secret, logs.getvalue())


if __name__ == "__main__":
    unittest.main()
