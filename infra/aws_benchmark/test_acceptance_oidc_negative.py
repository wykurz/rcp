"""Offline signed negative-context, exact-denial and session-recovery tests."""
from dataclasses import replace
from datetime import datetime, timezone
import copy
import json
import unittest
from unittest.mock import Mock, patch

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from botocore.exceptions import ClientError

from . import acceptance_oidc_negative as probe
from . import acceptance_oidc_negative_workflow as workflow

NOW = 1800000000
SHA = "a" * 40
RUN = "12345678"
REQUEST_ID = "11111111-1111-4111-8111-111111111111"
SECRET = "must-never-emit-token-credential-or-service-body"


def config(**updates):
    return dict(schema=probe.SCHEMA, account="381183037896", region="us-east-1",
        source_commit=SHA, role_arn=probe.ROLE, role_id=probe.ROLE_ID,
        workflow_ref=probe.WORKFLOW, workflow_name=probe.NAME, run_id=RUN, run_attempt="1",
        actor="wykurz", actor_id="483465", event_name="workflow_dispatch",
        not_before=NOW - 60, expires_at=NOW + 300, recovery_cutoff=NOW + 3600,
        approval_capture_sha256="b" * 64) | updates


def claims(**updates):
    return dict(iss=probe.auth.ISSUER, aud="sts.amazonaws.com",
        sub="repo:wykurz/rcp:ref:refs/heads/main", exp=NOW + 300, iat=NOW, nbf=NOW - 300,
        jti="test-only-jti", repository="wykurz/rcp", repository_id="655073755",
        repository_owner="wykurz", repository_owner_id="483465", ref="refs/heads/main",
        workflow_ref=probe.WORKFLOW, workflow=probe.NAME, workflow_sha=SHA, sha=SHA,
        run_id=RUN, run_attempt="1", actor="wykurz", actor_id="483465",
        event_name="workflow_dispatch") | updates


def denial(code="AccessDenied", status=403, operation="AssumeRoleWithWebIdentity", request=REQUEST_ID):
    return ClientError({"Error": {"Code": code, "Message": SECRET},
        "ResponseMetadata": {"HTTPStatusCode": status, "RequestId": request}}, operation)


def session_response():
    session = "rcp-oidc-negative-" + RUN + "-1"
    arn = "arn:aws:sts::381183037896:assumed-role/" + probe.ROLE.rsplit("/", 1)[1] + "/" + session
    return dict(Credentials=dict(AccessKeyId=SECRET, SecretAccessKey=SECRET, SessionToken=SECRET,
        Expiration=datetime.fromtimestamp(NOW + 900, timezone.utc)),
        AssumedRoleUser=dict(Arn=arn, AssumedRoleId=probe.ROLE_ID + ":" + session),
        ResponseMetadata=dict(HTTPStatusCode=200, RequestId=REQUEST_ID))


class NegativeCoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.signer = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(cls.signer.public_key()))
        jwk.update(kid="test-key", use="sig", alg="RS256")
        cls.jwks = json.dumps({"keys": [jwk]}).encode()

    def setUp(self):
        self.network = patch("socket.create_connection", side_effect=AssertionError("unexpected network"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def token(self, updates=None, signer=None):
        return jwt.encode(claims(**(updates or {})), signer or self.signer,
            algorithm="RS256", headers={"kid": "test-key"})

    def run_case(self, *, token=None, cfg=None, client=None, evidence=None, clock=None, progress=None):
        client = client or Mock()
        if client.assume_role_with_web_identity.side_effect is None:
            client.assume_role_with_web_identity.side_effect = denial()
        factory = Mock(return_value=client)
        observations = []
        record = evidence if evidence is not None else lambda value: observations.append(copy.deepcopy(value))
        result = probe.run_probe(cfg or config(), lambda: token or self.token(),
            evidence=record, factory=factory, jwks_fetcher=lambda: self.jwks,
            clock=clock or (lambda: NOW), monotonic=lambda: 1, progress=progress)
        return result, client, factory, observations

    def test_signed_negative_context_requires_exact_denial_and_one_send(self):
        result, client, factory, evidence = self.run_case()
        self.assertTrue(result["ok"])
        self.assertFalse(result["live_accepted"])
        self.assertEqual(result["diagnostics"]["request_id"], REQUEST_ID)
        self.assertFalse(result["diagnostics"]["recovery"]["possible_session"])
        self.assertIsNone(result["diagnostics"]["recovery"]["reconcile_until"])
        self.assertEqual([item["event"] for item in evidence], ["before-sts-send", "denied"])
        self.assertTrue(evidence[0]["diagnostics"]["recovery"]["possible_session"])
        self.assertEqual(client.assume_role_with_web_identity.call_count, 1)
        request = client.assume_role_with_web_identity.call_args.kwargs
        self.assertEqual(request["RoleArn"], probe.ROLE)
        self.assertEqual(request["DurationSeconds"], 900)
        self.assertEqual(request["Policy"], probe.positive.SESSION_POLICY)
        factory.assert_called_once_with()
        client.close.assert_called_once_with()
        self.assertNotIn(SECRET, json.dumps(result))

    def test_invalid_signatures_never_construct_sts_client(self):
        result, client, factory, _ = self.run_case(token=self.token(signer=self.other))
        self.assertFalse(result["ok"])
        factory.assert_not_called()
        client.assume_role_with_web_identity.assert_not_called()

    def test_wrong_signed_context_is_not_a_live_negative(self):
        for field, value in dict(workflow="AWS production benchmark",
                actor_id="999", run_attempt="2", event_name="push", run_id="999",
                aud="other", ref="refs/heads/other", sha="c" * 40).items():
            with self.subTest(field=field):
                result, _, factory, _ = self.run_case(token=self.token({field: value}))
                self.assertFalse(result["ok"])
                factory.assert_not_called()

    def test_bad_config_holds_before_any_token_or_client(self):
        for field, value in dict(role_arn="root", role_id="AROA" + "Z" * 17,
                source_commit="main", run_attempt="2", recovery_cutoff=NOW + 500,
                expires_at=NOW + 10000, not_before=True, extra=SECRET).items():
            cfg = config() | {field: value}
            reader, factory, evidence = Mock(), Mock(), Mock()
            result = probe.run_probe(cfg, reader, evidence=evidence, factory=factory,
                clock=lambda: NOW, monotonic=lambda: 0)
            with self.subTest(field=field):
                self.assertFalse(result["ok"])
                reader.assert_not_called()
                factory.assert_not_called()
                evidence.assert_not_called()

    def test_other_errors_missing_request_id_and_wrong_operation_hold(self):
        cases = [denial("InvalidIdentityToken", 400), denial("ExpiredToken", 400),
            denial("IDPRejectedClaim", 403), denial("Throttling", 400),
            denial(status=503), denial(operation="GetCallerIdentity"), denial(request=None),
            denial(request=SECRET)]
        for error in cases:
            client = Mock()
            client.assume_role_with_web_identity.side_effect = error
            result, _, _, _ = self.run_case(client=client)
            with self.subTest(code=error.response["Error"]["Code"]):
                self.assertFalse(result["ok"])
                self.assertTrue(result["diagnostics"]["recovery"]["possible_session"])
                self.assertEqual(client.assume_role_with_web_identity.call_count, 1)
                self.assertNotIn(SECRET, json.dumps(result))

    def test_transport_loss_preserves_conservative_horizon_without_retry(self):
        client = Mock()
        client.assume_role_with_web_identity.side_effect = TimeoutError(SECRET)
        result, _, _, evidence = self.run_case(client=client)
        recovery = result["diagnostics"]["recovery"]
        self.assertFalse(result["ok"])
        self.assertTrue(recovery["possible_session"])
        self.assertEqual(recovery["reconcile_until"], NOW + 300 + 300 + 900 + 60)
        self.assertEqual(client.assume_role_with_web_identity.call_count, 1)
        self.assertEqual([v["event"] for v in evidence], ["before-sts-send"])

    def test_unexpected_issuance_retains_only_safe_actual_metadata(self):
        client = Mock()
        client.assume_role_with_web_identity.side_effect = lambda **kwargs: session_response()
        result, _, factory, evidence = self.run_case(client=client)
        recovery = result["diagnostics"]["recovery"]
        self.assertFalse(result["ok"])
        self.assertEqual(recovery["session_expires_at"], NOW + 900)
        self.assertEqual(recovery["session_identity"], "expected")
        self.assertGreaterEqual(recovery["reconcile_until"], NOW + 900 + 60)
        self.assertEqual(evidence[-1]["event"], "unexpected-session-response")
        factory.assert_called_once_with()
        self.assertNotIn(SECRET, json.dumps(result) + json.dumps(evidence))

    def test_unexpected_metadata_captured_before_post_response_clock_expiry(self):
        tick = [NOW]
        client = Mock()
        def issued(**request):
            tick[0] = NOW + 4000
            return session_response()
        client.assume_role_with_web_identity.side_effect = issued
        result, _, _, _ = self.run_case(client=client, clock=lambda: tick[0])
        self.assertFalse(result["ok"])
        self.assertEqual(result["diagnostics"]["recovery"]["session_expires_at"], NOW + 900)

    def test_cancellation_after_send_leaves_conservative_state_and_closes_client(self):
        progress = probe.Progress()
        client = Mock()
        client.assume_role_with_web_identity.side_effect = workflow.Interrupted()
        with self.assertRaises(workflow.Interrupted):
            self.run_case(client=client, progress=progress)
        self.assertTrue(progress.recovery.possible_session)
        self.assertEqual(progress.recovery.reconcile_until, NOW + 1560)
        client.close.assert_called_once_with()

    def test_before_send_journal_failure_prevents_send(self):
        result, client, _, _ = self.run_case(evidence=Mock(side_effect=OSError(SECRET)))
        self.assertFalse(result["ok"])
        client.assume_role_with_web_identity.assert_not_called()
        client.close.assert_called_once_with()
        self.assertTrue(result["diagnostics"]["recovery"]["possible_session"])

    def test_after_denial_journal_failure_holds_without_imaginary_issuance(self):
        count = [0]
        def evidence(value):
            count[0] += 1
            if count[0] == 2:
                raise OSError(SECRET)
        result, client, _, _ = self.run_case(evidence=evidence)
        self.assertFalse(result["ok"])
        self.assertFalse(result["diagnostics"]["recovery"]["possible_session"])
        self.assertEqual(client.assume_role_with_web_identity.call_count, 1)

    def test_unexpected_malformed_metadata_never_echoed_or_trusted(self):
        for value in [None, SECRET, {}, dict(Credentials=SECRET),
                dict(Credentials=dict(Expiration=datetime.fromtimestamp(NOW + 900)))]:
            client = Mock()
            client.assume_role_with_web_identity.side_effect = lambda **kw: value
            result, _, _, _ = self.run_case(client=client)
            self.assertFalse(result["ok"])
            self.assertIsNone(result["diagnostics"]["recovery"]["session_expires_at"])
            self.assertNotIn(SECRET, json.dumps(result))

    def test_long_observed_expiration_extends_horizon_and_flags_missing_coverage(self):
        client = Mock()
        response = session_response()
        response["Credentials"]["Expiration"] = datetime.fromtimestamp(NOW + 50000, timezone.utc)
        client.assume_role_with_web_identity.side_effect = lambda **kw: response
        result, _, _, _ = self.run_case(client=client)
        recovery = result["diagnostics"]["recovery"]
        self.assertFalse(result["ok"])
        self.assertEqual(recovery["session_expires_at"], NOW + 50000)
        self.assertEqual(recovery["reconcile_until"], NOW + 50060)
        self.assertTrue(recovery["coverage_exceeded"])

    def test_untrusted_progress_labels_never_enter_output(self):
        progress = probe.Progress(stage=SECRET, error_code=SECRET, request_id=SECRET,
            recovery=probe.Recovery(True, True, -1, None, None, SECRET))
        output = json.dumps(probe._hold(progress))
        self.assertNotIn(SECRET, output)
        self.assertIn("unknown", output)


if __name__ == "__main__":
    unittest.main()
