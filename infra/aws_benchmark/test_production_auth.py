"""Signed admission identity tests; all RSA keys and claims are local fixtures."""

import base64
from dataclasses import FrozenInstanceError
import json
import ssl
import http.client
from pathlib import Path
import traceback
import unittest
from unittest.mock import Mock, patch

import jwt
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from infra.aws_benchmark import production_auth as auth


REPOSITORY = "example/project"
WORKFLOW = REPOSITORY + "/.github/workflows/aws-benchmark-production.yml@refs/heads/main"
COLLECT = REPOSITORY + "/.github/workflows/aws-benchmark-collect.yml@refs/heads/main"
AUDIENCE = "rcp-benchmark-production-admission"
NOW = 1800000000


def encoded(value):
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def fixture_claims(**updates):
    claims = dict(iss=auth.ISSUER, aud=AUDIENCE, sub="repo:example/project:ref:refs/heads/main",
        exp=NOW + 300, iat=NOW, nbf=NOW - 300, jti="test-only-jti", repository=REPOSITORY,
        repository_id="1234", repository_owner="example", repository_owner_id="5678",
        ref=auth.MAIN_REF, workflow_ref=WORKFLOW, workflow_sha="a" * 40, sha="a" * 40,
        run_id="10001", run_attempt="1", actor="test-owner", actor_id="9876",
        event_name="workflow_dispatch")
    claims.update(updates)
    return claims


def fixture_jwk(key, kid="test-key"):
    result = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    result.update(kid=kid, use="sig", alg="RS256")
    return result


class ProductionAuthTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.signer = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.other_signer = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.jwk = fixture_jwk(cls.signer)
        cls.jwks = json.dumps({"keys": [cls.jwk]}).encode()

    def setUp(self):
        self.now = NOW
        self.fetch = Mock(return_value=self.jwks)
        self.verifier = self.make_verifier()
        self.network = patch("socket.create_connection", side_effect=AssertionError("unexpected network"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def make_verifier(self, **updates):
        settings = dict(repository=REPOSITORY, repository_id="1234", repository_owner_id="5678",
            audience=AUDIENCE, workflow_refs={WORKFLOW, COLLECT}, clock=lambda: self.now,
            jwks_fetcher=self.fetch)
        settings.update(updates)
        return auth.GitHubIdentityVerifier(**settings)

    def token(self, claims=None, signer=None, headers=None):
        return jwt.encode(fixture_claims() if claims is None else claims,
            signer or self.signer, algorithm="RS256", headers=headers or {"kid": "test-key"})

    def raw_token(self, payload, header=None):
        header = header or '{"alg":"RS256","typ":"JWT","kid":"test-key"}'
        unsigned = encoded(header.encode()) + "." + encoded(payload.encode())
        signature = self.signer.sign(unsigned.encode(), padding.PKCS1v15(), hashes.SHA256())
        return unsigned + "." + encoded(signature)

    def assert_rejected(self, token):
        with self.assertRaises(auth.GitHubIdentityError) as caught:
            self.verifier.verify(token)
        self.assertEqual(str(caught.exception), "invalid GitHub admission token")

    def test_signed_identity_is_immutable_and_run_key_ignores_attempt(self):
        first = self.verifier.verify(self.token())
        second = self.verifier.verify(self.token(fixture_claims(run_attempt="2", jti="different")))
        self.assertEqual(first.run_key, ("1234", "10001"))
        self.assertEqual(second.run_key, first.run_key)
        self.assertEqual(second.run_attempt, "2")
        self.assertEqual(first.actor_id, "9876")
        self.assertEqual(first.issued_at, NOW)
        self.assertEqual(first.expires_at, NOW + 300)
        self.assertNotIn("token", repr(first).lower())
        with self.assertRaises(FrozenInstanceError):
            first.actor_id = "999"
        self.fetch.assert_called_once_with()

    def test_both_expected_workflows_and_events_are_accepted(self):
        for workflow in (WORKFLOW, COLLECT):
            for event in ("workflow_dispatch", "schedule"):
                with self.subTest(workflow=workflow, event=event):
                    result = self.verifier.verify(self.token(fixture_claims(
                        workflow_ref=workflow, event_name=event)))
                    self.assertEqual(result.workflow_ref, workflow)
                    self.assertEqual(result.event_name, event)

    def test_invalid_signature_and_payload_tampering_are_rejected(self):
        self.assert_rejected(self.token(signer=self.other_signer))
        parts = self.token().split(".")
        parts[1] = encoded(json.dumps(fixture_claims(actor_id="99999")).encode())
        self.assert_rejected(".".join(parts))

    def test_unapproved_algorithms_and_header_key_sources_are_rejected_before_fetch(self):
        payload = json.dumps(fixture_claims())
        for extra in ({"alg": "none"}, {"alg": "HS256"}, {"alg": "RS512"}, {"typ": "other"},
                      {"kid": "missing key!"}, {"kid": ["test-key"]}, {"kid": "x" * 129},
                      {"jku": "https://attacker.invalid/keys"}, {"jwk": self.jwk},
                      {"x5u": "https://attacker.invalid/key"}, {"crit": ["b64"]}, {"b64": False}):
            with self.subTest(extra=extra):
                header = dict(alg="RS256", typ="JWT", kid="test-key")
                header.update(extra)
                self.assert_rejected(self.raw_token(payload, json.dumps(header)))
        self.fetch.assert_not_called()

    def test_hmac_token_cannot_use_the_rsa_public_key_as_a_secret(self):
        token = jwt.encode(fixture_claims(), b"test-only-symmetric-key-32-bytes!!",
                           algorithm="HS256", headers={"kid": "test-key"})
        self.assert_rejected(token)
        self.fetch.assert_not_called()

    def test_missing_required_claims_are_rejected(self):
        for name in fixture_claims():
            with self.subTest(name=name):
                claims = fixture_claims()
                del claims[name]
                self.assert_rejected(self.token(claims))
        self.fetch.assert_not_called()

    def test_issuer_audience_and_signed_repository_identity_must_match(self):
        cases = dict(iss=["https://attacker.invalid", auth.ISSUER + "/", [auth.ISSUER]],
            aud=["sts.amazonaws.com", [AUDIENCE], AUDIENCE + "-other", None],
            repository=["other/project", "Example/project", "example/other"],
            repository_id=["1235", 1234], repository_owner=["other", "Example"],
            repository_owner_id=["5679", 5678])
        for name, values in cases.items():
            for value in values:
                with self.subTest(name=name, value=value):
                    self.assert_rejected(self.raw_token(json.dumps(fixture_claims(**{name: value}))))

    def test_main_ref_workflow_revision_and_event_are_bound_to_the_signed_identity(self):
        cases = dict(ref=["refs/pull/1/merge", "refs/heads/feature", "refs/tags/main"],
            workflow_ref=[WORKFLOW.replace("example/project", "other/project"),
                WORKFLOW.replace("production.yml", "other.yml"),
                WORKFLOW.replace("refs/heads/main", "refs/heads/feature"), [], None],
            workflow_sha=["b" * 40, "a" * 39, "A" * 40, None],
            sha=["b" * 40, "a" * 39, "A" * 40, None],
            event_name=["pull_request", "pull_request_target", "workflow_run", "push", "dynamic", [], None])
        for name, values in cases.items():
            for value in values:
                with self.subTest(name=name, value=value):
                    self.assert_rejected(self.token(fixture_claims(**{name: value})))

    def test_identifiers_require_bounded_positive_decimal_strings(self):
        for name in ("run_id", "run_attempt", "actor_id"):
            for value in (0, 1, True, "0", "01", "-1", "1.5", "1e10", " 1", "1\n", "1" * 21, [], None):
                with self.subTest(name=name, value=value):
                    self.assert_rejected(self.token(fixture_claims(**{name: value})))
        for name in ("jti", "actor", "sub"):
            for value in (None, False, 123, [], ""):
                with self.subTest(name=name, value=value):
                    self.assert_rejected(self.token(fixture_claims(**{name: value})))
        self.assert_rejected(self.token(fixture_claims(actor="owner\n")))
        self.assert_rejected(self.token(fixture_claims(jti="x" * 129)))
        self.assert_rejected(self.token(fixture_claims(sub="x" * 1025)))

    def test_expired_future_overlong_and_inconsistent_times_are_rejected(self):
        for updates in ({"exp": NOW}, {"exp": NOW - 1}, {"iat": NOW + 61}, {"nbf": NOW + 61},
                        {"exp": NOW + 601}, {"iat": NOW - 301}, {"nbf": NOW + 300},
                        {"iat": NOW - 600, "exp": NOW + 1}, {"exp": 2**53}):
            with self.subTest(updates=updates):
                self.assert_rejected(self.token(fixture_claims(**updates)))
        for name in ("exp", "iat", "nbf"):
            for value in (True, False, str(NOW), float(NOW), 0, -1, None, []):
                with self.subTest(name=name, value=value):
                    self.assert_rejected(self.token(fixture_claims(**{name: value})))

    def test_github_documented_and_current_backdated_nbf_samples_are_valid(self):
        # github.com/en/actions/concepts/security/openid-connect documents the first tuple.
        for before, issued, expires in ((1632492967, 1632493567, 1632493867),
                                       (1784379753, 1784380053, 1784380353)):
            with self.subTest(issued=issued):
                self.now = issued
                verifier = self.make_verifier()
                result = verifier.verify(self.token(fixture_claims(nbf=before, iat=issued, exp=expires)))
                self.assertEqual(result.issued_at, issued)

    def test_forward_clock_skew_is_bounded_and_does_not_extend_expiry(self):
        self.verifier.verify(self.token(fixture_claims(iat=NOW + 60, nbf=NOW + 60, exp=NOW + 360)))
        self.assert_rejected(self.token(fixture_claims(iat=NOW + 61, nbf=NOW, exp=NOW + 360)))
        self.assert_rejected(self.token(fixture_claims(iat=NOW, nbf=NOW + 61)))
        self.assert_rejected(self.token(fixture_claims(iat=NOW - 300, nbf=NOW - 900, exp=NOW)))
        self.assert_rejected(self.token(fixture_claims(exp=NOW + 6 * 3600)))

    def test_exact_lifetime_limit_is_valid_and_expiration_has_no_grace_period(self):
        token = self.token(fixture_claims(exp=NOW + auth.MAX_TOKEN_LIFETIME_SECONDS))
        self.verifier.verify(token)
        self.now = NOW + auth.MAX_TOKEN_LIFETIME_SECONDS - 0.01
        self.verifier.verify(token)
        self.now = NOW + auth.MAX_TOKEN_LIFETIME_SECONDS
        self.assert_rejected(token)

    def test_token_expiring_during_key_retrieval_is_rejected(self):
        self.verifier = self.make_verifier(clock=Mock(side_effect=[NOW, NOW + 300]))
        self.assert_rejected(self.token())

    def test_duplicate_claims_headers_and_nested_json_are_rejected(self):
        payload = json.dumps(fixture_claims())
        self.assert_rejected(self.raw_token('{"actor_id":"999",' + payload[1:]))
        self.assert_rejected(self.raw_token(payload,
            '{"alg":"HS256","alg":"RS256","typ":"JWT","kid":"test-key"}'))
        self.assert_rejected(self.raw_token(payload[:-1] + ',"extra":{"x":1,"x":2}}'))
        self.fetch.assert_not_called()

    def test_nonfinite_and_nonobject_json_are_rejected(self):
        payload = json.dumps(fixture_claims())
        for suffix in (',"extra":NaN}', ',"extra":Infinity}', ',"extra":1e999}'):
            self.assert_rejected(self.raw_token(payload[:-1] + suffix))
        for value in ("[]", "null", "true", "1", '"claims"'):
            self.assert_rejected(self.raw_token(value))
        nested = '[' * 2000 + '0' + ']' * 2000
        self.assert_rejected(self.raw_token(payload[:-1] + ',"extra":' + nested + '}'))
        self.fetch.assert_not_called()

    def test_token_input_and_base64_are_bounded_and_canonical(self):
        token = self.token()
        parts = token.split(".")
        for value in (None, 123, b"bytes", "", "x" * (auth.MAX_TOKEN_BYTES + 1),
                      "abc.def", token + ".extra", "\u00e9" + token, token + "=",
                      parts[0] + "=." + ".".join(parts[1:]), token + "\n"):
            with self.subTest(kind=type(value), size=len(value) if isinstance(value, str) else 0):
                self.assert_rejected(value)
        self.fetch.assert_not_called()

    def test_jwks_failures_are_sanitized_and_fetches_are_rate_limited(self):
        token = self.token()
        self.fetch.side_effect = OSError("secret-token:" + token)
        for _ in range(10):
            try:
                self.verifier.verify(token)
            except auth.GitHubIdentityError as error:
                self.assertNotIn(token, "".join(traceback.format_exception(error)))
            else:
                self.fail("fetch failure accepted")
        self.fetch.assert_called_once_with()
        self.now += auth.JWKS_REFRESH_SECONDS
        with self.assertRaises(auth.GitHubIdentityTransientError):
            self.verifier.verify(token)
        self.assertEqual(self.fetch.call_count, 2)

    def test_transient_key_fetch_and_cooldown_preserve_type_then_recover_with_fresh_proof(self):
        token = self.token()
        self.fetch.side_effect = TimeoutError("synthetic sensitive upstream")
        for advance in (0, 5, 10):
            self.now += advance
            with self.assertRaises(auth.GitHubIdentityTransientError) as caught:
                self.verifier.verify(token)
            self.assertEqual(caught.exception.retry_after_seconds, auth.JWKS_REFRESH_SECONDS)
            self.assertNotIn("synthetic sensitive upstream", "".join(traceback.format_exception(caught.exception)))
        self.assertEqual(self.fetch.call_count, 1)
        self.now = NOW + auth.JWKS_REFRESH_SECONDS
        self.fetch.side_effect = None
        self.verifier.verify(token)
        self.assertEqual(self.fetch.call_count, 2)

    def test_invalid_keys_and_certificate_validation_never_become_transient(self):
        for value in (b"malformed", ssl.SSLCertVerificationError("bad certificate"),
                      http.client.InvalidURL("bad url")):
            self.verifier = self.make_verifier()
            self.fetch.side_effect = value if isinstance(value, Exception) else None
            self.fetch.return_value = value
            for _ in range(2):
                with self.assertRaises(auth.GitHubIdentityError) as caught:
                    self.verifier.verify(self.token())
                self.assertNotIsInstance(caught.exception, auth.GitHubIdentityTransientError)

    def test_official_key_fetch_server_errors_and_late_response_are_transient(self):
        reply, connection = Mock(status=503), Mock()
        connection.getresponse.return_value = reply
        with patch.object(auth.http.client, "HTTPSConnection", return_value=connection):
            with self.assertRaises(auth.GitHubIdentityTransientError):
                auth._fetch_official_jwks()
        reply.read1.assert_not_called()
        reply.status = 200
        reply.getheader.return_value = "identity"
        with patch.object(auth.http.client, "HTTPSConnection", return_value=connection), \
                patch.object(auth.time, "monotonic", side_effect=[0, 4]):
            with self.assertRaises(auth.GitHubIdentityTransientError):
                auth._fetch_official_jwks()

    def test_unknown_key_refresh_is_bounded_and_rotation_can_recover(self):
        self.verifier.verify(self.token())
        rotated = self.token(signer=self.other_signer, headers={"kid": "rotated-key"})
        for _ in range(10):
            self.assert_rejected(rotated)
        self.assertEqual(self.fetch.call_count, 1)
        self.now += auth.JWKS_REFRESH_SECONDS
        self.fetch.return_value = json.dumps({"keys": [fixture_jwk(self.other_signer, "rotated-key")]}).encode()
        result = self.verifier.verify(rotated)
        self.assertEqual(result.run_id, "10001")
        self.assertEqual(self.fetch.call_count, 2)
        self.assert_rejected(self.token())
        self.assertEqual(self.fetch.call_count, 2)

    def test_expired_cache_is_not_used_when_refresh_fails(self):
        token = self.token(fixture_claims(exp=NOW + 600))
        self.verifier.verify(token)
        self.now += auth.JWKS_CACHE_SECONDS
        self.fetch.side_effect = OSError("offline")
        for _ in range(2):
            with self.assertRaises(auth.GitHubIdentityTransientError):
                self.verifier.verify(token)
        self.assertEqual(self.verifier._keys, {})
        self.assertIsNone(self.verifier._fetched_at)
        self.assertEqual(self.fetch.call_count, 2)

    def test_clock_rollback_does_not_extend_cached_key_validity(self):
        self.verifier.verify(self.token())
        self.now -= 1
        self.assert_rejected(self.token(fixture_claims(iat=NOW - 2)))
        self.assertEqual(self.fetch.call_count, 1)

    def test_invalid_key_sets_and_keys_are_rejected(self):
        invalid_entries = [dict(self.jwk, kty="oct"), dict(self.jwk, alg="HS256"),
            dict(self.jwk, use="enc"), dict(self.jwk, key_ops=["sign"]),
            dict(self.jwk, kid="x" * 129), dict(self.jwk, n="x" * 685),
            dict(self.jwk, e="Aw"), dict(self.jwk, d="private"),
            dict(self.jwk, x5u="https://attacker.invalid"), dict(self.jwk, n="invalid"),
            fixture_jwk(rsa.generate_private_key(public_exponent=65537, key_size=1024))]
        documents = [{"keys": [entry]} for entry in invalid_entries]
        documents += [{"keys": []}, {"keys": [self.jwk, self.jwk]},
                      {"keys": [self.jwk] * (auth.MAX_JWKS_KEYS + 1)}, {"keys": {}}, {}]
        for document in documents:
            with self.subTest(document_type=list(document)):
                self.verifier = self.make_verifier()
                self.fetch.return_value = json.dumps(document).encode()
                self.assert_rejected(self.token())
        for raw in (b"", b" " * (auth.MAX_JWKS_BYTES + 1), b"[]", b'{"keys":[],"keys":[]}',
                    b"{broken}", b"\xff", "not bytes"):
            with self.subTest(raw_type=type(raw)):
                self.verifier = self.make_verifier()
                self.fetch.return_value = raw
                self.assert_rejected(self.token())

    def test_cache_is_bounded_by_one_key_set(self):
        self.verifier.verify(self.token())
        for iteration in range(1, 5):
            self.now = NOW + iteration * auth.JWKS_REFRESH_SECONDS
            kid = "rotation-" + str(iteration)
            self.fetch.return_value = json.dumps({"keys": [fixture_jwk(self.signer, kid)]}).encode()
            self.verifier.verify(self.token(headers={"kid": kid}))
            self.assertEqual(list(self.verifier._keys), [kid])

    def test_configuration_is_copied_and_restricts_main_workflows_and_audience(self):
        refs = {WORKFLOW}
        verifier = self.make_verifier(workflow_refs=refs)
        refs.add(COLLECT)
        with self.assertRaises(auth.GitHubIdentityError):
            verifier.verify(self.token(fixture_claims(workflow_ref=COLLECT)))
        for updates in ({"audience": "sts.amazonaws.com"}, {"audience": ""},
                        {"workflow_refs": set()}, {"workflow_refs": WORKFLOW},
                        {"workflow_refs": {WORKFLOW.replace("main", "feature")}},
                        {"workflow_refs": {WORKFLOW.replace(REPOSITORY, "other/repo")}},
                        {"repository_id": "0"}, {"repository_owner_id": 1234}):
            with self.subTest(updates=updates):
                with self.assertRaises(auth.GitHubIdentityError):
                    self.make_verifier(**updates)

    def test_official_fetch_uses_fixed_tls_host_path_limits_and_no_redirects(self):
        response = Mock(status=200)
        response.getheader.return_value = "identity"
        response.read1.side_effect = [self.jwks, b""]
        connection = Mock()
        connection.getresponse.return_value = response
        with patch.object(auth.http.client, "HTTPSConnection", return_value=connection) as connect:
            self.assertEqual(auth._fetch_official_jwks(), self.jwks)
        self.assertEqual(connect.call_args.args, ("token.actions.githubusercontent.com",))
        self.assertEqual(connect.call_args.kwargs["timeout"], auth.JWKS_TIMEOUT_SECONDS)
        connection.request.assert_called_once_with("GET", "/.well-known/jwks",
                                                  headers={"Accept": "application/json"})
        self.assertEqual(response.read1.call_args_list[0].args, (auth.MAX_JWKS_BYTES + 1,))
        connection.close.assert_called_once_with()
        response.close.assert_called_once_with()
        for status in (301, 302, 307, 404, 503):
            response.status = status
            response.read1.reset_mock()
            with patch.object(auth.http.client, "HTTPSConnection", return_value=connection):
                with self.assertRaises(auth.GitHubIdentityError):
                    auth._fetch_official_jwks()
            response.read1.assert_not_called()

    def test_official_fetch_rejects_oversize_compression_and_elapsed_deadline(self):
        response = Mock(status=200)
        response.getheader.return_value = "identity"
        response.read1.side_effect = [b"x" * (auth.MAX_JWKS_BYTES + 1)]
        connection = Mock()
        connection.getresponse.return_value = response
        with patch.object(auth.http.client, "HTTPSConnection", return_value=connection):
            with self.assertRaises(auth.GitHubIdentityError):
                auth._fetch_official_jwks()
        response.getheader.return_value = "gzip"
        with patch.object(auth.http.client, "HTTPSConnection", return_value=connection):
            with self.assertRaises(auth.GitHubIdentityError):
                auth._fetch_official_jwks()
        response.getheader.return_value = "identity"
        response.read1.reset_mock()
        # late headers fail when control returns; this does not assert a hard DNS/header timeout
        with patch.object(auth.http.client, "HTTPSConnection", return_value=connection), \
                patch.object(auth.time, "monotonic", side_effect=[0, auth.JWKS_TIMEOUT_SECONDS + 1]):
            with self.assertRaises(auth.GitHubIdentityError):
                auth._fetch_official_jwks()
        response.read1.assert_not_called()

    def test_production_dependencies_are_separate_pinned_and_hash_checked(self):
        path = Path(__file__).parent
        content = (path / "production-requirements.txt").read_text()
        lines = [line.strip() for line in content.replace("\\\n", "").splitlines()
                 if line.strip() and not line.startswith("#")]
        self.assertEqual(lines.pop(0), "-r requirements.txt")
        self.assertEqual(len(lines), 4)
        expected = {"PyJWT", "cryptography", "cffi", "pycparser"}
        self.assertEqual({line.split("==")[0] for line in lines}, expected)
        for line in lines:
            self.assertRegex(line, r"^[A-Za-z]+==[0-9]+(?:\.[0-9]+)*\s+(?:--hash=sha256:[0-9a-f]{64}\s*)+$")
        worker_closure = (path / "requirements.txt").read_text().lower()
        for package in expected:
            self.assertNotIn(package.lower() + "==", worker_closure)


if __name__ == "__main__":
    unittest.main()
