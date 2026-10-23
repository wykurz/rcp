"""Offline App-authentication tests; keys/tokens are synthetic and sockets are blocked."""

from datetime import datetime, timezone
from contextlib import redirect_stdout, redirect_stderr
import io
import json
import logging
import ssl
import http.client
import traceback
import unittest
from unittest.mock import Mock, patch

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from . import production_github_app as app
from .production_github import GitHubRunError, GitHubRateLimitError, GitHubTransientError

NOW = 1800000000
PATH = "/repos/example/project/actions/runs/100/attempts/1"
SECRET_ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:/rcp/production/github-read-app-Ab1Cd2"
TOKEN = "synthetic-opaque-installation-token-" + "x" * 100


def config(**updates):
    return app.GitHubAppConfig(**(dict(repository="example/project", repository_id="123",
        repository_owner_id="456", region="us-east-1", account="123456789012",
        app_client_id="Iv23.test-client", installation_id="789", secret_arn=SECRET_ARN) | updates))


def response(value, status=200, headers=None):
    return app.Response(status, headers or {}, json.dumps(value).encode())


class Clock:
    def __init__(self):
        self.wall, self.mono = NOW, 100
    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds


class GitHubAppTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.pem = cls.key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
        cls.other_pem = cls.other_key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()).decode()

    def setUp(self):
        self.clock = Clock()
        self.secret = dict(ARN=SECRET_ARN, Name="/rcp/production/github-read-app", VersionId="a" * 32,
                           VersionStages=["AWSCURRENT"], SecretString=self.pem)
        self.secrets = Mock()
        self.secrets.get_secret_value.side_effect = lambda **kwargs: self.secret
        self.minted, self.proofs = [], []
        self.mint_update, self.scope_update = {}, None
        self.app_update, self.installation_update = {}, {}
        self.proof_statuses = []
        self.transport = Mock(side_effect=self.http)
        self.provider = app.GitHubAppProvider(config(), secrets=self.secrets, transport=self.transport,
            clock=lambda: self.clock.wall, monotonic=lambda: self.clock.mono)
        self.network = patch("socket.create_connection", side_effect=AssertionError("unexpected network"))
        self.network.start()
        self.addCleanup(self.network.stop)
        app.cached_provider.cache_clear()
        self.addCleanup(app.cached_provider.cache_clear)

    def http(self, method, path, headers, body, **kwargs):
        self.assertIn("Authorization", headers)
        if path == "/app":
            return response(self.app_document() | self.app_update)
        if path == "/app/installations/789":
            return response(self.installation_document() | self.installation_update)
        if method == "POST":
            self.minted.append((path, headers["Authorization"][7:], json.loads(body)))
            return response(dict(token=TOKEN + str(len(self.minted)), permissions=dict(app.PERMISSIONS),
                repository_selection="selected", expires_at=datetime.fromtimestamp(
                    self.clock.wall + 3600, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")) | self.mint_update, 201)
        if path == app.REPOSITORIES_PATH:
            return response(self.scope_update if self.scope_update is not None else dict(total_count=1,
                repositories=[dict(id=123, full_name="example/project", owner=dict(id=456, login="example"))]))
        self.proofs.append((path, headers["Authorization"]))
        status = self.proof_statuses.pop(0) if self.proof_statuses else 200
        return response(dict(proof=True), status)

    def app_document(self):
        return dict(id=321, client_id="Iv23.test-client", owner=dict(id=456, login="example"),
                    permissions=dict(app.PERMISSIONS), events=[], installations_count=1)

    def installation_document(self):
        return dict(id=789, app_id=321, account=dict(id=456, login="example"), target_id=456,
                    permissions=dict(app.PERMISSIONS), events=[], repository_selection="selected",
                    suspended_at=None, suspended_by=None)

    def read(self, path=PATH):
        with self.provider.observation() as fetch:
            return fetch(path)

    def test_scoped_mint_exact_current_secret_signed_jwt_and_fixed_inventory(self):
        self.assertEqual(json.loads(self.read()), dict(proof=True))
        self.secrets.get_secret_value.assert_called_once_with(SecretId=SECRET_ARN, VersionStage="AWSCURRENT")
        path, signed, body = self.minted[0]
        self.assertEqual(path, "/app/installations/789/access_tokens")
        self.assertEqual(body, dict(repository_ids=[123], permissions={"actions": "read", "metadata": "read"}))
        claims = jwt.decode(signed, self.key.public_key(), algorithms=["RS256"],
                           options={"verify_exp": False, "verify_iat": False})
        self.assertEqual(claims, dict(iat=NOW - 60, exp=NOW + 300, iss="Iv23.test-client"))
        self.assertEqual([call.args[1] for call in self.transport.call_args_list],
                         ["/app", "/app/installations/789", path, app.REPOSITORIES_PATH, PATH])
        self.assertEqual(self.proofs[0][1], "Bearer " + TOKEN + "1")
        self.assertNotIn(TOKEN, repr(self.provider._cached))
        self.assertNotIn(self.pem[:50], repr(self.provider.__dict__))

    def test_app_and_installation_drift_fails_before_mint_and_proof(self):
        app_cases = [dict(id=True), dict(id="321"), dict(id=0), dict(client_id="other"),
            dict(owner=dict(id=457, login="example")), dict(owner=dict(id="456", login="example")),
            dict(owner=dict(id=456, login="other")), dict(installations_count=True),
            dict(installations_count="1"), dict(installations_count=2), dict(events=["push"]),
            dict(permissions=app.PERMISSIONS | {"contents": "write"})]
        install_cases = [dict(id="789"), dict(id=True), dict(id=790), dict(app_id="321"),
            dict(app_id=True), dict(app_id=322), dict(target_id="456"), dict(target_id=True),
            dict(target_id=457), dict(account=dict(id=457, login="example")),
            dict(account=dict(id=456, login="other")), dict(account=dict(id="456", login="example")),
            dict(permissions={"actions": "write", "metadata": "read"}), dict(events=["workflow_run"]),
            dict(repository_selection="all"), dict(suspended_at="now"), dict(suspended_by={})]
        for target, cases in (("app_update", app_cases), ("installation_update", install_cases)):
            for update in cases:
                with self.subTest(target=target, fields=list(update)):
                    setattr(self, target, update)
                    with self.assertRaises(GitHubRunError) as caught:
                        self.read()
                    self.assertNotIsInstance(caught.exception, GitHubTransientError)
                    self.assertIsNone(self.provider._cached)
            setattr(self, target, {})
        self.assertEqual(self.minted, [])
        self.assertEqual(self.proofs, [])

    def test_missing_required_app_and_installation_fields_fail_closed(self):
        for path, original in (("/app", self.app_document()),
                               ("/app/installations/789", self.installation_document())):
            for key in original:
                incomplete = {name: value for name, value in original.items() if name != key}
                def missing(method, requested, headers, body, **kwargs):
                    return response(incomplete) if requested == path else self.http(method, requested, headers, body, **kwargs)
                self.transport.side_effect = missing
                with self.subTest(path=path, missing=key), self.assertRaises(GitHubRunError):
                    self.read()
                self.assertIsNone(self.provider._cached)
        self.assertEqual(self.minted, [])
        self.assertEqual(self.proofs, [])

    def test_each_remint_rechecks_drift_with_no_anonymous_or_unscoped_fallback(self):
        self.read()
        self.proof_statuses = [401]
        self.app_update = dict(permissions=app.PERMISSIONS | {"contents": "write"})
        with self.assertRaises(GitHubRunError):
            self.read()
        self.assertEqual(len(self.minted), 1)
        self.assertEqual(self.proofs[-1][0], PATH)
        self.assertIsNone(self.provider._cached)
        self.assertEqual(self.transport.call_args_list[-1].args[:2], ("GET", "/app"))

    def test_app_metadata_errors_remain_bounded_and_preserve_retry_class(self):
        for path in ("/app", "/app/installations/789"):
            for status in (401, 403, 404, 429, 503):
                self.transport.reset_mock()
                def fail(method, requested, headers, body, **kwargs):
                    return response({}, status) if requested == path else self.http(method, requested, headers, body, **kwargs)
                self.transport.side_effect = fail
                with self.subTest(path=path, status=status), self.assertRaises(GitHubRunError) as caught:
                    self.read()
                self.assertEqual(isinstance(caught.exception, GitHubTransientError), status == 503)
                self.assertEqual(isinstance(caught.exception, GitHubRateLimitError), status == 429)
                self.assertIsNone(self.provider._cached)
                self.assertLessEqual(self.transport.call_count, 2)
                self.clock.advance(61)
        self.assertEqual(self.minted, [])
        self.assertEqual(self.proofs, [])

    def test_configuration_rejects_cross_account_region_secret_and_unsafe_identifiers(self):
        cases = [dict(repository="../project"), dict(repository="example/.."), dict(repository_id="01"),
            dict(repository_owner_id=456), dict(installation_id="0"), dict(app_client_id="a\r\nHeader"),
            dict(app_client_id=""), dict(app_client_id="x" * 101), dict(secret_arn=SECRET_ARN + "extra"),
            dict(secret_arn=SECRET_ARN.replace("123456789012", "999999999999")),
            dict(secret_arn=SECRET_ARN.replace("us-east-1", "us-west-2")),
            dict(secret_arn=SECRET_ARN.replace("github-read-app", "other"))]
        for updates in cases:
            with self.subTest(fields=list(updates)), self.assertRaises(GitHubRunError):
                config(**updates)
        self.transport.assert_not_called()
        config(app_client_id="Iv1.ab1112223334445c")
        config(app_client_id="Iv23newOpaqueClientId")

    def test_environment_factory_copies_only_non_secret_pins_and_is_lazy(self):
        expected = config()
        env = dict(RCP_PRODUCTION_REPOSITORY=expected.repository,
            RCP_PRODUCTION_REPOSITORY_ID=expected.repository_id,
            RCP_PRODUCTION_OWNER_ID=expected.repository_owner_id,
            RCP_PRODUCTION_GITHUB_APP_CLIENT_ID=expected.app_client_id,
            RCP_PRODUCTION_GITHUB_APP_INSTALLATION_ID=expected.installation_id,
            RCP_PRODUCTION_GITHUB_APP_SECRET_ARN=expected.secret_arn,
            UNRELATED_SECRET="synthetic-unused-secret")
        with patch("boto3.client") as client:
            actual = app.GitHubAppConfig.from_environment(env, region=expected.region, account=expected.account)
            self.assertEqual(actual, expected)
            env["RCP_PRODUCTION_REPOSITORY"] = "other/repository"
            self.assertEqual(actual.repository, expected.repository)
            self.assertNotIn("synthetic-unused-secret", repr(actual))
            client.assert_not_called()
        self.secrets.get_secret_value.assert_not_called()
        self.transport.assert_not_called()

    def test_environment_factory_rejects_missing_malformed_and_cross_scope_values(self):
        expected = config()
        env = dict(RCP_PRODUCTION_REPOSITORY=expected.repository,
            RCP_PRODUCTION_REPOSITORY_ID=expected.repository_id,
            RCP_PRODUCTION_OWNER_ID=expected.repository_owner_id,
            RCP_PRODUCTION_GITHUB_APP_CLIENT_ID=expected.app_client_id,
            RCP_PRODUCTION_GITHUB_APP_INSTALLATION_ID=expected.installation_id,
            RCP_PRODUCTION_GITHUB_APP_SECRET_ARN=expected.secret_arn)
        for key in env:
            for value in (None, ""):
                with self.subTest(key=key, value=value), self.assertRaises(GitHubRunError):
                    app.GitHubAppConfig.from_environment(env | {key: value}, region=expected.region, account=expected.account)
        for value in (None, [], "environment"):
            with self.assertRaises(GitHubRunError):
                app.GitHubAppConfig.from_environment(value, region=expected.region, account=expected.account)
        for region, account in (("us-west-2", expected.account), (expected.region, "999999999999")):
            with self.assertRaises(GitHubRunError):
                app.GitHubAppConfig.from_environment(env, region=region, account=account)
        self.transport.assert_not_called()

    def test_cache_binds_whole_configuration_and_constructs_no_sdk_clients(self):
        with patch("boto3.client") as client:
            first = app.cached_provider(config())
            self.assertIs(first, app.cached_provider(config()))
            self.assertIsNot(first, app.cached_provider(config(installation_id="790")))
            self.assertIsNot(first, app.cached_provider(config(app_client_id="other-app")))
            client.assert_not_called()

    def test_lazy_sdk_client_is_fixed_endpoint_no_retries_and_current_only(self):
        self.provider._secrets = None
        with patch("boto3.client", return_value=self.secrets) as client:
            self.read()
        call = client.call_args
        self.assertEqual(call.args, ("secretsmanager",))
        self.assertEqual(call.kwargs["region_name"], "us-east-1")
        self.assertEqual(call.kwargs["endpoint_url"], "https://secretsmanager.us-east-1.amazonaws.com")
        settings = call.kwargs["config"]
        self.assertEqual(settings.retries, {"total_max_attempts": 1})
        self.assertEqual(settings.proxies, {})
        self.assertTrue(settings.ignore_configured_endpoint_urls)
        self.assertEqual((settings.connect_timeout, settings.read_timeout), (1, 3))

    def test_reuses_only_token_and_never_producer_response_or_key(self):
        self.read()
        self.read()
        self.assertEqual(len(self.minted), 1)
        self.assertEqual(len(self.proofs), 2)
        self.assertEqual(self.secrets.get_secret_value.call_count, 1)
        self.assertEqual(self.transport.call_count, 6)
        self.assertNotIn("key", self.provider.__dict__)
        self.assertNotIn("pem", self.provider.__dict__)

    def test_wall_or_monotonic_expiry_and_clock_rollback_invalidate_cached_token(self):
        for mode in ("wall", "mono", "rollback"):
            with self.subTest(mode=mode):
                self.provider._cached = None
                self.read()
                before = len(self.minted)
                if mode == "wall":
                    self.clock.wall += 3300
                elif mode == "mono":
                    self.clock.mono += 3300
                else:
                    self.clock.wall -= 10
                self.read()
                self.assertEqual(len(self.minted), before + 1)

    def test_one_401_refresh_fetches_new_current_key_and_retries_same_get(self):
        self.read()
        self.secret = self.secret | dict(SecretString=self.other_pem, VersionId="b" * 32)
        self.proof_statuses = [401, 200]
        self.read()
        self.assertEqual(len(self.minted), 2)
        self.assertEqual(self.secrets.get_secret_value.call_count, 2)
        self.assertEqual(self.proofs[-2][0], self.proofs[-1][0])
        jwt.decode(self.minted[-1][1], self.other_key.public_key(), algorithms=["RS256"],
                   options={"verify_exp": False, "verify_iat": False})
        self.assertNotEqual(self.proofs[-2][1], self.proofs[-1][1])

    def test_refresh_limit_is_shared_across_all_gets_and_second_401_fails_closed(self):
        self.proof_statuses = [401, 200, 401]
        with self.assertRaises(GitHubRunError), self.provider.observation() as fetch:
            fetch(PATH)
            fetch(PATH.replace("/attempts/1", ""))
        self.assertEqual(len(self.minted), 2)
        self.assertEqual(len(self.proofs), 3)

    def test_cold_six_read_observation_including_one_refresh_fits_exact_17_calls(self):
        self.proof_statuses = [200] * 5 + [401, 200]
        with self.provider.observation() as fetch:
            for _ in range(6):
                fetch(PATH)
            self.assertEqual(fetch.calls, 17)
            with self.assertRaises(GitHubRunError):
                fetch(PATH)
        self.assertEqual(len(self.proofs), 7)
        self.assertEqual(len(self.minted), 2)

    def test_refresh_and_slow_secret_do_not_reset_observation_deadline(self):
        self.read()
        original = self.http
        def slow(*args, **kwargs):
            self.clock.advance(11)
            return original(*args, **kwargs)
        self.transport.side_effect = slow
        self.proof_statuses = [401]
        with self.assertRaises(GitHubRunError):
            self.read()
        self.assertEqual(len(self.proofs), 2)
        self.assertEqual(len(self.minted), 1)
        self.provider._cached = None
        self.transport.reset_mock()
        def slow_secret(**kwargs):
            self.clock.advance(21)
            return self.secret
        self.secrets.get_secret_value.side_effect = slow_secret
        with self.assertRaises(GitHubRunError):
            self.read()
        self.transport.assert_not_called()

    def test_cache_and_invalidation_lock_waits_share_the_observation_budget(self):
        self.read()
        lock = self.provider._lock
        fake = Mock()
        fake.acquire.return_value = False
        self.provider._lock = fake
        with self.assertRaises(GitHubRunError):
            self.read()
        fake.acquire.assert_called_once_with(timeout=20)
        self.provider._lock = lock
        with self.provider.observation() as fetch:
            fetch(PATH)
            self.provider._lock = fake
            fake.reset_mock()
            self.proof_statuses = [401]
            with self.assertRaises(GitHubRunError):
                fetch(PATH)
            fake.acquire.assert_called_once_with(timeout=20)
        self.assertEqual(len(self.minted), 1)

    def test_transport_errors_and_invalid_mint_json_are_redacted_without_fallback(self):
        for failure in (TimeoutError(TOKEN), RuntimeError(self.pem)):
            with self.subTest(kind=type(failure)):
                self.transport.side_effect = failure
                with self.assertRaises(GitHubRunError) as caught:
                    self.read()
                trace = "".join(traceback.format_exception(caught.exception))
                self.assertNotIn(TOKEN, trace)
                self.assertNotIn("PRIVATE KEY", trace)
        for raw in (b'{"token":"one","token":"two"}', b'{"token":NaN}', b'[' * 20 + b'0' + b']' * 20):
            self.transport.side_effect = lambda *a, **k: app.Response(201, {}, raw)
            with self.subTest(size=len(raw)), self.assertRaises(GitHubRunError):
                self.read()
        self.assertEqual(self.proofs, [])

    def test_redirects_and_non_success_mint_status_never_follow_or_refresh(self):
        for status in (301, 302, 307, 308, 400, 401, 403, 404, 500):
            with self.subTest(status=status):
                self.transport.reset_mock()
                self.transport.side_effect = lambda *a, **k: response({}, status)
                with self.assertRaises(GitHubRunError):
                    self.read()
                self.assertEqual(self.transport.call_count, 1)
        self.assertEqual(self.proofs, [])

    def test_observation_cannot_be_reused_or_used_outside_context(self):
        fetch = self.provider.observation()
        with self.assertRaises(GitHubRunError):
            fetch(PATH)
        with fetch:
            fetch(PATH)
        with self.assertRaises(GitHubRunError):
            fetch(PATH)
        with self.assertRaises(GitHubRunError):
            fetch.__enter__()

    def test_invalid_secret_identity_stage_type_and_key_are_redacted(self):
        for update in (dict(ARN="other"), dict(Name="other"), dict(VersionId="bad"),
                dict(VersionStages=["AWSPREVIOUS"]), dict(SecretBinary=b"bad"), dict(SecretString=None),
                dict(SecretString="secret-sentinel"), dict(SecretString="x" * (app.MAX_PEM_BYTES + 1))):
            with self.subTest(fields=list(update)):
                self.provider._cached = None
                self.secrets.get_secret_value.side_effect = None
                self.secrets.get_secret_value.return_value = self.secret | update
                with self.assertRaises(GitHubRunError) as caught:
                    self.read()
                self.assertNotIn("secret-sentinel", "".join(traceback.format_exception(caught.exception)))
        self.transport.assert_not_called()

    def test_secret_denial_or_malformed_response_has_no_anonymous_fallback(self):
        self.secrets.get_secret_value.side_effect = RuntimeError("synthetic-private-key-error")
        with self.assertRaises(GitHubRunError) as caught:
            self.read()
        self.assertNotIn("synthetic-private-key-error", "".join(traceback.format_exception(caught.exception)))
        self.transport.assert_not_called()

    def test_wrong_permissions_selection_tokens_and_expiration_fail_before_proofs(self):
        future = lambda delta: datetime.fromtimestamp(self.clock.wall + delta, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cases = [dict(permissions={"actions": "write", "metadata": "read"}),
            dict(permissions={"actions": "read"}), dict(permissions=app.PERMISSIONS | {"contents": "read"}),
            dict(repository_selection="all"), dict(token=""), dict(token="bad\nheader"),
            dict(token="x" * 8193), dict(expires_at=future(0)), dict(expires_at=future(300)),
            dict(expires_at=future(3661)), dict(expires_at="not-time")]
        for update in cases:
            with self.subTest(fields=list(update)):
                self.mint_update = update
                with self.assertRaises(GitHubRunError):
                    self.read()
        self.assertEqual(self.proofs, [])

    def test_wrong_repository_owner_count_and_pagination_fail_before_proofs(self):
        repo = dict(id=123, full_name="example/project", owner=dict(id=456, login="example"))
        cases = [dict(total_count=2, repositories=[repo]), dict(total_count=True, repositories=[repo]),
            dict(total_count=1, repositories=[])]
        for change in (dict(id=124), dict(id=True), dict(full_name="other/project"),
                       dict(owner=dict(id=457, login="example")), dict(owner=dict(id=456, login="other"))):
            cases.append(dict(total_count=1, repositories=[repo | change]))
        for value in cases:
            with self.subTest(value=value):
                self.scope_update = value
                with self.assertRaises(GitHubRunError):
                    self.read()
        self.assertEqual(self.proofs, [])
        self.transport.side_effect = lambda *a, **k: response({}, headers={"link": "<next>; rel=next"})
        with self.assertRaises(GitHubRunError):
            self.read()

    def test_forbidden_paths_fail_before_credentials_or_transport(self):
        for path in ("https://evil.invalid/path", "//api.github.com/path", PATH + "/logs",
                PATH.replace("example/project", "other/project"), "/app", app.REPOSITORIES_PATH,
                "/app/installations/789/access_tokens", PATH + "?page=2", PATH + "\r\nHeader: x"):
            with self.subTest(path=path), self.assertRaises(GitHubRunError):
                self.read(path)
        self.secrets.get_secret_value.assert_not_called()
        self.transport.assert_not_called()

    def test_403_permission_denial_is_distinct_from_primary_secondary_and_429_quota(self):
        self.read()
        for status, headers, body, quota in ((403, {}, {"message": "Resource not accessible by integration"}, False),
            (403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": str(NOW + 400)}, {}, True),
            (403, {}, {"message": "You have exceeded a secondary rate limit."}, True),
            (429, {"retry-after": "120"}, {}, True)):
            with self.subTest(status=status, headers=headers):
                self.provider._quota_wall = self.provider._quota_mono = 0
                self.transport.side_effect = lambda *a, **k: response(body, status, headers)
                with self.assertRaises(GitHubRunError) as caught:
                    self.read()
                self.assertEqual(isinstance(caught.exception, GitHubRateLimitError), quota)
                if quota:
                    before = self.transport.call_count
                    with self.assertRaises(GitHubRateLimitError):
                        self.read()
                    self.assertEqual(self.transport.call_count, before)
                else:
                    self.assertEqual(self.provider._quota_wall, 0)
        self.assertEqual(len(self.minted), 1)

    def test_quota_cooldown_blocks_new_secret_reads_and_preserves_reset_delay(self):
        self.transport.side_effect = lambda *a, **k: response({}, 429,
            {"retry-after": "120", "x-ratelimit-reset": str(NOW + 400), "x-ratelimit-remaining": "0"})
        with self.assertRaises(GitHubRateLimitError) as caught:
            self.read()
        self.assertEqual(caught.exception.retry_after_seconds, 401)
        with self.assertRaises(GitHubRateLimitError):
            self.read()
        self.assertEqual(self.secrets.get_secret_value.call_count, 1)
        self.assertEqual(self.transport.call_count, 1)

    def test_secondary_quota_ignores_primary_reset_when_quota_remains(self):
        for status in (403, 429):
            for remaining in ("4990", None):
                with self.subTest(status=status, remaining=remaining):
                    self.provider._quota_wall = self.provider._quota_mono = 0
                    self.transport.side_effect = lambda *a, **k: response({}, status,
                        {"retry-after": "120", "x-ratelimit-reset": str(NOW + 3500),
                         "x-ratelimit-remaining": remaining})
                    with self.assertRaises(GitHubRateLimitError) as caught:
                        self.read()
                    self.assertEqual(caught.exception.retry_after_seconds, 120)
                    self.clock.advance(120)
                    self.transport.side_effect = self.http
                    self.read()

    def test_transport_and_server_failures_are_typed_sanitized_transients_at_each_phase(self):
        for phase in ("mint", "scope", "proof"):
            for error in (TimeoutError(TOKEN), ConnectionResetError(TOKEN),
                          ssl.SSLEOFError(TOKEN), http.client.RemoteDisconnected(TOKEN), 502):
                self.provider._cached = None
                self.transport.reset_mock()
                def fail(method, path, headers, body, **kwargs):
                    actual = "mint" if method == "POST" else "scope" if path == app.REPOSITORIES_PATH else "proof"
                    if actual == phase:
                        if isinstance(error, int):
                            return response({}, error)
                        raise error
                    return self.http(method, path, headers, body, **kwargs)
                self.transport.side_effect = fail
                with self.subTest(phase=phase, error=type(error)), self.assertRaises(GitHubTransientError) as caught:
                    self.read()
                self.assertEqual(caught.exception.retry_after_seconds, 5)
                self.assertNotIn(TOKEN, "".join(traceback.format_exception(caught.exception)))
                self.assertLessEqual(self.transport.call_count, 5)

    def test_secret_transients_are_typed_but_access_denial_and_invalid_data_are_permanent(self):
        from botocore.exceptions import ClientError, ReadTimeoutError
        for failure, transient in ((ReadTimeoutError(endpoint_url="https://synthetic.invalid"), True),
                (ClientError({"Error": {"Code": "ThrottlingException"}}, "GetSecretValue"), True),
                (ClientError({"Error": {"Code": "InternalServiceError"},
                              "ResponseMetadata": {"HTTPStatusCode": 500}}, "GetSecretValue"), True),
                (ClientError({"Error": {"Code": "AccessDeniedException"}}, "GetSecretValue"), False),
                (ValueError("synthetic-private-key"), False)):
            self.secrets.get_secret_value.side_effect = failure
            with self.subTest(kind=type(failure)), self.assertRaises(GitHubRunError) as caught:
                self.read()
            self.assertEqual(isinstance(caught.exception, GitHubTransientError), transient)
            self.assertNotIn("synthetic-private-key", "".join(traceback.format_exception(caught.exception)))
        self.transport.assert_not_called()

    def test_lazy_secret_client_construction_transport_failure_is_retryable(self):
        self.provider._secrets = None
        with patch("boto3.client", side_effect=TimeoutError("synthetic credentials endpoint")):
            with self.assertRaises(GitHubTransientError):
                self.read()
        self.assertIsNone(self.provider._secrets)
        self.transport.assert_not_called()

    def test_certificate_invalid_response_and_post_mint_401_remain_permanent(self):
        failures = [ssl.SSLCertVerificationError("synthetic certificate"), http.client.InvalidURL("bad url")]
        for failure in failures:
            self.transport.side_effect = failure
            with self.assertRaises(GitHubRunError) as caught:
                self.read()
            self.assertNotIsInstance(caught.exception, GitHubTransientError)
        for status in (401, 403, 404):
            self.transport.side_effect = lambda method, path, headers, body, **kwargs: (
                response({}, status) if path == app.REPOSITORIES_PATH
                else self.http(method, path, headers, body, **kwargs))
            with self.subTest(status=status), self.assertRaises(GitHubRunError) as caught:
                self.read()
            self.assertNotIsInstance(caught.exception, GitHubTransientError)
            self.assertIsNone(self.provider._cached)
        self.assertEqual(self.proofs, [])

    def test_observation_and_lock_deadlines_are_transient_without_resetting_limits(self):
        with self.assertRaises(GitHubTransientError), self.provider.observation() as fetch:
            self.clock.advance(app.OBSERVATION_SECONDS)
            fetch(PATH)
        self.transport.assert_not_called()
        self.provider._lock = Mock()
        self.provider._lock.acquire.return_value = False
        with self.assertRaises(GitHubTransientError):
            self.read()
        self.provider._lock.acquire.assert_called_once_with(timeout=app.OBSERVATION_SECONDS)

    def test_real_https_request_serialization_pins_method_path_headers_body_and_verified_tls(self):
        expiry = datetime.fromtimestamp(NOW + 3600, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        replies = [response(self.app_document()), response(self.installation_document()),
            response(dict(token=TOKEN, permissions=app.PERMISSIONS,
            repository_selection="selected", expires_at=expiry), 201),
            response(dict(total_count=1, repositories=[dict(id=123, full_name="example/project",
                owner=dict(id=456, login="example"))])), response(dict(proof=True))]
        sockets = [WireSocket(item) for item in replies]
        tls = []
        def wrap(context, sock, *, server_hostname, **kwargs):
            tls.append((context.verify_mode, context.check_hostname, server_hostname))
            return sock
        self.provider._transport = app._https
        with patch("socket.create_connection", side_effect=sockets) as connect, \
                patch.object(ssl.SSLContext, "wrap_socket", wrap):
            self.assertEqual(json.loads(self.read()), dict(proof=True))
        self.assertEqual([call.args[0] for call in connect.call_args_list], [("api.github.com", 443)] * 5)
        self.assertEqual(tls, [(ssl.CERT_REQUIRED, True, "api.github.com")] * 5)
        expected = [("GET", "/app"), ("GET", "/app/installations/789"),
            ("POST", "/app/installations/789/access_tokens"), ("GET", app.REPOSITORIES_PATH), ("GET", PATH)]
        for sock, (method, path) in zip(sockets, expected):
            head, body = b"".join(sock.sent).split(b"\r\n\r\n", 1)
            lines = head.decode().split("\r\n")
            self.assertEqual(lines[0], f"{method} {path} HTTP/1.1")
            headers = dict(line.split(": ", 1) for line in lines[1:])
            self.assertEqual(headers["Host"], "api.github.com")
            self.assertEqual(headers["Accept"], "application/vnd.github+json")
            self.assertEqual(headers["X-GitHub-Api-Version"], "2026-03-10")
            self.assertEqual(headers["User-Agent"], "rcp-production-reader")
            self.assertEqual(headers["Accept-Encoding"], "identity")
            self.assertTrue(sock.closed)
            if method == "POST":
                self.assertEqual(headers["Content-Type"], "application/json")
                self.assertEqual(int(headers["Content-Length"]), len(body))
                self.assertEqual(json.loads(body), dict(repository_ids=[123], permissions=app.PERMISSIONS))
                self.assertTrue(headers["Authorization"].startswith("Bearer "))
                claims = jwt.decode(headers["Authorization"][7:], self.key.public_key(), algorithms=["RS256"],
                                   options={"verify_exp": False, "verify_iat": False})
                self.assertEqual(claims["iss"], config().app_client_id)
            elif path in ("/app", "/app/installations/789"):
                claims = jwt.decode(headers["Authorization"][7:], self.key.public_key(), algorithms=["RS256"],
                                   options={"verify_exp": False, "verify_iat": False})
                self.assertEqual(claims["iss"], config().app_client_id)
                self.assertEqual(body, b"")
                self.assertNotIn("Content-Type", headers)
            else:
                self.assertEqual(headers["Authorization"], "Bearer " + TOKEN)
                self.assertEqual(body, b"")
                self.assertNotIn("Content-Type", headers)

    def test_invalid_clock_fails_closed(self):
        for clock in (float("nan"), float("inf"), -1, True):
            with self.subTest(clock=clock):
                self.clock.wall = clock
                with self.assertRaises(GitHubRunError):
                    self.read()
        self.transport.assert_not_called()

    def test_sdk_debug_and_sanitized_errors_do_not_log_secret_or_token(self):
        out = io.StringIO()
        log = logging.getLogger("botocore.parsers")
        old_level, old_handlers, old_filters, old_propagate = log.level, log.handlers[:], log.filters[:], log.propagate
        try:
            log.setLevel(logging.DEBUG)
            log.handlers, log.filters, log.propagate = [logging.StreamHandler(out)], [], False
            def read(**kwargs):
                log.debug("raw synthetic secret %s", self.pem)
                logging.getLogger("botocore.endpoint").debug("raw synthetic token %s", TOKEN)
                return self.secret
            self.secrets.get_secret_value.side_effect = read
            with redirect_stdout(out), redirect_stderr(out):
                self.read()
            self.assertEqual(out.getvalue(), "")
            application = logging.getLogger("rcp.synthetic-admission")
            with self.assertLogs(application, level="INFO") as logs:
                application.info("safe admission diagnostic")
            self.assertIn("safe admission diagnostic", logs.output[0])
        finally:
            log.setLevel(old_level)
            log.handlers, log.filters, log.propagate = old_handlers, old_filters, old_propagate


class WireSocket:
    """Capture real http.client serialization without a socket or TLS handshake."""
    def __init__(self, response):
        self.sent, self.closed = [], False
        self.reply = io.BytesIO(f"HTTP/1.1 {response.status} Synthetic\r\nContent-Length: {len(response.body)}\r\n\r\n".encode()
                                + response.body)
    def setsockopt(self, *args):
        pass
    def settimeout(self, *args):
        pass
    def sendall(self, value):
        self.sent.append(value)
    def makefile(self, *args):
        return self.reply
    def close(self):
        self.closed = True


class TransportTests(unittest.TestCase):
    def test_fixed_tls_header_transport_disables_debug_and_bounds_body(self):
        reply, connection = Mock(status=200, length=0), Mock()
        reply.getheader.return_value = None
        reply.read1.side_effect = [b"{}", b""]
        connection.getresponse.return_value = reply
        with patch.object(app.http.client, "HTTPSConnection", return_value=connection) as connect:
            result = app._https("GET", PATH, {"Authorization": "Bearer synthetic"}, None,
                                timeout=3, deadline=20, monotonic=lambda: 1)
        self.assertEqual(connect.call_args.args, ("api.github.com",))
        self.assertEqual(connect.call_args.kwargs["timeout"], 3)
        connection.set_debuglevel.assert_called_once_with(0)
        self.assertEqual(result.body, b"{}")
        reply.close.assert_called_once()
        connection.close.assert_called_once()

    def test_late_headers_oversized_and_compressed_responses_close_transport(self):
        for mode in ("late", "oversized", "compressed"):
            reply, connection = Mock(status=200, length=0), Mock()
            reply.getheader.side_effect = lambda name: "gzip" if mode == "compressed" and name == "content-encoding" else None
            reply.read1.return_value = b"x" * (app.MAX_RESPONSE_BYTES + 1)
            connection.getresponse.return_value = reply
            with self.subTest(mode=mode), patch.object(app.http.client, "HTTPSConnection", return_value=connection):
                with self.assertRaises(GitHubRunError):
                    app._https("GET", PATH, {"Authorization": "Bearer synthetic"}, None,
                               timeout=3, deadline=20, monotonic=lambda: 21 if mode == "late" else 1)
            reply.close.assert_called_once()
            connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
