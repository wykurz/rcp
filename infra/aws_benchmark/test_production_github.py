"""Offline tests for binding public GitHub run observations to signed identity."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import traceback
import unittest
from unittest.mock import Mock, patch

from . import production_github as github
from .production_auth import VerifiedGitHubIdentity
from .production_policy import MAX_RUN_AGE_SECONDS, producer_fingerprint


NOW = datetime(2026, 10, 22, 20, 0, tzinfo=timezone.utc)
REPOSITORY = "example/project"
PATH = "/repos/example/project/actions/runs/10001"
WORKFLOW = REPOSITORY + "/.github/workflows/aws-benchmark-production.yml@refs/heads/main"


def identity(**updates):
    value = VerifiedGitHubIdentity(repository=REPOSITORY, repository_id="1234",
        repository_owner="example", repository_owner_id="5678", ref="refs/heads/main",
        workflow_ref=WORKFLOW, workflow_sha="a" * 40, sha="a" * 40, run_id="10001",
        run_attempt="2", actor="test-owner", actor_id="9876", event_name="workflow_dispatch",
        jti="test-only", issued_at=int(NOW.timestamp()), expires_at=int(NOW.timestamp()) + 300)
    return replace(value, **updates)


def payload(**updates):
    return dict(id=10001, run_attempt=2,
        repository=dict(id=1234, full_name=REPOSITORY, owner=dict(id=5678)),
        head_sha="a" * 40, head_branch="main", event="workflow_dispatch", actor=dict(id=9876),
        path=".github/workflows/aws-benchmark-production.yml", status="in_progress",
        conclusion=None, created_at=(NOW - timedelta(minutes=30)).isoformat()) | updates


def raw(value):
    return json.dumps(value).encode()


class ProductionGitHubTests(unittest.TestCase):
    def setUp(self):
        self.attempt = payload(created_at=(NOW - timedelta(minutes=1)).isoformat())
        self.original = payload()
        self.fetch = Mock(side_effect=lambda path: raw(self.attempt if "/attempts/" in path else self.original))
        self.reader = self.make_reader()
        self.network = patch("socket.create_connection", side_effect=AssertionError("unexpected network"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def make_reader(self, **updates):
        return github.GitHubRunReader(**(dict(repository=REPOSITORY, repository_id="1234",
            repository_owner_id="5678", fetcher=self.fetch, clock=lambda: NOW.timestamp()) | updates))

    def assert_rejected(self, who=None):
        with self.assertRaises(github.GitHubRunError) as caught:
            self.reader.read(who or identity())
        self.assertEqual(str(caught.exception), "GitHub workflow run could not be verified")

    def test_original_run_creation_is_preserved_and_proof_binds_all_producer_fields(self):
        proof = self.reader.read(identity())
        self.assertEqual(proof.created_at, self.original["created_at"])
        self.assertNotEqual(proof.created_at, self.attempt["created_at"])
        self.assertEqual(proof.observed_at, NOW.isoformat())
        self.assertEqual(proof.producer_fingerprint, producer_fingerprint(identity()))
        self.assertEqual([call.args[0] for call in self.fetch.call_args_list], [PATH + "/attempts/2", PATH])

    def test_new_token_and_recent_attempt_cannot_refresh_stale_original_run(self):
        self.original["created_at"] = (NOW - timedelta(seconds=MAX_RUN_AGE_SECONDS + 1)).isoformat()
        self.attempt["created_at"] = NOW.isoformat()
        self.assert_rejected()

    def test_exact_age_limit_is_accepted(self):
        self.original["created_at"] = (NOW - timedelta(seconds=MAX_RUN_AGE_SECONDS)).isoformat()
        self.reader.read(identity())

    def test_each_response_must_match_the_exact_signed_producer_and_active_state(self):
        changes = [dict(id=10002), dict(run_attempt=1), dict(head_sha="b" * 40),
            dict(head_branch="feature"), dict(event="pull_request"), dict(actor=dict(id=111)),
            dict(path=".github/workflows/other.yml"), dict(status="completed", conclusion="success"),
            dict(status="waiting"), dict(conclusion="cancelled"),
            dict(repository=dict(id=999, full_name=REPOSITORY, owner=dict(id=5678))),
            dict(repository=dict(id=1234, full_name="other/project", owner=dict(id=5678))),
            dict(repository=dict(id=1234, full_name=REPOSITORY, owner=dict(id=999)))]
        for side in ("attempt", "original"):
            for change in changes:
                with self.subTest(side=side, fields=list(change)):
                    before = getattr(self, side)
                    setattr(self, side, payload(**change))
                    self.assert_rejected()
                    setattr(self, side, before)

    def test_current_run_must_not_have_advanced_to_another_attempt(self):
        self.original["run_attempt"] = 3
        self.assert_rejected()

    def test_future_stale_missing_and_unzoned_timestamps_fail_closed(self):
        for side in ("attempt", "original"):
            for created in ((NOW + timedelta(seconds=1)).isoformat(),
                            (NOW - timedelta(hours=3)).isoformat(),
                            NOW.replace(tzinfo=None).isoformat(), "not a timestamp", None, 123):
                with self.subTest(side=side, timestamp_type=type(created)):
                    before = getattr(self, side)["created_at"]
                    getattr(self, side)["created_at"] = created
                    self.assert_rejected()
                    getattr(self, side)["created_at"] = before

    def test_age_and_observed_time_are_evaluated_after_both_reads(self):
        self.original["created_at"] = (NOW - timedelta(seconds=MAX_RUN_AGE_SECONDS - 1)).isoformat()
        def clock():
            self.assertEqual(self.fetch.call_count, 2)
            return NOW.timestamp() + 2
        self.reader = self.make_reader(clock=clock)
        self.assert_rejected()

    def test_manual_and_scheduled_queued_or_running_producers_are_supported(self):
        for event in ("workflow_dispatch", "schedule"):
            for status in ("queued", "in_progress"):
                with self.subTest(event=event, status=status):
                    self.attempt.update(event=event, status=status)
                    self.original.update(event=event, status=status)
                    self.reader.read(identity(event_name=event))

    def test_repository_trust_settings_and_positive_ids_are_checked_before_fetch(self):
        cases = [dict(repository="other/project"), dict(repository="example/../other"),
            dict(repository_id="999"), dict(repository_owner_id="999"), dict(repository_owner="other")]
        cases.extend({field: value} for field in ("run_id", "run_attempt")
                     for value in ("0", "01", "-1", "1?x", "../1", "1/2", "1\n", "1" * 21, 1, True))
        for updates in cases:
            with self.subTest(fields=list(updates)):
                self.assert_rejected(identity(**updates))
        self.fetch.assert_not_called()
        with self.assertRaises(github.GitHubRunError):
            self.reader.read(dict(run_id="10001"))
        self.fetch.assert_not_called()

    def test_invalid_server_trust_configuration_is_rejected(self):
        for updates in (dict(repository="https://attacker.invalid"), dict(repository="example/.."),
                        dict(repository="example/."), dict(repository="example/project?query"),
                        dict(repository_id=1234), dict(repository_id="0"), dict(repository_owner_id="01")):
            with self.subTest(fields=list(updates)), self.assertRaises(github.GitHubRunError):
                self.make_reader(**updates)

    def test_official_integer_schema_and_nested_objects_are_required(self):
        changes = [dict(id="10001"), dict(id=True), dict(run_attempt=2.0), dict(actor=[]),
            dict(actor=dict(id="9876")), dict(repository=None),
            dict(repository=dict(id=1234, full_name=REPOSITORY, owner=[])),
            dict(repository=dict(id=1234, full_name=REPOSITORY, owner=dict(id=True)))]
        for update in changes:
            with self.subTest(fields=list(update)):
                self.attempt = payload(**update)
                self.assert_rejected()

    def test_input_bytes_json_shape_depth_duplicates_and_nonfinite_values_are_bounded(self):
        duplicate = raw(payload())[:-1] + b',"id":10001}'
        too_deep = raw(payload())[:-1] + b',"extra":' + b'[' * 20 + b'0' + b']' * 20 + b'}'
        nested_duplicate = raw(payload())[:-1] + b',"extra":{"x":1,"x":2}}'
        nonfinite = raw(payload())[:-1] + b',"extra":NaN}'
        overflow = raw(payload())[:-1] + b',"extra":1e999}'
        for value in (b"", b"x" * (github.MAX_RESPONSE_BYTES + 1), b"[]", b"null", b"{", b"\xff",
                      "text", duplicate, too_deep, nested_duplicate, nonfinite, overflow):
            with self.subTest(kind=type(value)):
                self.fetch.side_effect = None
                self.fetch.return_value = value
                self.assert_rejected()

    def test_fetch_failures_are_sanitized_and_never_retried(self):
        for error in (TimeoutError("secret fixture response"), OSError("secret fixture response")):
            with self.subTest(kind=type(error)):
                self.fetch.reset_mock()
                self.fetch.side_effect = error
                try:
                    self.reader.read(identity())
                except github.GitHubRunError as caught:
                    self.assertNotIn("secret fixture response", "".join(traceback.format_exception(caught)))
                else:
                    self.fail("fetch failure was accepted")
                self.assertEqual(self.fetch.call_count, 1)
        self.fetch.reset_mock()
        self.fetch.side_effect = [raw(self.attempt), TimeoutError("second read failure")]
        self.assert_rejected()
        self.assertEqual(self.fetch.call_count, 2)

    def test_invalid_clock_cannot_create_a_proof(self):
        for value in (None, True, float("nan"), float("inf"), -1, "time"):
            with self.subTest(kind=type(value)):
                self.reader = self.make_reader(clock=lambda: value)
                self.assert_rejected()

    def test_default_fetch_uses_fixed_tls_host_versioned_get_and_no_credentials(self):
        response = Mock(status=200)
        response.getheader.return_value = "identity"
        response.read1.side_effect = [raw(self.attempt), b""]
        connection = Mock()
        connection.getresponse.return_value = response
        with patch.object(github.http.client, "HTTPSConnection", return_value=connection) as connect:
            self.assertEqual(github._fetch_official(PATH + "/attempts/2"), raw(self.attempt))
        self.assertEqual(connect.call_args.args, ("api.github.com",))
        self.assertEqual(connect.call_args.kwargs["timeout"], github.SOCKET_TIMEOUT_SECONDS)
        request = connection.request.call_args
        self.assertEqual(request.args, ("GET", PATH + "/attempts/2"))
        self.assertEqual(request.kwargs["headers"], {"Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2026-03-10", "User-Agent": "rcp-production-admission"})
        self.assertEqual(response.read1.call_args_list[0].args, (github.MAX_RESPONSE_BYTES + 1,))
        response.close.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_redirects_rate_limits_and_other_http_errors_never_yield_metadata(self):
        for status in (301, 302, 307, 308, 304, 401, 403, 404, 410, 429, 500, 503):
            with self.subTest(status=status):
                response = Mock(status=status)
                connection = Mock()
                connection.getresponse.return_value = response
                with patch.object(github.http.client, "HTTPSConnection", return_value=connection):
                    with self.assertRaises(github.GitHubRunError):
                        github._fetch_official(PATH)
                response.read1.assert_not_called()
                connection.request.assert_called_once()
                response.close.assert_called_once_with()
                connection.close.assert_called_once_with()

    def test_oversized_or_compressed_responses_are_rejected(self):
        for compressed in (False, True):
            response = Mock(status=200)
            response.getheader.return_value = "gzip" if compressed else "identity"
            response.read1.return_value = b"x" * (github.MAX_RESPONSE_BYTES + 1)
            connection = Mock()
            connection.getresponse.return_value = response
            with patch.object(github.http.client, "HTTPSConnection", return_value=connection):
                with self.assertRaises(github.GitHubRunError):
                    github._fetch_official(PATH)
            response.close.assert_called_once_with()
            connection.close.assert_called_once_with()

    def test_late_headers_are_rejected_when_control_returns_to_the_body_deadline_check(self):
        response = Mock(status=200)
        response.getheader.return_value = "identity"
        connection = Mock()
        connection.getresponse.return_value = response
        with patch.object(github.http.client, "HTTPSConnection", return_value=connection), \
                patch.object(github.time, "monotonic", side_effect=[0, 4]):
            with self.assertRaises(github.GitHubRunError):
                github._fetch_official(PATH)
        response.read1.assert_not_called()
        response.close.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_fetch_rejects_any_unexpected_path_before_opening_a_connection(self):
        for path in ("https://attacker.invalid/path", "//attacker.invalid/path", PATH + "?next=1",
                     PATH + "/logs", PATH + "/attempts/0", PATH.replace("project", ".."),
                     PATH + "\r\nHeader: x"):
            with self.subTest(path=path), patch.object(github.http.client, "HTTPSConnection") as connect:
                with self.assertRaises(github.GitHubRunError):
                    github._fetch_official(path)
                connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
