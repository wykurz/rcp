"""Offline workflow, signed-token and real HTTP parser tests; never live acceptance."""

import ast
from datetime import datetime, timezone
import http.client
import io
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
import yaml

from infra.aws_benchmark import acceptance_oidc_workflow as lane
from infra.aws_benchmark import acceptance_oidc as probe
from infra.aws_benchmark.test_production_auth import fixture_claims, fixture_jwk

NOW = 1800000000
SHA = "a" * 40
SECRET = "TEST-ONLY-NEVER-LOG"
ENDPOINT = "https://run-actions-1.actions.githubusercontent.com/token?job=1"
ROOT = Path(__file__).resolve().parents[2]


def approval(**updates):
    value = dict(schema=lane.APPROVAL_SCHEMA, source_commit=SHA,
        not_before=NOW - 30, expires_at=NOW + 300, approval_capture_sha256="b" * 64)
    value.update(updates)
    return json.dumps(value)


def environment(**updates):
    value = dict(RCP_ACCEPTANCE_MODE="identity-test", RCP_PRODUCTION_ENABLED="false",
        RCP_REQUESTED_REVISION="", RCP_IDENTITY_APPROVAL=approval(),
        GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755",
        GITHUB_REPOSITORY_OWNER="wykurz", GITHUB_REPOSITORY_OWNER_ID="483465",
        GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch",
        GITHUB_ACTOR="wykurz", GITHUB_ACTOR_ID="483465", GITHUB_TRIGGERING_ACTOR="wykurz",
        GITHUB_RUN_ATTEMPT="1", GITHUB_RUN_ID="10001", GITHUB_SHA=SHA,
        GITHUB_WORKFLOW_SHA=SHA, GITHUB_WORKFLOW_REF=lane.WORKFLOW,
        GITHUB_WORKFLOW="AWS production benchmark", RUNNER_ENVIRONMENT="github-hosted",
        ACTIONS_ID_TOKEN_REQUEST_URL=ENDPOINT, ACTIONS_ID_TOKEN_REQUEST_TOKEN=SECRET)
    value.update(updates)
    return value


def http_response(body, *, status=200, declared=None, extra=b""):
    """Use stdlib framing, including Content-Length truncation, without a socket."""
    raw = (b"HTTP/1.1 " + str(status).encode() + b" Test\r\nContent-Length: " +
           str(len(body) if declared is None else declared).encode() + b"\r\n" + extra + b"\r\n" + body)
    sock = Mock()
    sock.makefile.return_value = io.BytesIO(raw)
    response = http.client.HTTPResponse(sock)
    response.begin()
    return response


class WorkflowAdapterTests(unittest.TestCase):
    def setUp(self):
        network = patch("socket.create_connection", side_effect=AssertionError("unexpected network"))
        network.start()
        self.addCleanup(network.stop)

    def test_all_context_and_approval_failures_precede_tokens_or_sts(self):
        valid = environment()
        invalid = [dict(valid, **{key: "other"}) for key in valid
                   if key not in {"ACTIONS_ID_TOKEN_REQUEST_URL", "ACTIONS_ID_TOKEN_REQUEST_TOKEN"}]
        invalid += [environment(RCP_IDENTITY_APPROVAL=approval(**change)) for change in (
            dict(not_before=NOW + 1), dict(expires_at=NOW + probe.SECONDS),
            dict(expires_at=NOW + 3601), dict(source_commit="c" * 40),
            dict(approval_capture_sha256="bad"), dict(extra="ignored?"))]
        invalid += [environment(RCP_PRODUCTION_ENABLED="FALSE"), environment(RCP_ACCEPTANCE_MODE="IDENTITY-TEST")]
        invalid += [environment(RCP_IDENTITY_APPROVAL=raw) for raw in (
            "{}", "[]", '{"schema":1,"schema":2}', "x" * 4097)]
        for env in invalid:
            with self.subTest(changed={k: v for k, v in env.items() if v != valid.get(k)}):
                fetch, runner = Mock(), Mock()
                self.assertEqual(lane.run(env, source=lambda: SHA, clock=lambda: NOW,
                    fetch=fetch, runner=runner), probe._hold())
                fetch.assert_not_called()
                runner.assert_not_called()

    def test_checkout_and_workflow_sha_must_match_actual_approved_commit(self):
        for source in (lambda: "c" * 40, Mock(side_effect=ValueError(SECRET))):
            fetch, runner = Mock(), Mock()
            self.assertEqual(lane.run(environment(), source=source, clock=lambda: NOW,
                fetch=fetch, runner=runner), probe._hold())
            fetch.assert_not_called()
            runner.assert_not_called()
        cfg = lane.configuration(environment(), source=lambda: SHA, clock=lambda: NOW)
        self.assertEqual((cfg["role_arn"], cfg["role_id"], cfg["admission_audience"]),
                         (lane.ROLE, lane.ROLE_ID, lane.AUDIENCE))
        self.assertEqual(cfg["run_id"], "10001")
        self.assertNotIn(SECRET, json.dumps(cfg))

    def test_actual_git_checkout_rejects_dirty_tracked_and_untracked_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
            def git(*args):
                return subprocess.run(["git", *args], cwd=root, env=env, check=True,
                    capture_output=True, timeout=5).stdout.decode().strip()
            git("init")
            (root / "source").write_text("approved")
            git("add", "source")
            git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "fixture")
            with patch.object(lane, "ROOT", root):
                self.assertEqual(lane.checkout_commit(), git("rev-parse", "HEAD"))
                (root / "extra").write_text("unexpected")
                with self.assertRaises(Exception):
                    lane.checkout_commit()
                (root / "extra").unlink()
                (root / "source").write_text("changed")
                with self.assertRaises(Exception):
                    lane.checkout_commit()

    def test_two_signed_audiences_use_existing_identity_only_adapter(self):
        signer = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwks = json.dumps({"keys": [fixture_jwk(signer)]}).encode()
        def token(audience, env):
            claims = fixture_claims(repository="wykurz/rcp", repository_id="655073755",
                repository_owner="wykurz", repository_owner_id="483465",
                sub="repo:wykurz/rcp:ref:refs/heads/main", workflow_ref=lane.WORKFLOW,
                workflow_sha=SHA, sha=SHA, actor="wykurz", actor_id="483465", aud=audience)
            return jwt.encode(claims, signer, algorithm="RS256", headers={"kid": "test-key"})
        fetch = Mock(side_effect=token)
        name = "rcp-acceptance-10001-1"
        arn = "arn:aws:sts::381183037896:assumed-role/" + lane.ROLE.rsplit("/", 1)[1] + "/" + name
        uid = lane.ROLE_ID + ":" + name
        credentials = dict(AccessKeyId="ASIA" + "B" * 16, SecretAccessKey=SECRET,
            SessionToken=SECRET, Expiration=datetime.fromtimestamp(NOW + 900, timezone.utc))
        unsigned, signed = Mock(), Mock()
        unsigned.assume_role_with_web_identity.return_value = dict(
            Provider=probe.auth.ISSUER, Audience="sts.amazonaws.com",
            SubjectFromWebIdentityToken="repo:wykurz/rcp:ref:refs/heads/main",
            Credentials=credentials, AssumedRoleUser=dict(Arn=arn, AssumedRoleId=uid))
        signed.get_caller_identity.return_value = dict(Account="381183037896", Arn=arn, UserId=uid)
        factory = Mock(side_effect=[unsigned, signed])
        def runner(cfg, reader, **kwargs):
            return probe.run_probe(cfg, reader, factory=factory, jwks_fetcher=lambda: jwks, **kwargs)
        result = lane.run(environment(), source=lambda: SHA, clock=lambda: NOW, fetch=fetch, runner=runner)
        self.assertTrue(result["ok"], result)
        self.assertEqual([c.args[0] for c in fetch.call_args_list], [lane.AUDIENCE, "sts.amazonaws.com"])
        self.assertEqual(unsigned.assume_role_with_web_identity.call_count, 1)
        self.assertEqual(unsigned.assume_role_with_web_identity.call_args.kwargs["Policy"], probe.SESSION_POLICY)
        signed.get_caller_identity.assert_called_once_with()
        self.assertEqual([c[0] for c in signed.method_calls], ["get_caller_identity", "close"])
        self.assertFalse(result["live_accepted"])
        self.assertNotIn(SECRET, json.dumps(result))

    def test_failure_or_second_reader_attempt_never_retries_token_requests(self):
        for failing in (1, 2):
            fetch = Mock(side_effect=[ValueError(SECRET)] if failing == 1 else ["a.b.c", ValueError(SECRET)])
            def runner(cfg, reader, **kwargs):
                return reader()
            result = lane.run(environment(), source=lambda: SHA, clock=lambda: NOW, fetch=fetch, runner=runner)
            self.assertEqual(result, probe._hold(probe.Progress(stage=probe.Stage.TOKEN_REQUEST)))
            self.assertEqual(fetch.call_count, failing)
        fetch = Mock(return_value="a.b.c")
        def repeated(cfg, reader, **kwargs):
            reader()
            return reader()
        self.assertEqual(lane.run(environment(), source=lambda: SHA, clock=lambda: NOW,
            fetch=fetch, runner=repeated), probe._hold(probe.Progress(stage=probe.Stage.TOKEN_REQUEST)))
        self.assertEqual(fetch.call_count, 2)

    def test_expiration_between_fetches_stops_before_second_request(self):
        now = [NOW]
        def fetch(*args):
            now[0] = NOW + 301
            return "a.b.c"
        fetch = Mock(side_effect=fetch)
        def runner(cfg, reader, **kwargs):
            return reader()
        self.assertEqual(lane.run(environment(), source=lambda: SHA, clock=lambda: now[0],
            fetch=fetch, runner=runner), probe._hold(probe.Progress(stage=probe.Stage.TOKEN_REQUEST)))
        fetch.assert_called_once()

    def test_endpoint_and_bearer_validation_precedes_any_connection(self):
        invalid = ["http://run.actions.githubusercontent.com/path",
            "https://evil.example/path", "https://actions.githubusercontent.com.evil.example/path",
            "https://actions.githubusercontent.com/path", "https://user@run.actions.githubusercontent.com/path",
            "https://@run.actions.githubusercontent.com/path", "https://:@run.actions.githubusercontent.com/path",
            "https://run.actions.githubusercontent.com:444/path", ENDPOINT + "#x", ENDPOINT + "&audience=x",
            ENDPOINT + "&%61udience=x", ENDPOINT + "&AUDIENCE=x", "\n" + ENDPOINT,
            ENDPOINT + "\t", ENDPOINT + "&x=" + "a" * 8192]
        for url in invalid:
            factory = Mock()
            with self.subTest(url=url[:80]), self.assertRaisesRegex(ValueError, "^GitHub identity token request failed$"):
                lane.request_token(lane.AUDIENCE, environment(ACTIONS_ID_TOKEN_REQUEST_URL=url),
                                   connection_factory=factory)
            factory.assert_not_called()
        for bearer in ("", "x\r\nHeader:x", "x y", "x" * 16385):
            factory = Mock()
            with self.assertRaises(ValueError):
                lane.request_token(lane.AUDIENCE, environment(ACTIONS_ID_TOKEN_REQUEST_TOKEN=bearer),
                                   connection_factory=factory)
            factory.assert_not_called()

    def test_real_http_framing_bounded_request_and_cleanup_without_proxy(self):
        connection = Mock()
        response = http_response(b'{"value":"aa.bb.cc"}')
        connection.getresponse.return_value = response
        factory = Mock(return_value=connection)
        with patch.dict(os.environ, HTTPS_PROXY="http://evil.invalid"):
            self.assertEqual(lane.request_token(lane.AUDIENCE, environment(), connection_factory=factory), "aa.bb.cc")
        self.assertEqual(factory.call_args.args, ("run-actions-1.actions.githubusercontent.com",))
        context = factory.call_args.kwargs["context"]
        self.assertTrue(context.check_hostname)
        self.assertEqual(connection.request.call_count, 1)
        args = connection.request.call_args
        self.assertEqual(args.args, ("GET", "/token?job=1&audience=" + lane.AUDIENCE))
        self.assertEqual(args.kwargs["headers"]["Authorization"], "Bearer " + SECRET)
        self.assertEqual(args.kwargs["headers"]["Connection"], "close")
        connection.close.assert_called_once()
        self.assertTrue(response.closed)

    def test_transport_rejections_truncation_and_errors_never_leak_or_retry(self):
        cases = [
            http_response(b'{"value":"a.b.c"}', status=302, extra=b"Location: https://evil.invalid\r\n"),
            http_response(SECRET.encode(), status=403), http_response(b"x", status=503),
            http_response(b'{"value":"a.b.c"}', declared=100),
            http_response(b'{"value":"a.b.c"}', extra=b"Content-Encoding: gzip\r\n"),
            http_response(b"x" * (lane.MAX_RESPONSE + 1)),
            http_response(b'{"value":"a.b.c","value":"d.e.f"}'),
            http_response(b'{"value":"not a token"}'), http_response(b"\xff"),
        ]
        for response in cases:
            connection = Mock()
            connection.getresponse.return_value = response
            factory = Mock(return_value=connection)
            with self.assertRaisesRegex(ValueError, "^GitHub identity token request failed$"):
                lane.request_token(lane.AUDIENCE, environment(), connection_factory=factory)
            self.assertEqual(connection.request.call_count, 1)
            self.assertTrue(response.closed)
            connection.close.assert_called_once()
        connection = Mock()
        connection.request.side_effect = OSError(SECRET + ENDPOINT)
        with self.assertRaisesRegex(ValueError, "^GitHub identity token request failed$"):
            lane.request_token(lane.AUDIENCE, environment(), connection_factory=Mock(return_value=connection))
        connection.close.assert_called_once()

    def test_cancellation_during_transport_cleanup_stops_before_next_token_or_sts(self):
        connection = Mock()
        connection.getresponse.return_value = http_response(b'{"value":"a.b.c"}')
        connection.close.side_effect = lane.Interrupted()
        factory = Mock(return_value=connection)
        fetch = Mock(side_effect=lambda aud, env: lane.request_token(aud, env, connection_factory=factory))
        sts = Mock()
        def adapter(cfg, reader, **kwargs):
            reader()
            return sts()
        def runner(env, **kwargs):
            return lane.run(env, source=lambda: SHA, clock=lambda: NOW, fetch=fetch, runner=adapter, **kwargs)
        output = io.StringIO()
        self.assertEqual(lane.main([], env=environment(), output=output, runner=runner), 1)
        self.assertEqual(json.loads(output.getvalue()), probe._hold(probe.Progress(stage=probe.Stage.TOKEN_REQUEST)))
        fetch.assert_called_once()
        sts.assert_not_called()

    def test_outer_cancellation_retains_only_fixed_inner_stage_and_provider_labels(self):
        def runner(env, *, progress):
            progress.stage = probe.Stage.STS_RESPONSE_IDENTITY
            progress.provider = probe.ProviderForm.EXPECTED_PROVIDER_ARN
            raise lane.Interrupted(SECRET)
        output = io.StringIO()
        self.assertEqual(lane.main([], env=environment(), output=output, runner=runner), 1)
        result = json.loads(output.getvalue())
        self.assertEqual(result, probe._hold(probe.Progress(
            stage=probe.Stage.STS_RESPONSE_IDENTITY, provider=probe.ProviderForm.EXPECTED_PROVIDER_ARN)))
        self.assertNotIn(SECRET, output.getvalue())

    def test_cli_and_process_alarm_or_cancellation_emit_only_sanitized_hold(self):
        result = subprocess.run([sys.executable, "-B", "-m", "infra.aws_benchmark.acceptance_oidc_workflow"],
            cwd=ROOT, env={"PATH": os.environ["PATH"]}, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout), probe._hold())
        self.assertEqual(result.stderr, "")
        for cancel in (False, True):
            program = """import time
from infra.aws_benchmark import acceptance_oidc_workflow as lane
lane.probe.SECONDS = 0.2 if not CANCEL else 5
def blocked(env, **kwargs):
    print('ready', flush=True)
    time.sleep(10)
original = lane.main
lane.main = lambda: original([], env={}, runner=blocked)
raise SystemExit(lane.cli())
"""
            program = program.replace("CANCEL", repr(cancel))
            process = subprocess.Popen([sys.executable, "-B", "-c", program], cwd=ROOT,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.assertEqual(process.stdout.readline().strip(), "ready")
                if cancel:
                    process.send_signal(signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 1)
                self.assertEqual(json.loads(stdout), probe._hold())
                self.assertEqual(stderr, "")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()


class WorkflowModeTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load((ROOT / ".github/workflows/aws-benchmark-production.yml").read_text(),
                                  Loader=yaml.BaseLoader)

    def enabled_jobs(self, mode, *, enabled="false", **updates):
        values = {"inputs.mode": mode, "github.repository": "wykurz/rcp",
            "github.repository_id": "655073755", "github.ref": "refs/heads/main",
            "github.event_name": "workflow_dispatch", "github.actor_id": "483465",
            "github.run_attempt": 1, "vars.RCP_AWS_PRODUCTION_ENABLED": enabled}
        values.update(updates)
        selected = set()
        for name, job in self.workflow["jobs"].items():
            expression = job["if"]
            expression = re.sub(r"\b(?:inputs|github|vars)\.[A-Za-z_]+",
                                lambda match: repr(values[match[0]]), expression)
            expression = expression.replace("&&", " and ").replace("||", " or ")
            tree = ast.parse(expression, mode="eval")
            allowed = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.Compare, ast.Eq, ast.Constant)
            self.assertTrue(all(isinstance(node, allowed) for node in ast.walk(tree)))
            def evaluate(node):
                if isinstance(node, ast.Constant):
                    return node.value
                if isinstance(node, ast.BoolOp):
                    children = [evaluate(child) for child in node.values]
                    return all(children) if isinstance(node.op, ast.And) else any(children)
                if isinstance(node, ast.Compare):
                    left, right = evaluate(node.left), evaluate(node.comparators[0])
                    if type(left) is str and type(right) is str:
                        return left.casefold() == right.casefold()
                    if type(left) is not type(right):
                        def number(value):
                            try:
                                return float(value or 0)
                            except (TypeError, ValueError):
                                return math.nan
                        return number(left) == number(right)
                    return left == right
                raise AssertionError("unsupported workflow expression")
            condition = evaluate(tree.body)
            needs = job.get("needs", [])
            needs = [needs] if isinstance(needs, str) else needs
            if condition and all(parent in selected for parent in needs):
                selected.add(name)
        return selected

    def test_mode_dependency_graph_has_no_route_from_identity_to_benchmark(self):
        for mode in ("", "unknown", "identity-test", "benchmark", "IDENTITY-TEST", "BENCHMARK"):
            for enabled in ("", "false", "FALSE", "true", "TRUE", "unknown"):
                expected = ({"identity-test"} if mode.casefold() == "identity-test" and enabled.casefold() in {"", "false"} else
                    {"validate", "build", "submit"} if mode.casefold() == "benchmark" and enabled.casefold() == "true" else set())
                self.assertEqual(self.enabled_jobs(mode, enabled=enabled), expected)
        self.assertEqual(self.enabled_jobs("identity-test", **{"github.run_attempt": "1"}), {"identity-test"})
        for field, value in (("github.actor_id", "1"), ("github.ref", "refs/heads/debug"),
                             ("github.event_name", "schedule"), ("github.run_attempt", 2),
                             ("github.repository_id", "1"), ("github.repository", "fork/rcp")):
            self.assertEqual(self.enabled_jobs("identity-test", **{field: value}), set())
        self.assertEqual(set(self.workflow["on"]), {"workflow_dispatch"})
        self.assertEqual(self.workflow["on"]["workflow_dispatch"]["inputs"]["mode"]["default"], "benchmark")

    def test_identity_job_has_only_pinned_controller_dependencies_probe_and_safe_artifact(self):
        job = self.workflow["jobs"]["identity-test"]
        self.assertEqual(job["permissions"], {"contents": "read", "id-token": "write"})
        self.assertEqual(job["cache-mode"], "none")
        self.assertEqual(job["timeout-minutes"], "5")
        self.assertEqual(job["runs-on"], "ubuntu-24.04")
        self.assertNotIn("environment", job)
        self.assertNotIn("needs", job)
        for forbidden in ("secrets.", "production_client", "download-artifact", "configure-aws", "cargo", "just "):
            self.assertNotIn(forbidden, str(job))
        steps = job["steps"]
        self.assertEqual(steps[0]["with"], {"ref": "${{ github.workflow_sha }}", "persist-credentials": "false"})
        self.assertEqual(steps[1]["with"], {"python-version": "3.12"})
        self.assertIn("--only-binary=:all: --require-hashes", steps[2]["run"])
        self.assertEqual(steps[3]["run"].strip(),
            'python3 -B -m infra.aws_benchmark.acceptance_oidc_workflow > "$RUNNER_TEMP/oidc-observation.json"')
        self.assertEqual(set(steps[3]["env"]), {"RCP_ACCEPTANCE_MODE", "RCP_PRODUCTION_ENABLED",
                                             "RCP_REQUESTED_REVISION", "RCP_IDENTITY_APPROVAL"})
        self.assertEqual(steps[4]["with"]["path"], "${{ runner.temp }}/oidc-observation.json")
        self.assertEqual(steps[4]["with"]["retention-days"], "30")
        self.assertEqual(steps[4]["if"], "always()")


if __name__ == "__main__":
    unittest.main()
