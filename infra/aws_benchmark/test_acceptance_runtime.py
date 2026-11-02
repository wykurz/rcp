"""Offline fixed-binding/runtime contract tests; no service clients leave the process."""

import copy
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from botocore.exceptions import ClientError

from . import acceptance_runtime as runtime
from .acceptance_bindings import validate_bindings, parse_bindings, binding_bytes, digest_bindings
from .acceptance_cases import Hold
from .production_github import GitHubRunError


def fixture(kind="reader-admission"):
    role = "arn:aws:iam::381183037896:role/production-admission"
    name = "rcp-production-benchmark-f13-admission-probe"
    cases = dict(account="381183037896", region="us-east-1", role_arn=role,
        reader_kind="admission", secret_arn="arn:aws:secretsmanager:us-east-1:381183037896:secret:/rcp/production/github-read-app-Ab1234",
        current_version="a" * 32, previous_version="b" * 32,
        unrelated_secret_arn="arn:aws:secretsmanager:us-east-1:381183037896:secret:/rcp/acceptance/reader-denied-Cd5678",
        app_client_id="Iv1.reviewed", installation_id="12345")
    inventory = dict(current_version=cases["current_version"], previous_version=cases["previous_version"],
                     previous_stage_exists=True, unrelated_secret_exists=True)
    proof = dict(run_id="123", run_attempt="1", head_sha="c" * 40)
    log = "/aws/lambda/rcp-production-benchmark-admission"
    if kind in {"s3", "dynamodb"}:
        nonce = "a" * 16
        name = "rcp-production-acceptance-" + kind + "-" + nonce
        role = "arn:aws:iam::381183037896:role/" + name
        cases = dict(account="381183037896", region="us-east-1", role_arn=role,
                     bucket="rcp-production-acceptance-" + nonce, table="rcp-production-acceptance-" + nonce, nonce=nonce)
        inventory, proof, log = None, None, "/aws/lambda/" + name
    elif kind == "reader-collector":
        cases["reader_kind"] = "collector"
        cases["role_arn"] = role = role.replace("admission", "collector")
        name, log = name.replace("admission", "collector"), log.replace("admission", "collector")
    return dict(schema="rcp-production-acceptance-v1", kind=kind, account="381183037896", region="us-east-1",
                source_commit="d" * 40, role_arn=role, role_id="AROA" + "A" * 17,
                function_name=name, function_version="1", log_group=log, not_before=900, expires_at=5000,
                approval_capture_sha256="e" * 64, cases=cases, fixture_inventory=inventory, proof=proof)


def context(config):
    seconds = 45 if config["kind"].startswith("reader-") else 120
    return SimpleNamespace(invoked_function_arn="arn:aws:lambda:us-east-1:381183037896:function:" +
        config["function_name"] + ":1", function_version="1", log_group_name=config["log_group"],
        get_remaining_time_in_millis=lambda: seconds * 1000 - 1)


def identity(config):
    return dict(Account=config["account"], Arn="arn:aws:sts::" + config["account"] + ":assumed-role/" +
        config["role_arn"].rsplit("/", 1)[1] + "/probe-session", UserId=config["role_id"] + ":probe-session")


class FakeSecrets:
    def __init__(self, config):
        self.config, self.calls, self.closed = config, [], False
    def get_secret_value(self, **request):
        self.calls.append(request)
        c = self.config["cases"]
        if len(self.calls) == 1:
            return dict(ARN=c["secret_arn"], VersionId=c["current_version"], VersionStages=["AWSCURRENT"],
                        SecretString="secret-value-never-emit")
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "sensitive-do-not-emit"},
                           "ResponseMetadata": {"HTTPStatusCode": 400}}, "GetSecretValue")
    def close(self):
        self.closed = True


def proof(config, secrets):
    c = config["cases"]
    return dict(repository_id="655073755", owner_id="483465", app_client_id=c["app_client_id"],
                installation_id=c["installation_id"], actions="read", metadata="read", fixed_proof_read=True)


class BindingTests(unittest.TestCase):
    def test_all_fixed_phases_validate_without_clients(self):
        for kind in ("reader-admission", "reader-collector", "s3", "dynamodb"):
            cfg = fixture(kind)
            self.assertEqual(validate_bindings(cfg), cfg)
            self.assertEqual(parse_bindings(binding_bytes(cfg)), cfg)
            self.assertEqual(len(digest_bindings(cfg)), 64)
    def test_unknown_or_mutated_bindings_are_rejected(self):
        mutations = [("unexpected", "secret"), ("account", "000000000000"), ("role_id", "session"),
                     ("function_version", "$LATEST"), ("expires_at", True), ("source_commit", "main"),
                     ("expires_at", 1000000), ("region", "us-west-2"), ("function_name", "other")]
        for key, value in mutations:
            cfg = fixture()
            cfg[key] = value
            with self.subTest(key=key), self.assertRaises((Hold, TypeError)):
                validate_bindings(cfg)
    def test_storage_cannot_adopt_production_role(self):
        cfg = fixture("s3")
        cfg["role_arn"] = cfg["cases"]["role_arn"] = "arn:aws:iam::381183037896:role/production-admission"
        with self.assertRaises(Hold):
            validate_bindings(cfg)
    def test_fixture_truth_values_are_not_integers(self):
        for name in ("previous_stage_exists", "unrelated_secret_exists"):
            cfg = fixture()
            cfg["fixture_inventory"][name] = 1
            with self.assertRaises(Hold):
                validate_bindings(cfg)

    def test_missing_fixture_evidence_and_duplicate_json_hold(self):
        cfg = fixture()
        cfg["fixture_inventory"]["previous_stage_exists"] = False
        with self.assertRaises(Hold):
            validate_bindings(cfg)
        with self.assertRaises(Hold):
            parse_bindings(b'{"kind":"s3","kind":"dynamodb"}')
    def test_caller_mutation_does_not_change_validated_snapshot(self):
        cfg = fixture()
        checked = validate_bindings(cfg)
        cfg["cases"]["current_version"] = "f" * 32
        self.assertNotEqual(cfg, checked)


class RuntimeTests(unittest.TestCase):
    def run_reader(self, cfg=None, ident=None, event=None, ctx=None):
        cfg = cfg or fixture()
        secrets = FakeSecrets(cfg)
        sts = mock.Mock()
        sts.get_caller_identity.return_value = ident or identity(cfg)
        factory = mock.Mock(side_effect=lambda service, region: {"sts": sts, "secretsmanager": secrets}[service])
        result = runtime.run_probe(cfg, {} if event is None else event, ctx or context(cfg),
            factory=factory, proof_reader=proof, clock=lambda: 1000)
        return result, factory, sts, secrets
    def test_exact_reader_case_observation_never_claims_acceptance(self):
        result, factory, sts, secrets = self.run_reader()
        self.assertTrue(result["ok"])
        self.assertFalse(result["live_accepted"])
        self.assertEqual(result["bindings_sha256"], digest_bindings(fixture()))
        self.assertEqual(factory.call_args_list, [mock.call("sts", "us-east-1"), mock.call("secretsmanager", "us-east-1")])
        self.assertEqual(len(secrets.calls), 5)
        self.assertTrue(secrets.closed)
        sts.close.assert_called_once()
        self.assertNotIn("secret-value", json.dumps(result))
    def test_invalid_event_context_or_expiry_precedes_all_clients(self):
        cfg = fixture()
        cases = [({"target": "other"}, context(cfg)), ({}, SimpleNamespace(invoked_function_arn="bad"))]
        for event, ctx in cases:
            factory = mock.Mock()
            self.assertFalse(runtime.run_probe(cfg, event, ctx, factory=factory, clock=lambda: 1000)["ok"])
            factory.assert_not_called()
        factory = mock.Mock()
        self.assertFalse(runtime.run_probe(cfg, {}, context(cfg), factory=factory, clock=lambda: 4990)["ok"])
        factory.assert_not_called()
    def test_immutable_role_id_and_account_are_required_before_secret_read(self):
        cfg = fixture()
        for key, value in (("Account", "000000000000"), ("UserId", cfg["role_id"]),
                           ("UserId", "AROA" + "B" * 17 + ":probe-session"), ("Arn", cfg["role_arn"])):
            ident = identity(cfg)
            ident[key] = value
            result, factory, sts, secrets = self.run_reader(ident=ident)
            self.assertFalse(result["ok"])
            self.assertEqual(factory.call_count, 1)
            self.assertFalse(secrets.calls)
            sts.close.assert_called_once()
    def test_provider_exception_and_sdk_response_are_redacted(self):
        cfg = fixture()
        sts = mock.Mock()
        sts.get_caller_identity.side_effect = RuntimeError("jwt-secret-and-credentials")
        result = runtime.run_probe(cfg, {}, context(cfg), factory=lambda *_: sts, clock=lambda: 1000)
        self.assertFalse(result["ok"])
        self.assertNotIn("jwt-secret", json.dumps(result))
        sts.close.assert_called_once()
    def test_sdk_clients_have_no_retries_proxy_or_configured_endpoint(self):
        with mock.patch("boto3.client") as create:
            runtime.sdk_client("secretsmanager", "us-east-1")
        args = create.call_args.kwargs
        self.assertEqual(args["endpoint_url"], "https://secretsmanager.us-east-1.amazonaws.com")
        cfg = args["config"]
        self.assertEqual(cfg.retries, {"total_max_attempts": 1})
        self.assertEqual(cfg.proxies, {})
        self.assertTrue(cfg.ignore_configured_endpoint_urls)
        self.assertEqual((cfg.connect_timeout, cfg.read_timeout), (1, 3))
    def test_handler_rejects_warm_replay_and_does_not_execute_missing_config(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "acceptance-bindings.json"
            with mock.patch.object(runtime, "BINDING_FILE", path), mock.patch.object(runtime, "_USED", False):
                self.assertFalse(runtime.lambda_handler({}, None)["ok"])
                path.write_bytes(binding_bytes(fixture()))
                with mock.patch.object(runtime, "run_probe") as execute:
                    self.assertFalse(runtime.lambda_handler({}, context(fixture()))["ok"])
                    execute.assert_not_called()
    def test_provider_secret_refresh_cannot_adopt_rotated_or_misbound_key(self):
        cfg = fixture()
        c = cfg["cases"]
        current = dict(ARN=c["secret_arn"], VersionId=c["current_version"],
                       VersionStages=["AWSCURRENT"], SecretString="never-log-key")
        client = mock.Mock()
        bound = runtime.BoundCurrentSecret(c, client)
        request = dict(SecretId=c["secret_arn"], VersionStage="AWSCURRENT")
        client.get_secret_value.return_value = current
        self.assertEqual(bound.get_secret_value(**request), current)
        for patch in (dict(VersionId="f" * 32), dict(ARN=c["unrelated_secret_arn"]),
                      dict(VersionStages=["AWSPREVIOUS"]), dict(VersionStages=["AWSCURRENT", "AWSCURRENT"])):
            client.get_secret_value.return_value = dict(current, **patch)
            with self.assertRaises(Hold):
                bound.get_secret_value(**request)
        client.reset_mock()
        with self.assertRaises(Hold):
            bound.get_secret_value(SecretId=c["secret_arn"])
        client.get_secret_value.assert_not_called()

    def test_actual_proof_provider_receives_bound_secret_adapter(self):
        cfg = fixture()
        secrets = mock.Mock()
        secrets.get_secret_value.return_value = dict(ARN=cfg["cases"]["secret_arn"],
            VersionId="f" * 32, VersionStages=["AWSCURRENT"], SecretString="rotated-sensitive")
        class Provider:
            def __init__(self, *args, **kwargs):
                self.secrets = kwargs["secrets"]
            @contextmanager
            def observation(self):
                self.secrets.get_secret_value(SecretId=cfg["cases"]["secret_arn"], VersionStage="AWSCURRENT")
                yield lambda _: self.fail("proof must not run")
        with self.assertRaises(GitHubRunError):
            runtime.github_proof(cfg, secrets, provider_factory=Provider)
        self.assertEqual(secrets.get_secret_value.call_count, 1)

    def test_actual_terminal_reader_binds_fixed_run_and_job_wire_shapes(self):
        cfg = fixture()
        repo = dict(id=655073755, full_name="wykurz/rcp", owner=dict(id=483465, login="wykurz"))
        run = dict(id=123, run_attempt=1, repository=repo, actor=dict(id=483465),
                   head_sha="c" * 40, status="completed", conclusion="success",
                   path=".github/workflows/rust.yml", event="push", head_branch="main")
        calls = []
        class Provider:
            @contextmanager
            def observation(self):
                def fetch(path):
                    calls.append(path)
                    if path.endswith("pending_deployments"):
                        return b"[]"
                    if "/jobs?" in path:
                        return json.dumps(dict(total_count=1, jobs=[dict(run_id=123, head_sha="c" * 40, status="completed")])).encode()
                    return json.dumps(run).encode()
                yield fetch
        output = runtime.github_proof(cfg, object(), provider_factory=lambda *_, **__: Provider())
        self.assertEqual(output, proof(cfg, None))
        self.assertEqual(len(calls), 5)
        self.assertTrue(all(path.startswith("/repos/wykurz/rcp/actions/runs/123") for path in calls))
        run["head_sha"] = "f" * 40
        with self.assertRaises(Hold):
            runtime.github_proof(cfg, object(), provider_factory=lambda *_, **__: Provider())


if __name__ == "__main__":
    unittest.main()
