"""Manual negative-workflow gates, safe runner evidence and cancellation."""
import functools
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
import yaml

from . import acceptance_oidc_negative as probe
from .acceptance_cases import Hold
from . import acceptance_oidc_negative_workflow as adapter
from .test_acceptance_oidc_negative import NOW, SHA, RUN, SECRET, config, claims, denial


def environment(directory):
    approval = dict(schema=adapter.APPROVAL_SCHEMA, source_commit=SHA,
        not_before=NOW - 60, expires_at=NOW + 300, recovery_cutoff=NOW + 3600,
        approval_capture_sha256="b" * 64)
    return dict(GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755",
        GITHUB_REPOSITORY_OWNER="wykurz", GITHUB_REPOSITORY_OWNER_ID="483465",
        GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch",
        GITHUB_ACTOR="wykurz", GITHUB_ACTOR_ID="483465", GITHUB_TRIGGERING_ACTOR="wykurz",
        GITHUB_RUN_ATTEMPT="1", GITHUB_RUN_ID=RUN, GITHUB_WORKFLOW_REF=probe.WORKFLOW,
        GITHUB_WORKFLOW=probe.NAME, GITHUB_SHA=SHA, GITHUB_WORKFLOW_SHA=SHA,
        RUNNER_ENVIRONMENT="github-hosted", RUNNER_TEMP=directory,
        RCP_PRODUCTION_ENABLED="false", RCP_IDENTITY_APPROVAL=json.dumps(approval))


class NegativeWorkflowTests(unittest.TestCase):
    def test_valid_configuration_pins_exact_ref_actor_source_role_and_window(self):
        self.assertEqual(adapter.configuration(environment("/tmp"), source=lambda: SHA,
            clock=lambda: NOW), config())

    def test_invalid_context_source_or_approval_never_fetches_a_token(self):
        mutations = dict(GITHUB_ACTOR_ID="999", GITHUB_TRIGGERING_ACTOR="other",
            GITHUB_REF="refs/heads/other", GITHUB_EVENT_NAME="push", GITHUB_RUN_ATTEMPT="2",
            GITHUB_WORKFLOW="AWS production benchmark", GITHUB_SHA="c" * 40,
            RCP_PRODUCTION_ENABLED="true", RCP_IDENTITY_APPROVAL='{"schema":"a","schema":"b"}')
        for key, value in mutations.items():
            fetch = Mock()
            result = adapter.run(environment("/tmp") | {key: value}, fetch=fetch,
                source=lambda: SHA, clock=lambda: NOW)
            with self.subTest(key=key):
                self.assertFalse(result["ok"])
                fetch.assert_not_called()
        fetch = Mock()
        self.assertFalse(adapter.run(environment("/tmp"), fetch=fetch,
            source=lambda: "c" * 40, clock=lambda: NOW)["ok"])
        fetch.assert_not_called()

    def test_existing_and_symlink_journal_hold_without_fetch_and_preserve_target(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / adapter.JOURNAL_NAME
            # Public binary fixture tests preservation; leak markers belong only in leak checks.
            sentinel = b"public existing-file fixture\x00preserve exact bytes\n"
            path.write_bytes(sentinel)
            fetch = Mock()
            result = adapter.run(environment(directory), fetch=fetch,
                source=lambda: SHA, clock=lambda: NOW)
            self.assertFalse(result["ok"])
            fetch.assert_not_called()
            self.assertEqual(path.read_bytes(), sentinel)
            path.unlink()
            target = Path(directory) / "target"
            target.write_bytes(sentinel)
            path.symlink_to(target)
            self.assertFalse(adapter.run(environment(directory), fetch=fetch,
                source=lambda: SHA, clock=lambda: NOW)["ok"])
            fetch.assert_not_called()
            self.assertEqual(target.read_bytes(), sentinel)

    def test_journal_rejects_extra_fields_or_labels(self):
        with tempfile.TemporaryDirectory() as directory:
            with adapter.Journal(directory, config()) as journal:
                value = dict(event="configuration", diagnostics=probe.Progress().snapshot())
                journal.emit(value)
                for bad in (value | {"token": SECRET}, dict(event=SECRET, diagnostics=value["diagnostics"]),
                        dict(event="denied", diagnostics=value["diagnostics"] | {"provider": SECRET})):
                    with self.assertRaises(Hold):
                        journal.emit(bad)
            path = Path(directory) / adapter.JOURNAL_NAME
            self.assertNotIn(SECRET, path.read_text())
            self.assertEqual(len(path.read_text().splitlines()), 1)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)

    def test_real_adapter_signed_fixture_single_send_and_fsynced_journal(self):
        signer = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(signer.public_key()))
        jwk.update(kid="test-key", use="sig", alg="RS256")
        jwks = json.dumps({"keys": [jwk]}).encode()
        token = jwt.encode(claims(), signer, algorithm="RS256", headers={"kid": "test-key"})
        client = Mock()
        client.assume_role_with_web_identity.side_effect = denial()
        runner = functools.partial(probe.run_probe, factory=Mock(return_value=client),
            jwks_fetcher=lambda: jwks, monotonic=lambda: 0)
        with tempfile.TemporaryDirectory() as directory, patch("socket.create_connection",
                side_effect=AssertionError("unexpected network")):
            fetch = Mock(return_value=token)
            with patch.object(adapter.os, "fsync", wraps=adapter.os.fsync) as fsync:
                result = adapter.run(environment(directory), source=lambda: SHA,
                    clock=lambda: NOW, fetch=fetch, runner=runner)
                self.assertEqual(fsync.call_count, 3)
            self.assertTrue(result["ok"])
            fetch.assert_called_once_with("sts.amazonaws.com", environment(directory))
            entries = [json.loads(x) for x in (Path(directory) / adapter.JOURNAL_NAME).read_text().splitlines()]
            self.assertEqual([v["event"] for v in entries], ["configuration", "before-sts-send", "denied"])
            self.assertEqual([v["sequence"] for v in entries], [0, 1, 2])
            self.assertEqual(len({v["bindings_sha256"] for v in entries}), 1)
            self.assertNotIn(token, json.dumps(entries) + json.dumps(result))
            self.assertEqual(client.assume_role_with_web_identity.call_count, 1)

    def test_token_reader_refuses_a_second_request(self):
        fetch = Mock(return_value="test-only-not-a-jwt")
        def twice(config, reader, **kwargs):
            reader()
            reader()
        with tempfile.TemporaryDirectory() as directory:
            result = adapter.run(environment(directory), fetch=fetch, runner=twice,
                source=lambda: SHA, clock=lambda: NOW)
            self.assertFalse(result["ok"])
            self.assertEqual(fetch.call_count, 1)

    def test_main_cancellation_retains_safe_uncertain_horizon(self):
        output = io.StringIO()
        def cancelled(env, *, progress):
            progress.stage = probe.Stage.STS
            progress.recovery = probe.Recovery(True, NOW, NOW + 300, None, NOW + 1560)
            raise adapter.Interrupted(SECRET)
        status = adapter.main([], env={}, output=output, runner=cancelled)
        result = json.loads(output.getvalue())
        self.assertEqual(status, 1)
        self.assertTrue(result["diagnostics"]["recovery"]["possible_session"])
        self.assertEqual(result["diagnostics"]["recovery"]["reconcile_until"], NOW + 1560)
        self.assertNotIn(SECRET, output.getvalue())

    def test_cli_arms_timer_and_all_cancellation_signals(self):
        with patch.object(adapter.signal, "signal") as handler, \
                patch.object(adapter.signal, "setitimer") as timer, \
                patch.object(adapter, "main", return_value=0):
            self.assertEqual(adapter.cli(), 0)
        self.assertEqual(handler.call_count, 3)
        self.assertEqual(timer.call_args_list[0].args, (adapter.signal.ITIMER_REAL, 45))
        self.assertEqual(timer.call_args_list[-1].args, (adapter.signal.ITIMER_REAL, 0))

    def test_workflow_is_manual_owner_main_readonly_and_bounded(self):
        path = Path(__file__).resolve().parents[2] / ".github/workflows/aws-benchmark-identity-denial.yml"
        value = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(value["on"]), {"workflow_dispatch"})
        self.assertEqual(value["permissions"], {})
        job = value["jobs"]["deny-wrong-workflow"]
        self.assertEqual(job["permissions"], {"contents": "read", "id-token": "write"})
        self.assertEqual(job["timeout-minutes"], "5")
        self.assertEqual(job["cache-mode"], "none")
        self.assertNotIn("environment", job)
        for guard in ("github.ref == 'refs/heads/main'", "github.actor_id == '483465'",
                "github.run_attempt == 1", "vars.RCP_AWS_PRODUCTION_ENABLED == 'false'"):
            self.assertIn(guard, job["if"])
        self.assertEqual(job["steps"][0]["with"]["persist-credentials"], "false")
        self.assertEqual(job["steps"][0]["with"]["ref"], "$" + "{{ github.workflow_sha }}")
        self.assertEqual(job["steps"][-1]["with"]["retention-days"], "30")
        self.assertEqual(len(job["steps"][-1]["with"]["path"].splitlines()), 2)


if __name__ == "__main__":
    unittest.main()
