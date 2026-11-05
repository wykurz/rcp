"""Offline manual-mode isolation, approval validation and actual CLI output checks."""

import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest
from unittest.mock import Mock

import yaml

from . import acceptance_publication as probe
from . import acceptance_publication_workflow as lane
from . import test_acceptance_publication as fixtures
from .test_acceptance_publication import NOW, SHA, SECRET, approval, token

ROOT = Path(__file__).resolve().parents[2]


def environment(**updates):
    value = dict(RCP_PUBLICATION_MODE="reader-test", RCP_PUBLICATION_APPROVAL=json.dumps(approval()),
        RCP_PRODUCTION_ENABLED="false", RCP_PUBLISH_ENABLED="false", RCP_TRUST_READY="false",
        GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755",
        GITHUB_REPOSITORY_OWNER="wykurz", GITHUB_REPOSITORY_OWNER_ID="483465",
        GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_ACTOR="wykurz",
        GITHUB_ACTOR_ID="483465", GITHUB_TRIGGERING_ACTOR="wykurz", GITHUB_RUN_ATTEMPT="1",
        GITHUB_RUN_ID="10001", GITHUB_SHA=SHA, GITHUB_WORKFLOW_SHA=SHA,
        GITHUB_WORKFLOW_REF=probe.WORKFLOW, GITHUB_WORKFLOW=probe.WORKFLOW_NAME,
        RUNNER_ENVIRONMENT="github-hosted", ACTIONS_ID_TOKEN_REQUEST_TOKEN=SECRET,
        ACTIONS_ID_TOKEN_REQUEST_URL="https://run-actions-1.actions.githubusercontent.com/token?job=1")
    value.update(updates)
    return value


class PublicationWorkflowAdapterTests(unittest.TestCase):
    def test_all_context_approval_and_source_mismatches_precede_tokens_and_clients(self):
        valid = environment()
        invalid = [dict(valid, **{key: "wrong"}) for key in valid if not key.startswith("ACTIONS_")]
        invalid += [environment(RCP_PUBLICATION_APPROVAL=json.dumps({**approval(), **change})) for change in (
            dict(source_commit="c" * 40), dict(not_before=NOW + 1), dict(expires_at=NOW + 45),
            dict(approval_nonce="bad"), dict(role_id="AROA" + "X" * 17), dict(fixture_capture_sha256=""),
            dict(call_plan=probe.CALL_PLAN[:-1]), dict(run_id="10001"), dict(extra="unrecognized"))]
        invalid += [environment(RCP_PUBLICATION_APPROVAL=raw) for raw in ("{}", "[]",
            '{"schema":1,"schema":2}', "x" * 8193)]
        invalid += [environment(**{key: "FALSE"}) for key in
            ("RCP_PRODUCTION_ENABLED", "RCP_PUBLISH_ENABLED", "RCP_TRUST_READY")]
        invalid += [environment(GITHUB_RUN_ID="0"), environment(GITHUB_RUN_ID="001"),
            environment(RCP_PUBLICATION_MODE="READER-TEST")]
        for env in invalid:
            fetch, runner = Mock(), Mock()
            with self.subTest(change={key: value for key, value in env.items() if valid.get(key) != value}):
                result = lane.run(env, clock=lambda: NOW, source=lambda: SHA, fetch=fetch, runner=runner)
                self.assertEqual(result, probe.hold())
                fetch.assert_not_called()
                runner.assert_not_called()
        for source in (lambda: "c" * 40, Mock(side_effect=ValueError(SECRET))):
            fetch, runner = Mock(), Mock()
            self.assertEqual(lane.run(valid, clock=lambda: NOW, source=source, fetch=fetch, runner=runner), probe.hold())
            fetch.assert_not_called()
            runner.assert_not_called()

    def test_approval_nonce_is_correlated_with_actual_run_without_global_single_use_claim(self):
        first = lane.configuration(environment(), clock=lambda: NOW, source=lambda: SHA)
        second = lane.configuration(environment(GITHUB_RUN_ID="20002"), clock=lambda: NOW, source=lambda: SHA)
        self.assertEqual(first["approval_nonce"], second["approval_nonce"])
        self.assertEqual(first["run_id"], "10001")
        self.assertEqual(second["run_id"], "20002")
        self.assertEqual(first["run_attempt"], "1")
        self.assertNotEqual(first, second)
        self.assertEqual(lane.checkout_commit.__module__, "infra.aws_benchmark.acceptance_oidc_workflow")

    def test_empty_or_false_flags_allow_only_one_sts_audience_token_request(self):
        for value in ("", "false"):
            fetch = Mock(return_value=token())
            def runner(cfg, reader, **kwargs):
                self.assertEqual(cfg["run_id"], "10001")
                self.assertEqual(reader(), token())
                return dict(probe.hold(kwargs["progress"]), ok=True)
            result = lane.run(environment(RCP_PRODUCTION_ENABLED=value, RCP_PUBLISH_ENABLED=value, RCP_TRUST_READY=value),
                clock=lambda: NOW, source=lambda: SHA, fetch=fetch, runner=runner)
            self.assertTrue(result["ok"])
            fetch.assert_called_once()
            self.assertEqual(fetch.call_args.args[0], "sts.amazonaws.com")

    def test_token_failure_second_reader_attempt_and_expiry_never_retry(self):
        def runner(cfg, reader, **kwargs):
            reader()
            return reader()
        for fetch in (Mock(side_effect=ValueError(SECRET)), Mock(return_value=token())):
            result = lane.run(environment(), clock=lambda: NOW, source=lambda: SHA, fetch=fetch, runner=runner)
            self.assertEqual(result["diagnostics"], {"stage": "token-request"})
            self.assertFalse(result["ok"])
            fetch.assert_called_once()
            self.assertNotIn(SECRET, json.dumps(result))
        now = [NOW]
        def expired(cfg, reader, **kwargs):
            now[0] = NOW + 301
            return reader()
        fetch = Mock()
        self.assertFalse(lane.run(environment(), clock=lambda: now[0], source=lambda: SHA,
            fetch=fetch, runner=expired)["ok"])
        fetch.assert_not_called()

    def test_real_missing_runner_token_capability_holds_before_any_sdk_client(self):
        for missing in ("ACTIONS_ID_TOKEN_REQUEST_URL", "ACTIONS_ID_TOKEN_REQUEST_TOKEN"):
            env = environment()
            del env[missing]
            factory = Mock()
            def runner(cfg, reader, **kwargs):
                return probe.run_probe(cfg, reader, factory=factory, monotonic=lambda: 0, **kwargs)
            result = lane.run(env, clock=lambda: NOW, source=lambda: SHA, runner=runner)
            self.assertEqual(result, probe.hold(probe.Progress(probe.Stage.TOKEN_REQUEST)))
            factory.assert_not_called()
            self.assertNotIn(SECRET, json.dumps(result))

    def test_main_serializes_the_actual_positive_result_and_sanitized_hold(self):
        case = fixtures.PublicationProbeTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        def run(env, progress):
            def runner(cfg, reader, **kwargs):
                return probe.run_probe(cfg, reader, factory=case.factory, monotonic=lambda: 0, **kwargs)
            return lane.run(env, progress=progress, clock=lambda: NOW, source=lambda: SHA,
                fetch=lambda *args: token(), runner=runner)
        output = io.StringIO()
        self.assertEqual(lane.main([], env=environment(), output=output, runner=run), 0)
        result = json.loads(output.getvalue())
        self.assertTrue(result["ok"])
        self.assertFalse(result["live_accepted"])
        self.assertEqual(result["diagnostics"], {"stage": "complete"})
        self.assertEqual(result["role_id"], probe.ROLE_ID)
        for value in (SECRET, token(), case.credentials["AccessKeyId"]):
            self.assertNotIn(value, output.getvalue())
        runner = Mock()
        output = io.StringIO()
        self.assertEqual(lane.main([SECRET], env=environment(), output=output, runner=runner), 1)
        self.assertEqual(json.loads(output.getvalue()), probe.hold())
        runner.assert_not_called()

    def test_real_cli_no_approval_or_tokens_emits_one_hold_json_without_traceback(self):
        env = {"PATH": os.environ["PATH"], "PYTHONOPTIMIZE": "2", "RCP_PUBLICATION_MODE": "reader-test"}
        for arguments in ([], [SECRET]):
            result = subprocess.run([sys.executable, "-B", "-m", "infra.aws_benchmark.acceptance_publication_workflow",
                *arguments], cwd=ROOT, env=env, text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stdout), probe.hold())
            self.assertEqual(result.stderr, "")
            self.assertNotIn(SECRET, result.stdout)

    def test_real_cli_positive_output_is_from_the_full_injected_reader_path(self):
        program = '''
from infra.aws_benchmark import acceptance_publication_workflow as lane, acceptance_publication as probe
from infra.aws_benchmark.test_acceptance_publication import PublicationProbeTests, NOW, SHA, token
from infra.aws_benchmark.test_acceptance_publication_workflow import environment
case = PublicationProbeTests()
case.setUp()
def run(env, progress):
    def runner(cfg, reader, **kwargs):
        return probe.run_probe(cfg, reader, factory=case.factory, monotonic=lambda:0, **kwargs)
    return lane.run(env, progress=progress, clock=lambda:NOW, source=lambda:SHA,
        fetch=lambda *args:token(), runner=runner)
try:
    raise SystemExit(lane.main([], env=environment(), runner=run))
finally:
    case.doCleanups()
'''
        result = subprocess.run([sys.executable, "-B", "-c", program], cwd=ROOT,
            text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        output = json.loads(result.stdout)
        self.assertTrue(output["ok"])
        self.assertFalse(output["live_accepted"])
        self.assertEqual(output["run_id"], "10001")
        self.assertEqual(result.stderr, "")
        self.assertNotIn(SECRET, result.stdout)
        self.assertNotIn(token(), result.stdout)

    def test_actual_process_timeout_and_sigterm_emit_sanitized_stage_hold(self):
        for cancel in (False, True):
            program = '''
import signal, sys
from infra.aws_benchmark import acceptance_publication_workflow as lane, acceptance_publication as probe
def blocked(env, progress):
    progress.stage = probe.Stage.POSITIVE_OBJECT
    print('ready', file=sys.stderr, flush=True)
    signal.pause()
actual = lane.main
lane.main = lambda:actual([], runner=blocked)
probe.SECONDS = 0.2
raise SystemExit(lane.cli())
'''
            process = subprocess.Popen([sys.executable, "-B", "-c", program], cwd=ROOT,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                self.assertEqual(process.stderr.readline().strip(), "ready")
                if cancel:
                    process.send_signal(signal.SIGTERM)
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 1)
                self.assertEqual(json.loads(stdout), probe.hold(probe.Progress(probe.Stage.POSITIVE_OBJECT)))
                self.assertEqual(stderr, "")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()


class PublicationReaderWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load((ROOT / ".github/workflows/aws-benchmark-publish.yml").read_text(), Loader=yaml.BaseLoader)
        self.job = self.workflow["jobs"]["reader-test"]

    def test_manual_mode_defaults_to_publish_and_reader_does_not_depend_on_writer_jobs(self):
        inputs = self.workflow["on"]["workflow_dispatch"]["inputs"]
        self.assertEqual(inputs["mode"]["default"], "publish")
        self.assertEqual(inputs["mode"]["options"], ["publish", "reader-test"])
        self.assertEqual(set(inputs), {"mode", "reader_approval"})
        for fragment in ("inputs.mode == 'reader-test'", "github.repository == 'wykurz/rcp'",
                "github.repository_id == '655073755'", "github.ref == 'refs/heads/main'",
                "github.event_name == 'workflow_dispatch'", "github.actor_id == '483465'", "github.run_attempt == '1'"):
            self.assertIn(fragment, self.job["if"])
        for flag in ("RCP_AWS_PRODUCTION_ENABLED", "RCP_AWS_PRODUCTION_PUBLISH_ENABLED", "RCP_AWS_HISTORY_TRUST_READY"):
            self.assertIn("(vars." + flag + " == '' || vars." + flag + " == 'false')", self.job["if"])
        self.assertNotIn("needs", self.job)
        self.assertNotIn("environment", self.job)
        for name in ("download", "export", "publish", "pages"):
            self.assertIn("inputs.mode == 'publish'", self.workflow["jobs"][name]["if"])
            self.assertIn("vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'true'", self.workflow["jobs"][name]["if"])
            self.assertIn("vars.RCP_AWS_HISTORY_TRUST_READY == 'true'", self.workflow["jobs"][name]["if"])

    def test_reader_job_has_only_reviewed_dependencies_probe_and_sanitized_artifact(self):
        self.assertEqual(self.job["permissions"], {"contents": "read", "id-token": "write"})
        self.assertEqual(self.job["timeout-minutes"], "5")
        self.assertEqual(self.job["cache-mode"], "none")
        steps = self.job["steps"]
        self.assertEqual(len(steps), 5)
        self.assertEqual(steps[0]["with"], {"ref": "${{ github.workflow_sha }}", "persist-credentials": "false"})
        self.assertEqual(steps[1]["with"], {"python-version": "3.12"})
        self.assertIn("--only-binary=:all: --require-hashes", steps[2]["run"])
        self.assertEqual(steps[3]["run"].strip(),
            'python3 -B -m infra.aws_benchmark.acceptance_publication_workflow > "$RUNNER_TEMP/publication-reader-observation.json"')
        self.assertEqual(steps[3]["env"]["RCP_PUBLICATION_APPROVAL"], "${{ inputs.reader_approval }}")
        self.assertEqual(steps[4]["if"], "always()")
        self.assertEqual(steps[4]["with"]["path"], "${{ runner.temp }}/publication-reader-observation.json")
        self.assertEqual(steps[4]["with"]["retention-days"], "30")
        for step in steps:
            if "uses" in step:
                self.assertRegex(step["uses"], r"^[A-Za-z0-9_/-]+@[0-9a-f]{40}$")
            if "run" in step:
                result = subprocess.run(["bash", "-n"], input=step["run"], text=True, capture_output=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
        for forbidden in ("secrets.", "production_publish", "publication_push", "create-github-app-token",
                "download-artifact", "configure-aws", "lambda", "invoke", "set -x", "tee "):
            self.assertNotIn(forbidden, str(self.job))


if __name__ == "__main__":
    unittest.main()
