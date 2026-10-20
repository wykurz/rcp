"""Offline contracts between the workflow, baked worker, harness, and mailbox."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

from benchmarks import run as harness
from infra.aws_benchmark import cache_client, common, worker


ROOT = Path(__file__).resolve().parents[2]


def workflow():
    return yaml.safe_load((ROOT / ".github/workflows/aws-benchmark.yml").read_text())


class BuildContractTests(unittest.TestCase):
    def test_validate_job_exports_current_protocol_binary_limit(self):
        job = workflow()["jobs"]["validate"]
        step = next(step for step in job["steps"] if step.get("id") == "protocol")
        self.assertEqual(job["outputs"]["max-binary"], "$" + "{{ steps.protocol.outputs.max-binary }}")
        self.assertEqual(workflow()["jobs"]["build"]["env"]["MAX_BINARY"],
                         "$" + "{{ needs.validate.outputs.max-binary }}")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            source = step["run"].split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
            env = dict(os.environ, GITHUB_OUTPUT=str(output), GITHUB_RUN_ID="123", GITHUB_RUN_ATTEMPT="1",
                       BASELINE="a" * 40, CANDIDATE="a" * 40, SUITE="aa-smoke",
                       GITHUB_REF="refs/heads/main", GITHUB_SHA="c" * 40, GITHUB_REPOSITORY="wykurz/rcp",
                       GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark.yml@refs/heads/main",
                       GITHUB_WORKFLOW_SHA="c" * 40, GITHUB_EVENT_NAME="workflow_dispatch")
            subprocess.run([sys.executable, "-c", source], cwd=ROOT, env=env, check=True, timeout=10)
            self.assertEqual(output.read_text(), f"max-binary={common.MAX_BINARY}\n")

    def prepare_copies(self, directory, contents):
        root = Path(directory)
        source = root / "target/x86_64-unknown-linux-musl/release"
        source.mkdir(parents=True)
        for name in {key.split("/")[1] for key in common.BINARIES}:
            (source / name).write_bytes(contents)
        tools = root / "tools"
        tools.mkdir()
        strip = tools / "strip"
        # emulate debug removal to exercise the real workflow shell and its ordering
        strip.write_text(f"#!{sys.executable}\n"
                         "import pathlib, sys\n"
                         "assert sys.argv[1] == '--strip-debug'\n"
                         "path = pathlib.Path(sys.argv[2])\n"
                         "path.write_bytes(path.read_bytes().removesuffix(b'DEBUG'))\n")
        strip.chmod(0o755)
        env = dict(os.environ, RUNNER_TEMP=str(root / "runner"), MAX_BINARY="64",
                   PATH=str(tools) + os.pathsep + os.environ["PATH"])
        step = next(step for step in workflow()["jobs"]["build"]["steps"]
                    if step.get("name") == "Build before opening the expensive worker")
        script = step["run"][step["run"].index('mkdir -p "$RUNNER_TEMP/binaries"'):]
        result = subprocess.run(["bash", "-e", "-c", script], cwd=root, env=env,
                                capture_output=True, text=True, timeout=10)
        return result, source, root / "runner/binaries"

    def test_build_strips_only_copies_then_accepts_bounded_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            original = b"x" * 64 + b"DEBUG"
            result, source, output = self.prepare_copies(directory, original)
            self.assertEqual(result.returncode, 0, result.stderr)
            names = {key.split("/")[1] for key in common.BINARIES}
            self.assertEqual({path.name for path in output.iterdir()}, names)
            for name in names:
                self.assertEqual((source / name).read_bytes(), original)
                self.assertEqual((output / name).read_bytes(), b"x" * 64)

    def test_build_rejects_empty_or_oversized_artifacts_before_upload(self):
        for contents in (b"DEBUG", b"x" * 65 + b"DEBUG"):
            with self.subTest(size=len(contents)), tempfile.TemporaryDirectory() as directory:
                result, _, _ = self.prepare_copies(directory, contents)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("required range is 1..64", result.stdout)

    def test_every_sdk_requirement_has_exact_version_and_wheel_hash(self):
        requirements = (ROOT / "infra/aws_benchmark/requirements.txt").read_text()
        lines = [line.strip() for line in requirements.replace("\\\n", "").splitlines()
                 if line.strip() and not line.startswith("#")]
        self.assertEqual(lines.pop(0), "--only-binary=:all:")
        names = []
        for line in lines:
            match = re.fullmatch(r"([a-z0-9-]+)==([0-9]+(?:\.[a-z0-9]+)*)\s+--hash=sha256:[0-9a-f]{64}", line)
            self.assertIsNotNone(match, line)
            names.append(match[1])
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(set(names), {"boto3", "botocore", "jmespath", "python-dateutil",
                                     "s3transfer", "six", "urllib3"})

    def test_both_privileged_install_paths_require_hashes(self):
        installer = (ROOT / "infra/aws_benchmark/install-worker.sh").read_text()
        installs = [line for line in installer.splitlines() if "/pip install " in line]
        installs += [line for step in workflow()["jobs"]["benchmark"]["steps"]
                     for line in step.get("run", "").splitlines() if '/pip" install ' in line]
        self.assertEqual(len(installs), 2)
        for line in installs:
            self.assertIn("install --require-hashes -r infra/aws_benchmark/requirements.txt", line)

    def test_supervisor_failure_powers_off_disposable_host(self):
        stack = yaml.safe_load((ROOT / "infra/aws_benchmark/stack.yaml").read_text())
        data = stack["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]
        user_data = data["UserData"]["Fn::Base64"]["Fn::Sub"]
        self.assertIn("--property=FailureAction=poweroff", user_data)
        self.assertIn("--property=KillMode=control-group", user_data)
        self.assertIn(f"--property=RuntimeMaxSec={common.MAX_WINDOW + common.MAX_OPEN_LEAD}", user_data)
        self.assertIn("--property=SuccessAction=poweroff", user_data)

    def test_cache_shim_accepts_the_real_harness_drop_invocation(self):
        with patch.object(harness.sys, "platform", "linux"), patch.object(harness.subprocess, "run") as run:
            harness._prepare_cache("linux-drop-caches", Path("/unused"), 30)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args_list[0].args[0], ["sync"])
        self.assertEqual(run.call_args.args[0], ["sudo", *cache_client.EXPECTED])
        self.assertEqual(run.call_args.kwargs["timeout"], 30)


    def test_worker_argv_parses_and_resolves_every_allowlisted_suite(self):
        for suite, selected in common.SUITES.items():
            with self.subTest(suite=suite):
                command = worker.command(dict(suite=suite, baseline="a" * 40), 600)
                args = harness._arguments(command[command.index("benchmarks.run") + 1:])
                # resolve the baked worker's selected manifest inside the checkout
                manifest_path = ROOT / "benchmarks" / args.manifest.name
                manifest = harness.load_manifest(manifest_path)
                cases = harness._select(manifest["cases"], args.cases, "case")
                variants = harness._select(manifest["variants"], args.variants, "variant")
                self.assertEqual([case["id"] for case in cases], list(selected))
                self.assertEqual([variant["id"] for variant in variants], ["rcp-default"])
                self.assertTrue(args.local_resources)
                self.assertTrue(args.no_timings)
                self.assertEqual(args.mode, "local")

    def test_userdata_submission_failure_powers_off_without_requiring_worker_directory(self):
        stack = yaml.safe_load((ROOT / "infra/aws_benchmark/stack.yaml").read_text())
        user_data = stack["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"]["UserData"]["Fn::Base64"]["Fn::Sub"]
        script = user_data.replace("$" + "{Artifacts}", "test-bucket").replace("$" + "{AWS::Region}", "us-east-1")
        for launch_status in (None, 0, 7):
            with self.subTest(launch_status=launch_status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                trace = root / "trace.jsonl"
                stub = (f"#!{sys.executable}\n"
                        "import json, os, pathlib, sys\n"
                        "name = pathlib.Path(sys.argv[0]).name\n"
                        "with open(os.environ['TRACE'], 'a') as stream:\n"
                        "    print(json.dumps([name, *sys.argv[1:]]), file=stream)\n"
                        "sys.exit(int(os.environ['LAUNCH_STATUS']) if name == 'systemd-run' else 0)\n")
                for name in ("systemctl", "systemd-run"):
                    if name == "systemd-run" and launch_status is None:
                        continue
                    executable = root / name
                    executable.write_text(stub)
                    executable.chmod(0o755)
                env = dict(os.environ, PATH=str(root), TRACE=str(trace), LAUNCH_STATUS=str(launch_status))
                result = subprocess.run(["/bin/bash", "-c", script], cwd=root, env=env,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = [json.loads(line) for line in trace.read_text().splitlines()]
                if launch_status == 0:
                    self.assertEqual(len(calls), 1)
                    self.assertEqual(calls[0][0], "systemd-run")
                    self.assertIn("--working-directory=/opt/rcp-benchmark", calls[0])
                    self.assertIn("--property=SuccessAction=poweroff", calls[0])
                    self.assertIn("--property=FailureAction=poweroff", calls[0])
                else:
                    self.assertEqual(calls[-1], ["systemctl", "poweroff"])


if __name__ == "__main__":
    unittest.main()
