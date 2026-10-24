"""Offline capability, provenance and Git reconciliation checks for publication."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import yaml

from . import production_publish as publish
from . import history_contract as contract

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/aws-benchmark-publish.yml"
SHA = "a" * 40
INVENTORY = "b" * 64
REF = "refs/heads/aws-benchmark-history"
REMOTE = "https://github.com/wykurz/rcp.git"


def trusted_environment(directory):
    return dict(os.environ, RUNNER_TEMP=directory, GITHUB_REPOSITORY="wykurz/rcp",
        GITHUB_REF="refs/heads/main", GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_ACTOR_ID="483465", GITHUB_SHA=SHA,
        GITHUB_WORKFLOW_SHA=SHA, GITHUB_RUN_ATTEMPT="1",
        GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark-publish.yml@refs/heads/main",
        EXPECTED_READER=SHA, EXPECTED_INVENTORY=INVENTORY, EXPECTED_ARTIFACT="123",
        GITHUB_STEP_SUMMARY=str(Path(directory) / "summary"), GITHUB_OUTPUT=str(Path(directory) / "job-output"),
        PUBLICATION_ENABLED="true", TRUST_READY="true", CLIENT_ID="Iv23.test-client", INSTALLATION_ID="5678")


class PublicationWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        self.jobs = self.workflow["jobs"]
        self.phases = {name: self.jobs[name] for name in ("download", "export", "publish")}

    def test_manual_disabled_main_only_pipeline_has_exact_separated_capabilities(self):
        self.assertEqual(set(self.workflow["on"]), {"workflow_dispatch"})
        self.assertIn(self.workflow["on"]["workflow_dispatch"], (None, ""))
        self.assertEqual(set(self.jobs), {"download", "export", "publish", "pages"})
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        expected = {"download": {"contents": "read", "id-token": "write"},
                    "export": {"contents": "read"}, "publish": {"contents": "read"}}
        for name, job in self.phases.items():
            self.assertEqual(job["permissions"], expected[name])
            self.assertEqual(job["runs-on"], "ubuntu-24.04")
            self.assertLessEqual(int(job["timeout-minutes"]), 15)
            if name == "publish":
                self.assertEqual(job["environment"], {"name": contract.ENVIRONMENT, "deployment": "false"})
            else:
                self.assertNotIn("environment", job)
            for guard in ("github.repository == 'wykurz/rcp'", "github.ref == 'refs/heads/main'",
                          "github.event_name == 'workflow_dispatch'", "github.actor_id == '483465'",
                          "vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'true'", "github.run_attempt == '1'",
                          "vars.RCP_AWS_HISTORY_TRUST_READY == 'true'"):
                self.assertIn(guard, job["if"])
        self.assertEqual(self.jobs["export"]["needs"], "download")
        self.assertEqual(self.jobs["publish"]["needs"], "export")
        self.assertEqual(self.jobs["publish"]["concurrency"],
                         {"group": "aws-production-history-publish", "cancel-in-progress": "false"})

    def test_actual_revision_guards_reject_foreign_or_malformed_runtime_inputs(self):
        for name, job in self.phases.items():
            source = next(item["run"] for item in job["steps"] if "publication_identity identity" in item.get("run", ""))
            mutations = ({}, {"GITHUB_REPOSITORY": "other/rcp"}, {"GITHUB_REF": "refs/heads/debug"},
                {"GITHUB_EVENT_NAME": "pull_request"}, {"GITHUB_ACTOR_ID": "1"}, {"GITHUB_SHA": "main"},
                {"GITHUB_WORKFLOW_SHA": "c" * 40}, {"GITHUB_WORKFLOW_REF": "other"}, {"GITHUB_RUN_ATTEMPT": "2"})
            if name != "download":
                mutations += ({"EXPECTED_READER": "c" * 40}, {"EXPECTED_INVENTORY": "bad"},
                              {"EXPECTED_ARTIFACT": "123,456"}, {"EXPECTED_ARTIFACT": ""})
            for change in mutations:
                with self.subTest(job=name, change=change), tempfile.TemporaryDirectory() as directory:
                    env = {**trusted_environment(directory), **change}
                    result = subprocess.run(["bash", "-e", "-c", source], cwd=ROOT, env={**env, "PYTHONOPTIMIZE": "1"}, capture_output=True, timeout=10)
                    self.assertEqual(result.returncode == 0, not change)

    def test_actions_are_immutable_and_controller_checkout_is_always_current_sha(self):
        for job in self.phases.values():
            for item in job["steps"]:
                if "uses" in item:
                    self.assertRegex(item["uses"], r"^[a-zA-Z0-9_/-]+@[0-9a-f]{40}$")
                    if item["uses"].startswith("actions/checkout@"):
                        self.assertEqual(item["with"], {"ref": "${{ github.sha }}", "persist-credentials": "false"})

    def test_exact_current_run_artifact_ids_and_trusted_inventory_hash_cross_each_boundary(self):
        for current, previous in (("export", "download"), ("publish", "export")):
            job = self.jobs[current]
            downloads = [item for item in job["steps"] if item.get("uses", "").startswith("actions/download-artifact@")]
            self.assertEqual(len(downloads), 1)
            values = downloads[0]["with"]
            self.assertEqual(values["artifact-ids"], "${{ needs." + previous + ".outputs.artifact_id }}")
            self.assertEqual(values["merge-multiple"], "true")
            self.assertEqual(values["digest-mismatch"], "error")
            self.assertEqual(set(values), {"artifact-ids", "path", "merge-multiple", "digest-mismatch"})
            guard = next(item["env"] for item in job["steps"] if "publication_identity identity" in item.get("run", ""))
            for field, output in (("EXPECTED_READER", "reader_revision"), ("EXPECTED_INVENTORY", "inventory_sha256"),
                                  ("EXPECTED_ARTIFACT", "artifact_id")):
                self.assertEqual(guard[field], "${{ needs." + previous + ".outputs." + output + " }}")
            invoke = next(item for item in job["steps"] if "production_publish " + current in item.get("run", ""))
            self.assertIn('--reader-revision "$GITHUB_SHA"', invoke["run"])
            self.assertIn('--expected-sha256 "$EXPECTED_INVENTORY"', invoke["run"])
            self.assertEqual(invoke["env"]["EXPECTED_INVENTORY"], "${{ needs." + previous + ".outputs.inventory_sha256 }}")
        self.assertEqual(self.jobs["download"]["outputs"]["artifact_id"], "${{ steps.raw.outputs.artifact-id }}")
        self.assertEqual(self.jobs["export"]["outputs"]["artifact_id"], "${{ steps.public.outputs.artifact-id }}")

    def test_raw_evidence_is_short_lived_and_never_delivered_to_writer(self):
        for name, directory in (("download", "retired-evidence"), ("export", "public-records")):
            uploads = [item for item in self.jobs[name]["steps"] if item.get("uses", "").startswith("actions/upload-artifact@")]
            self.assertEqual(len(uploads), 1)
            self.assertEqual(uploads[0]["with"]["path"], "${{ runner.temp }}/" + directory + "/")
            self.assertEqual(uploads[0]["with"]["retention-days"], "1")
            self.assertEqual(uploads[0]["with"]["if-no-files-found"], "error")
            self.assertNotIn("if", uploads[0])
            self.assertNotIn("overwrite", uploads[0]["with"])
        self.assertNotIn("retired-evidence", str(self.jobs["publish"]))
        self.assertNotIn("production_publish download", str(self.jobs["publish"]))

    def test_export_has_no_cloud_or_writer_credential_and_no_data_branch_checkout(self):
        export = str(self.jobs["export"])
        for forbidden in ("id-token", "github.token", "secrets.", "ROLE:", "aws-actions", "configure-aws", "pip install"):
            self.assertNotIn(forbidden, export)
        for job in self.phases.values():
            source = "\n".join(item.get("run", "") for item in job["steps"])
            self.assertIn("git init --bare", source)
            self.assertIn(REF + ":" + REF, source)
            for forbidden in ("git checkout", "git reset", "git clean", "--mirror", "git pull"):
                self.assertNotIn(forbidden, source)
            self.assertEqual(job["env"]["GIT_CONFIG_GLOBAL"], "/dev/null")
            self.assertEqual(job["env"]["GIT_TERMINAL_PROMPT"], "0")

    def test_real_preparation_rejects_semantic_tampering_after_correct_inventory(self):
        step = next(item for item in self.jobs["publish"]["steps"] if item.get("id") == "prepare")
        for malformed, empty, expected_success, config in ((False, False, True, {}), (True, False, False, {}), (False, True, True, {}),
                (False, False, False, {"CLIENT_ID": "bad value"}), (False, False, False, {"INSTALLATION_ID": ""})):
            with self.subTest(malformed=malformed, empty=empty, config=config), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "public-records"
                source.mkdir()
                value = json.loads((ROOT / "infra/aws_benchmark/fixtures/production-public-report-v1.json").read_text())
                value["run_key"] = key = "repo-655073755-run-123"
                if malformed:
                    value["schema"] = "unrecognized-format"
                raw = publish.encoded(value)
                refs = []
                if not empty:
                    (source / (key + ".json")).write_bytes(raw)
                    refs.append(dict(run_key=key, bytes=len(raw), sha256=publish.digest(raw)))
                publish.persist(source / "index.json", dict(schema=publish.PUBLIC_SCHEMA, repository_id=contract.REPOSITORY_ID,
                    reader_revision=SHA, observed_at="2026-10-23T00:00:00Z", active=dict(state="idle", retired=False),
                    observations=[] if empty else [dict(run_key=key, state="ready", retired=True)], records=refs))
                bin_dir = root / "bin"
                bin_dir.mkdir()
                git = bin_dir / "git"
                real_git = __import__("shutil").which("git")
                remote = root / "accepted.git"
                subprocess.run([real_git, "init", "--bare", str(remote)], capture_output=True, check=True)
                tree = subprocess.run([real_git, "--git-dir=" + str(remote), "mktree"], input="", capture_output=True,
                    text=True, check=True).stdout.strip()
                initial = subprocess.run([real_git, "--git-dir=" + str(remote), "commit-tree", tree, "-m", "Accepted canary"],
                    capture_output=True, text=True, env=publish.git_env(), check=True).stdout.strip()
                subprocess.run([real_git, "--git-dir=" + str(remote), "update-ref", REF, initial], capture_output=True, check=True)
                git.write_text("#!" + sys.executable + "\n" +
                    "import os, sys\n" +
                    "if 'ls-remote' in sys.argv:\n" +
                    "    if sys.argv[-1] == 'refs/heads/main':\n" +
                    "        print('a' * 40 + '\\trefs/heads/main')\n" +
                    "        raise SystemExit(0)\n" +
                    "sys.argv=[" + repr(str(remote)) + " if x=='https://github.com/wykurz/rcp.git' else x for x in sys.argv]\n" +
                    "if 'push' in sys.argv: raise SystemExit('preparation must never push')\n" +
                    "os.execv(" + repr(real_git) + ", [" + repr(real_git) + ", *sys.argv[1:]])\n")
                git.chmod(0o700)
                env = dict(trusted_environment(directory), EXPECTED_INVENTORY=publish.inventory(source),
                           PATH=str(bin_dir) + os.pathsep + os.environ["PATH"], PYTHONOPTIMIZE="1")
                env.update(config)
                env.pop("GH_TOKEN", None)
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step["run"]], cwd=ROOT,
                    env=env, capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode == 0, expected_success, result.stdout + result.stderr)
                if expected_success:
                    output = (root / "job-output").read_text()
                    self.assertRegex(output, r"^baseline=[0-9a-f]{40}\ncommit=(?:[0-9a-f]{40})?\n")
                    self.assertEqual(output == "baseline=" + initial + "\ncommit=\nhistory_commit=" + initial + "\n", empty)
                else:
                    self.assertFalse((root / "job-output").exists())

    def test_writer_mints_only_for_prepared_additions_and_secrets_never_reach_preparation(self):
        steps = self.jobs["publish"]["steps"]
        mint = next(i for i, item in enumerate(steps) if item.get("id") == "history-token")
        prepare = next(i for i, item in enumerate(steps) if item.get("id") == "prepare")
        self.assertLess(prepare, mint)
        self.assertIn("production_publish publish", steps[prepare]["run"])
        self.assertEqual(steps[mint - 1]["run"], "python3 -m infra.aws_benchmark.publication_identity current-main")
        for step in steps[mint - 1:]:
            self.assertEqual(step["if"], "steps.prepare.outputs.commit != ''")
        self.assertEqual(steps[mint]["with"], {
            "client-id": "${{ vars." + contract.CLIENT_VARIABLE + " }}",
            "private-key": "${{ secrets." + contract.WRITER_SECRET_NAME + " }}",
            "owner": contract.REPOSITORY.split("/")[0], "repositories": contract.REPOSITORY.split("/")[1],
            "permission-contents": "write", "permission-metadata": "read", "skip-token-revoke": "false"})
        self.assertEqual(steps[-1]["env"]["GH_TOKEN"], "${{ steps.history-token.outputs.token }}")
        for item in steps[:mint]:
            for forbidden in ("secrets.", "GH_TOKEN", "github.token", "id-token", "production_publish validate"):
                self.assertNotIn(forbidden, str(item))
        self.assertEqual(sum("secrets." in str(item) for item in steps), 1)
        self.assertNotIn("continue-on-error", str(steps))
        self.assertNotIn("assert ", str(steps))
        self.assertIn("needs.export.outputs.record_count != '0'", self.jobs["publish"]["if"])
        self.assertIn("needs.export.outputs.record_count != ''", self.jobs["publish"]["if"])
        self.assertEqual(self.jobs["export"]["outputs"]["record_count"], "${{ steps.export.outputs.record_count }}")
        guard = next(x for x in steps if "publication_identity identity" in x.get("run", ""))
        self.assertEqual(guard["env"]["PUBLICATION_ENABLED"], "${{ vars." + contract.PUBLICATION_FLAG + " }}")
        self.assertEqual(guard["env"]["TRUST_READY"], "${{ vars." + contract.TRUST_FLAG + " }}")
        self.assertNotIn("GIT_AUTHOR_NAME", self.jobs["publish"]["env"])

    def test_only_reviewed_publisher_and_canary_may_reference_writer_environment_or_key(self):
        from .workflow_inventory import check_workflows
        check_workflows(ROOT / ".github/workflows")

    def test_pages_reuses_reviewed_workflow_only_with_verified_history_commit(self):
        job = self.jobs["pages"]
        self.assertEqual(job["needs"], "publish")
        self.assertEqual(job["uses"], "./.github/workflows/benchmark-pages.yml")
        self.assertEqual(job["with"], {"aws_history_commit": "${{ needs.publish.outputs.history_commit }}"})
        self.assertEqual(job["permissions"], {"contents": "read", "pages": "write", "id-token": "write"})
        for guard in ("vars.RCP_BENCHMARK_PAGES == 'true'", "vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'true'",
                      "needs.publish.outputs.history_commit != ''", "github.ref == 'refs/heads/main'",
                      "github.actor_id == '483465'"):
            self.assertIn(guard, job["if"])
        self.assertNotIn("secrets", job)
        self.assertNotIn("concurrency", job)
        self.assertEqual(self.jobs["publish"]["outputs"], {"history_commit": "${{ steps.append.outputs.history_commit || steps.prepare.outputs.history_commit }}"})

    def test_every_embedded_shell_program_has_valid_syntax(self):
        jobs = {**self.phases, "canary": yaml.load((WORKFLOW.parent / contract.CANARY_WORKFLOW).read_text(), Loader=yaml.BaseLoader)["jobs"]["verify"]}
        for name, job in jobs.items():
            for item in job["steps"]:
                if "run" in item:
                    with self.subTest(job=name, step=item.get("name")):
                        result = subprocess.run(["bash", "-n"], input=item["run"], text=True, capture_output=True, timeout=5)
                        self.assertEqual(result.returncode, 0, result.stderr)

    def test_guards_run_before_any_cloud_call_and_match_flags_exactly(self):
        for name in ("download", "export", "publish"):
            steps = self.jobs[name]["steps"]
            guard = steps[1]
            self.assertIn("publication_identity identity", guard["run"])
            self.assertIn("--flags", guard["run"])
            for key, variable in (("PUBLICATION_ENABLED", contract.PUBLICATION_FLAG), ("TRUST_READY", contract.TRUST_FLAG)):
                self.assertEqual(guard["env"][key], "${{ vars." + variable + " }}")
                for value in ("false", "", "True", "true\n"):
                    with self.subTest(job=name, key=key, value=value), tempfile.TemporaryDirectory() as directory:
                        result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", guard["run"]], cwd=ROOT,
                            env={**trusted_environment(directory), key: value, "PYTHONOPTIMIZE": "2"},
                            text=True, capture_output=True, timeout=10)
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("readiness is closed", result.stderr)

    def test_all_workflow_key_and_identity_mappings_share_the_contract(self):
        canary = yaml.load((WORKFLOW.parent / contract.CANARY_WORKFLOW).read_text(), Loader=yaml.BaseLoader)["jobs"]["verify"]
        publish_steps = self.jobs["publish"]["steps"]
        publish_mint = next(x for x in publish_steps if x.get("id") == "history-token")
        canary_mint = next(x for x in canary["steps"] if x.get("id") == "writer")
        self.assertEqual(canary_mint["with"], publish_mint["with"])
        for mapping in (next(x for x in publish_steps if x.get("id") == "prepare")["env"], canary["env"]):
            self.assertEqual(mapping["CLIENT_ID"], "${{ vars." + contract.CLIENT_VARIABLE + " }}")
            self.assertEqual(mapping["INSTALLATION_ID"], "${{ vars." + contract.INSTALLATION_VARIABLE + " }}")
        for steps in (publish_steps, canary["steps"]):
            step = next(x for x in steps if "publication_identity installation" in x.get("run", ""))
            self.assertEqual(step["env"]["EXPECTED_INSTALLATION"], "${{ vars." + contract.INSTALLATION_VARIABLE + " }}")
        self.assertEqual(canary["env"]["CANARY_ENABLED"], "${{ vars." + contract.CANARY_FLAG + " }}")
        self.assertEqual(canary["env"]["TRUST_READY"], "${{ vars." + contract.TRUST_FLAG + " }}")
        self.assertEqual(canary["env"]["PUBLISH_ENABLED"], "${{ vars." + contract.PUBLICATION_FLAG + " }}")

    def test_installation_shell_executes_before_token_use_under_optimized_python(self):
        for path in (WORKFLOW, WORKFLOW.parent / contract.CANARY_WORKFLOW):
            jobs = yaml.load(path.read_text(), Loader=yaml.BaseLoader)["jobs"]
            steps = jobs["publish" if path == WORKFLOW else "verify"]["steps"]
            step = next(x for x in steps if "publication_identity installation" in x.get("run", ""))
            for actual in ("5678", "5679", "", "05678"):
                with self.subTest(workflow=path.name, actual=actual), tempfile.TemporaryDirectory() as directory:
                    binary = Path(directory) / "python3"
                    log = Path(directory) / "commands"
                    binary.write_text("#!" + sys.executable + "\nimport os,sys\n" +
                        "with open(" + repr(str(log)) + ", 'a') as stream: stream.write(' '.join(sys.argv[1:])+'\\n')\n" +
                        "if sys.argv[1:]==['-m','infra.aws_benchmark.publication_identity','installation']: os.execv(sys.executable,[sys.executable,*sys.argv[1:]])\n" +
                        "raise SystemExit(0)\n")
                    binary.chmod(0o700)
                    env = dict(trusted_environment(directory), PATH=directory + os.pathsep + os.environ["PATH"],
                        EXPECTED_INSTALLATION="5678", ACTUAL_INSTALLATION=actual, CANARY_MODE="bootstrap", PYTHONOPTIMIZE="2")
                    result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step["run"]], cwd=ROOT,
                        env=env, capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode == 0, actual == "5678", result.stderr)
                    commands = log.read_text() if log.exists() else ""
                    self.assertIn("publication_identity installation", commands)
                    if actual != "5678":
                        self.assertNotIn("history_canary writer", commands)

    def test_canary_preflight_and_resume_evidence_precede_every_mutation_and_mint(self):
        job = yaml.load((WORKFLOW.parent / contract.CANARY_WORKFLOW).read_text(), Loader=yaml.BaseLoader)["jobs"]["verify"]
        steps = job["steps"]
        self.assertEqual(job["permissions"], {"contents": "write", "actions": "read"})
        self.assertEqual(steps[1]["run"].strip(), "python3 -m infra.aws_benchmark.publication_identity canary-current-main")
        metadata = next(i for i, x in enumerate(steps) if "canary_recovery" in x.get("run", ""))
        download = next(i for i, x in enumerate(steps) if x.get("uses", "").startswith("actions/download-artifact@"))
        check = next(i for i, x in enumerate(steps) if "history_canary resume-check" in x.get("run", ""))
        ordinary = next(i for i, x in enumerate(steps) if "history_canary ordinary" in x.get("run", ""))
        mint = next(i for i, x in enumerate(steps) if x.get("id") == "writer")
        self.assertLess(metadata, download)
        self.assertLess(download, check)
        self.assertLess(check, mint)
        self.assertLess(ordinary, mint)
        for index in (metadata, download, check):
            self.assertEqual(steps[index]["if"], "inputs.mode == 'resume'")
        self.assertEqual(steps[ordinary]["if"], "inputs.mode == 'bootstrap'")
        self.assertEqual(steps[download]["with"], {
            "artifact-ids": "${{ inputs.source_artifact_id }}", "run-id": "${{ inputs.source_run_id }}",
            "github-token": "${{ github.token }}", "path": "${{ runner.temp }}/canary-source",
            "merge-multiple": "true", "digest-mismatch": "error"})
        self.assertIn('--source-evidence "$RUNNER_TEMP/canary-source/canary.jsonl"', steps[check]["run"])
        self.assertIn('--source-sha256 "$SOURCE_SHA256"', steps[check]["run"])
        for index in (1, mint - 1):
            self.assertIn("publication_identity canary-current-main", steps[index]["run"])
        self.assertEqual(steps[-1]["with"]["retention-days"], "90")
        self.assertIn("canary-source/canary.jsonl", steps[-1]["with"]["path"])
        for key in ("CLIENT_ID", "INSTALLATION_ID", "CANARY_ENABLED", "TRUST_READY", "PUBLISH_ENABLED", "CANARY_MODE"):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                env = dict(trusted_environment(directory), GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/" +
                    contract.CANARY_WORKFLOW + "@refs/heads/main", CANARY_ENABLED="true", TRUST_READY="false",
                    PUBLISH_ENABLED="false", CANARY_MODE="bootstrap", PYTHONOPTIMIZE="2")
                env[key] = "invalid value"
                result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", steps[1]["run"]], cwd=ROOT,
                    env=env, capture_output=True, text=True, timeout=10)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Publication identity check failed:", result.stderr)

    def test_each_main_publication_stage_holds_on_missing_or_shadow_only_history(self):
        for name in ("download", "export", "publish"):
            step = next(x for x in self.jobs[name]["steps"] if "ls-remote" in x.get("run", ""))
            for shadow in (False, True):
                with self.subTest(stage=name, shadow=shadow), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    binary = root / "git"
                    log = root / "commands"
                    binary.write_text("#!" + sys.executable + "\nimport os,sys,pathlib\n" +
                        "a=sys.argv[1:]\n" +
                        "with open(" + repr(str(log)) + ", 'a') as stream: stream.write(' '.join(a)+'\\n')\n" +
                        "if 'ls-remote' in a:\n" +
                        " if a[-1]=='refs/heads/main': print('a'*40+'\\trefs/heads/main'); raise SystemExit(0)\n" +
                        " if " + repr(shadow) + ": print('b'*40+'\\trefs/heads/shadow/refs/heads/aws-benchmark-history'); raise SystemExit(0)\n" +
                        " raise SystemExit(2)\n" +
                        "if 'init' in a: pathlib.Path(a[-1]).mkdir(); raise SystemExit(0)\n" +
                        "raise SystemExit('unexpected Git operation')\n")
                    binary.chmod(0o700)
                    env = dict(trusted_environment(directory), PATH=directory + os.pathsep + os.environ["PATH"], PYTHONOPTIMIZE="2")
                    result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step["run"]], cwd=ROOT,
                        env=env, text=True, capture_output=True, timeout=10)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("Protected history", result.stderr)
                    self.assertNotIn("fetch", log.read_text())
                    self.assertFalse((root / "job-output").exists())

    def test_canary_environment_mode_matches_publication_and_main_reads_use_ordinary_token(self):
        canary = yaml.load((WORKFLOW.parent / contract.CANARY_WORKFLOW).read_text(), Loader=yaml.BaseLoader)["jobs"]["verify"]
        self.assertEqual(canary["environment"], self.jobs["publish"]["environment"])
        for step in canary["steps"]:
            if "publication_identity canary-current-main" in step.get("run", ""):
                self.assertEqual(step["env"]["GH_TOKEN"], "${{ github.token }}")


if __name__ == "__main__":
    unittest.main()
