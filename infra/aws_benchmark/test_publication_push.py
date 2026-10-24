"""Real Git and actual workflow-shell coverage for fixed append retries."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

from . import production_publish as publish
from . import publication_push as push
from .history_contract import HISTORY_REF, REPOSITORY_ID
from .history_git import git_env
from .test_production_publish_workflow import ROOT, SHA, WORKFLOW, trusted_environment


class FixedAppendTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.remote = self.root / "remote.git"
        self.repository = self.root / "work.git"
        self.real_run = subprocess.run
        for repository in (self.remote, self.repository):
            self.git("init", "--bare", str(repository))
        self.baseline = self.commit()
        self.git("--git-dir=" + str(self.repository), "push", str(self.remote), self.baseline + ":" + HISTORY_REF)
        self.prepared = self.commit(self.baseline)

    def git(self, *args, input=None):
        return self.real_run(["git", *args], input=input, env=git_env(), text=True,
            capture_output=True, check=True).stdout.strip()

    def commit(self, parent=None):
        tree = self.git("--git-dir=" + str(self.repository), "mktree", input="")
        return self.git("--git-dir=" + str(self.repository), "commit-tree", tree,
            *(["-p", parent] if parent else []), "-m", "fixed append test")

    def invoke(self, mode):
        calls = []
        def command(args, **kwargs):
            calls.append((args, kwargs["env"]))
            if "push" in args:
                index = sum("push" in a for a, _ in calls)
                if mode == "foreign":
                    foreign = self.commit(self.baseline)
                    # a distinct tree-independent commit message prevents equality
                    tree = self.git("--git-dir=" + str(self.repository), "mktree", input="")
                    foreign = self.git("--git-dir=" + str(self.repository), "commit-tree", tree,
                        "-p", self.baseline, "-m", "foreign writer")
                    self.git("--git-dir=" + str(self.repository), "push", str(self.remote), foreign + ":" + HISTORY_REF)
                    return subprocess.CompletedProcess(args, 1, "", "synthetic lost response")
                if mode == "fail" or mode == "retry" and index == 1:
                    return subprocess.CompletedProcess(args, 1, "", "synthetic unavailable")
                result = self.real_run(args, **kwargs)
                if mode == "lost":
                    result.returncode = 1
                if mode == "timeout":
                    raise subprocess.TimeoutExpired(args, 30)
                return result
            return self.real_run(args, **kwargs)
        with patch.dict(os.environ, GH_TOKEN="do-not-inherit", WRITER_TOKEN="do-not-inherit"), \
                patch.object(push.subprocess, "run", side_effect=command):
            result = push.append(self.repository, self.prepared, self.baseline, "synthetic-token", SHA,
                remote=str(self.remote), main_check=lambda _revision: None)
        return result, calls

    def test_exact_prepared_commit_survives_retry_and_lost_success(self):
        for mode, expected_pushes in (("retry", 2), ("lost", 1), ("timeout", 1)):
            with self.subTest(mode=mode):
                self.git("--git-dir=" + str(self.remote), "update-ref", HISTORY_REF, self.baseline)
                result, calls = self.invoke(mode)
                self.assertEqual(result, self.prepared)
                writes = [(args, env) for args, env in calls if "push" in args]
                self.assertEqual(len(writes), expected_pushes)
                for args, environment in calls:
                    self.assertNotIn("GH_TOKEN", environment)
                    self.assertNotIn("WRITER_TOKEN", environment)
                    headers = [environment[key] for key in environment if key.startswith("GIT_CONFIG_KEY_")]
                    self.assertEqual("http.https://github.com/.extraheader" in headers, "push" in args)
                for args, _ in writes:
                    self.assertIn("--force-with-lease=" + HISTORY_REF + ":" + self.baseline, args)
                    self.assertEqual(args[-1], self.prepared + ":" + HISTORY_REF)
                self.assertEqual(self.git("--git-dir=" + str(self.remote), "rev-parse", HISTORY_REF), self.prepared)

    def test_foreign_fast_forward_is_held_without_adoption(self):
        with self.assertRaisesRegex(ValueError, "foreign tip"):
            self.invoke("foreign")
        self.assertNotEqual(self.git("--git-dir=" + str(self.remote), "rev-parse", HISTORY_REF), self.prepared)

    def test_failed_pushes_are_bounded_without_repreparation(self):
        with self.assertRaisesRegex(ValueError, "three bounded attempts"):
            self.invoke("fail")
        self.assertEqual(self.git("--git-dir=" + str(self.remote), "rev-parse", HISTORY_REF), self.baseline)

    def test_wrong_parent_or_root_cannot_use_lease_to_replace_history(self):
        for commit, baseline in ((self.baseline, self.baseline), (self.prepared, ""), (self.prepared, "f" * 40)):
            with self.subTest(commit=commit, baseline=baseline), self.assertRaisesRegex(ValueError, "direct child|baseline missing"):
                push.append(self.repository, commit, baseline, "test", SHA,
                    remote=str(self.remote), main_check=lambda _revision: None)
        self.assertEqual(self.git("--git-dir=" + str(self.remote), "rev-parse", HISTORY_REF), self.baseline)

    def test_current_main_ignores_real_git_tail_matching_shadow(self):
        from . import publication_identity
        for ref in ("refs/heads/main", "refs/heads/shadow/refs/heads/main"):
            self.git("--git-dir=" + str(self.remote), "update-ref", ref, self.baseline)
        with patch.object(publication_identity, "REMOTE", str(self.remote)):
            publication_identity.current_main(self.baseline)
        self.git("--git-dir=" + str(self.repository), "push", str(self.remote), self.prepared + ":refs/heads/main")
        with patch.object(publication_identity, "REMOTE", str(self.remote)), self.assertRaisesRegex(ValueError, "current main"):
            publication_identity.current_main(self.baseline)

    def test_changed_main_prevents_first_write(self):
        def changed(_revision):
            raise ValueError("main moved")
        with self.assertRaisesRegex(ValueError, "main moved"):
            push.append(self.repository, self.prepared, self.baseline, "test", SHA,
                remote=str(self.remote), main_check=changed)
        self.assertEqual(self.git("--git-dir=" + str(self.remote), "rev-parse", HISTORY_REF), self.baseline)


class PublicationShellTests(unittest.TestCase):
    def test_actual_prepare_installation_and_push_cli_emit_exact_output(self):
        steps = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)["jobs"]["publish"]["steps"]
        prepare = next(x for x in steps if x.get("id") == "prepare")
        continuity = next(x for x in steps if "publication_identity installation" in x.get("run", ""))
        append = next(x for x in steps if x.get("id") == "append")
        for fail_first in (False, True):
            with self.subTest(fail_first=fail_first), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                remote = root / "remote.git"
                real_git = shutil.which("git")
                subprocess.run([real_git, "init", "--bare", str(remote)], capture_output=True, check=True)
                tree = subprocess.run([real_git, "--git-dir=" + str(remote), "mktree"], input="", capture_output=True,
                    text=True, check=True).stdout.strip()
                initial = subprocess.run([real_git, "--git-dir=" + str(remote), "commit-tree", tree, "-m", "Accepted canary"],
                    capture_output=True, text=True, env=git_env(), check=True).stdout.strip()
                subprocess.run([real_git, "--git-dir=" + str(remote), "update-ref", HISTORY_REF, initial], capture_output=True, check=True)
                source = root / "public-records"
                source.mkdir()
                value = json.loads((ROOT / "infra/aws_benchmark/fixtures/production-public-report-v1.json").read_text())
                value["run_key"] = key = "repo-655073755-run-123"
                raw = publish.encoded(value)
                (source / (key + ".json")).write_bytes(raw)
                publish.persist(source / "index.json", dict(schema=publish.PUBLIC_SCHEMA, repository_id=REPOSITORY_ID,
                    reader_revision=SHA, observed_at="2026-10-23T00:00:00Z", active=dict(state="idle", retired=False),
                    observations=[dict(run_key=key, state="ready", retired=True)],
                    records=[dict(run_key=key, bytes=len(raw), sha256=publish.digest(raw))]))
                bin_dir = root / "bin"
                bin_dir.mkdir()
                wrapper = bin_dir / "git"
                wrapper.write_text("#!" + sys.executable + "\n" +
                    "import os,sys,pathlib\n" +
                    "a=sys.argv[1:]\n" +
                    "if 'ls-remote' in a and a[-1]=='refs/heads/main': print('a'*40+'\\trefs/heads/main'); raise SystemExit(0)\n" +
                    "a=[" + repr(str(remote)) + " if x=='https://github.com/wykurz/rcp.git' else x for x in a]\n" +
                    "if 'push' in a:\n" +
                    " p=pathlib.Path(" + repr(str(root / "push-count")) + "); n=int(p.read_text()) if p.exists() else 0; p.write_text(str(n+1))\n" +
                    " if " + repr(fail_first) + " and n==0: raise SystemExit(1)\n" +
                    "os.execv(" + repr(real_git) + ", [" + repr(real_git) + ",*a])\n")
                wrapper.chmod(0o700)
                env = dict(trusted_environment(directory), PATH=str(bin_dir) + os.pathsep + os.environ["PATH"],
                    EXPECTED_INVENTORY=publish.inventory(source), PYTHONOPTIMIZE="2")
                env.pop("GH_TOKEN", None)
                def run(step, environment):
                    return subprocess.run(["bash", "-e", "-o", "pipefail", "-c", step["run"]], cwd=ROOT,
                        env=environment, text=True, capture_output=True, timeout=25)
                result = run(prepare, env)
                self.assertEqual(result.returncode, 0, result.stderr)
                output = Path(env["GITHUB_OUTPUT"])
                prepared = dict(line.split("=", 1) for line in output.read_text().splitlines())
                self.assertEqual(prepared["baseline"], initial)
                self.assertRegex(prepared["commit"], r"^[0-9a-f]{40}$")
                result = run(continuity, {**env, "EXPECTED_INSTALLATION": "5678", "ACTUAL_INSTALLATION": "5678"})
                self.assertEqual(result.returncode, 0, result.stderr)
                output.write_text("")
                result = run(append, {**env, "PREPARED_COMMIT": prepared["commit"], "PREPARED_BASELINE": prepared["baseline"],
                    "GH_TOKEN": "synthetic-token"})
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(output.read_text(), "history_commit=" + prepared["commit"] + "\n")
                self.assertEqual(int((root / "push-count").read_text()), 2 if fail_first else 1)
                self.assertNotIn("synthetic-token", result.stdout + result.stderr)
                records, tip = publish.history(remote)
                self.assertEqual(tip, prepared["commit"])
                self.assertEqual(list(records), [key])


if __name__ == "__main__":
    unittest.main()
