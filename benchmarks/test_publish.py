"""Behavioral checks for immutable history publication using local Git remotes."""

import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock


PROJECT = Path(__file__).resolve().parent.parent


def result(run_id):
    return {
        "schema_version": 1,
        "run_id": run_id,
        "timestamp": "2026-09-26T00:00:00Z",
        "status": "complete",
        "revision": {"commit": "a" * 40, "branch": "main", "dirty": False},
        "context": {
            "runner_label": "test", "topology": "local", "cache_policy": "source-warm",
            "timing_policy": "command-completion", "source": {}, "destination": {},
        },
        "tools": {},
        "cases": [{"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}],
        "variants": [{"id": "rcp-default", "tool": "rcp", "args": [], "processes": 1}],
        "trials": [{
            "case_id": "tiny", "variant_id": "rcp-default", "iteration": 1,
            "elapsed_seconds": 1.0, "exit_codes": [0], "status": "ok",
            "commands": [["rcp", "source", "destination"]],
            "validation": {"ok": True}, "logs": [],
        }],
        "summaries": [{
            "series_id": "b" * 64, "case_id": "tiny", "variant_id": "rcp-default",
            "unit": "seconds", "median": 1.0, "minimum": 1.0, "maximum": 1.0,
            "stdev": 0.0, "samples": [1.0], "files_per_second": 1.0,
        }],
    }


class PublishTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="rcp-publish-test-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.remote = self.root / "remote.git"
        self.checkout = self.root / "checkout"
        self.environment = os.environ.copy()
        self.environment["PYTHONPATH"] = str(PROJECT)
        self.environment["GIT_CONFIG_GLOBAL"] = os.devnull
        self.environment["GIT_CONFIG_NOSYSTEM"] = "1"
        self.git(self.root, "init", "--bare", str(self.remote))
        self.git(self.root, "init", str(self.checkout))
        self.git(self.checkout, "config", "user.name", "Test")
        self.git(self.checkout, "config", "user.email", "test@example.invalid")
        self.git(self.checkout, "commit", "--allow-empty", "-m", "Caller commit")
        self.git(self.checkout, "remote", "add", "origin", str(self.remote))

    def git(self, directory, *arguments, check=True):
        return subprocess.run(
            ["git", "-C", str(directory), *arguments], env=self.environment,
            text=True, capture_output=True, check=check, timeout=10,
        )

    def write_result(self, run_id, **changes):
        record = result(run_id)
        record.update(changes)
        path = self.root / f"input-{len(list(self.root.glob('input-*')))}.json"
        path.write_text(json.dumps(record), encoding="utf-8")
        return path

    def command(self, path, *arguments):
        return [
            sys.executable, "-m", "benchmarks.publish", "--result", str(path),
            "--repository", "owner/repository", "--remote", "origin", *arguments,
        ]

    def publish(self, path, *arguments):
        return subprocess.run(
            self.command(path, *arguments), cwd=self.checkout, env=self.environment,
            text=True, capture_output=True, timeout=25,
        )

    def history(self):
        return self.git(self.remote, "rev-parse", "refs/heads/benchmark-history").stdout.strip()

    def test_creates_history_without_changing_callers_checkout(self):
        (self.checkout / "user-file").write_text("keep this\n", encoding="utf-8")
        self.git(self.checkout, "add", "user-file")
        before = self.git(self.checkout, "status", "--porcelain=v1").stdout
        caller_head = self.git(self.checkout, "rev-parse", "HEAD").stdout
        path = self.write_result("1" * 32)
        completed = self.publish(path)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        stored = self.git(self.remote, "show", f"{self.history()}:runs/{'1' * 32}.json")
        self.assertEqual(json.loads(stored.stdout), json.loads(path.read_text()))
        self.assertEqual(self.git(self.checkout, "status", "--porcelain=v1").stdout, before)
        self.assertEqual(self.git(self.checkout, "rev-parse", "HEAD").stdout, caller_head)

    def test_appends_without_replacing_previous_runs(self):
        first = self.publish(self.write_result("1" * 32))
        self.assertEqual(first.returncode, 0, first.stderr)
        first_head = self.history()
        second = self.publish(self.write_result("2" * 32))
        self.assertEqual(second.returncode, 0, second.stderr)
        self.git(self.remote, "merge-base", "--is-ancestor", first_head, self.history())
        paths = self.git(self.remote, "ls-tree", "-r", "--name-only", self.history()).stdout
        self.assertEqual(paths.splitlines(), [f"runs/{'1' * 32}.json", f"runs/{'2' * 32}.json"])

    def test_identical_record_is_idempotent_despite_json_formatting(self):
        path = self.write_result("1" * 32)
        first = self.publish(path)
        self.assertEqual(first.returncode, 0, first.stderr)
        previous = self.history()
        path.write_text(json.dumps(json.loads(path.read_text()), indent=4), encoding="utf-8")
        again = self.publish(path)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(self.history(), previous)
        self.assertIn("already published", again.stdout)

    def test_fetches_only_history_tip_and_preserves_parent_on_append(self):
        from benchmarks.publish import publish
        for character in ("1", "2", "3"):
            completed = self.publish(self.write_result(character * 32))
            self.assertEqual(completed.returncode, 0, completed.stderr)
        previous = self.history()
        client = self.root / "publication-client"
        client.mkdir()
        path = self.write_result("4" * 32)
        with contextlib.chdir(self.checkout), mock.patch.dict(os.environ, self.environment, clear=True), mock.patch(
            "benchmarks.publish.tempfile.TemporaryDirectory", return_value=contextlib.nullcontext(str(client)),
        ):
            identifier, commit, created = publish(path, "owner/repository")
        self.assertEqual(identifier, "4" * 32)
        self.assertTrue(created)
        self.assertEqual(self.git(client, "rev-parse", "--is-shallow-repository").stdout.strip(), "true")
        self.assertEqual(self.git(client, "rev-list", "--count", commit).stdout.strip(), "2")
        self.assertEqual(self.git(self.remote, "rev-parse", f"{commit}^").stdout.strip(), previous)
        self.assertEqual(self.git(self.remote, "rev-list", "--count", commit).stdout.strip(), "4")
        again = self.publish(path)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(self.history(), commit)

    def test_conflicting_run_does_not_change_history(self):
        first = self.publish(self.write_result("1" * 32))
        self.assertEqual(first.returncode, 0, first.stderr)
        previous = self.history()
        changed = self.write_result("1" * 32, timestamp="2026-09-27T00:00:00Z")
        conflict = self.publish(changed)
        self.assertNotEqual(conflict.returncode, 0)
        self.assertIn("conflict", conflict.stderr.lower())
        self.assertEqual(self.history(), previous)

    def test_publishes_failed_terminal_run_without_timing_samples(self):
        path = self.write_result("1" * 32, status="failed", summaries=[], error="copy failed")
        record = json.loads(path.read_text())
        record["trials"][0].update({
            "status": "failed", "elapsed_seconds": None, "exit_codes": [1],
            "validation": {"ok": False},
        })
        path.write_text(json.dumps(record), encoding="utf-8")
        completed = self.publish(path)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        stored = self.git(self.remote, "show", f"{self.history()}:runs/{'1' * 32}.json")
        self.assertEqual(json.loads(stored.stdout), record)

    def test_finalizes_interrupted_run_without_rewriting_successes_or_original_artifact(self):
        from benchmarks.publish import publish
        record = result("1" * 32)
        record["status"] = "running"
        record["cases"].append({"id": "later", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1})
        record["trials"].append({
            "case_id": "later", "variant_id": "rcp-default", "iteration": 1,
            "status": "running", "exit_codes": [], "validation": {"ok": False},
            "commands": [["rcp", "later-source", "later-destination"]], "logs": [],
        })
        path = self.write_result("1" * 32, **{key: value for key, value in record.items() if key != "run_id"})
        original = path.read_bytes()
        completed = self.publish(path, "--finalize-interrupted")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        previous = self.history()
        stored = json.loads(self.git(self.remote, "show", f"{previous}:runs/{record['run_id']}.json").stdout)
        self.assertEqual(stored["status"], "failed")
        self.assertIn("interrupted", stored["error"].lower())
        self.assertEqual(stored["summaries"], record["summaries"])
        self.assertEqual(stored["trials"][0], record["trials"][0])
        self.assertEqual(stored["trials"][1]["status"], "failed")
        self.assertFalse(stored["trials"][1]["validation"]["ok"])
        self.assertIn("interrupted", stored["trials"][1]["validation"]["error"].lower())
        self.assertEqual(stored["trials"][1]["commands"], record["trials"][1]["commands"])
        self.assertEqual(stored["context"]["publication_recovery"], {
            "publisher": "benchmarks.publish", "original_status": "running", "producer_ended_asserted": True,
        })
        with contextlib.chdir(self.checkout), mock.patch.dict(os.environ, self.environment, clear=True):
            identifier, commit, created = publish(path, "owner/repository", finalize_interrupted=True)
        self.assertEqual((identifier, commit, created), (record["run_id"], previous, False))
        self.assertEqual(self.history(), previous)
        self.assertEqual(path.read_bytes(), original)

    def test_finalizes_interruption_before_any_measurement(self):
        path = self.write_result("1" * 32, status="running", cases=[], variants=[], trials=[], summaries=[])
        completed = self.publish(path, "--finalize-interrupted")
        self.assertEqual(completed.returncode, 0, completed.stderr)
        stored = json.loads(self.git(self.remote, "show", f"{self.history()}:runs/{'1' * 32}.json").stdout)
        self.assertEqual(stored["status"], "failed")
        self.assertEqual(stored["trials"], [])
        self.assertEqual(stored["summaries"], [])
        self.assertIn("interrupted", stored["error"].lower())

    def test_finalization_flag_preserves_existing_terminal_records(self):
        for character, status in (("1", "complete"), ("2", "failed")):
            with self.subTest(status=status):
                path = self.write_result(character * 32, status=status)
                completed = self.publish(path, "--finalize-interrupted")
                self.assertEqual(completed.returncode, 0, completed.stderr)
                stored = self.git(self.remote, "show", f"{self.history()}:runs/{character * 32}.json")
                self.assertEqual(json.loads(stored.stdout), json.loads(path.read_text()))

    def test_incomplete_or_invalid_results_never_create_remote_history(self):
        for change in ({"status": "running"}, {"schema_version": 2}, {"run_id": "../bad"}):
            with self.subTest(change=change):
                path = self.write_result("1" * 32)
                record = json.loads(path.read_text())
                record.update(change)
                path.write_text(json.dumps(record), encoding="utf-8")
                completed = self.publish(path)
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn(next(iter(change)), completed.stderr)
                self.assertFalse(self.git(self.remote, "show-ref", check=False).stdout)

    def test_rejects_duplicate_input_keys_without_creating_history(self):
        path = self.write_result("1" * 32)
        path.write_text(path.read_text().replace('"schema_version": 1', '"schema_version": 2, "schema_version": 1'), encoding="utf-8")
        completed = self.publish(path)
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("duplicate JSON key", completed.stderr)
        self.assertFalse(self.git(self.remote, "show-ref", check=False).stdout)

    def test_rejects_ambiguous_or_nonfinite_existing_history_before_duplicate_comparison(self):
        path = self.write_result("1" * 32)
        for extra in ('"schema_version": 2,', '"extra": NaN,', '"extra": Infinity,'):
            with self.subTest(extra=extra):
                directory = self.checkout / "runs"
                directory.mkdir(exist_ok=True)
                (directory / f"{'1' * 32}.json").write_text("{" + extra + path.read_text()[1:], encoding="utf-8")
                self.git(self.checkout, "add", "runs")
                self.git(self.checkout, "commit", "-m", "Seed invalid historical JSON")
                self.git(self.checkout, "push", "origin", "HEAD:refs/heads/benchmark-history")
                previous = self.history()
                completed = self.publish(path)
                self.assertNotEqual(completed.returncode, 0)
                self.assertIn("JSON", completed.stderr)
                self.assertNotIn("conflicting", completed.stderr)
                self.assertEqual(self.history(), previous)

    def test_reuses_included_authentication_without_copying_repository_configuration(self):
        from benchmarks.publish import publication_environment
        credentials = self.root / "credentials.config"
        credentials.write_text(
            '[http "https://benchmark.invalid/"]\n\textraheader = AUTHORIZATION: test-only\n'
            '[credential "https://benchmark.invalid"]\n\tusername = test-user\n',
            encoding="utf-8",
        )
        self.git(self.checkout, "config", "include.path", str(credentials))
        environment = publication_environment(self.checkout)
        for key, expected in (
            ("http.https://benchmark.invalid/.extraheader", "AUTHORIZATION: test-only"),
            ("credential.https://benchmark.invalid.username", "test-user"),
        ):
            completed = subprocess.run(
                ["git", "-C", str(self.root), "config", "--get", key],
                env=environment, text=True, capture_output=True, check=True,
            )
            self.assertEqual(completed.stdout.strip(), expected)
        self.assertNotIn(str(credentials), json.dumps(environment))

    def test_credential_helper_can_read_workflow_token_in_publication_environment(self):
        from benchmarks.publish import publication_environment
        helper = self.root / "credential-helper"
        helper.write_text(
            '#!/bin/sh\n[ "$1" = get ] || exit 0\n'
            'printf "username=test-user\\npassword=%s\\n" "$GH_TOKEN"\n',
            encoding="utf-8",
        )
        helper.chmod(0o755)
        self.git(self.checkout, "config", "credential.https://github.com.helper", "")
        self.git(self.checkout, "config", "--add", "credential.https://github.com.helper", str(helper))
        with mock.patch.dict(os.environ, {**self.environment, "GH_TOKEN": "test-only-token"}, clear=True):
            environment = publication_environment(self.checkout)
        completed = subprocess.run(
            ["git", "-C", str(self.root), "credential", "fill"],
            env=environment, input="protocol=https\nhost=github.com\n\n",
            text=True, capture_output=True, check=True, timeout=10,
        )
        credentials = dict(line.split("=", 1) for line in completed.stdout.splitlines())
        self.assertEqual(credentials["username"], "test-user")
        self.assertEqual(credentials["password"], "test-only-token")

    def test_git_environment_does_not_redirect_the_publishers_temporary_index(self):
        caller_index = self.checkout / ".git" / "index"
        (self.checkout / "user-file").write_text("keep this\n", encoding="utf-8")
        self.git(self.checkout, "add", "user-file")
        before = caller_index.read_bytes()
        self.environment["GIT_INDEX_FILE"] = str(caller_index)
        completed = self.publish(self.write_result("1" * 32))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(caller_index.read_bytes(), before)

    def test_preserves_valueless_boolean_and_explicit_empty_auth_configuration(self):
        from benchmarks.publish import publication_environment
        with (self.checkout / ".git" / "config").open("a", encoding="utf-8") as config:
            config.write("\n[http]\n\tsslVerify\n[credential]\n\thelper =\n")
        completed = self.publish(self.write_result("1" * 32))
        self.assertEqual(completed.returncode, 0, completed.stderr)
        with mock.patch.dict(os.environ, self.environment, clear=True):
            environment = publication_environment(self.checkout)
        for arguments, expected in (
            (("--type=bool", "--get", "http.sslVerify"), "true\n"),
            (("--get", "credential.helper"), "\n"),
        ):
            copied = subprocess.run(
                ["git", "-C", str(self.root), "config", *arguments],
                env=environment, text=True, capture_output=True, check=True,
            )
            self.assertEqual(copied.stdout, expected)

    def test_git_timeout_diagnostic_does_not_expose_authentication_in_arguments(self):
        from benchmarks.publish import main
        secret = "https://test-user:never-print-this@example.invalid/repo"
        timeout = subprocess.TimeoutExpired(["git", "fetch", secret], 60)
        diagnostic = io.StringIO()
        with mock.patch("benchmarks.publish.subprocess.run", side_effect=timeout), contextlib.redirect_stderr(diagnostic):
            status = main([
                "--result", str(self.write_result("1" * 32)),
                "--repository", "owner/repository", "--remote", secret,
            ])
        self.assertNotEqual(status, 0)
        self.assertNotIn("never-print-this", diagnostic.getvalue())

    def test_refuses_publishing_into_main(self):
        completed = self.publish(self.write_result("1" * 32), "--branch", "main")
        self.assertNotEqual(completed.returncode, 0)
        self.assertFalse(self.git(self.remote, "show-ref", check=False).stdout)

    def test_concurrent_append_retries_without_losing_either_result(self):
        seeded = self.publish(self.write_result("0" * 32))
        self.assertEqual(seeded.returncode, 0, seeded.stderr)
        original = self.history()
        hook = self.remote / "hooks" / "pre-receive"
        hook.write_text(
            "#!/bin/sh\n"
            f"printf 'attempt\\n' >> '{self.root / 'attempts'}'\n"
            f"if mkdir '{self.root / 'first-push'}' 2>/dev/null; then\n"
            "  count=0\n"
            f"  while [ ! -e '{self.root / 'second-push'}' ]; do\n"
            "    count=$((count + 1))\n"
            "    [ \"$count\" -lt 200 ] || exit 1\n"
            "    sleep 0.02\n"
            "  done\n"
            "else\n"
            f"  touch '{self.root / 'second-push'}'\n"
            "fi\n",
            encoding="utf-8",
        )
        hook.chmod(0o755)
        children = [subprocess.Popen(
            self.command(self.write_result(character * 32)), cwd=self.checkout,
            env=self.environment, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ) for character in ("1", "2")]
        try:
            for child in children:
                stdout, stderr = child.communicate(timeout=25)
                self.assertEqual(child.returncode, 0, stdout + stderr)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate()
        paths = self.git(self.remote, "ls-tree", "-r", "--name-only", self.history()).stdout
        self.assertEqual(paths.splitlines(), [f"runs/{character * 32}.json" for character in ("0", "1", "2")])
        self.git(self.remote, "merge-base", "--is-ancestor", original, self.history())
        self.assertEqual(self.git(self.remote, "rev-list", "--count", self.history()).stdout.strip(), "3")
        self.assertGreaterEqual(len((self.root / "attempts").read_text().splitlines()), 3)

    def test_permanent_push_failure_stops_after_bounded_attempts(self):
        hook = self.remote / "hooks" / "pre-receive"
        hook.write_text(
            f"#!/bin/sh\nprintf 'attempt\\n' >> '{self.root / 'attempts'}'\nexit 1\n",
            encoding="utf-8",
        )
        hook.chmod(0o755)
        completed = self.publish(self.write_result("1" * 32), "--attempts", "2")
        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(len((self.root / "attempts").read_text().splitlines()), 2)
        self.assertFalse(self.git(self.remote, "show-ref", check=False).stdout)


if __name__ == "__main__":
    unittest.main()
