import json
import os
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

import yaml

from benchmarks.test_publish import result as publication_result


class BenchmarkWorkflowTests(unittest.TestCase):
    def workflow(self):
        return yaml.safe_load((Path(__file__).resolve().parent.parent / ".depot/workflows/benchmarks.yml").read_text())

    def measure_arguments(self, *, event, mode="loopback", baseline="", case="all", cache="linux-drop-caches"):
        measure = next(step for step in self.workflow()["jobs"]["benchmark"]["steps"] if step.get("name") == "Measure copies")
        script = 'just() { printf "%s\\n" "$@"; }\nsha256sum() { printf "%064d  -\\n" 0; }\n' + measure["run"]
        environment = {**os.environ, "BENCHMARK_EVENT": event, "BENCHMARK_MODE": mode, "SELECTED_CASE": case, "SELECTED_CACHE": cache, "RUNNER_TEMP": "/tmp", "RCP_BENCH_BASELINE_BIN": baseline}
        result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True, text=True, check=True)
        arguments = result.stdout.splitlines()
        self.assertEqual(arguments[0], "benchmark-run")
        return arguments

    def values(self, arguments, option):
        return [arguments[index + 1] for index, item in enumerate(arguments) if item == option]

    def test_pr_smoke_checks_balance_each_mode_with_baseline(self):
        for mode, repetitions in (("local", "5"), ("loopback", "4")):
            with self.subTest(mode=mode):
                arguments = self.measure_arguments(event="pull_request", mode=mode, baseline="/tmp/base-binaries")
                self.assertEqual(self.values(arguments, "--repetitions"), [repetitions])
                self.assertEqual(self.values(arguments, "--baseline-bin-dir"), ["/tmp/base-binaries"])
                self.assertEqual(self.values(arguments, "--purpose"), ["smoke"])
                self.assertEqual(self.values(arguments, "--cache"), ["source-warm"])
                self.assertEqual(self.values(arguments, "--mode"), [mode])

    def test_main_push_is_only_a_smoke_check(self):
        arguments = self.measure_arguments(event="push")
        self.assertEqual(self.values(arguments, "--purpose"), ["smoke"])
        self.assertEqual(self.values(arguments, "--case"), ["tiny-10k", "medium-4k", "large-100"])
        self.assertNotIn("--baseline-bin-dir", arguments)

    def test_transport_profile_is_supplied_only_for_loopback(self):
        for mode in ("local", "loopback"):
            with self.subTest(mode=mode):
                arguments = self.measure_arguments(event="schedule", mode=mode)
                self.assertEqual(self.values(arguments, "--ssh-transport-profile"), ["0" * 64] if mode == "loopback" else [])

    def test_scheduled_performance_runs_balance_each_mode(self):
        for mode, repetitions in (("local", "4"), ("loopback", "3")):
            with self.subTest(mode=mode):
                arguments = self.measure_arguments(event="schedule", mode=mode)
                self.assertEqual(self.values(arguments, "--repetitions"), [repetitions])
                self.assertEqual(self.values(arguments, "--purpose"), ["performance"])
                self.assertEqual(self.values(arguments, "--cache"), ["linux-drop-caches"])
                self.assertEqual(self.values(arguments, "--case"), ["tiny-1m", "medium-128k", "large-120"])
                self.assertEqual(self.values(arguments, "--timeout"), ["1200"])

    def test_manual_performance_run_honors_case_and_cache(self):
        arguments = self.measure_arguments(event="workflow_dispatch", mode="local", case="large-120", cache="source-warm")
        self.assertEqual(self.values(arguments, "--case"), ["large-120"])
        self.assertEqual(self.values(arguments, "--cache"), ["source-warm"])
        self.assertEqual(self.values(arguments, "--purpose"), ["performance"])

    def test_history_attempts_both_modes_and_dispatches_after_partial_failure(self):
        history = self.workflow()["jobs"]["history"]
        publish = next(step for step in history["steps"] if step.get("id") == "publish")
        dispatch = next(step for step in history["steps"] if step.get("name") == "Request dashboard update")
        self.assertEqual(dispatch["if"], "always() && steps.publish.outputs.published == 'true'")
        for failed_mode in ("local", "loopback"):
            with self.subTest(failed_mode=failed_mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for mode in ("local", "loopback"):
                    result = root / "downloaded-results" / f"benchmark-results-{mode}" / "results.json"
                    result.parent.mkdir(parents=True)
                    result.write_text("{}")
                script = 'gh() { :; }\npython3() { printf "%s\\n" "$*" >> "$ATTEMPTS"; [[ "$*" != *"benchmark-results-$FAILED_MODE/"* ]]; }\n' + publish["run"]
                environment = {**os.environ, "GITHUB_OUTPUT": str(root / "output"), "GITHUB_REPOSITORY": "test/repo", "ATTEMPTS": str(root / "attempts"), "FAILED_MODE": failed_mode}
                result = subprocess.run(["bash", "-c", script], cwd=root, env=environment, capture_output=True, text=True)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(len((root / "attempts").read_text().splitlines()), 2)
                self.assertEqual((root / "output").read_text(), "published=true\n")

    def test_history_publishes_from_checkout_without_origin(self):
        history = self.workflow()["jobs"]["history"]
        publish = next(step for step in history["steps"] if step.get("id") == "publish")
        with tempfile.TemporaryDirectory(prefix="rcp-workflow-publish-") as directory:
            root = Path(directory)
            checkout = root / "checkout"
            remote = root / "history.git"
            environment = {
                **os.environ,
                "PYTHONPATH": str(Path(__file__).resolve().parent.parent),
                "GIT_CONFIG_GLOBAL": str(root / "gitconfig"),
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_ALLOW_PROTOCOL": "file",
                "GITHUB_OUTPUT": str(root / "output"),
                "GITHUB_REPOSITORY": "test/repo",
                "GH_TOKEN": "test-only-token",
                "AUTH_SETUP": str(root / "auth-setup"),
            }

            def git(*arguments):
                return subprocess.run(
                    ["git", *arguments], env=environment,
                    capture_output=True, text=True, check=True, timeout=10,
                )

            git("init", "--quiet", str(checkout))
            git("init", "--quiet", "--bare", str(remote))
            git("config", "--global", f"url.{remote.as_uri()}.insteadOf", "https://github.com/test/repo.git")
            self.assertEqual(git("-C", str(checkout), "remote").stdout, "")
            records = [publication_result(character * 32) for character in ("1", "2")]
            for mode, record in zip(("local", "loopback"), records):
                path = checkout / "downloaded-results" / f"benchmark-results-{mode}" / "results.json"
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps(record))
            script = (
                'gh() { [[ "$*" == "auth setup-git --hostname github.com" && "$GH_TOKEN" == "test-only-token" ]] || return 1; '
                'printf "configured\\n" > "$AUTH_SETUP"; }\n'
            ) + publish["run"]
            completed = subprocess.run(
                ["bash", "-c", script], cwd=checkout, env=environment,
                capture_output=True, text=True, timeout=25,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(publish["env"]["GH_TOKEN"], "${{ github.token }}")
            self.assertEqual((root / "auth-setup").read_text(), "configured\n")
            self.assertEqual((root / "output").read_text(), "published=true\n")
            for record in records:
                stored = git("-C", str(remote), "show", f"benchmark-history:runs/{record['run_id']}.json")
                self.assertEqual(json.loads(stored.stdout), record)
            self.assertEqual(git("-C", str(checkout), "remote").stdout, "")

    def test_each_mode_keeps_a_distinct_artifact(self):
        job = self.workflow()["jobs"]["benchmark"]
        self.assertEqual(job["strategy"]["matrix"]["mode"], ["local", "loopback"])
        upload = next(step for step in job["steps"] if step.get("name") == "Upload measurements and logs")
        self.assertEqual(upload["with"]["name"], "benchmark-results-${{ matrix.mode }}")
        history = self.workflow()["jobs"]["history"]
        self.assertNotIn("'push'", history["if"])
        download = next(step for step in history["steps"] if step.get("id") == "measurements")
        self.assertEqual(download["with"]["pattern"], "benchmark-results-*")
        self.assertNotIn("merge-multiple", download["with"])

    def test_pages_packages_rendered_history_for_direct_pinned_upload(self):
        project = Path(__file__).resolve().parent.parent
        workflow = yaml.safe_load((project / ".github/workflows/benchmark-pages.yml").read_text())
        steps = workflow["jobs"]["publish"]["steps"]
        upload = next(step for step in steps if "upload" in step.get("uses", ""))
        self.assertRegex(upload["uses"], r"^actions/upload-artifact@[0-9a-f]{40}$")
        self.assertEqual(upload["with"], {
            "name": "github-pages", "path": "${{ runner.temp }}/artifact.tar",
            "retention-days": 1, "if-no-files-found": "error",
        })
        render = next(step for step in steps if step.get("name") == "Render history using trusted main-branch code")
        summarize = next(step for step in steps if step.get("name") == "Summarize historical changes")
        self.assertEqual(summarize["env"]["REPORT_URL"], "${{ steps.deployment.outputs.page_url }}")
        deployment = next(step for step in steps if step.get("id") == "deployment")
        self.assertLess(steps.index(deployment), steps.index(summarize))
        package = next(step for step in steps if step.get("name") == "Package dashboard for Pages")
        self.assertLess(steps.index(render), steps.index(package))
        self.assertLess(steps.index(package), steps.index(upload))
        with tempfile.TemporaryDirectory(prefix="rcp-pages-package-") as directory:
            root = Path(directory)
            history = root / "benchmark-history-data" / "runs"
            history.mkdir(parents=True)
            record = publication_result("1" * 32)
            (history / "run.json").write_text(json.dumps(record))
            environment = {**os.environ, "PYTHONPATH": str(project), "RUNNER_TEMP": str(root)}
            subprocess.run(
                ["bash", "-e", "-c", render["run"]], cwd=root, env=environment,
                capture_output=True, text=True, check=True, timeout=10,
            )
            environment.update(REPORT_URL="https://example.test/rcp/", GITHUB_STEP_SUMMARY=str(root / "job-summary"))
            subprocess.run(
                ["bash", "-e", "-c", summarize["run"]], cwd=root, env=environment,
                capture_output=True, text=True, check=True, timeout=10,
            )
            summary = (root / "job-summary").read_text()
            self.assertIn(f"(https://example.test/rcp/index.html#run-{record['run_id']})", summary)
            self.assertNotIn("(index.html#", summary)
            site = root / "_site"
            (site / "linked-index.html").symlink_to("index.html")
            os.link(site / "index.html", site / "hardlinked-index.html")
            for excluded in (".git", ".github"):
                (site / excluded).mkdir()
                (site / excluded / "private").write_text("not published")
            subprocess.run(
                ["bash", "-e", "-c", package["run"]], cwd=root, env=environment,
                capture_output=True, text=True, check=True, timeout=10,
            )
            with tarfile.open(root / "artifact.tar") as archive:
                members = archive.getmembers()
                self.assertTrue(all(not member.issym() and not member.islnk() for member in members))
                self.assertEqual({member.name.removeprefix("./") for member in members if member.isfile()}, {
                    "index.html", "history.json", "changes.json", "changes.md", "linked-index.html", "hardlinked-index.html",
                })
                for filename in ("index.html", "linked-index.html", "hardlinked-index.html"):
                    self.assertEqual(archive.extractfile(f"./{filename}").read(), (site / "index.html").read_bytes())
                self.assertEqual(json.load(archive.extractfile("./history.json")), {"schema_version": 1, "runs": [record]})


if __name__ == "__main__":
    unittest.main()
