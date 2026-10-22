import json
import os
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

import yaml

from benchmarks.test_changes import Fragment, observation


class BenchmarkWorkflowTests(unittest.TestCase):
    def test_pages_accepts_main_push_manual_and_pinned_reusable_publication(self):
        project = Path(__file__).resolve().parent.parent
        workflow = yaml.load((project / ".github/workflows/benchmark-pages.yml").read_text(),
                             Loader=yaml.BaseLoader)
        self.assertEqual(set(workflow["on"]), {"push", "workflow_dispatch", "workflow_call"})
        self.assertEqual(workflow["on"]["workflow_call"]["inputs"], {"aws_history_commit": {
            "description": "Exact public history commit returned by the successful history writer",
            "type": "string", "required": "true"}})
        guard = workflow["jobs"]["publish"]["if"]
        self.assertIn("github.repository == 'wykurz/rcp'", guard)
        self.assertIn("github.ref == 'refs/heads/main'", guard)
        self.assertEqual(workflow["on"]["push"]["branches"], ["main"])

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
        self.assertEqual(summarize["if"], "always() && steps.render.outcome == 'success'")
        self.assertEqual(render["id"], "render")
        deployment = next(step for step in steps if step.get("id") == "deployment")
        self.assertLess(steps.index(deployment), steps.index(summarize))
        package = next(step for step in steps if step.get("id") == "package")
        self.assertLess(steps.index(render), steps.index(package))
        self.assertLess(steps.index(package), steps.index(upload))
        with tempfile.TemporaryDirectory(prefix="rcp-pages-package-") as directory:
            root = Path(directory)
            history = root / "benchmark-history-data" / "runs"
            history.mkdir(parents=True)
            before = observation("1", "2026-09-26T12:00:00Z")
            record = observation("2", "2026-09-27T12:00:00Z")
            record["context"]["runner_label"] = "literal (index.html#run-metadata)"
            (history / "before.json").write_text(json.dumps(before))
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
            parsed = Fragment(summary)
            reference = parsed.rows()[0][4]
            self.assertEqual(reference["children"][0]["attrs"]["href"], "https://example.test/rcp/ci-benchmarks/index.html#run-" + before["run_id"])
            self.assertIn(before["timestamp"], parsed.text(reference))
            self.assertTrue(all(link["attrs"]["href"].startswith("https://example.test/rcp/ci-benchmarks/index.html#run-") for link in parsed.elements("a")))
            self.assertIn("literal (index.html#run-metadata)", summary)
            # a failed deployment has no URL; the always() summary still retains the numeric evidence
            environment.update(REPORT_URL="", GITHUB_STEP_SUMMARY=str(root / "failed-deploy-summary"))
            subprocess.run(["bash", "-e", "-c", summarize["run"]], cwd=root, env=environment, capture_output=True, text=True, check=True, timeout=10)
            fallback = Fragment((root / "failed-deploy-summary").read_text())
            self.assertFalse(fallback.elements("a"))
            self.assertIn(before["run_id"], fallback.text(fallback.rows()[0][4]))
            self.assertEqual(fallback.text(fallback.rows()[0][3]), "1.000x (+0.0%)")
            site = root / "_site"
            history_page = site / "ci-benchmarks/index.html"
            self.assertIn('href="../reports/index.html"', history_page.read_text())
            # the report suite covers rendering; packaging must include public pages alongside history
            (site / "index.html").write_text('<!doctype html><title>Project landing page</title>')
            (site / "reports").mkdir()
            (site / "reports/index.html").write_text('<!doctype html><title>Reports</title>')
            (site / "linked-index.html").symlink_to("ci-benchmarks/index.html")
            os.link(history_page, site / "hardlinked-index.html")
            expected_files = {path.relative_to(site).as_posix() for path in site.rglob('*') if path.is_file()}
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
                self.assertEqual({member.name.removeprefix("./") for member in members if member.isfile()}, expected_files)
                for filename in ("ci-benchmarks/index.html", "linked-index.html", "hardlinked-index.html"):
                    self.assertEqual(archive.extractfile(f"./{filename}").read(), history_page.read_bytes())
                self.assertEqual(json.load(archive.extractfile("./ci-benchmarks/history.json")), {"schema_version": 1, "runs": [before, record]})


if __name__ == "__main__":
    unittest.main()
