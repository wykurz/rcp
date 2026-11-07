"""Offline contracts and real render/package checks for the Pages artifact handoff."""

import hashlib
import json
import os
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

from benchmarks.test_changes import Fragment, observation
from infra.aws_benchmark import production_reports as reports
from infra.aws_benchmark.common import ValidationError

PROJECT = Path(__file__).resolve().parent.parent
READER = "a" * 40
PINS = {
    "actions/checkout": "3d3c42e5aac5ba805825da76410c181273ba90b1",
    "actions/download-artifact": "3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
    "actions/upload-artifact": "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
    "actions/deploy-pages": "d6db90164ac5ed86f2b6aed7e0febac5b3c0c03e",
}


class BenchmarkWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load((PROJECT / ".github/workflows/benchmark-pages.yml").read_text(),
                                  Loader=yaml.BaseLoader)
        self.render = self.workflow["jobs"]["render"]
        self.deploy = self.workflow["jobs"]["deploy"]
        self.steps = self.render["steps"]

    def step(self, name):
        return next(step for step in self.steps if step.get("name") == name)

    def run_script(self, script, root, environment, *, check=True):
        result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script], cwd=root,
                                env=environment, capture_output=True, text=True, timeout=30)
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def batch(self, root, *, state="ready", active="idle"):
        directory = root / "public-records"
        directory.mkdir()
        raw = (PROJECT / "infra/aws_benchmark/fixtures/production-public-report-v1.json").read_bytes()
        value = json.loads(raw)
        value["run_key"] = "repo-655073755-run-123"
        raw = json.dumps(value).encode()
        entries = []
        if state == "ready":
            (directory / (value["run_key"] + ".json")).write_bytes(raw)
            entries = [dict(run_key=value["run_key"], sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))]
        index = dict(schema=reports.SCHEMA, repository_id="655073755", reader_revision=READER,
            observed_at="2026-11-07T12:00:00+00:00", retention_days=90, discovery_complete=True,
            processing_complete=state != "deferred", active=dict(state=active, retired=False),
            observations=[dict(state=state, retired=state == "ready", run_key=value["run_key"])],
            records=entries)
        (directory / "index.json").write_text(json.dumps(index))
        environment = dict(os.environ, PYTHONPATH=str(PROJECT), RUNNER_TEMP=str(root),
            EXPECTED_ARTIFACT="123", EXPECTED_READER=READER, EXPECTED_INVENTORY=reports.inventory(directory),
            GITHUB_OUTPUT=str(root / "job-output"), GITHUB_STEP_SUMMARY=str(root / "job-summary"))
        return value, raw, environment

    def site_environment(self):
        return dict(GITHUB_REPOSITORY="wykurz/rcp", GITHUB_REPOSITORY_ID="655073755",
            GITHUB_REF="refs/heads/main", GITHUB_SHA=READER, GITHUB_WORKFLOW_SHA=READER,
            GITHUB_ACTOR_ID="483465", GITHUB_RUN_ATTEMPT="1", GITHUB_EVENT_NAME="push",
            GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/aws-benchmark-publish.yml@refs/heads/main",
            PAGES_ENABLED="true", PUBLICATION_ENABLED="true",
            EXPECTED_ARTIFACT="123", EXPECTED_INVENTORY="b" * 64, EXPECTED_READER=READER)

    def test_pages_accepts_main_push_manual_and_exact_reusable_artifact_inputs(self):
        workflow = self.workflow
        self.assertEqual(set(workflow["on"]), {"push", "workflow_dispatch", "workflow_call"})
        self.assertEqual(workflow["on"]["push"]["branches"], ["main"])
        inputs = workflow["on"]["workflow_call"]["inputs"]
        self.assertEqual(set(inputs), {"aws_records_artifact", "aws_inventory_sha256", "aws_reader_revision"})
        for item in inputs.values():
            self.assertEqual(item["type"], "string")
            self.assertEqual(item["required"], "true")
        for name, input_name in (("EXPECTED_ARTIFACT", "aws_records_artifact"),
                ("EXPECTED_INVENTORY", "aws_inventory_sha256"), ("EXPECTED_READER", "aws_reader_revision")):
            self.assertEqual(workflow["env"][name], "${{ inputs." + input_name + " }}")
        for guard in ("github.repository == 'wykurz/rcp'", "github.repository_id == '655073755'",
                "github.ref == 'refs/heads/main'", "github.run_attempt == '1'",
                "vars.RCP_BENCHMARK_PAGES == 'true'",
                "(inputs.aws_records_artifact != '' && vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED == 'true')",
                "(inputs.aws_records_artifact == '' && vars.RCP_AWS_PRODUCTION_PUBLISH_ENABLED != 'true')"):
            self.assertIn(guard, self.render["if"])
        self.assertEqual(workflow["concurrency"], {"group": "benchmark-pages-publish", "cancel-in-progress": "false"})

    def test_render_has_no_cloud_or_pages_privileges_and_deploy_does_not_render(self):
        self.assertEqual(set(self.workflow["jobs"]), {"render", "deploy"})
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        self.assertEqual(self.render["permissions"], {"contents": "read"})
        self.assertNotIn("environment", self.render)
        self.assertEqual(self.deploy["permissions"], {"contents": "read", "pages": "write", "id-token": "write"})
        self.assertEqual(self.deploy["environment"]["name"], "github-pages")
        self.assertEqual(self.deploy["needs"], "render")
        self.assertEqual(self.deploy["if"], "needs.render.outputs.current == 'true'")
        for job, timeout in ((self.render, "15"), (self.deploy, "10")):
            self.assertEqual(job["runs-on"], "ubuntu-24.04")
            self.assertEqual(job["cache-mode"], "none")
            self.assertEqual(job["timeout-minutes"], timeout)
        text = json.dumps(self.workflow)
        for forbidden in ("secrets.", "configure-aws-credentials", "create-github-app-token",
                "production_reports download", "production_publish ", "--aws-history", "aws-benchmark-history"):
            self.assertNotIn(forbidden, text)
        privileged_steps = self.deploy["steps"]
        self.assertEqual(len(privileged_steps), 3)
        self.assertEqual(privileged_steps[1]["run"],
                         "python3 -B -m infra.aws_benchmark.production_reports site-gate")
        self.assertEqual(privileged_steps[2]["id"], "deployment")
        setup = next(step for step in self.steps if step.get("uses") == "./.depot/actions/setup-report-python")
        self.assertNotIn("with", setup)  # never opt the rendering job into the AWS SDK test closure

    def test_fixed_action_pins_and_uncredentialed_exact_source_checkouts(self):
        seen = set()
        for job in (self.render, self.deploy):
            for step in job["steps"]:
                action = step.get("uses", "")
                if not action or action.startswith("./"):
                    continue
                name, revision = action.split("@")
                self.assertIn(name, PINS)
                self.assertEqual(revision, PINS[name])
                seen.add(name)
                if name == "actions/checkout":
                    self.assertEqual(step["with"]["persist-credentials"], "false")
        self.assertEqual(seen, set(PINS))
        for job in (self.render, self.deploy):
            self.assertEqual(job["steps"][0]["with"],
                             {"ref": "${{ github.sha }}", "persist-credentials": "false"})
        data = next(step for step in self.steps if step.get("with", {}).get("path") == "benchmark-history-data")
        self.assertEqual(data["with"], {"ref": "benchmark-history", "path": "benchmark-history-data",
                                       "persist-credentials": "false"})

    def test_artifact_binding_verification_and_fresh_main_gates_precede_packaging(self):
        download = next(step for step in self.steps if step.get("uses", "").startswith("actions/download-artifact@"))
        self.assertEqual(download["with"], {"artifact-ids": "${{ inputs.aws_records_artifact }}",
            "path": "${{ runner.temp }}/public-records", "merge-multiple": "true", "digest-mismatch": "error"})
        verify = self.step("Verify the exact predecessor and complete retained view without cloud credentials")
        self.assertEqual(download["if"], "steps.source.outputs.current == 'true' && inputs.aws_records_artifact != ''")
        self.assertEqual(verify["if"], download["if"])
        for binding in ('--input "$RUNNER_TEMP/public-records"', '--reader-revision "$EXPECTED_READER"',
                        '--expected-sha256 "$EXPECTED_INVENTORY"', "--require-complete"):
            self.assertIn(binding, verify["run"])
        landing = self.step("Render project landing page and maintained reports")
        for binding in ('--aws-records "$RUNNER_TEMP/public-records"', '--aws-reader-revision "$EXPECTED_READER"',
                        '--aws-inventory-sha256 "$EXPECTED_INVENTORY"', '"${args[@]}"'):
            self.assertIn(binding, landing["run"])
        source = next(step for step in self.steps if step.get("id") == "source")
        current = next(step for step in self.steps if step.get("id") == "current")
        package = next(step for step in self.steps if step.get("id") == "package")
        for gate in (source, current):
            self.assertEqual(gate["run"], "python3 -B -m infra.aws_benchmark.production_reports site-gate")
        order = [source, download, verify, self.step("Render history using trusted main-branch code"), landing, current, package]
        self.assertEqual([self.steps.index(step) for step in order], sorted(self.steps.index(step) for step in order))
        self.assertEqual(landing["if"], "steps.source.outputs.current == 'true'")
        self.assertNotIn("continue-on-error", verify)
        self.assertNotIn("continue-on-error", self.render)
        self.assertEqual(self.render["outputs"], {"current": "${{ steps.current.outputs.current }}",
            "artifact_name": "github-pages-${{ github.run_id }}-${{ github.run_attempt }}"})
        self.assertEqual(package["if"], "steps.current.outputs.current == 'true'")
        upload = next(step for step in self.steps if step.get("uses", "").startswith("actions/upload-artifact@"))
        self.assertEqual(upload["if"], package["if"])
        self.assertLess(self.steps.index(package), self.steps.index(upload))
        self.assertEqual(upload["with"], {"name": self.render["outputs"]["artifact_name"],
            "path": "${{ runner.temp }}/artifact.tar", "retention-days": "1", "if-no-files-found": "error"})
        deployment = self.deploy["steps"][-1]
        self.assertEqual(deployment["if"], "steps.current.outputs.current == 'true'")
        self.assertEqual(deployment["with"], {"artifact_name": "${{ needs.render.outputs.artifact_name }}"})

    def test_enabled_reporting_cannot_fall_back_to_docs_or_accept_partial_proofs(self):
        environment = self.site_environment()
        self.assertEqual(reports.site_identity(environment), READER)
        bad = [{key: ""} for key in ("EXPECTED_ARTIFACT", "EXPECTED_INVENTORY", "EXPECTED_READER")]
        bad += [dict(EXPECTED_ARTIFACT="", EXPECTED_INVENTORY="", EXPECTED_READER=""),
                dict(EXPECTED_ARTIFACT="123,456"), dict(EXPECTED_INVENTORY="invalid"),
                dict(EXPECTED_READER="c" * 40), dict(PUBLICATION_ENABLED="false"),
                dict(GITHUB_WORKFLOW_SHA="c" * 40), dict(GITHUB_ACTOR_ID="1"),
                dict(GITHUB_REPOSITORY_ID="1"), dict(GITHUB_RUN_ATTEMPT="2")]
        for change in bad:
            with self.subTest(change=change):
                observe = Mock(side_effect=AssertionError("untrusted mode must fail before observing main"))
                with self.assertRaises(ValidationError):
                    reports.site_gate(environment | change, observe=observe)
                observe.assert_not_called()
        docs = environment | dict(EXPECTED_ARTIFACT="", EXPECTED_INVENTORY="", EXPECTED_READER="",
            GITHUB_WORKFLOW_REF="wykurz/rcp/.github/workflows/benchmark-pages.yml@refs/heads/main")
        for flag in ("", "false"):
            self.assertEqual(reports.site_identity(docs | dict(PUBLICATION_ENABLED=flag)), READER)
        for flag in ("true", "invalid"):
            with self.subTest(flag=flag), self.assertRaises(ValidationError):
                reports.site_identity(docs | dict(PUBLICATION_ENABLED=flag))

    def test_stale_main_emits_no_deployment_permission_and_unavailable_observation_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}):
                self.assertFalse(reports.site_gate(self.site_environment(), observe=lambda: "c" * 40))
                self.assertIn("current=false\n", output.read_text())
                output.unlink()
                self.assertTrue(reports.site_gate(self.site_environment(), observe=lambda: READER))
                self.assertIn("current=true\n", output.read_text())
                output.unlink()
                with self.assertRaises(ConnectionError):
                    reports.site_gate(self.site_environment(), observe=Mock(side_effect=ConnectionError))
                self.assertFalse(output.exists())

    def test_real_inspection_rejects_held_incomplete_or_mismatched_evidence_before_render(self):
        script = self.step("Verify the exact predecessor and complete retained view without cloud credentials")["run"]
        cases = [
            ("held-record", dict(state="held"), {}, False),
            ("deferred-record", dict(state="deferred"), {}, False),
            ("held-owner", dict(active="held"), {}, False),
            ("wrong-reader", {}, dict(EXPECTED_READER="c" * 40), False),
            ("wrong-digest", {}, dict(EXPECTED_INVENTORY="0" * 64), False),
            ("complete", {}, {}, True),
        ]
        for label, batch_options, overrides, valid in cases:
            with self.subTest(case=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _, _, environment = self.batch(root, **batch_options)
                result = self.run_script(script + "\nprintf rendered > render-reached", root,
                                         environment | overrides, check=False)
                self.assertEqual(result.returncode == 0, valid, result.stdout + result.stderr)
                self.assertEqual((root / "render-reached").exists(), valid)

    def test_real_render_summary_and_tar_include_retained_reports_and_depot_history(self):
        render = self.step("Render history using trusted main-branch code")
        landing = self.step("Render project landing page and maintained reports")
        verify = self.step("Verify the exact predecessor and complete retained view without cloud credentials")
        summarize = self.step("Summarize historical changes")
        package = next(step for step in self.steps if step.get("id") == "package")
        self.assertEqual(summarize["if"], "always() && steps.render.outcome == 'success'")
        self.assertEqual(render["id"], "render")
        self.assertNotIn("env", summarize)  # render has no deployment URL yet
        with tempfile.TemporaryDirectory(prefix="rcp-pages-package-") as directory:
            root = Path(directory)
            public, raw_public, environment = self.batch(root)
            (root / "scripts").symlink_to(PROJECT / "scripts", target_is_directory=True)
            history = root / "benchmark-history-data" / "runs"
            history.mkdir(parents=True)
            before = observation("1", "2026-09-26T12:00:00Z")
            record = observation("2", "2026-09-27T12:00:00Z")
            record["context"]["runner_label"] = "literal (index.html#run-metadata)"
            (history / "before.json").write_text(json.dumps(before))
            (history / "run.json").write_text(json.dumps(record))
            for step in (verify, render, landing, summarize):
                self.run_script(step["run"], root, environment)
            summary = (root / "job-summary").read_text()
            parsed = Fragment(summary)
            self.assertFalse(parsed.elements("a"))
            self.assertIn(before["run_id"], parsed.text(parsed.rows()[0][4]))
            self.assertIn(before["timestamp"], parsed.text(parsed.rows()[0][4]))
            self.assertEqual(parsed.text(parsed.rows()[0][3]), "1.000x (+0.0%)")
            self.assertIn("literal (index.html#run-metadata)", summary)
            self.assertIn("Retained AWS reporting: 1 ready", summary)
            site = root / "_site"
            history_page = site / "ci-benchmarks/index.html"
            self.assertIn('href="../reports/index.html"', history_page.read_text())
            self.assertIn('href="aws-benchmarks/index.html"', (site / "index.html").read_text())
            self.assertIn('href="../aws-benchmarks/index.html"', (site / "reports/index.html").read_text())
            self.assertIn('ci-benchmarks/changes.html', (site / "changes.html").read_text())
            self.assertIn("Still-retained records under the 90-day AWS lifecycle", (site / "aws-benchmarks/index.html").read_text())
            public_path = "aws-benchmarks/records/" + public["run_key"] + ".json"
            self.assertEqual((site / public_path).read_bytes(), raw_public)
            self.assertTrue((site / "aws-benchmarks/runs" / (public["run_key"] + ".html")).is_file())
            (site / "linked-index.html").symlink_to("ci-benchmarks/index.html")
            os.link(history_page, site / "hardlinked-index.html")
            expected_files = {path.relative_to(site).as_posix() for path in site.rglob("*") if path.is_file()}
            for excluded in (".git", ".github"):
                (site / excluded).mkdir()
                (site / excluded / "private").write_text("not published")
            self.run_script(package["run"], root, environment)
            with tarfile.open(root / "artifact.tar") as archive:
                members = archive.getmembers()
                self.assertTrue(all(not member.issym() and not member.islnk() for member in members))
                self.assertEqual({member.name.removeprefix("./") for member in members if member.isfile()}, expected_files)
                for filename in ("ci-benchmarks/index.html", "linked-index.html", "hardlinked-index.html"):
                    self.assertEqual(archive.extractfile(f"./{filename}").read(), history_page.read_bytes())
                self.assertEqual(archive.extractfile("./" + public_path).read(), raw_public)
                self.assertEqual(json.load(archive.extractfile("./ci-benchmarks/history.json")),
                                 {"schema_version": 1, "runs": [before, record]})


if __name__ == "__main__":
    unittest.main()
