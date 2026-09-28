"""Behavior tests for benchmark timing collection and reports."""

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from benchmarks import report, run
from benchmarks import timings
from benchmarks.test_report import sample_run


def timing_report(role="rcp-master"):
    return {"schema_version": 1, "identifier": role, "pid": 123,
            "scopes": [{"name": "operation", "count": 2, "finished": 1,
                        "interrupted": 1, "total_seconds": 3.0,
                        "mean_seconds": 1.5, "p50_seconds": 1.0,
                        "p95_seconds": 2.0, "max_seconds": 2.0}]}


class TimingTests(unittest.TestCase):
    def _fixture(self, root, write_report=True):
        binary = root / "bin"
        binary.mkdir()
        filegen = binary / "filegen"
        filegen.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'0'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n")
        rcp = binary / "rcp"
        writer = """\nif prefix:\n roles=[('rcp-master',123)]\n if '--force-remote' in sys.argv: roles += [('rcpd-source',124),('rcpd-destination',125)]\n for role,pid in roles:\n  report={'schema_version':1,'identifier':role,'pid':pid,'scopes':[{'name':'operation','count':1,'finished':1,'interrupted':0,'total_seconds':0.5,'mean_seconds':0.5,'p50_seconds':0.5,'p95_seconds':0.5,'max_seconds':0.5}]}\n  pathlib.Path(prefix+f'-{role}-host-{pid}-stamp.timings.json').write_text(json.dumps(report))\n""" if write_report else ""
        rcp.write_text("#!/usr/bin/env python3\nimport json,pathlib,shutil,sys\nif '--version' in sys.argv: print('rcp 1'); sys.exit(0)\nif '--help' in sys.argv: print('--timings=PREFIX'); sys.exit(0)\nprefix=next((arg.split('=',1)[1] for arg in sys.argv if arg.startswith('--timings=')),None)\nshutil.copytree(sys.argv[-2].removeprefix('localhost:'),sys.argv[-1])\n" + writer)
        rcpd = binary / "rcpd"
        rcpd.write_text("#!/bin/sh\necho 'rcpd 1'\n")
        for executable in (filegen, rcp, rcpd):
            executable.chmod(0o755)
        manifest = root / "manifest.json"
        manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}], "variants": [{"id": "rcp-default", "tool": "rcp", "args": [], "processes": 1}]}))
        return binary, manifest

    def test_supported_rcp_collects_by_default_and_opt_out_splits_series(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = self._fixture(root)
            args = ["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--bin-dir", str(binary), "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--repetitions", "1"]
            timed = run.main([*args, "--output", str(root / "timed")])
            disabled = run.main([*args, "--no-timings", "--output", str(root / "disabled")])
            self.assertEqual(timed["context"]["timing_collection"]["rcp-default"], "coarse")
            self.assertEqual(timed["trials"][0]["timings"]["reports"], [timing_report() | {"scopes": [{"name": "operation", "count": 1, "finished": 1, "interrupted": 0, "total_seconds": 0.5, "mean_seconds": 0.5, "p50_seconds": 0.5, "p95_seconds": 0.5, "max_seconds": 0.5}]}])
            self.assertEqual(disabled["trials"][0]["timings"], {"status": "disabled", "reports": []})
            self.assertNotEqual(timed["summaries"][0]["series_id"], disabled["summaries"][0]["series_id"])
            self.assertIn("Cumulative elapsed", (root / "timed" / "summary.md").read_text())
            self.assertEqual(report.parse_result((root / "timed" / "results.json").read_text()), timed)

    def test_loopback_trial_collects_master_and_both_daemon_roles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = self._fixture(root)
            ssh = binary / "ssh"
            ssh.write_text("#!/bin/sh\nif [ \"$1\" = '-V' ]; then echo OpenSSH_test >&2; exit 0; fi\nexit 2\n")
            ssh.chmod(0o755)
            with mock.patch.object(run.shutil, "which", side_effect=lambda name: str(ssh) if name == "ssh" else None):
                result = run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--mode", "loopback", "--bin-dir", str(binary), "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--repetitions", "1", "--output", str(root / "out")])
            self.assertEqual({item["identifier"] for item in result["trials"][0]["timings"]["reports"]}, {"rcp-master", "rcpd-source", "rcpd-destination"})
            self.assertEqual(report.parse_result((root / "out" / "results.json").read_text()), result)

    def test_failed_loopback_keeps_valid_reports_and_command_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = self._fixture(root)
            rcp = binary / "rcp"
            rcp.write_text("#!/usr/bin/env python3\nimport json,pathlib,sys\nif '--version' in sys.argv: print('rcp 1'); sys.exit(0)\nif '--help' in sys.argv: print('--timings=PREFIX'); sys.exit(0)\nprefix=next(arg.split('=',1)[1] for arg in sys.argv if arg.startswith('--timings='))\nfor role,pid in [('rcp-master',123),('rcpd-destination',125)]:\n report={'schema_version':1,'identifier':role,'pid':pid,'scopes':[{'name':'operation','count':1,'finished':0,'interrupted':1,'total_seconds':0.5,'mean_seconds':0.5,'p50_seconds':0.5,'p95_seconds':0.5,'max_seconds':0.5}]}\n pathlib.Path(prefix+f'-{role}-host-{pid}-stamp.timings.json').write_text(json.dumps(report))\npathlib.Path(prefix+'-rcpd-source-host-124-stamp.timings.json').write_text('')\nsys.exit(1)\n")
            rcp.chmod(0o755)
            ssh = binary / "ssh"
            ssh.write_text("#!/bin/sh\nif [ \"$1\" = '-V' ]; then echo OpenSSH_test >&2; exit 0; fi\nexit 2\n")
            ssh.chmod(0o755)
            with mock.patch.object(run.shutil, "which", side_effect=lambda name: str(ssh) if name == "ssh" else None):
                with self.assertRaisesRegex(RuntimeError, "command failed or timed out"):
                    run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--mode", "loopback", "--bin-dir", str(binary), "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--repetitions", "1", "--output", str(root / "out")])
            saved = report.parse_result((root / "out" / "results.json").read_text())
            trial = saved["trials"][0]
            self.assertEqual(trial["exit_codes"], [1])
            self.assertEqual(trial["validation"]["error"], "command failed or timed out")
            self.assertIn("rcpd-source", trial["validation"]["timing_error"])
            self.assertEqual({item["identifier"] for item in trial["timings"]["reports"]}, {"rcp-master", "rcpd-destination"})

    def test_advertised_support_with_missing_report_fails_and_keeps_trial(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = self._fixture(root, write_report=False)
            with self.assertRaisesRegex(RuntimeError, "missing timing report"):
                run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--bin-dir", str(binary), "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--repetitions", "1", "--output", str(root / "out")])
            saved = report.parse_result((root / "out" / "results.json").read_text())
            self.assertEqual(saved["status"], "failed")
            self.assertEqual(saved["trials"][0]["timings"], {"status": "coarse", "reports": []})

    def test_successful_copy_with_malformed_summary_fails_timing_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = self._fixture(root)
            rcp = binary / "rcp"
            rcp.write_text("#!/usr/bin/env python3\nimport pathlib,shutil,sys\nif '--version' in sys.argv: print('rcp 1'); sys.exit(0)\nif '--help' in sys.argv: print('--timings=PREFIX'); sys.exit(0)\nprefix=next(arg.split('=',1)[1] for arg in sys.argv if arg.startswith('--timings='))\nshutil.copytree(sys.argv[-2],sys.argv[-1])\npathlib.Path(prefix+'-rcp-master-host-123-stamp.timings.json').write_text('')\n")
            rcp.chmod(0o755)
            with self.assertRaisesRegex(RuntimeError, "timing collection failed"):
                run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--bin-dir", str(binary), "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--repetitions", "1", "--output", str(root / "out")])
            trial = report.parse_result((root / "out" / "results.json").read_text())["trials"][0]
            self.assertEqual(trial["exit_codes"], [0])
            self.assertIn("Expecting value", trial["validation"]["timing_error"])
            self.assertEqual(trial["status"], "failed")

    def test_older_baseline_is_explicitly_unsupported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary, manifest = self._fixture(root)
            baseline = root / "baseline"
            baseline.mkdir()
            old = baseline / "rcp"
            old.write_text("#!/usr/bin/env python3\nimport shutil,sys\nif '--version' in sys.argv: print('old rcp'); sys.exit(0)\nif '--help' in sys.argv: print('old help'); sys.exit(0)\nshutil.copytree(sys.argv[-2],sys.argv[-1])\n")
            old.chmod(0o755)
            old_daemon = baseline / "rcpd"
            old_daemon.write_text("#!/bin/sh\necho 'old rcpd'\n")
            old_daemon.chmod(0o755)
            result = run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default", "--bin-dir", str(binary), "--baseline-bin-dir", str(baseline), "--source-root", str(root), "--destination-root", str(root), "--cache", "uncontrolled", "--repetitions", "1", "--output", str(root / "out")])
            self.assertEqual(result["context"]["timing_collection"], {"rcp-default": "coarse", "rcp-baseline": "unsupported"})
            self.assertEqual(result["context"]["timing_capability"], {"rcp-default": True, "rcp-baseline": False})
            baseline_trial = next(trial for trial in result["trials"] if trial["variant_id"] == "rcp-baseline")
            self.assertEqual(baseline_trial["timings"], {"status": "unsupported", "reports": []})
            self.assertNotIn("--timings", " ".join(baseline_trial["commands"][0]))

    def test_collection_rejects_duplicate_keys_and_oversize_files(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "trace"
            artifact = Path(directory) / "trace-rcp-master-host-123-stamp.timings.json"
            artifact.write_text('{"schema_version":1,"schema_version":1}')
            with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
                timings.collect(prefix, ["rcp-master"], successful=False)
            artifact.write_text(" " * (timings.MAX_REPORT_BYTES + 1))
            with self.assertRaisesRegex(ValueError, "invalid timing report file"):
                timings.collect(prefix, ["rcp-master"], successful=False)

    def test_collection_rejects_filename_with_wrong_pid(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "trace"
            artifact = Path(directory) / "trace-rcp-master-host-999-stamp.timings.json"
            artifact.write_text(json.dumps(timing_report()))
            with self.assertRaisesRegex(ValueError, "filename"):
                timings.collect(prefix, ["rcp-master"], successful=False)

    def test_rejects_malformed_timing_artifacts(self):
        for mutation in (
            lambda item: item.update(schema_version=True),
            lambda item: item.update(pid=False),
            lambda item: item["scopes"][0].update(count=3),
            lambda item: item["scopes"][0].update(total_seconds=-1),
            lambda item: item["scopes"][0].update(p95_seconds=float("inf")),
            lambda item: item["scopes"][0].update(name="<script>" * 1000),
        ):
            with self.subTest(mutation=mutation):
                item = timing_report()
                mutation(item)
                with self.assertRaises(ValueError):
                    timings.validate_report(item)

    def test_collection_rejects_missing_or_duplicate_roles_on_success(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "trace"
            with self.assertRaisesRegex(ValueError, "missing.*rcp-master"):
                timings.collect(prefix, ["rcp-master"], successful=True)
            for index in (1, 2):
                item = timing_report()
                item["pid"] = index
                (Path(directory) / f"trace-rcp-master-host-{index}-stamp.timings.json").write_text(json.dumps(item))
            with self.assertRaisesRegex(ValueError, "duplicate.*rcp-master"):
                timings.collect(prefix, ["rcp-master"], successful=True)

    def test_collection_accepts_parallel_role_cardinality_above_sixty_four(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "trace"
            roles = ["rcp-master", "rcpd-source", "rcpd-destination"] * 22
            for pid, role in enumerate(roles, 1):
                item = timing_report(role)
                item["pid"] = pid
                (Path(directory) / f"trace-{role}-host-{pid}-stamp.timings.json").write_text(json.dumps(item))
            self.assertEqual(len(timings.collect(prefix, roles, successful=True)), 66)

    def test_failed_trial_retains_partial_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "trace"
            (Path(directory) / "trace-rcp-master-host-123-stamp.timings.json").write_text(json.dumps(timing_report()))
            (Path(directory) / "trace-rcp-master-host-123-stamp.json").write_text("not a timing report")
            (Path(directory) / "trace-rcp-master-host-123-stamp.scopes.json").write_text("not a timing report")
            self.assertEqual(timings.collect(prefix, ["rcp-master", "rcpd-source", "rcpd-destination"], successful=False), [timing_report()])

    def test_collection_keeps_valid_reports_after_many_malformed_files(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "trace"
            for index in range(12):
                (Path(directory) / f"trace-broken-host-{index}-stamp.timings.json").write_text("")
            for pid, role in enumerate(("rcp-master", "rcpd-source", "rcpd-destination"), 1):
                item = timing_report(role)
                item["pid"] = pid
                (Path(directory) / f"trace-{role}-host-{pid}-stamp.timings.json").write_text(json.dumps(item))
            with self.assertRaises(timings.CollectionError) as failure:
                timings.collect(prefix, ["rcp-master", "rcpd-source", "rcpd-destination"], successful=False)
            self.assertEqual({item["identifier"] for item in failure.exception.reports}, {"rcp-master", "rcpd-source", "rcpd-destination"})

    def test_result_accepts_history_without_timings_and_validates_new_reports(self):
        old = sample_run()
        self.assertIs(report.validate_result(old), old)
        new = sample_run()
        new["context"]["timing_collection"] = {"rcp-default": "coarse"}
        for trial in new["trials"]:
            trial["timings"] = {"status": "coarse", "reports": [timing_report(), timing_report("rcpd-source"), timing_report("rcpd-destination")]}
        self.assertIs(report.validate_result(new), new)
        new["trials"][0]["timings"]["reports"].pop()
        with self.assertRaisesRegex(ValueError, "missing timing report"):
            report.validate_result(new)
        new["trials"][0]["timings"]["reports"].append(timing_report("rcpd-source"))
        new["trials"][0]["timings"]["reports"][0]["scopes"][0]["count"] = 4
        with self.assertRaisesRegex(ValueError, "count"):
            report.validate_result(new)

    def test_declared_collection_requires_trial_timings(self):
        current = sample_run()
        current["context"]["timing_request"] = "automatic"
        current["context"]["timing_collection"] = {"rcp-default": "coarse"}
        current["context"]["timing_capability"] = {"rcp-default": True}
        with self.assertRaisesRegex(ValueError, "trials\\[0\\]\\.timings"):
            report.validate_result(current)
        current["trials"][0]["timings"] = {"status": "coarse", "reports": [timing_report(), timing_report("rcpd-source"), timing_report("rcpd-destination")]}
        with self.assertRaisesRegex(ValueError, "trials\\[1\\]\\.timings"):
            report.validate_result(current)

    def test_new_trial_requires_collection_policy_even_if_mapping_removed(self):
        current = sample_run()
        current["context"]["timing_request"] = "automatic"
        with self.assertRaisesRegex(ValueError, "context.timing_collection"):
            report.validate_result(current)

    def test_dashboard_shows_repeat_role_and_scope_with_safe_text(self):
        if not shutil.which("node"):
            self.skipTest("Node is unavailable")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            current = sample_run()
            current["context"]["timing_collection"] = {"rcp-default": "coarse"}
            malicious = '<img src=x onerror="alert(1)">'
            for trial in current["trials"]:
                item = timing_report()
                malicious_scope = dict(item["scopes"][0])
                malicious_scope["name"] = malicious
                item["scopes"].append(malicious_scope)
                trial["timings"] = {"status": "coarse", "reports": [item, timing_report("rcpd-source"), timing_report("rcpd-destination")]}
            source = root / "results.json"
            source.write_text(json.dumps(current))
            output = root / "site"
            report.render(source, output)
            page = (output / "index.html").read_text()
            self.assertNotIn(malicious, page)
            script = """const fs=require('fs'),vm=require('vm'); const html=fs.readFileSync(process.argv[1],'utf8'); const data=html.match(/<script type=\"application\\/json\" id=\"history-data\">([\\s\\S]*?)<\\/script>/)[1]; const code=html.match(/<script>\\s*([\\s\\S]*?)<\\/script>/)[1]; class E{constructor(tag){this.tagName=tag;this.children=[];this.textContent='';this.value='';this.style={}} append(...xs){this.children.push(...xs)} replaceChildren(...xs){this.children=xs} setAttribute(){} addEventListener(){}} const ids=Object.fromEntries(['stats','case-filter','variant-filter','group-filter','series-filter','chart','legend','run-rows'].map(x=>[x,new E(x)])); ids['history-data']=new E('script');ids['history-data'].textContent=data;vm.runInNewContext(code,{document:{getElementById:x=>ids[x],createElement:x=>new E(x),createElementNS:(_,x)=>new E(x)}}); const flatten=x=>[x,...x.children.flatMap(flatten)];console.log(flatten(ids['run-rows']).map(x=>x.textContent).join(' '))"""
            process = subprocess.run(["node", "-e", script, str(output / "index.html")], capture_output=True, text=True)
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertIn("Cumulative elapsed", process.stdout)
            self.assertIn("rcp-master", process.stdout)
            self.assertIn(malicious, process.stdout)

    def test_dashboard_labels_historical_results_without_timings(self):
        if not shutil.which("node"):
            self.skipTest("Node is unavailable")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "results.json"
            source.write_text(json.dumps(sample_run()))
            output = root / "site"
            report.render(source, output)
            self.assertIn("No scoped timings", (output / "index.html").read_text())

    def test_series_identity_changes_with_effective_timing_policy(self):
        case = {"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}
        variant = {"id": "rcp-default", "tool": "rcp", "args": [], "processes": 1}
        environment = {"filesystem": {"source": {}, "destination": {}}}
        first = run.series_id(case, variant, "uncontrolled", "local", "local", environment, {}, timing_collection="coarse")
        second = run.series_id(case, variant, "uncontrolled", "local", "local", environment, {}, timing_collection="disabled")
        self.assertNotEqual(first, second)

    def test_run_level_request_splits_non_rcp_series(self):
        case = {"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}
        variant = {"id": "cp-a", "tool": "cp", "args": ["-a"], "processes": 1}
        environment = {"filesystem": {"source": {}, "destination": {}}}
        automatic = run.series_id(case, variant, "uncontrolled", "local", "local", environment, {}, timing_collection="not_applicable", timing_request="automatic")
        disabled = run.series_id(case, variant, "uncontrolled", "local", "local", environment, {}, timing_collection="not_applicable", timing_request="disabled")
        self.assertNotEqual(automatic, disabled)


if __name__ == "__main__":
    unittest.main()
