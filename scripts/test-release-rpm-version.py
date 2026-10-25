#!/usr/bin/env python3
"""Exercise RPM metadata mapping and the actual workflow package arguments offline."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/release-rpm-version.py"
CASES = {
    "1.2.3": "1.2.3",
    "0.41.0-rc.1": "0.41.0~rc.1",
    "1.2.3-rc-1.2": "1.2.3~rc_1.2",
    "1.2.3+build.7": "1.2.3+build.7",
    "1.2.3+build-with-hyphen.7": "1.2.3+build_with_hyphen.7",
    "1.2.3-rc.1+build-with-hyphen.7": "1.2.3~rc.1+build_with_hyphen.7",
}


class RpmVersionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="rcp-rpm-version-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.manifest = self.root / "Cargo.toml"

    def manifest_version(self, version):
        self.manifest.write_text('[workspace.package]\nversion = ' + json.dumps(version) + '\n')

    def invoke(self, tag):
        return subprocess.run([sys.executable, str(HELPER), tag], cwd=self.root,
                              capture_output=True, text=True)

    def test_cli_preserves_identity_and_maps_rpm_metadata(self):
        for version, expected in CASES.items():
            with self.subTest(version=version):
                self.manifest_version(version)
                before = self.manifest.read_bytes()
                result = self.invoke("v" + version)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, expected + "\n")
                self.assertEqual(self.manifest.read_bytes(), before)
                self.assertNotIn("-", expected)

    def test_mismatched_tag_cannot_emit_metadata(self):
        self.manifest_version("1.2.3")
        for tag in ("v1.2.4", "1.2.3", "refs/tags/v1.2.3", "v1.2.3\n"):
            with self.subTest(tag=tag):
                result = self.invoke(tag)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("must match", result.stderr)

    def test_invalid_versions_cannot_emit_metadata(self):
        for version in ("1.2", "01.2.3", "1.2.3-01", "1.2.3-rc..1", "1.2.3+", "1.2.3~rc.1",
                        "1.2.3+build_name", "1.2.3+\"injection", "1.2.3\nversion=evil", "1.2.3-é"):
            with self.subTest(version=version):
                self.manifest_version(version)
                result = self.invoke("v" + version)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")

    def test_missing_or_malformed_manifest_fails_without_metadata(self):
        for content in (None, "[workspace", "[workspace]\n", "[workspace.package]\nversion = 123\n"):
            with self.subTest(content=content):
                if content is not None:
                    self.manifest.write_text(content)
                result = self.invoke("v1.2.3")
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")

    def test_workflow_passes_metadata_to_each_target_and_package(self):
        flow = yaml.load((ROOT / ".github/workflows/release.yml").read_text(), Loader=yaml.BaseLoader)
        scripts = self.root / "scripts"
        scripts.mkdir()
        shutil.copyfile(HELPER, scripts / HELPER.name)
        # record the real shell's argument boundaries without compiling or publishing packages
        cargo = scripts / "cargo-host.sh"
        cargo.write_text("#!/usr/bin/env python3\nimport json, os, sys\n"
                         "with open(os.environ['RPM_TRACE'], 'a') as out:\n"
                         "    out.write(json.dumps([os.environ.get('CARGO_BUILD_TARGET'), sys.argv[1:]]) + '\\n')\n")
        cargo.chmod(0o755)
        binaries = self.root / "bin"
        binaries.mkdir()
        zip_mock = binaries / "zip"
        zip_mock.write_text("#!/usr/bin/env python3\nimport pathlib, sys\npathlib.Path(sys.argv[1]).touch()\n")
        zip_mock.chmod(0o755)
        for job_name, target in (("build-rpm-amd64", "x86_64-unknown-linux-musl"),
                                 ("build-rpm-arm64", "aarch64-unknown-linux-musl")):
            job = flow["jobs"][job_name]
            step = next(s for s in job["steps"] if s.get("name") == "Build RPM packages")
            install = next(s for s in job["steps"] if s.get("name") == "Install cargo-generate-rpm")
            self.assertIn("cargo-generate-rpm --version 0.21.0 --locked", install["run"])
            self.assertNotIn("continue-on-error", step)
            self.assertNotIn("if", step)
            self.assertNotIn("defaults", flow)
            self.assertNotIn("defaults", job)
            self.assertNotIn("working-directory", step)
            self.assertEqual(step.get("shell", "bash"), "bash")
            for version, expected in CASES.items():
                with self.subTest(job=job_name, version=version):
                    self.manifest_version(version)
                    trace = self.root / "trace.jsonl"
                    trace.unlink(missing_ok=True)
                    env = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"],
                               RELEASE_TAG="v" + version, RPM_TRACE=str(trace))
                    env.pop("BASH_ENV", None)
                    result = subprocess.run(["bash", "-e", "-c", step["run"]], cwd=self.root,
                                            env=env, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    calls = [json.loads(line) for line in trace.read_text().splitlines()]
                    generated = [(arch, args) for arch, args in calls if args[0] == "generate-rpm"]
                    self.assertEqual(generated, [(target, ["generate-rpm", "--target=" + target, "-p", package,
                                                          "-s", 'version = "' + expected + '"'])
                                                 for package in ("rcp", "rlink", "rrm", "rcmp", "filegen")])
                    trace.unlink()
                    env["RELEASE_TAG"] = "v9.9.9"
                    mismatch = subprocess.run(["bash", "-e", "-c", step["run"]], cwd=self.root,
                                              env=env, capture_output=True, text=True)
                    self.assertNotEqual(mismatch.returncode, 0)
                    self.assertFalse(trace.exists(), "version mismatch must fail before packaging")


if __name__ == "__main__":
    unittest.main()
