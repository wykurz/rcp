#!/usr/bin/env python3
"""Exercise package inventories and the release workflow that assembles them."""
from collections import Counter
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/check-release-packages.py"
spec = importlib.util.spec_from_file_location("check_release_packages", SCRIPT)
M = importlib.util.module_from_spec(spec)
spec.loader.exec_module(M)


def filename(name, kind, arch, version="1.2.3"):
    architecture = M.ARCHITECTURES[arch][kind]
    return (f"{name}_{version}-1_{architecture}.deb" if kind == "deb"
            else f"{name}-{version}-1.{architecture}.rpm")


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def inventory(self, kind, arch, omit=()):
        directory = self.root / f"{kind}-{arch}"
        directory.mkdir()
        expected = list(M.packages(ROOT, kind).values())
        for name in expected:
            if name not in omit:
                (directory / filename(name, kind, arch)).write_bytes(b"package fixture")
        return directory, expected

    def test_all_formats_and_architectures_accept_exact_inventory(self):
        for kind in ("deb", "rpm"):
            for arch in M.ARCHITECTURES:
                with self.subTest(kind=kind, arch=arch):
                    directory, expected = self.inventory(kind, arch)
                    self.assertIn("rcp-tools-rchm", expected)
                    self.assertEqual(len(expected), 6)
                    M.validate(directory, kind, arch, expected)

    def test_missing_rchm_rejects_each_bundle(self):
        for kind in ("deb", "rpm"):
            for arch in M.ARCHITECTURES:
                with self.subTest(kind=kind, arch=arch):
                    directory, expected = self.inventory(kind, arch, omit=("rcp-tools-rchm",))
                    with self.assertRaisesRegex(ValueError, "missing=.*rcp-tools-rchm"):
                        M.validate(directory, kind, arch, expected)

    def test_duplicate_versions_reject_each_format(self):
        for kind in ("deb", "rpm"):
            directory, expected = self.inventory(kind, "amd64")
            (directory / filename("rcp-tools-rchm", kind, "amd64", "1.2.4")).write_bytes(b"stale package")
            with self.assertRaisesRegex(ValueError, "duplicates=.*rcp-tools-rchm"):
                M.validate(directory, kind, "amd64", expected)

    def test_unexpected_package_and_wrong_architecture_reject_each_format(self):
        for kind in ("deb", "rpm"):
            directory, expected = self.inventory(kind, "amd64")
            for name, arch in (("unrelated", "amd64"), ("rcp-tools-rchm", "arm64")):
                with self.subTest(kind=kind, name=name, arch=arch):
                    path = directory / filename(name, kind, arch)
                    path.write_bytes(b"unexpected package")
                    with self.assertRaisesRegex(ValueError, "unexpected"):
                        M.validate(directory, kind, "amd64", expected)
                    path.unlink()

    def test_longer_rpm_name_cannot_satisfy_missing_rchm(self):
        directory, expected = self.inventory("rpm", "amd64", omit=("rcp-tools-rchm",))
        (directory / filename("rcp-tools-rchm-extra", "rpm", "amd64")).write_bytes(b"wrong package")
        with self.assertRaisesRegex(ValueError, "unexpected"):
            M.validate(directory, "rpm", "amd64", expected)

    def test_empty_files_and_symlinks_are_not_packages(self):
        directory, expected = self.inventory("deb", "amd64", omit=("rcp-tools-rchm",))
        path = directory / filename("rcp-tools-rchm", "deb", "amd64")
        path.touch()
        with self.assertRaisesRegex(ValueError, "nonempty regular file"):
            M.validate(directory, "deb", "amd64", expected)
        path.unlink()
        path.symlink_to(directory / filename("rcp-tools-rcp", "deb", "amd64"))
        with self.assertRaisesRegex(ValueError, "nonempty regular file"):
            M.validate(directory, "deb", "amd64", expected)

    def test_missing_directory_and_empty_directory_fail_closed(self):
        for directory in (self.root, self.root / "absent"):
            with self.assertRaisesRegex(ValueError, "missing="):
                M.validate(directory, "deb", "amd64", ["rcp-tools-rchm"])

    def test_cli_exits_nonzero_for_missing_rchm_and_zero_for_complete_inventory(self):
        directory, expected = self.inventory("deb", "amd64", omit=("rcp-tools-rchm",))
        command = [sys.executable, str(SCRIPT), "deb", "amd64", str(directory)]
        failed = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(failed.returncode, 1)
        self.assertIn("rcp-tools-rchm", failed.stderr)
        (directory / filename("rcp-tools-rchm", "deb", "amd64")).write_bytes(b"package fixture")
        passed = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(passed.returncode, 0, passed.stderr)
        self.assertIn("Validated deb/amd64", passed.stdout)

    def test_controller_validates_old_source_without_requiring_helper_in_that_source(self):
        source = self.root / "old-source"
        (source / "legacy").mkdir(parents=True)
        (source / "Cargo.toml").write_text('[workspace]\nmembers = ["legacy"]\n')
        (source / "legacy/Cargo.toml").write_text(
            '[package]\nname = "legacy-tool"\n[package.metadata.deb]\n')
        directory = self.root / "old-packages"
        directory.mkdir()
        (directory / filename("legacy-tool", "deb", "amd64")).write_bytes(b"old package fixture")
        command = [sys.executable, "-I", str(SCRIPT), "deb", "amd64", str(directory)]
        result = subprocess.run(command + ["--source-root", str(source)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("legacy-tool", result.stdout)
        self.assertFalse((source / "scripts/check-release-packages.py").exists())
        wrong_source = subprocess.run(command, capture_output=True, text=True)
        self.assertNotEqual(wrong_source.returncode, 0)

    def test_each_workflow_builds_all_declared_packages_and_checks_before_zip(self):
        flow = yaml.load((ROOT / ".github/workflows/release.yml").read_text(), Loader=yaml.BaseLoader)
        for kind in ("deb", "rpm"):
            for arch, target in (("amd64", "x86_64"), ("arm64", "aarch64")):
                with self.subTest(kind=kind, arch=arch):
                    steps = flow["jobs"][f"build-{kind}-{arch}"]["steps"]
                    build = next(step for step in steps if step.get("name") ==
                                 ("Build Debian packages" if kind == "deb" else "Build RPM packages"))
                    run = build["run"]
                    command = "deb" if kind == "deb" else "generate-rpm"
                    selected = re.findall(rf"cargo-host\.sh {command}\b[^\n]*? -p ([^\s]+)", run)
                    declared = M.packages(ROOT, kind)
                    expected = declared.values() if kind == "deb" else declared.keys()
                    self.assertEqual(Counter(selected), Counter(expected))
                    directory = ("target/debian" if kind == "deb"
                                 else f"target/{target}-unknown-linux-musl/generate-rpm")
                    check = ('python3 -I "$GITHUB_WORKSPACE/controller/scripts/check-release-packages.py" '
                             f'{kind} {arch} {directory} --source-root "$GITHUB_WORKSPACE"')
                    controllers = [step for step in steps if step.get("with", {}).get("path") == "controller"]
                    self.assertEqual(len(controllers), 1)
                    self.assertEqual(controllers[0]["with"]["ref"], "${{ github.sha }}")
                    self.assertLess(steps.index(controllers[0]), steps.index(build))
                    bundle = f"zip rcp-{kind}-{arch}-"
                    self.assertEqual(run.count(check), 1)
                    self.assertLess(run.index(check), run.index(bundle))
                    self.assertNotIn("continue-on-error", build)
                    self.assertNotIn("|| true", run)


if __name__ == "__main__":
    unittest.main()
