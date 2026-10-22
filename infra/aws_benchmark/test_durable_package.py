"""Packaging must exclude local configuration and reject linked handler sources."""

from pathlib import Path
import csv
import hashlib
import io
import subprocess
import sys
import zipfile
from unittest.mock import patch
import tempfile
import unittest

from infra.aws_benchmark import durable_package as packager
from infra.aws_benchmark.durable_package import SOURCES, source_files


class PackageSourcesTests(unittest.TestCase):
    def test_only_handler_closure_is_packaged_even_with_local_python_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in SOURCES:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# reviewed source")
            private = root / "infra/aws_benchmark/local_credentials.py"
            private.write_text("# private local configuration")
            self.assertNotIn(private, list(source_files(root)))
            handler = root / "infra/aws_benchmark/durable_aws.py"
            handler.unlink()
            handler.symlink_to(private)
            with self.assertRaisesRegex(ValueError, "ordinary reviewed file"):
                list(source_files(root))

    def test_missing_handler_dependency_fails_instead_of_building_partial_package(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "ordinary reviewed file"):
                list(source_files(Path(directory)))


class BuiltPackageTests(unittest.TestCase):
    def wheel(self, root):
        wheel = root / "local_fixture-1.0-py3-none-any.whl"
        with zipfile.ZipFile(wheel, "w") as archive:
            archive.writestr("local_fixture.py", "VALUE = 1\n")
            archive.writestr("local_fixture-1.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: local-fixture\nVersion: 1.0\n")
            archive.writestr("local_fixture-1.0.dist-info/RECORD",
                "local_fixture.py,,\nlocal_fixture-1.0.dist-info/METADATA,,\nlocal_fixture-1.0.dist-info/RECORD,,\n../../bin/probe,,\n")
        return wheel

    def installer(self, wheel, executable):
        def install(stage, wheels):
            with zipfile.ZipFile(wheel) as archive:
                for name in archive.namelist():
                    path = stage / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(archive.read(name))
            script = ("#!" + executable + "\nprint('fixture')\n").encode()
            (stage / "bin").mkdir()
            (stage / "bin/probe").write_bytes(script)
            record = stage / "local_fixture-1.0.dist-info/RECORD"
            record.write_text(record.read_text().replace("../../bin/probe,,",
                "../../bin/probe,sha256=" + hashlib.sha256(script).hexdigest() + "," + str(len(script))))
        return install

    def isolated_import(self, archive, directory):
        script = ("import sys; sys.path.insert(0, sys.argv[1]); "
                  "from infra.aws_benchmark import durable_aws; "
                  "assert callable(durable_aws.coordinator) and callable(durable_aws.watchdog)")
        return subprocess.run([sys.executable, "-I", "-S", "-c", script, str(archive)],
                              cwd=directory, capture_output=True, text=True)

    def test_build_is_reproducible_without_installed_script_paths_or_external_imports(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = self.wheel(root)
            outputs = [root / "one.zip", root / "two.zip"]
            for output, interpreter in zip(outputs, ("/one/venv/bin/python", "/different/venv/bin/python3")):
                with patch.object(packager, "install_dependencies", self.installer(wheel, interpreter)):
                    packager.package(output, root)
            self.assertEqual(outputs[0].read_bytes(), outputs[1].read_bytes())
            with zipfile.ZipFile(outputs[0]) as archive:
                self.assertFalse(any(name.startswith("bin/") for name in archive.namelist()))
                self.assertTrue(all(member.compress_type == zipfile.ZIP_STORED for member in archive.infolist()))
                record = archive.read("local_fixture-1.0.dist-info/RECORD").decode()
                self.assertNotIn("bin/", record)
                rows = list(csv.reader(io.StringIO(record)))
                self.assertTrue(all(row[0] in archive.namelist() for row in rows))
            imported = self.isolated_import(outputs[0], root)
            self.assertEqual(imported.returncode, 0, imported.stderr)

    def test_build_needs_no_compression_library(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = self.wheel(root)
            output = root / "stored.zip"
            with patch.object(packager, "install_dependencies", self.installer(wheel, "/fixture/python")):
                with patch.object(zipfile, "zlib", None):
                    packager.package(output, root)
            with zipfile.ZipFile(output) as archive:
                self.assertEqual(archive.read("local_fixture.py"), b"VALUE = 1\n")
                self.assertTrue(all(member.compress_size == member.file_size for member in archive.infolist()))
            imported = self.isolated_import(output, root)
            self.assertEqual(imported.returncode, 0, imported.stderr)

    def test_built_package_missing_transitive_handler_dependency_fails_isolated_import(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheel = self.wheel(root)
            output = root / "missing.zip"
            reduced = tuple(name for name in SOURCES if not name.endswith("/durable.py"))
            with patch.object(packager, "SOURCES", reduced), patch.object(packager, "install_dependencies",
                                                                        self.installer(wheel, "/fixture/python")):
                packager.package(output, root)
            imported = self.isolated_import(output, root)
            self.assertNotEqual(imported.returncode, 0)
            self.assertIn("durable", imported.stderr)

    def test_record_normalization_rejects_malformed_rows_before_creating_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "invalid.zip"
            def install(stage, wheels):
                record = stage / "fixture.dist-info/RECORD"
                record.parent.mkdir()
                record.write_text("broken,row\n")
            with patch.object(packager, "install_dependencies", install):
                with self.assertRaisesRegex(ValueError, "malformed"):
                    packager.package(output, root)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
