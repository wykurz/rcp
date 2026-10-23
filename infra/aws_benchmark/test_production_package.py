"""Offline production package isolation, reproducibility and input-boundary tests."""

from contextlib import chdir
import csv
import hashlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import zipfile

from infra.aws_benchmark import production_package as package


class InputsTests(unittest.TestCase):
    def test_explicit_source_closure_excludes_configuration_and_cli(self):
        sources = package.source_snapshot(package.ROOT)
        package.verify_source_closure(sources)
        self.assertIn("infra/aws_benchmark/production_collection.py", sources)
        self.assertIn("infra/aws_benchmark/production_collection_handler.py", sources)
        self.assertIn("infra/aws_benchmark/production_startup.py", sources)
        self.assertIn("infra/aws_benchmark/production_errors.py", sources)
        self.assertIn("infra/aws_benchmark/production_transport.py", sources)
        self.assertIn("infra/aws_benchmark/production_projection.py", sources)
        self.assertIn("infra/aws_benchmark/production_objects.py", sources)
        self.assertEqual(package.ENTRYPOINTS["infra.aws_benchmark.production_collection_handler"], ("lambda_handler",))
        self.assertIn("assets/site-theme.html", sources)
        self.assertNotIn("infra/aws_benchmark/production_client.py", sources)
        self.assertFalse(any("test_" in name or "/.git/" in name or "credentials" in name for name in sources))

    def test_missing_entrypoint_or_new_local_dependency_is_rejected(self):
        sources = package.source_snapshot(package.ROOT)
        del sources["infra/aws_benchmark/production_collection.py"]
        with self.assertRaisesRegex(ValueError, "entrypoint source is missing"):
            package.verify_source_closure(sources)
        sources = package.source_snapshot(package.ROOT)
        sources["infra/aws_benchmark/production_handler.py"] += b"\nfrom . import private_configuration\n"
        with self.assertRaisesRegex(ValueError, "unreviewed or missing"):
            package.verify_source_closure(sources)

    def test_startup_verifier_cannot_be_omitted_from_the_lambda_closure(self):
        sources = package.source_snapshot(package.ROOT)
        del sources["infra/aws_benchmark/production_startup.py"]
        with self.assertRaisesRegex(ValueError, "unreviewed or missing source dependency.*production_startup"):
            package.verify_source_closure(sources)

    def test_reconciliation_and_projection_dependencies_cannot_be_omitted(self):
        for name in ("production_errors", "production_projection", "production_objects", "production_transport"):
            with self.subTest(name=name):
                sources = package.source_snapshot(package.ROOT)
                del sources["infra/aws_benchmark/" + name + ".py"]
                with self.assertRaisesRegex(ValueError, "unreviewed or missing source dependency.*" + name):
                    package.verify_source_closure(sources)

    def test_unpinned_external_dependency_cannot_silently_enter_package(self):
        sources = package.source_snapshot(package.ROOT)
        sources["infra/aws_benchmark/production_handler.py"] += b"\nimport unreviewed_dependency\n"
        with self.assertRaisesRegex(ValueError, "unlisted runtime dependency"):
            package.verify_source_closure(sources)

    def test_symlink_leaf_and_parent_do_not_read_outside_the_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ordinary").mkdir()
            (root / "ordinary/value.py").write_text("ordinary")
            (root / "linked").symlink_to(root / "ordinary", target_is_directory=True)
            (root / "linked.py").symlink_to(root / "ordinary/value.py")
            for name in ("linked/value.py", "linked.py"):
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, "ordinary file"):
                    package.ordinary_bytes(root, name)
            self.assertEqual(package.ordinary_bytes(root, "ordinary/value.py"), b"ordinary")

    def test_missing_fifo_oversized_and_traversal_inputs_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.mkfifo(root / "pipe")
            (root / "large.py").write_bytes(b"abc")
            for name in ("missing.py", "pipe", "large.py", "../outside.py", "/absolute.py"):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    package.ordinary_bytes(root, name, limit=2)

    def test_empty_and_linked_wheels_fail_before_installation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "absent"):
                package.wheel_snapshot(root)
            (root / "ordinary").write_bytes(b"wheel")
            (root / "linked.whl").symlink_to(root / "ordinary")
            with self.assertRaisesRegex(ValueError, "ordinary file"):
                package.wheel_snapshot(root)


class CopyContractProbeTests(unittest.TestCase):
    def operation(self, *, present=True, exclude=True, header="x-amz-object-annotation-directive"):
        from botocore.model import StringShape
        from botocore.session import get_session
        operation = get_session().get_service_model("s3").operation_model("CopyObject")
        # explicit model fixture tests both paths on an older optional development SDK
        if present:
            operation.input_shape.members["AnnotationDirective"] = StringShape("AnnotationDirective", {
                "type": "string", "enum": ["COPY", "EXCLUDE"] if exclude else ["COPY"],
                "location": "header", "locationName": header})
        else:
            operation.input_shape.members.pop("AnnotationDirective", None)
        return operation

    def probe(self, operation):
        session = Mock()
        session.get_service_model.return_value.operation_model.return_value = operation
        namespace = {}
        with patch("botocore.session.get_session", return_value=session):
            exec(package.S3_COPY_PROBE, namespace)
        import botocore
        session.register_component.assert_called_once()
        name, loader = session.register_component.call_args.args
        self.assertEqual(name, "data_loader")
        self.assertEqual(loader.search_paths, [str(Path(botocore.__file__).resolve().parent / "data")])
        return namespace

    def test_deployment_probe_requires_supported_model_and_exact_wire_header(self):
        self.assertIn(package.S3_COPY_PROBE, package.ISOLATED_PROBE)
        result = self.probe(self.operation())
        self.assertEqual(result["copy_wire"]["headers"]["x-amz-object-annotation-directive"], "EXCLUDE")
        for operation, message in ((self.operation(present=False), "lacks required"),
                                   (self.operation(exclude=False), "lacks required"),
                                   (self.operation(header="x-amz-wrong-header"), "wire contract")):
            with self.subTest(message=message), self.assertRaisesRegex(RuntimeError, message):
                self.probe(operation)

    def test_unmodified_development_sdk_is_rejected_if_it_lacks_deployed_capability(self):
        from botocore.session import get_session
        operation = get_session().get_service_model("s3").operation_model("CopyObject")
        if "AnnotationDirective" not in operation.input_shape.members:
            with self.assertRaisesRegex(RuntimeError, "lacks required"):
                exec(package.S3_COPY_PROBE, {})
        else:
            namespace = {}
            exec(package.S3_COPY_PROBE, namespace)
            self.assertEqual(namespace["copy_wire"]["headers"]["x-amz-object-annotation-directive"], "EXCLUDE")


class BuiltPackageTests(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory) / "source"
        root.mkdir()
        for name, data in package.source_snapshot(package.ROOT).items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        for name in package.REQUIREMENTS:
            (root / name).write_bytes((package.ROOT / name).read_bytes())
        (root / "infra/aws_benchmark/local_credentials.py").write_text("PRIVATE_MARKER")
        wheels = Path(directory) / "wheels"
        wheels.mkdir()
        (wheels / "fixture-1.0-py3-none-any.whl").write_bytes(b"a pinned test fixture")
        return root, wheels

    def install(self, interpreter):
        def installer(stage, wheels, requirements, python312):
            self.assertEqual((wheels / "fixture-1.0-py3-none-any.whl").read_bytes(), b"a pinned test fixture")
            self.assertTrue((requirements / "requirements.txt").is_file())
            (stage / "fixture.py").write_text("VALUE = 1\n")
            (stage / "bin").mkdir()
            (stage / "bin/helper").write_text("#!" + interpreter + "\n")
            metadata = stage / "fixture-1.0.dist-info"
            metadata.mkdir()
            (metadata / "RECORD").write_text("fixture.py,,\nfixture-1.0.dist-info/RECORD,,\n../../bin/helper,,\n")
        return installer

    def build(self, root, wheels, output, *, installer=None):
        with patch.object(package, "ROOT", root), patch.object(package, "install_dependencies", installer or self.install("/one")), \
                patch.object(package, "verify_package", return_value={"offline_fixture": True}):
            return package.package(output, wheels, Path("/test/python312"))

    def test_archive_and_receipt_are_deterministic_and_do_not_include_local_material(self):
        with tempfile.TemporaryDirectory() as directory:
            root, wheels = self.fixture(directory)
            outputs = [Path(directory) / "one.zip", Path(directory) / "two.zip"]
            receipts = [self.build(root, wheels, output, installer=self.install(interpreter))
                        for output, interpreter in zip(outputs, ("/one/python", "/different/python"))]
            self.assertEqual(outputs[0].read_bytes(), outputs[1].read_bytes())
            self.assertEqual(receipts[0], receipts[1])
            self.assertEqual(receipts[0]["archive_sha256"], hashlib.sha256(outputs[0].read_bytes()).hexdigest())
            with zipfile.ZipFile(outputs[0]) as archive:
                self.assertFalse(any("credentials" in name or name.startswith("bin/") or "__pycache__" in name
                                     for name in archive.namelist()))
                self.assertNotIn(b"PRIVATE_MARKER", outputs[0].read_bytes())
                self.assertTrue(all(member.compress_type == zipfile.ZIP_DEFLATED and
                                    member.date_time == (2020, 1, 1, 0, 0, 0) for member in archive.infolist()))
                rows = csv.reader(io.StringIO(archive.read("fixture-1.0.dist-info/RECORD").decode()))
                self.assertTrue(all(row[0] in archive.namelist() for row in rows))

    def test_relative_interpreter_survives_the_probe_working_directory_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root, wheels = self.fixture(directory)
            interpreter = Path(directory) / "tools/python312"
            interpreter.parent.mkdir()
            interpreter.symlink_to(sys.executable)
            output = Path(directory) / "relative.zip"
            observed = []
            install = self.install("/one")
            def installer(stage, wheel_stage, requirements, python312):
                observed.append(python312)
                install(stage, wheel_stage, requirements, python312)
            probe = "import json, os; print(json.dumps({'probe_cwd': os.getcwd()}))"
            with patch.object(package, "ROOT", root), patch.object(package, "install_dependencies", installer), \
                    patch.object(package, "ISOLATED_PROBE", probe), chdir(directory):
                receipt = package.package(output, wheels, Path("tools/python312"))
            self.assertEqual(observed, [interpreter])
            probe_cwd = Path(receipt["verification"]["probe_cwd"])
            self.assertTrue(probe_cwd.is_relative_to(directory))
            self.assertNotEqual(probe_cwd, Path(directory))
            self.assertTrue(output.is_file())

    def test_missing_source_fails_without_creating_partial_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root, wheels = self.fixture(directory)
            (root / "infra/aws_benchmark/production_collection.py").unlink()
            output = Path(directory) / "missing.zip"
            with self.assertRaises(ValueError):
                self.build(root, wheels, output)
            self.assertFalse(output.exists())

    def test_linked_dependency_or_source_collision_fails_before_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root, wheels = self.fixture(directory)
            for kind in ("symlink", "source-collision"):
                def installer(stage, *_):
                    if kind == "symlink":
                        (stage / "external.py").symlink_to(root / "benchmarks/__init__.py")
                    else:
                        path = stage / "infra/aws_benchmark/common.py"
                        path.parent.mkdir(parents=True)
                        path.write_text("collision")
                output = Path(directory) / (kind + ".zip")
                with self.subTest(kind=kind), self.assertRaises((ValueError, FileExistsError)):
                    self.build(root, wheels, output, installer=installer)
                self.assertFalse(output.exists())

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root, wheels = self.fixture(directory)
            output = Path(directory) / "existing.zip"
            output.write_bytes(b"keep")
            with self.assertRaises(ValueError):
                self.build(root, wheels, output)
            self.assertEqual(output.read_bytes(), b"keep")

    def test_size_above_current_allowance_is_rejected_without_changing_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root, wheels = self.fixture(directory)
            output = Path(directory) / "large.zip"
            with patch.object(package, "CODE_LIMIT", 1):
                with self.assertRaisesRegex(ValueError, "existing 32 MiB"):
                    self.build(root, wheels, output)
            self.assertFalse(output.exists())
            self.assertEqual(package.CODE_LIMIT, 32 * 1024 * 1024)

    def test_deflated_members_equal_uncompressed_inputs_and_record_toolchain(self):
        with tempfile.TemporaryDirectory() as directory:
            root, wheels = self.fixture(directory)
            output = Path(directory) / "compressed.zip"
            receipt = self.build(root, wheels, output)
            with zipfile.ZipFile(output) as archive:
                for name, data in package.source_snapshot(root).items():
                    self.assertEqual(archive.read(name), data)
            self.assertEqual(receipt["compression"], package.compression_toolchain())
            self.assertEqual(receipt["compression"]["level"], 9)
            self.assertTrue(receipt["provisional"])

    def test_installer_is_offline_and_requires_all_pinned_hashes(self):
        with patch.object(subprocess, "run") as run:
            package.install_dependencies(Path("stage"), Path("wheels"), Path("requirements"), Path("python312"))
        command = run.call_args.args[0]
        self.assertIn("--require-hashes", command)
        self.assertIn("--no-index", command)
        self.assertIn("--isolated", command)
        self.assertIn("--no-compile", command)
        self.assertEqual(command[command.index("--python") + 1], "python312")
        self.assertTrue(run.call_args.kwargs["check"])

    @unittest.skipUnless(shutil.which("python3.12"), "CPython3.12 required for isolated import check")
    def test_isolated_verifier_blocks_network_before_handler_can_connect(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "network.zip"
            with zipfile.ZipFile(output, "w") as archive:
                archive.writestr("infra/__init__.py", "")
                archive.writestr("infra/aws_benchmark/__init__.py", "")
                archive.writestr("infra/aws_benchmark/production_handler.py", "import socket\nsocket.socket()\n")
            with patch.object(package, "ENTRYPOINTS", {"infra.aws_benchmark.production_handler": ("lambda_handler",)}):
                with self.assertRaises(subprocess.CalledProcessError) as raised:
                    package.verify_package(output, Path(shutil.which("python3.12")), root)
            self.assertIn("network is blocked", raised.exception.stderr)


if __name__ == "__main__":
    unittest.main()
