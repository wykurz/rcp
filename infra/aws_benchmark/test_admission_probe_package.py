"""Offline source closure, opaque artifact identity and disabled proposal tests."""
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from . import admission_probe_package as package
from . import admission_probe as runtime
from .test_admission_probe import bindings


def snapshot():
    sources = package.production.source_snapshot(package.ROOT)
    sources.update({name: package.production.ordinary_bytes(package.ROOT, name) for name in package.SOURCES})
    requirements = {name: package.production.ordinary_bytes(package.ROOT, name)
                    for name in package.production.REQUIREMENTS}
    builders = {name: package.production.ordinary_bytes(package.ROOT, name) for name in package.BUILDERS}
    return sources, requirements, builders


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.sources, self.requirements, self.builders = snapshot()
        self.config = bindings()

    def fake_base(self, output, wheels, python312):
        package.production.archive_members(output,
            {name: self.sources[name] for name in package.production.SOURCES})
        return dict(requirements_sha256=package.acceptance.hashes(
            {Path(k).name: v for k, v in self.requirements.items()}),
            wheel_sha256=package.acceptance.hashes({"fixture.whl": b"offline"}))

    def build(self, directory, verify=None):
        output, receipt = Path(directory) / "probe.zip", Path(directory) / "receipt.json"
        with patch.object(package, "reviewed_sources", return_value=(self.sources, self.requirements, self.builders)), \
             patch.object(package.acceptance, "wheel_inputs", return_value={"fixture.whl": b"offline"}), \
             patch.object(package.production, "package", side_effect=self.fake_base), \
             patch.object(package, "verify_package", side_effect=verify or
                (lambda *args: dict(admission=dict(invalid_phase_blocked=True, network_attempts=0)))):
            result = package.build_package(output, receipt, self.config, "wheels", "python312")
        return output, receipt, result

    def test_build_binds_new_runtime_and_fixed_case_closure(self):
        with tempfile.TemporaryDirectory() as directory:
            output, receipt_path, receipt = self.build(directory)
            with zipfile.ZipFile(output) as archive:
                self.assertEqual(archive.read(package.BINDING_FILE), runtime.binding_bytes(self.config))
                for name, data in self.sources.items():
                    self.assertEqual(archive.read(name), data)
                self.assertNotIn("infra/aws_benchmark/admission_probe_package.py", archive.namelist())
                self.assertFalse(any("/test_" in name for name in archive.namelist()))
            self.assertEqual(receipt["archive_sha256"], hashlib.sha256(output.read_bytes()).hexdigest())
            self.assertEqual(receipt["source_sha256"], package.acceptance.hashes(self.sources))
            self.assertEqual(json.loads(receipt_path.read_bytes()), receipt)
            self.assertFalse(receipt["live_accepted"])

    def test_tampered_or_unlisted_source_cannot_expand_runtime_closure(self):
        name = "infra/aws_benchmark/admission_probe.py"
        with self.assertRaisesRegex(ValueError, "unreviewed or missing"):
            package.production.verify_source_closure(self.sources | {
                name: self.sources[name] + b"\nfrom . import undeclared_secret_reader\n"})

    def test_declared_new_sources_must_be_tracked_and_unchanged(self):
        old_sources = {key: value for key, value in self.sources.items()
                       if key not in package.SOURCES[len(package.acceptance.SOURCES):]}
        old_builders = {key: value for key, value in self.builders.items() if key != package.BUILDERS[-1]}
        def git(arguments, **kwargs):
            self.assertNotIn("GIT_DIR", kwargs["env"])
            name = arguments[-1].split(":", 1)[1]
            return subprocess.CompletedProcess(arguments, 0, stdout=(self.sources | self.builders)[name])
        with patch.object(package.acceptance, "reviewed_sources",
                return_value=(old_sources, self.requirements, old_builders)), \
             patch.object(subprocess, "run", side_effect=git):
            self.assertEqual(package.reviewed_sources(self.config),
                (self.sources, self.requirements, self.builders))
        with patch.object(package.acceptance, "reviewed_sources",
                return_value=(old_sources, self.requirements, old_builders)), \
             patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout=b"changed")):
            with self.assertRaisesRegex(ValueError, "differs"):
                package.reviewed_sources(self.config)
        with patch.object(package.acceptance, "reviewed_sources",
                return_value=(old_sources, self.requirements, old_builders)), \
             patch.object(subprocess, "run", side_effect=subprocess.CalledProcessError(128, ["git"])):
            with self.assertRaisesRegex(ValueError, "tracked"):
                package.reviewed_sources(self.config)

    def test_existing_outputs_and_failed_verification_never_clobber_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "receipt.json"
            receipt.write_bytes(b"preserved")
            with self.assertRaisesRegex(ValueError, "existing evidence"):
                self.build(directory)
            self.assertEqual(receipt.read_bytes(), b"preserved")
        with tempfile.TemporaryDirectory() as directory:
            def failed(*args):
                raise RuntimeError("offline verification failed")
            with self.assertRaises(RuntimeError):
                self.build(directory, verify=failed)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_render_independently_rebuilds_before_trusting_opaque_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            output, receipt_path, receipt = self.build(directory)
            with patch.object(package, "build_package", return_value=receipt) as rebuild:
                original = package.acceptance.ordinary_input
                def held(path, **kwargs):
                    if Path(path).name == "expected.zip":
                        return output.read_bytes()
                    return original(path, **kwargs)
                with patch.object(package.acceptance, "ordinary_input", side_effect=held):
                    self.assertEqual(package.checked_artifact(output, receipt_path, self.config,
                        "wheels", "python312"), receipt)
                    rebuild.assert_called_once()
            with patch.object(package, "build_package", return_value=receipt), \
                 patch.object(package.acceptance, "ordinary_input", side_effect=[
                    receipt_path.read_bytes(), b"malicious archive", b"verified rebuild"]):
                with self.assertRaisesRegex(ValueError, "independent"):
                    package.checked_artifact(output, receipt_path, self.config, "wheels", "python312")

    def test_duplicate_receipt_is_rejected_before_any_rebuild_or_archive_decode(self):
        with tempfile.TemporaryDirectory() as directory:
            archive, receipt, _ = self.build(directory)
            raw = receipt.read_bytes()
            receipt.write_bytes(raw[:-2] + b', "schema":"conflicting"}')
            with patch.object(package, "build_package") as rebuild, patch.object(zipfile, "ZipFile") as decode:
                with self.assertRaises(ValueError):
                    package.checked_artifact(archive, receipt, self.config, "wheels", "python312")
                rebuild.assert_not_called()
                decode.assert_not_called()

    def test_edited_receipt_cannot_attest_supplied_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            archive, receipt_path, receipt = self.build(directory)
            receipt_path.write_bytes(package.acceptance.encoded(receipt | {"provisional": False}))
            original = package.acceptance.ordinary_input
            def held(path, **kwargs):
                if Path(path).name == "expected.zip":
                    return archive.read_bytes()
                return original(path, **kwargs)
            with patch.object(package, "build_package", return_value=receipt), \
                 patch.object(package.acceptance, "ordinary_input", side_effect=held), \
                 patch.object(zipfile, "ZipFile") as decode:
                with self.assertRaisesRegex(ValueError, "receipt differs"):
                    package.checked_artifact(archive, receipt_path, self.config, "wheels", "python312")
                decode.assert_not_called()

    def test_disabled_proposal_has_no_fixture_iam_trigger_or_enabled_execution(self):
        config = bindings()
        receipt = dict(schema=package.RECEIPT_SCHEMA, bindings_sha256=runtime.digest_bindings(config),
            archive_sha256="b" * 64)
        template, plan = package.deployment_proposal(config, receipt,
            "code-bucket", "admission/probe.zip", "immutable-object-version")
        resources = template["Resources"]
        self.assertEqual({value["Type"] for value in resources.values()},
            {"AWS::Lambda::Function", "AWS::Lambda::Version"})
        props = resources["AdmissionProbe"]["Properties"]
        self.assertEqual(props["ReservedConcurrentExecutions"], 0)
        self.assertEqual(props["Timeout"], 120)
        self.assertEqual(props["Role"], config["role_arn"])
        self.assertEqual(props["Code"]["S3ObjectVersion"], "immutable-object-version")
        self.assertEqual(props["Handler"], "infra.aws_benchmark.admission_probe.lambda_handler")
        self.assertEqual(plan["maximum_invocations"], 15)
        self.assertEqual(plan["payloads"], [dict(phase=phase) for phase in package.PHASES])
        self.assertFalse(plan["live_accepted"])
        for resource in resources.values():
            self.assertEqual(resource["DeletionPolicy"], "Retain")
        for version in ("", "null"):
            with self.assertRaises(ValueError):
                package.deployment_proposal(config, receipt, "code-bucket", "probe.zip", version)


if __name__ == "__main__":
    unittest.main()
