"""Offline build identity, disabled template, and retained-output acceptance contracts."""

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
import warnings
import zipfile

from infra.aws_benchmark import acceptance_package as package
from infra.aws_benchmark.acceptance_bindings import binding_bytes, digest_bindings
from infra.aws_benchmark.acceptance_cases import Hold


def bindings(kind="s3"):
    nonce = "a123456789abcdef"
    name = "rcp-production-acceptance-" + kind + "-" + nonce
    role = "arn:aws:iam::381183037896:role/" + name
    common = dict(account="381183037896", region="us-east-1", role_arn=role)
    cases = dict(common, bucket="rcp-production-acceptance-" + nonce,
                 table="rcp-production-acceptance-" + nonce, nonce=nonce)
    inventory, proof = None, None
    if kind.startswith("reader-"):
        reader = kind.removeprefix("reader-")
        name = "rcp-production-benchmark-f13-" + reader + "-probe"
        cases = dict(common, secret_arn="arn:aws:secretsmanager:us-east-1:381183037896:secret:/rcp/production/github-read-app-AbCd12",
            current_version="1" * 32, previous_version="2" * 32,
            unrelated_secret_arn="arn:aws:secretsmanager:us-east-1:381183037896:secret:/rcp/acceptance/reader-denied-XyZa34",
            reader_kind=reader, app_client_id="Iv23.test-client", installation_id="67890")
        inventory = dict(current_version=cases["current_version"], previous_version=cases["previous_version"],
                         previous_stage_exists=True, unrelated_secret_exists=True)
        proof = dict(run_id="123456", run_attempt="1", head_sha="b" * 40)
    return dict(schema="rcp-production-acceptance-v1", kind=kind, **common, source_commit="a" * 40,
        role_id="AROA" + "A" * 17, function_name=name, function_version="1", log_group="/aws/lambda/" + name,
        not_before=1900000000, expires_at=1900003600, approval_capture_sha256="c" * 64,
        cases=cases, fixture_inventory=inventory, proof=proof)


def snapshot():
    sources = package.production.source_snapshot(package.ROOT)
    sources.update({name: package.production.ordinary_bytes(package.ROOT, name) for name in package.SOURCES})
    requirements = {name: package.production.ordinary_bytes(package.ROOT, name) for name in package.production.REQUIREMENTS}
    builders = {name: package.production.ordinary_bytes(package.ROOT, name) for name in package.BUILDERS}
    return sources, requirements, builders


class PackageTests(unittest.TestCase):
    def setUp(self):
        self.sources, self.requirements, self.builders = snapshot()
        self.config = bindings()
    def fake_production(self, output, wheels, python312):
        members = {name: self.sources[name] for name in package.production.SOURCES}
        members["infra/__init__.py"] = b""
        members["offline_sdk_fixture.py"] = b"SDK_FIXTURE = True\n"
        package.production.archive_members(output, members)
        return dict(requirements_sha256=package.hashes({Path(k).name: v for k, v in self.requirements.items()}),
                    wheel_sha256=package.hashes({"pinned-fixture.whl": b"fixture"}))
    def verification(self):
        return {"production": dict(python="3.12.15", platform="Linux", machine="x86_64", network_blocked=True,
                    external_site_packages=False, native_crypto_verified=True, report_theme_verified=True,
                    s3_copy_annotation_exclude_verified=True),
                "acceptance": dict(acceptance_imports_verified=True, invalid_invocation_blocked=True,
                                   bindings_sha256=digest_bindings(self.config), network_attempts=0)}
    def build(self, directory, *, base=None, verifier=None):
        output, receipt = Path(directory) / "probe.zip", Path(directory) / "receipt.json"
        with patch.object(package, "reviewed_sources", return_value=(self.sources, self.requirements, self.builders)), \
                patch.object(package, "wheel_inputs", return_value={"pinned-fixture.whl": b"fixture"}), \
                patch.object(package.production, "package", side_effect=base or self.fake_production), \
                patch.object(package, "verify_acceptance", side_effect=verifier or (lambda *args: self.verification())):
            result = package.build_package(output, receipt, self.config, Path("offline-wheels"), Path("python312"))
        return output, receipt, result
    def checked(self, output, receipt, config=None):
        with patch.object(package, "reviewed_sources", return_value=(self.sources, self.requirements, self.builders)), \
                patch.object(package, "wheel_inputs", return_value={"pinned-fixture.whl": b"fixture"}), \
                patch.object(package.production, "package", side_effect=self.fake_production), \
                patch.object(package, "verify_acceptance", return_value=self.verification()):
            return package.checked_artifact(output, receipt, config or self.config, Path("offline-wheels"), Path("python312"))
    def test_build_extends_reviewed_closure_and_binds_exact_immutable_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            output, receipt_path, receipt = self.build(directory)
            with zipfile.ZipFile(output) as archive:
                self.assertEqual(archive.read(package.BINDING_FILE), binding_bytes(self.config))
                for name, data in self.sources.items():
                    self.assertEqual(archive.read(name), data)
                self.assertNotIn("infra/aws_benchmark/acceptance_package.py", archive.namelist())
                self.assertFalse(any("test_" in name or "credentials" in name for name in archive.namelist()))
            self.assertEqual(receipt["archive_sha256"], hashlib.sha256(output.read_bytes()).hexdigest())
            self.assertEqual(receipt["source_sha256"], package.hashes(self.sources))
            self.assertEqual(receipt["builder_sha256"], package.hashes(self.builders))
            self.assertEqual(receipt["bindings_sha256"], digest_bindings(self.config))
            self.assertEqual(json.loads(receipt_path.read_bytes()), receipt)
            self.assertEqual(receipt["code_limit_bytes"], 32 * 1024 * 1024)
            self.assertEqual(receipt["compression"], package.production.compression_toolchain())
            self.assertFalse(receipt["live_accepted"])
    def test_identical_inputs_produce_identical_artifacts(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            one, _, first_receipt = self.build(first)
            two, _, second_receipt = self.build(second)
            self.assertEqual(one.read_bytes(), two.read_bytes())
            self.assertEqual(first_receipt, second_receipt)
    def test_missing_or_malformed_bindings_fail_before_builder_or_outputs(self):
        changes = [{}, dict(self.config, source_commit="HEAD"), dict(self.config, role_arn="arn:aws:iam::381183037896:root"),
                   dict(self.config, account="123456789012"), dict(self.config, environment={"TOKEN": "private"})]
        with tempfile.TemporaryDirectory() as directory, patch.object(package.production, "package") as builder:
            for config in changes:
                with self.subTest(config=config), self.assertRaises((Hold, ValueError)):
                    package.build_package(Path(directory) / "probe.zip", Path(directory) / "receipt.json", config, "wheels", "python")
            self.assertEqual(list(Path(directory).iterdir()), [])
            builder.assert_not_called()
    def test_closure_rejects_new_unlisted_acceptance_dependency(self):
        source = self.sources | {package.SOURCES[-1]: self.sources[package.SOURCES[-1]] + b"\nfrom . import private_config\n"}
        with self.assertRaisesRegex(ValueError, "unreviewed or missing"):
            package.production.verify_source_closure(source)
    def test_source_mutation_between_provenance_and_base_build_is_rejected(self):
        def mutated(output, wheels, python312):
            original = self.sources[package.production.SOURCES[0]]
            self.sources[package.production.SOURCES[0]] = original + b"\n# changed\n"
            result = self.fake_production(output, wheels, python312)
            self.sources[package.production.SOURCES[0]] = original
            return result
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ValueError, "source changed"):
            self.build(directory, base=mutated)
            self.fail("changed source was accepted")
    def test_oversize_or_failed_verification_does_not_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(package, "CODE_LIMIT", 1), self.assertRaisesRegex(ValueError, "32 MiB"):
                self.build(directory)
            self.assertEqual(list(Path(directory).iterdir()), [])
            with self.assertRaises(RuntimeError):
                self.build(directory, verifier=Mock(side_effect=RuntimeError("offline verification failed")))
            self.assertEqual(list(Path(directory).iterdir()), [])
    def test_preexisting_receipt_prevents_any_build_and_never_clobbers_evidence(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(package.production, "package") as builder:
            receipt = Path(directory) / "receipt.json"
            receipt.write_bytes(b"retain previous evidence")
            with self.assertRaisesRegex(ValueError, "existing evidence"):
                package.build_package(Path(directory) / "probe.zip", receipt, self.config, "wheels", "python")
            builder.assert_not_called()
            self.assertEqual(receipt.read_bytes(), b"retain previous evidence")
            self.assertFalse((Path(directory) / "probe.zip").exists())
    def test_receipt_write_failure_retains_finished_archive(self):
        original = package.write_new
        def fail_receipt(path, data):
            if Path(path).name == "receipt.json":
                raise OSError("receipt write failed")
            original(path, data)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(package, "write_new", side_effect=fail_receipt), self.assertRaisesRegex(OSError, "receipt"):
                self.build(directory)
            self.assertTrue(zipfile.is_zipfile(Path(directory) / "probe.zip"))
            self.assertFalse((Path(directory) / "receipt.json").exists())
    def test_exclusive_create_catches_output_created_during_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "probe.zip"
            def concurrent(*args):
                output.write_bytes(b"other writer evidence")
                return self.verification()
            with self.assertRaises(FileExistsError):
                self.build(directory, verifier=concurrent)
            self.assertEqual(output.read_bytes(), b"other writer evidence")
            self.assertFalse((Path(directory) / "receipt.json").exists())
    def test_render_checks_artifact_config_source_and_receipt_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            output, receipt_path, receipt = self.build(directory)
            self.assertEqual(self.checked(output, receipt_path), receipt)
            changed = dict(self.config, approval_capture_sha256="d" * 64)
            with self.assertRaisesRegex(ValueError, "independent.*rebuild"):
                self.checked(output, receipt_path, changed)
            incomplete = dict(receipt, verification={"acceptance": self.verification()["acceptance"], "production": {}})
            receipt_path.write_bytes(package.encoded(incomplete))
            with self.assertRaisesRegex(ValueError, "receipt must match independent"):
                self.checked(output, receipt_path)
            receipt_path.write_bytes(package.encoded(receipt))
            output.write_bytes(output.read_bytes() + b"changed")
            with self.assertRaisesRegex(ValueError, "independent.*rebuild"):
                self.checked(output, receipt_path)
    def test_replaced_or_malformed_archive_cannot_be_attested_by_editing_its_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            output, receipt_path, receipt = self.build(directory)
            original_zip = zipfile.ZipFile
            with original_zip(output) as archive:
                members = [(info, archive.read(info)) for info in archive.infolist()]
            source = package.SOURCES[-1]
            changes = {
                "replaced": None,
                "source": [(info, b"raise RuntimeError('must never execute supplied code')\n" if info.filename == source else data)
                           for info, data in members],
                "dependency": [(info, b"UNPINNED = True\n" if info.filename == "offline_sdk_fixture.py" else data)
                               for info, data in members],
                "missing": [(info, data) for info, data in members if info.filename != source],
                "extra": members + [(zipfile.ZipInfo("sitecustomize.py"), b"unreviewed code")],
                "duplicate": members + [members[0]],
                "traversal": members + [(zipfile.ZipInfo("../escape.py"), b"escape")],
                "bindings": [(info, b"{}" if info.filename == package.BINDING_FILE else data) for info, data in members],
            }
            def trusted_archives_only(path, *args, **kwargs):
                self.assertNotEqual(Path(path), output, "render must not parse or execute supplied archives")
                return original_zip(path, *args, **kwargs)
            for kind, changed_members in changes.items():
                with self.subTest(kind=kind):
                    data = io.BytesIO()
                    if changed_members is None:
                        data.write(b"not a ZIP, with an edited matching receipt")
                    else:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore", UserWarning)
                            with original_zip(data, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
                                for info, content in changed_members:
                                    archive.writestr(info, content, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
                    forged = data.getvalue()
                    output.write_bytes(forged)
                    receipt_path.write_bytes(package.encoded(receipt | {
                        "archive_bytes": len(forged), "archive_sha256": hashlib.sha256(forged).hexdigest()}))
                    with patch.object(zipfile, "ZipFile", side_effect=trusted_archives_only), \
                            self.assertRaisesRegex(ValueError, "independent.*rebuild"):
                        self.checked(output, receipt_path)


class ProvenanceAndPathsTests(unittest.TestCase):
    def test_failed_output_flush_preserves_bytes_and_prevents_retry_clobber(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "evidence.json"
            with patch.object(os, "fsync", side_effect=OSError("storage sync failed")), self.assertRaises(OSError):
                package.write_new(output, b"retained partial-operation evidence")
            self.assertEqual(output.read_bytes(), b"retained partial-operation evidence")
            with self.assertRaises(FileExistsError):
                package.write_new(output, b"retry must not overwrite")
    def test_tracked_source_identity_rejects_dirty_untracked_and_wrong_commit(self):
        sources, requirements, builders = snapshot()
        values = sources | requirements | builders
        config = bindings()
        def git(arguments, **kwargs):
            self.assertNotIn("GIT_DIR", kwargs["env"])
            self.assertEqual(kwargs["env"]["GIT_NO_REPLACE_OBJECTS"], "1")
            if arguments[-3:] == ["rev-parse", "--verify", "HEAD"]:
                return Mock(stdout=(config["source_commit"] + "\n").encode())
            name = arguments[-1].split(":", 1)[1]
            return Mock(stdout=values[name])
        with patch.object(subprocess, "run", side_effect=git):
            self.assertEqual(package.reviewed_sources(config), (sources, requirements, builders))
            with self.assertRaisesRegex(ValueError, "HEAD"):
                package.reviewed_sources(dict(config, source_commit="b" * 40))
            values[package.SOURCES[0]] += b"\n# dirty\n"
            with self.assertRaisesRegex(ValueError, "differs"):
                package.reviewed_sources(config)
        with patch.object(subprocess, "run", side_effect=subprocess.CalledProcessError(128, ["git"])):
            with self.assertRaisesRegex(ValueError, "tracked"):
                package.reviewed_sources(config)
    def test_inputs_and_outputs_reject_symlinks_in_all_components(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ordinary").mkdir()
            (root / "ordinary/value").write_bytes(b"fixture")
            (root / "linked").symlink_to(root / "ordinary", target_is_directory=True)
            (root / "leaf").symlink_to(root / "ordinary/value")
            for name in ("linked/value", "leaf"):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    package.ordinary_input(root / name, limit=100)
            with self.assertRaises(OSError):
                package.write_new(root / "linked/output", b"no")
            with self.assertRaises(FileExistsError):
                package.write_new(root / "leaf", b"no")
            self.assertEqual((root / "ordinary/value").read_bytes(), b"fixture")
            with self.assertRaisesRegex(ValueError, "distinct"):
                package.new_outputs(root / "new", root / "new")
    def test_wheel_directory_links_and_linked_members_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wheels = root / "ordinary" / "wheels"
            wheels.mkdir(parents=True)
            (wheels / "test.whl").write_bytes(b"pinned fixture")
            (root / "linked").symlink_to(root / "ordinary", target_is_directory=True)
            with self.assertRaises(OSError):
                package.wheel_inputs(root / "linked" / "wheels")
            self.assertEqual(package.wheel_inputs(wheels), {"test.whl": b"pinned fixture"})
            (wheels / "linked.whl").symlink_to(wheels / "test.whl")
            with self.assertRaises(ValueError):
                package.wheel_inputs(wheels)


class ProposalTests(unittest.TestCase):
    def proposal(self, kind="s3"):
        config = bindings(kind)
        receipt = dict(archive_sha256="a" * 64, bindings_sha256=digest_bindings(config))
        return config, package.deployment_proposal(config, receipt, "reviewed-code-bucket", "acceptance/probe.zip", "exact-object-version")
    def test_template_has_only_exact_disabled_function_and_pinned_version(self):
        for kind in ("reader-admission", "reader-collector", "s3", "dynamodb"):
            with self.subTest(kind=kind):
                config, (template, plan) = self.proposal(kind)
                self.assertEqual(set(template["Resources"]), {"AcceptanceFunction", "AcceptanceVersion"})
                resource = template["Resources"]["AcceptanceFunction"]
                properties = resource["Properties"]
                self.assertEqual(resource["Type"], "AWS::Lambda::Function")
                self.assertEqual(resource["DeletionPolicy"], "Retain")
                self.assertEqual(set(properties), {"FunctionName", "Role", "Runtime", "Architectures", "Handler", "MemorySize",
                    "Timeout", "ReservedConcurrentExecutions", "Code", "LoggingConfig", "Tags"})
                self.assertEqual(properties["ReservedConcurrentExecutions"], 0)
                self.assertEqual(properties["Role"], config["role_arn"])
                self.assertEqual(properties["FunctionName"], config["function_name"])
                self.assertEqual(properties["LoggingConfig"], dict(LogFormat="Text", LogGroup=config["log_group"]))
                self.assertEqual(properties["Runtime"], "python3.12")
                self.assertEqual(properties["Architectures"], ["x86_64"])
                self.assertEqual(properties["MemorySize"], 256)
                self.assertEqual(properties["Timeout"], 45 if kind.startswith("reader-") else 120)
                self.assertEqual(properties["Code"]["S3ObjectVersion"], "exact-object-version")
                version = template["Resources"]["AcceptanceVersion"]["Properties"]
                self.assertEqual(version["CodeSha256"], "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo=")
                self.assertFalse(plan["live_accepted"])
    def test_invocation_is_separately_approved_qualified_sync_single_attempt_and_restored(self):
        config, (_, plan) = self.proposal()
        approvals = plan["approvals"]
        self.assertEqual(set(approvals), {"create_disabled", "invoke_once", "delete_probe"})
        self.assertEqual(approvals["create_disabled"]["operation"], "CREATE_ONLY")
        invoke = approvals["invoke_once"]
        self.assertEqual(invoke["sdk"]["retries"], {"total_max_attempts": 1})
        first, call, last = invoke["requests_in_order"]
        self.assertEqual(first["parameters"]["ReservedConcurrentExecutions"], 1)
        self.assertEqual(last["parameters"]["ReservedConcurrentExecutions"], 0)
        self.assertEqual(call, {"operation": "Invoke", "parameters": {"FunctionName": config["function_name"],
            "Qualifier": "1", "InvocationType": "RequestResponse", "LogType": "None", "Payload": "{}"}})
        self.assertIn("reconcile before any retry or deletion", invoke["failure_policy"])
        self.assertEqual(approvals["delete_probe"]["request"]["parameters"], {"FunctionName": config["function_name"]})
    def test_real_code_object_pins_required_without_fabricating_a_version(self):
        for values in (("bucket", "key", ""), ("bucket", "key", "null"), ("*", "key", "version"),
                       ("bucket", "../key", "version"), ("bucket", "key", "version\n")):
            with self.subTest(values=values), self.assertRaises(ValueError):
                package.code_pin(*values)
    def test_invalid_role_source_config_and_mismatched_binding_digest_fail_closed(self):
        config = bindings()
        receipt = dict(archive_sha256="a" * 64, bindings_sha256=digest_bindings(config))
        for change in (dict(role_arn="root"), dict(source_commit="main"), dict(function_version="$LATEST"),
                       dict(function_name="production"), dict(log_group="/aws/lambda/foreign")):
            with self.subTest(change=change), self.assertRaises((Hold, ValueError)):
                package.deployment_proposal(config | change, receipt, "bucket", "key", "version")
        with self.assertRaisesRegex(ValueError, "receipt"):
            package.deployment_proposal(config, receipt | {"bindings_sha256": "f" * 64}, "bucket", "key", "version")


if __name__ == "__main__":
    unittest.main()
