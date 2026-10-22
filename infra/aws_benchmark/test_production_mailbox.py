"""Instance mailbox isolation and worker regression tests; no AWS calls."""

import ast
from contextlib import contextmanager
from dataclasses import FrozenInstanceError
import io
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import zipfile

from infra.aws_benchmark import common, worker
from infra.aws_benchmark.production_mailbox import MailboxPaths
from infra.aws_benchmark.test_durable_worker import advance, durable_fixture
from infra.aws_benchmark.test_safety import ClaimConflict, FakeCloud

ACCOUNT = "123456789012"
REGION = "us-east-1"
INSTANCE = "i-" + "1" * 17
PATHS = MailboxPaths.for_instance(ACCOUNT, REGION, INSTANCE)


@contextmanager
def production_fixture():
    with durable_fixture() as values:
        root, cloud, measure, clock, identity, prepare = values
        cloud.objects = {PATHS.prefix + key: value for key, value in cloud.objects.items()}
        yield values


def run_production(cloud):
    worker.run(cloud.api["s3"], "bucket", direct=True, production_account=ACCOUNT, region=REGION)


def scoped_report(cloud):
    with zipfile.ZipFile(io.BytesIO(cloud.objects[PATHS.results])) as archive:
        return json.loads(archive.read("worker.json"))


class MailboxPathsTests(unittest.TestCase):
    def test_legacy_keys_and_write_semantics_are_unchanged(self):
        paths = MailboxPaths.legacy()
        self.assertEqual(paths.prefix, "")
        self.assertFalse(paths.scoped)
        self.assertEqual(paths.window, common.WINDOW_KEY)
        self.assertEqual(paths.request, common.REQUEST_KEY)
        self.assertEqual(paths.results, common.RESULTS_KEY)
        self.assertEqual(paths.status, common.STATUS_KEY)
        self.assertEqual(paths.write_conditions, {})
        for name in common.BINARIES:
            self.assertEqual(paths.binary(name), "input/" + name)

    def test_instance_arn_is_preserved_for_iam_variable_interpolation(self):
        self.assertEqual(PATHS.prefix,
            "production/instances/arn:aws:ec2:us-east-1:123456789012:instance/" + INSTANCE + "/")
        self.assertTrue(PATHS.scoped)
        self.assertEqual(PATHS.write_conditions, {"IfNoneMatch": "*", "ServerSideEncryption": "AES256"})
        self.assertEqual(PATHS.window, PATHS.prefix + common.WINDOW_KEY)
        self.assertEqual(PATHS.request, PATHS.prefix + common.REQUEST_KEY)
        self.assertEqual(PATHS.results, PATHS.prefix + common.RESULTS_KEY)
        self.assertEqual(PATHS.status, PATHS.prefix + common.STATUS_KEY)
        for name in common.BINARIES:
            self.assertEqual(PATHS.binary(name), PATHS.prefix + "input/" + name)

    def test_distinct_instances_accounts_and_regions_have_disjoint_keys(self):
        others = (MailboxPaths.for_instance(ACCOUNT, REGION, "i-" + "2" * 17),
                  MailboxPaths.for_instance("999999999999", REGION, INSTANCE),
                  MailboxPaths.for_instance(ACCOUNT, "eu-west-1", INSTANCE))
        def keys(paths):
            return {paths.window, paths.request, paths.results, paths.status,
                    *(paths.binary(name) for name in common.BINARIES)}
        for other in others:
            with self.subTest(other=other):
                self.assertFalse(keys(PATHS) & keys(other))

    def test_invalid_components_and_partial_identities_are_rejected(self):
        cases = ((None, None, None), (None, REGION, INSTANCE), (ACCOUNT, None, INSTANCE),
                 (ACCOUNT, REGION, None))
        cases += tuple((value, REGION, INSTANCE) for value in (
            "", "123", "1234567890123", "12345678901/", "12345678901*", True, 123456789012))
        cases += tuple((ACCOUNT, value, INSTANCE) for value in (
            "", "us-east-1/../", "us-east-*", "US-EAST-1", "us-gov-west-1", "cn-north-1", True))
        cases += tuple((ACCOUNT, REGION, value) for value in (
            "", "i-123", INSTANCE + "/../", "i-" + "A" * 17, "i-" + "?" * 17, True))
        for args in cases:
            with self.subTest(args=args), self.assertRaises(ValueError):
                MailboxPaths.for_instance(*args)
        with self.assertRaises(TypeError):
            MailboxPaths(prefix="arbitrary/")

    def test_binary_names_cannot_choose_other_keys(self):
        for name in ("../output/status.json", "candidate/../../control/window.json", "rcp",
                     "candidate/other", "candidate/rcp/", None, True):
            with self.subTest(name=name), self.assertRaises(ValueError):
                PATHS.binary(name)

    def test_identity_and_write_conditions_cannot_be_mutated(self):
        with self.assertRaises(FrozenInstanceError):
            PATHS.instance_id = "i-" + "2" * 17
        conditions = PATHS.write_conditions
        conditions.clear()
        self.assertEqual(PATHS.write_conditions, {"IfNoneMatch": "*", "ServerSideEncryption": "AES256"})


class InstalledWorkerTests(unittest.TestCase):
    def test_installer_module_closure_imports_worker_from_isolated_installed_tree(self):
        repository = Path(__file__).resolve().parents[2]
        source = Path(__file__).with_name("install-worker.sh").read_text()
        copies = re.findall(r"^cp infra/aws_benchmark/\{([a-z_,]+)\}\.py "
                            r"/opt/rcp-benchmark/infra/aws_benchmark/$", source, re.MULTILINE)
        self.assertEqual(len(copies), 1)
        smoke = source.split("progress verify-unprivileged\n", 1)[1]
        statements = [ast.literal_eval(line.strip()) for line in smoke.splitlines()
                      if line.strip().startswith("'import ")]
        self.assertEqual(len(statements), 1)
        with tempfile.TemporaryDirectory() as directory:
            installed = Path(directory)
            modules = installed / "infra/aws_benchmark"
            modules.mkdir(parents=True)
            for name in copies[0].split(","):
                shutil.copyfile(repository / "infra/aws_benchmark" / (name + ".py"), modules / (name + ".py"))
            shutil.copytree(repository / "benchmarks", installed / "benchmarks",
                            ignore=shutil.ignore_patterns("__pycache__"))
            script = "import sys; sys.path.insert(0, " + repr(str(installed)) + "); " + statements[0]
            script += "; assert 'infra.aws_benchmark.worker' in sys.modules"
            script += "; assert 'infra.aws_benchmark.production_mailbox' in sys.modules"
            script += "; assert sys.modules['infra.aws_benchmark.worker'].__file__.startswith(" + repr(str(installed)) + ")"
            result = subprocess.run([sys.executable, "-I", "-B", "-c", script], cwd=installed,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)


class ProductionWorkerTests(unittest.TestCase):
    def test_published_outputs_satisfy_worker_iam_request_conditions(self):
        from botocore.serialize import create_serializer
        from botocore.session import get_session
        from infra.aws_benchmark.test_production_stack import BUCKET, permits
        cloud = FakeCloud()
        status = common.completed_status("123-1", True)
        with patch.object(worker, "collect", return_value=b"archive"):
            worker.publish_report(cloud.api["s3"], BUCKET, None, {}, status, Mock(), paths=PATHS)
        calls = cloud.api["s3"].put_object.call_args_list
        self.assertEqual([call.kwargs["Key"] for call in calls], [PATHS.results, PATHS.status])
        operation = get_session().get_service_model("s3").operation_model("PutObject")
        serializer = create_serializer("rest-xml")
        origin = f"arn:aws:ec2:{REGION}:{ACCOUNT}:instance/{INSTANCE}"
        for call in calls:
            with self.subTest(key=call.kwargs["Key"]):
                request = serializer.serialize_to_request(call.kwargs, operation)
                context = {"s3:" + name.lower(): value for name, value in request["headers"].items()}
                context["ec2:SourceInstanceArn"] = origin
                resource = "arn:aws:s3:::" + call.kwargs["Bucket"] + "/" + call.kwargs["Key"]
                self.assertTrue(permits("WorkerRole", "s3:PutObject", resource, context))
                for header in ("s3:if-none-match", "s3:x-amz-server-side-encryption"):
                    missing = {key: value for key, value in context.items() if key != header}
                    self.assertFalse(permits("WorkerRole", "s3:PutObject", resource, missing))
        self.assertEqual(json.loads(cloud.objects[PATHS.status])["sha256"], common.digest(b"archive"))

    def test_legacy_publication_keeps_exact_unconditional_requests(self):
        cloud = FakeCloud()
        status = common.completed_status("123-1", True)
        with patch.object(worker, "collect", return_value=b"archive"):
            worker.publish_report(cloud.api["s3"], "bucket", None, {}, status, Mock())
        self.assertEqual([call.kwargs for call in cloud.api["s3"].put_object.call_args_list], [
            dict(Bucket="bucket", Key=common.RESULTS_KEY, Body=b"archive", ContentType="application/zip"),
            dict(Bucket="bucket", Key=common.STATUS_KEY, Body=json.dumps(status).encode(), ContentType="application/json")])

    def test_complete_worker_reads_and_writes_only_its_instance_mailbox(self):
        with production_fixture() as (_, cloud, measure, _, identity, prepare):
            run_production(cloud)
            metadata = scoped_report(cloud)
            self.assertTrue(metadata["status"]["success"])
            self.assertEqual(metadata["guest_instance_id"], INSTANCE)
            identity.assert_called_once()
            prepare.assert_called_once()
            measure.assert_called_once()
            reads = [call.kwargs["Key"] for call in cloud.api["s3"].get_object.call_args_list]
            self.assertEqual(reads, [PATHS.window, PATHS.request,
                                    *(PATHS.binary(name) for name in common.BINARIES)])
            writes = cloud.api["s3"].put_object.call_args_list
            self.assertEqual([call.kwargs["Key"] for call in writes], [PATHS.results, PATHS.status])
            self.assertTrue(all(call.kwargs["IfNoneMatch"] == "*" for call in writes))
            self.assertEqual(json.loads(cloud.objects[PATHS.status])["sha256"],
                             common.digest(cloud.objects[PATHS.results]))
            self.assertNotIn(common.STATUS_KEY, cloud.objects)
            self.assertNotIn(common.RESULTS_KEY, cloud.objects)

    def test_wrong_instance_assignment_expires_without_work_or_output(self):
        with production_fixture() as (root, cloud, measure, clock, _, prepare):
            value = json.loads(cloud.objects[PATHS.window])
            value["instance_id"] = "i-" + "2" * 17
            cloud.objects[PATHS.window] = json.dumps(value).encode()
            with patch.object(worker.time, "sleep", side_effect=lambda seconds: advance(clock, seconds)), \
                 self.assertRaises(TimeoutError):
                run_production(cloud)
            self.assertFalse((root / "persistent").exists())
            measure.assert_not_called()
            prepare.assert_not_called()
            cloud.api["s3"].put_object.assert_not_called()
            self.assertTrue(all(call.kwargs["Key"] == PATHS.window
                                for call in cloud.api["s3"].get_object.call_args_list))

    def test_missing_instance_mailbox_never_falls_back_to_valid_legacy_assignment(self):
        with durable_fixture() as (_, cloud, measure, clock, _, prepare):
            with patch.object(worker.time, "sleep", side_effect=lambda seconds: advance(clock, seconds)), \
                 self.assertRaises(TimeoutError):
                run_production(cloud)
            measure.assert_not_called()
            prepare.assert_not_called()
            cloud.api["s3"].put_object.assert_not_called()
            self.assertTrue(all(call.kwargs["Key"] == PATHS.window
                                for call in cloud.api["s3"].get_object.call_args_list))

    def test_mailbox_and_requested_instance_must_match_before_read(self):
        s3 = Mock()
        with self.assertRaisesRegex(ValueError, "mailbox and worker instance differ"):
            worker.durable_window(s3, "bucket", "i-" + "2" * 17, Mock(), paths=PATHS)
        self.assertEqual(s3.mock_calls, [])

    def test_production_requires_direct_mode_before_cloud_or_signal_work(self):
        s3 = Mock()
        with patch.object(worker.signal, "setitimer") as alarm, \
             patch.object(worker, "instance_identity") as identity, \
             self.assertRaisesRegex(ValueError, "requires direct"):
            worker.run(s3, "bucket", production_account=ACCOUNT, region=REGION)
        identity.assert_not_called()
        alarm.assert_not_called()
        self.assertEqual(s3.mock_calls, [])

    def test_binary_digest_mismatch_still_blocks_formatting_and_measurement(self):
        with production_fixture() as (_, cloud, measure, _, _, prepare):
            cloud.objects[PATHS.binary("candidate/rcp")] = b"different binary"
            run_production(cloud)
            status = json.loads(cloud.objects[PATHS.status])
            self.assertFalse(status["success"])
            self.assertEqual(status["error"]["message"], "binary digest mismatch")
            prepare.assert_not_called()
            measure.assert_not_called()

    def test_slow_download_preserves_existing_admission_floor(self):
        with production_fixture() as (_, cloud, measure, clock, _, prepare):
            def get(**kwargs):
                value = cloud.get(**kwargs)
                if kwargs["Key"] == PATHS.binary(common.BINARIES[-1]):
                    advance(clock, 559)
                return value
            cloud.api["s3"].get_object.side_effect = get
            run_production(cloud)
            metadata = scoped_report(cloud)
            self.assertFalse(metadata["admission"]["accepted"])
            self.assertEqual(metadata["admission"]["remaining_seconds"], 9851)
            prepare.assert_not_called()
            measure.assert_not_called()

    def test_reentry_preserves_first_archive_status_and_single_measurement(self):
        with production_fixture() as (root, cloud, measure, _, _, prepare):
            run_production(cloud)
            original = {key: cloud.objects[key] for key in (PATHS.results, PATHS.status)}
            marker = (root / "persistent/durable-started.json").read_bytes()
            with self.assertRaises(ClaimConflict):
                run_production(cloud)
            self.assertEqual({key: cloud.objects[key] for key in original}, original)
            self.assertEqual((root / "persistent/durable-started.json").read_bytes(), marker)
            self.assertEqual(measure.call_count, 1)
            self.assertEqual(prepare.call_count, 1)

    def test_archive_collision_reports_failure_without_replacing_prior_bytes(self):
        cloud = FakeCloud()
        cloud.objects[PATHS.results] = b"first archive"
        status = common.completed_status("123-1")
        status["success"] = True
        with patch.object(worker, "collect", return_value=b"new archive"):
            worker.publish_report(cloud.api["s3"], "bucket", None, {}, status, Mock(), paths=PATHS)
        self.assertEqual(cloud.objects[PATHS.results], b"first archive")
        published = json.loads(cloud.objects[PATHS.status])
        self.assertFalse(published["success"])
        self.assertEqual(published["upload_error"]["code"], "PreconditionFailed")
        self.assertNotIn("sha256", published)

    def test_status_collision_propagates_to_supervisor_shutdown(self):
        cloud = FakeCloud()
        cloud.objects[PATHS.status] = b"first terminal status"
        with patch.object(worker, "collect", return_value=b"archive"), self.assertRaises(ClaimConflict):
            worker.publish_report(cloud.api["s3"], "bucket", None, {},
                                  common.completed_status("123-1"), Mock(), paths=PATHS)
        self.assertEqual(cloud.objects[PATHS.status], b"first terminal status")
        self.assertEqual(cloud.objects[PATHS.results], b"archive")

    def test_cli_passes_only_validated_production_mode_and_explicit_region(self):
        argv = ["worker", "--region", REGION, "--bucket", "bucket", "--direct", "--production-account", ACCOUNT]
        with patch("sys.argv", argv), patch.object(worker.os, "geteuid", return_value=0), \
             patch.object(worker.os, "chdir"), patch.object(worker, "clients", return_value={"s3": "api"}), \
             patch.object(worker, "run") as run:
            worker.main()
        run.assert_called_once_with("api", "bucket", direct=True, production_account=ACCOUNT, region=REGION)
        argv.remove("--direct")
        with patch("sys.argv", argv), patch.object(worker, "clients") as clients, \
             self.assertRaisesRegex(ValueError, "requires --direct"):
            worker.main()
        clients.assert_not_called()


if __name__ == "__main__":
    unittest.main()
