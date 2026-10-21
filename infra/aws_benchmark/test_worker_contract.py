"""The baked worker command and offline approval share one measurement contract."""

import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile
from types import SimpleNamespace
from unittest.mock import patch

from benchmarks import operations, pairs, report, run as harness
from infra.aws_benchmark import common, results, worker
from infra.aws_benchmark.test_results import binding, bundle, measurement
from infra.aws_benchmark.test_worker_review import worker_fixture
from infra.aws_benchmark.worker_fixture import parsed_command


class WorkerContractTests(unittest.TestCase):
    def test_large_worker_dispatch_uses_manifest_summary_variant_and_archives_storage_evidence(self):
        manifest_path = Path(__file__).resolve().parents[2] / "benchmarks/large-cases.json"
        manifest = harness.load_manifest(manifest_path)
        case = next(case for case in manifest["cases"] if case["id"] == "bulk-1g-1tb")
        storage = dict(layout="two-disjoint-raid0-ext4-v1", capacity={"checked": True},
                       arrays={"source": {"device": "9:0"}, "destination": {"device": "9:1"}})
        with worker_fixture() as (root, cloud, measure, stop):
            for key in (common.WINDOW_KEY, common.REQUEST_KEY):
                value = json.loads(cloud.objects[key])
                value["suite"] = "bulk-1g-metadata"
                cloud.objects[key] = json.dumps(value).encode()
            (root / "benchmarks").mkdir()
            (root / "benchmarks/large-cases.json").write_text(manifest_path.read_text())
            events = []
            def prepare(data, selected, budget, uid, gid, execute):
                events.append("prepare")
                self.assertEqual(data, root / "data")
                self.assertEqual(selected, case)
                self.assertEqual((uid, gid), (123, 123))
                self.assertIs(execute, worker.execute)
                self.assertEqual(operations.expected_counts(selected),
                                 dict(directories=10, files=1000, bytes=1_000_000_000_000))
                return storage
            def dispatch(argv, **kwargs):
                events.append("measure")
                args = harness._arguments(argv[argv.index("benchmarks.run") + 1:])
                selected_manifest = harness.load_manifest(args.manifest)
                self.assertEqual(selected_manifest, manifest)
                self.assertEqual(args.manifest, root / "benchmarks/large-cases.json")
                self.assertEqual(args.cases, [case["id"]])
                self.assertEqual(args.variants, ["rcp-default"])
                variant, = [entry for entry in selected_manifest["variants"] if entry["id"] in args.variants]
                self.assertEqual(variant["args"], ["--summary"])
                self.assertTrue(operations.summary_supported(variant))
                self.assertTrue(args.streaming_verification)
                self.assertTrue(args.check_capacity)
                self.assertEqual(args.verification_mode, "metadata-size")
                self.assertEqual(args.verification_workers, 8)
                self.assertEqual(args.verification_inline_bytes, 1_048_576)
                self.assertEqual(args.timeout, 1200)
                self.assertEqual(args.repetitions, 2)
                self.assertEqual(args.source_root, root / "data/source")
                self.assertEqual(args.destination_root, root / "data/destination")
                self.assertEqual(args.bin_dir, root / "state/candidate")
                self.assertEqual(args.baseline_bin_dir, root / "state/baseline")
                self.assertEqual(args.source_storage_id, "c5d-raid0-ext4-source-v1")
                self.assertEqual(args.destination_storage_id, "c5d-raid0-ext4-destination-v1")
                args.output.mkdir()
                (args.output / "summary.md").write_text("fake harness summary\n")
                return SimpleNamespace(returncode=0)
            measure.side_effect = dispatch
            stop.side_effect = lambda: events.append("stop")
            with patch.object(worker.large_storage, "prepare", side_effect=prepare) as prepared:
                worker.run(cloud.api["s3"], "bucket")
                prepared.assert_called_once()
            worker.prepare_storage.assert_not_called()
            status = json.loads(cloud.objects[common.STATUS_KEY])
            self.assertTrue(status["success"], status)
            self.assertEqual(events, ["prepare", "measure", "stop"])
            with zipfile.ZipFile(io.BytesIO(cloud.objects[common.RESULTS_KEY])) as archive:
                metadata = json.loads(archive.read("worker.json"))
                self.assertEqual(metadata["storage"], storage)
                self.assertEqual(metadata["service_returncode"], 0)
                self.assertEqual(archive.read("benchmark/results/summary.md"), b"fake harness summary\n")

    def test_large_worker_rejects_duplicate_or_missing_case_before_storage_preparation(self):
        manifest_path = Path(__file__).resolve().parents[2] / "benchmarks/large-cases.json"
        for defect in ("duplicate", "missing"):
            with self.subTest(defect=defect), worker_fixture() as (root, cloud, measure, stop):
                for key in (common.WINDOW_KEY, common.REQUEST_KEY):
                    value = json.loads(cloud.objects[key])
                    value["suite"] = "bulk-1g-metadata"
                    cloud.objects[key] = json.dumps(value).encode()
                manifest = harness.load_manifest(manifest_path)
                if defect == "duplicate":
                    manifest["cases"].append(manifest["cases"][0])
                else:
                    manifest["cases"] = manifest["cases"][1:]
                (root / "benchmarks").mkdir()
                (root / "benchmarks/large-cases.json").write_text(json.dumps(manifest))
                with patch.object(worker.large_storage, "prepare") as prepared:
                    worker.run(cloud.api["s3"], "bucket")
                    prepared.assert_not_called()
                measure.assert_not_called()
                status = json.loads(cloud.objects[common.STATUS_KEY])
                self.assertFalse(status["success"])
                self.assertIn("case", status["error"]["message"])

    def test_inactive_large_suite_refuses_before_binary_download_or_storage_preparation(self):
        with worker_fixture() as (root, cloud, measure, stop):
            for key in (common.WINDOW_KEY, common.REQUEST_KEY):
                value = json.loads(cloud.objects[key])
                value["suite"] = "bulk-1g"
                cloud.objects[key] = json.dumps(value).encode()
            with patch.object(worker.large_storage, "prepare") as prepared:
                worker.run(cloud.api["s3"], "bucket")
                prepared.assert_not_called()
            worker.prepare_storage.assert_not_called()
            measure.assert_not_called()
            self.assertFalse((root / "state").exists())
            self.assertEqual([call.kwargs["Key"] for call in cloud.api["s3"].get_object.call_args_list],
                             [common.WINDOW_KEY, common.REQUEST_KEY])
            status = json.loads(cloud.objects[common.STATUS_KEY])
            self.assertFalse(status["success"])
            self.assertIn("disabled", status["error"]["message"])

    def test_real_worker_argv_matches_every_suite_contract(self):
        for suite, cases in common.SUITES.items():
            with self.subTest(suite=suite):
                expected = binding(suite)
                args, environment = parsed_command(expected)
                self.assertEqual(dict(paired_seed=args.paired_seed, repetitions=args.repetitions,
                    purpose=args.purpose, cache_policy=args.cache, runner_label=args.runner_label),
                    common.measurement_contract(suite))
                self.assertEqual(args.cases, list(cases))
                self.assertEqual(args.mode, "local")
                self.assertEqual(args.variants, ["rcp-default"])
                self.assertEqual(args.bin_dir, worker.STATE / "candidate")
                self.assertEqual(args.baseline_bin_dir, worker.STATE / "baseline")
                self.assertEqual(environment["GITHUB_REPOSITORY"], expected["repository"])
                self.assertEqual(environment["RCP_BENCH_BASELINE_COMMIT"], expected["baseline"])
                self.assertEqual(environment["LC_ALL"], "C")
                self.assertTrue(args.no_timings)
                self.assertTrue(args.local_resources)
                self.assertEqual(args.resource_time, Path("/usr/bin/time"))

    def test_large_qualification_profiles_are_bounded_and_never_history_eligible(self):
        for suite in common.LARGE_SUITES.keys() - common.METADATA_SUITES.keys():
            with self.subTest(suite=suite):
                expected = binding(suite)
                args, _ = parsed_command(expected)
                argv = worker.command(expected, 417)
                self.assertTrue(args.streaming_verification)
                self.assertTrue(args.check_capacity)
                self.assertEqual(args.verification_workers, 8)
                self.assertEqual(args.verification_inline_bytes, 1_048_576)
                self.assertNotIn("--verification-mode", argv)
                self.assertEqual(common.verification_contract(suite), dict(policy="sha256-tree-stream-v1", workers=8, inline_bytes=1_048_576))
                self.assertEqual(args.timeout, 1200)
                self.assertEqual(args.repetitions, 2)
                self.assertEqual(args.purpose, "smoke")
                self.assertEqual(args.cache, "linux-drop-caches")
                self.assertEqual(args.manifest.name, "large-cases.json")
                self.assertIn("--property=LimitFSIZE=1073741824", argv)
                self.assertIn("--property=RuntimeMaxSec=417", argv)
                self.assertIn("--property=PrivateNetwork=yes", argv)
                self.assertIn("--property=PrivateDevices=yes", argv)
                evidence, record, policy = self.inspected_measurement(expected)
                with self.assertRaisesRegex(ValueError, "diagnostic"):
                    results.approve(evidence, record, policy)

    def test_metadata_performance_suite_keeps_storage_deadline_and_proof_identity(self):
        expected = binding("bulk-1g-metadata")
        args, _ = parsed_command(expected)
        argv = worker.command(expected, 417)
        self.assertEqual(args.cases, ["bulk-1g-1tb"])
        self.assertTrue(args.streaming_verification)
        self.assertTrue(args.check_capacity)
        self.assertEqual(args.verification_mode, "metadata-size")
        self.assertEqual(args.verification_workers, 8)
        self.assertEqual(args.verification_inline_bytes, 1_048_576)
        self.assertEqual(args.timeout, 1200)
        self.assertEqual(args.repetitions, 2)
        self.assertEqual(args.purpose, "performance")
        self.assertEqual(args.runner_label, "aws-c5d24-ext4-raid0-metadata-v1")
        self.assertEqual(args.cache, "linux-drop-caches")
        self.assertEqual(args.source_storage_id, "c5d-raid0-ext4-source-v1")
        self.assertEqual(args.destination_storage_id, "c5d-raid0-ext4-destination-v1")
        self.assertIn("--property=RuntimeMaxSec=417", argv)
        self.assertEqual(common.RESERVE, 180)
        evidence, record, policy = self.inspected_measurement(expected)
        self.assertFalse(evidence["history_eligible"])
        verification = dict(policy="metadata-size-tree-v1", workers=8, inline_bytes=1_048_576)
        self.assertEqual(record["context"]["verification"], verification)
        self.assertEqual(evidence["expected_verification"], verification)
        self.assertEqual(len(record["trials"]), 4)
        self.assertIn("file content integrity not checked", results.summary(evidence, record))
        results.approve(evidence, record, policy)
        self.assertTrue(evidence["history_eligible"])
        self.assertEqual(results.derived_record(evidence, record)["context"]["aws_provenance"]["expected_verification"],
                         verification)

    def test_suite_verification_drift_cannot_be_inspected_or_approved(self):
        for suite, drift in (("bulk-1g-metadata", None),
                ("bulk-1g-metadata", dict(policy="sha256-tree-stream-v1", workers=8)),
                ("bulk-1g-metadata", dict(policy="metadata-size-tree-v1", workers=4)),
                ("bulk-1g", dict(policy="metadata-size-tree-v1", workers=8)),
                ("tiny", dict(policy="metadata-size-tree-v1", workers=8))):
            with self.subTest(suite=suite, drift=drift):
                expected = binding(suite)
                evidence, record, policy = self.inspected_measurement(expected)
                changed = copy.deepcopy(record)
                if drift is None:
                    changed["context"].pop("verification", None)
                else:
                    changed["context"]["verification"] = drift
                if common.measurement_contract(suite)["purpose"] == "performance":
                    with self.assertRaisesRegex(ValueError, "unexpected suite verification contract"):
                        results.approve(copy.deepcopy(evidence), changed, policy)
                with tempfile.TemporaryDirectory() as directory:
                    source = Path(directory)
                    bundle(source, expected, changed)
                    # focus the ingress contract test independently of generic proof-schema rejection
                    with patch.object(results.report, "validate_result", return_value=changed):
                        inspected, parsed, _ = results.inspect(source, expected, "success")
                    self.assertIsNone(parsed)
                    self.assertEqual(inspected["errors"], ["measurement record: unexpected suite verification contract"])
                    self.assertFalse(inspected["history_eligible"])

    def inspected_measurement(self, expected):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        source = Path(temporary.name)
        _, policy = bundle(source, expected)
        evidence, record, _ = results.inspect(source, expected, "success")
        self.assertEqual(evidence["errors"], [])
        return evidence, record, policy

    def test_approval_accepts_measurements_built_from_real_worker_arguments(self):
        for suite in common.SUITES.keys() - {"aa-smoke"} - common.LARGE_SUITES.keys():
            with self.subTest(suite=suite):
                evidence, record, policy = self.inspected_measurement(binding(suite))
                results.approve(evidence, record, policy)
                self.assertTrue(evidence["history_eligible"])

    def test_worker_and_approval_consume_shared_seed_repetitions_and_runner(self):
        with patch.dict(common.PERFORMANCE_CONTRACT,
                        paired_seed=19, repetitions=6, runner_label="test-contract-runner"):
            evidence, record, policy = self.inspected_measurement(binding())
            self.assertEqual(record["context"]["pairing"], pairs.configuration(19, 6))
            self.assertEqual(record["context"]["runner_label"], "test-contract-runner")
            results.approve(evidence, record, policy)
            self.assertTrue(evidence["history_eligible"])

    def test_worker_only_seed_repetitions_runner_or_cache_drift_cannot_be_approved(self):
        for field, value in (("paired_seed", 19), ("repetitions", 6),
                             ("runner_label", "test-worker-drift"), ("cache_policy", "source-warm")):
            with self.subTest(field=field):
                contract = dict(common.PERFORMANCE_CONTRACT, **{field: value})
                with patch.object(worker, "measurement_contract", return_value=contract):
                    evidence, record, policy = self.inspected_measurement(binding())
                with self.assertRaisesRegex(ValueError, "unexpected performance measurement contract"):
                    results.approve(evidence, record, policy)

    def test_each_performance_context_guard_rejects_a_changed_measurement(self):
        evidence, record, policy = self.inspected_measurement(binding())
        mutations = {
            "status": lambda value: value.update(status="failed"),
            "purpose": lambda value: value["context"].update(purpose="smoke"),
            "repository": lambda value: value["context"].update(repository="unrelated/repository"),
            "baseline": lambda value: value["context"].update(baseline_commit="9" * 40),
            "topology": lambda value: value["context"].update(topology="loopback"),
            "cache": lambda value: value["context"].update(cache_policy="uncontrolled"),
            "operation": lambda value: value["context"].update(operation_contract_revision="unknown"),
            "seed": lambda value: value["context"]["pairing"].update(seed=1),
            "repetitions": lambda value: value["context"]["pairing"].update(pairs_per_case=6),
            "runner": lambda value: value["context"].update(runner_label="unrelated-runner"),
            "cases": lambda value: value["cases"][0].update(id="unrelated-case"),
        }
        for name, mutate in mutations.items():
            with self.subTest(field=name):
                changed = copy.deepcopy(record)
                mutate(changed)
                with self.assertRaisesRegex(ValueError, "unexpected performance measurement contract"):
                    results.approve(copy.deepcopy(evidence), changed, policy)

    def test_failed_trial_source_verification_or_resource_collection_is_ineligible(self):
        evidence, record, policy = self.inspected_measurement(binding())
        for field, failure in (("source_validation", dict(ok=False)), ("resources", dict(status="failed"))):
            with self.subTest(field=field):
                changed = copy.deepcopy(record)
                changed["trials"][0][field] = failure
                with self.assertRaisesRegex(ValueError, "incomplete paired comparison"):
                    results.approve(copy.deepcopy(evidence), changed, policy)

    def test_per_suite_override_reaches_both_worker_and_approval(self):
        def contract(suite):
            value = common.measurement_contract(suite)
            if suite == "tiny":
                value.update(paired_seed=37, repetitions=6, runner_label="tiny-suite-override")
            return value
        with patch.object(worker, "measurement_contract", side_effect=contract), \
                patch.object(results, "measurement_contract", side_effect=contract):
            evidence, record, policy = self.inspected_measurement(binding("tiny"))
            results.approve(evidence, record, policy)
            self.assertTrue(evidence["history_eligible"])
            self.assertEqual(record["context"]["pairing"], pairs.configuration(37, 6))
            self.assertEqual(record["context"]["runner_label"], "tiny-suite-override")

    def test_absent_resource_context_and_trial_fields_are_refused_by_approval(self):
        expected = binding()
        raw = measurement(expected)
        raw["context"].pop("local_resources")
        for trial in raw["trials"]:
            trial.pop("resources")
        # the generic schema permits uninstrumented results; approval must not
        report.validate_result(raw)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / "input", root / "report"
            _, policy = bundle(source, expected, raw)
            evidence, record, _ = results.inspect(source, expected, "success")
            self.assertEqual(evidence["errors"], [])
            self.assertIsNotNone(record)
            evidence = results.ingest(source, output, expected, "success", policy)
            self.assertFalse(evidence["history_eligible"])
            self.assertEqual(evidence["errors"], ["history approval: incomplete paired comparison"])
            self.assertIsNotNone(evidence["controller"])
            self.assertIsNotNone(evidence["worker"])


if __name__ == "__main__":
    unittest.main()
