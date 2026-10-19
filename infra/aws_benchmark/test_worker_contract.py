"""The baked worker command and offline approval share one measurement contract."""

import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks import pairs, run as harness
from infra.aws_benchmark import common, results, worker
from infra.aws_benchmark.test_results import binding, bundle


def parsed_command(expected):
    command = worker.command(expected, 600)
    arguments = harness._arguments(command[command.index("benchmarks.run") + 1:])
    environment = dict(value.removeprefix("--setenv=").split("=", 1)
                       for value in command if value.startswith("--setenv="))
    return arguments, environment


class WorkerContractTests(unittest.TestCase):
    def test_real_worker_argv_matches_every_suite_contract(self):
        for suite, cases in common.SUITES.items():
            with self.subTest(suite=suite):
                expected = binding(suite)
                args, environment = parsed_command(expected)
                self.assertEqual(dict(topology=args.mode, variant=args.variants[0],
                    paired_seed=args.paired_seed, repetitions=args.repetitions, purpose=args.purpose,
                    cache_policy=args.cache, runner_label=args.runner_label,
                    no_timings=args.no_timings, local_resources=args.local_resources),
                    common.measurement_contract(suite))
                self.assertEqual(args.cases, list(cases))
                self.assertEqual(args.variants, [common.PERFORMANCE_CONTRACT["variant"]])
                self.assertEqual(args.bin_dir, worker.STATE / "candidate")
                self.assertEqual(args.baseline_bin_dir, worker.STATE / "baseline")
                self.assertEqual(environment["GITHUB_REPOSITORY"], expected["repository"])
                self.assertEqual(environment["RCP_BENCH_BASELINE_COMMIT"], expected["baseline"])
                self.assertEqual(environment["LC_ALL"], "C")
                self.assertTrue(args.no_timings)
                self.assertTrue(args.local_resources)
                self.assertEqual(args.resource_time, Path("/usr/bin/time"))

    def inspected_measurement(self, expected):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        source = Path(temporary.name)
        _, policy = bundle(source, expected)
        evidence, record, _ = results.inspect(source, expected, "success")
        self.assertEqual(evidence["errors"], [])
        args, environment = parsed_command(expected)
        self.assertEqual(record["context"]["pairing"], pairs.configuration(args.paired_seed, args.repetitions))
        self.assertEqual(record["context"]["topology"], args.mode)
        self.assertEqual(record["context"]["purpose"], args.purpose)
        self.assertEqual(record["context"]["cache_policy"], args.cache)
        self.assertEqual(record["context"]["runner_label"], args.runner_label)
        self.assertEqual(record["context"]["repository"], environment["GITHUB_REPOSITORY"])
        self.assertEqual(record["context"]["baseline_commit"], environment["RCP_BENCH_BASELINE_COMMIT"])
        self.assertEqual([case["id"] for case in record["cases"]], args.cases)
        self.assertEqual(len(record["trials"]), 2 * args.repetitions * len(args.cases))
        return evidence, record, policy

    def test_approval_accepts_measurements_built_from_real_worker_arguments(self):
        for suite in common.SUITES.keys() - {"aa-smoke"}:
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

    def test_missing_trial_source_verification_or_resource_collection_is_ineligible(self):
        evidence, record, policy = self.inspected_measurement(binding())
        for field, failure in (("source_validation", dict(ok=False)), ("resources", dict(status="failed"))):
            with self.subTest(field=field):
                changed = copy.deepcopy(record)
                changed["trials"][0][field] = failure
                with self.assertRaisesRegex(ValueError, "incomplete paired comparison"):
                    results.approve(copy.deepcopy(evidence), changed, policy)


if __name__ == "__main__":
    unittest.main()
