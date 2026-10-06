"""Paired measurements share the existing case correctness boundary."""

import json
from pathlib import Path
import shutil
import statistics
import subprocess
import tempfile
import unittest
from unittest import mock

from benchmarks import pairs, report, run
from benchmarks.test_report import sample_run


def paired_record(case_ids=("tiny",), repetitions=2):
    record = sample_run()
    record["context"].update(topology="local", measurement_environment={}, pairing=pairs.configuration(7, repetitions),
                             timing_request="disabled", timing_collection=dict.fromkeys(pairs.ROLES, "disabled"))
    record["phase_seconds"] = dict(generation=1, initial_verification=2)
    record["variants"] = [dict(record["variants"][0], id=role, args=[]) for role in pairs.ROLES]
    record["cases"] = [dict(record["cases"][0], id=case) for case in case_ids]
    template = record["trials"][0]
    record["trials"] = []
    record["summaries"] = []
    for case in case_ids:
        for iteration in range(1, repetitions + 1):
            for position, role in enumerate(pairs.order(record["context"]["pairing"], case, iteration)):
                record["trials"].append(dict(template, case_id=case, variant_id=role, iteration=iteration,
                    pairing=pairs.trial_metadata(record["context"]["pairing"], case, iteration, position),
                    phase_seconds=dict(preparation=1, verification=2, cleanup=3),
                    timings=dict(status="disabled", reports=[]), elapsed_seconds=1.0 if role == pairs.CANDIDATE else 2.0))
        for role in pairs.ROLES:
            samples = [row["elapsed_seconds"] for row in record["trials"] if row["case_id"] == case and row["variant_id"] == role]
            record["summaries"].append(dict(series_id="1"*64, case_id=case, variant_id=role,
                unit="seconds", median=statistics.median(samples), minimum=min(samples), maximum=max(samples),
                stdev=statistics.stdev(samples), samples=samples, files_per_second=1.0))
    record["phase_seconds"]["total"] = 4 + sum(row["elapsed_seconds"] + sum(row["phase_seconds"].values()) for row in record["trials"])
    return record


class PairBoundaryTests(unittest.TestCase):
    def test_source_corruption_excludes_the_entire_uncommitted_case(self):
        from benchmarks.test_timings import TimingTests
        for damaging_trial in (2, 4):
            with self.subTest(damaging_trial=damaging_trial), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary, manifest = TimingTests()._fixture(root)
                calls = []
                def execute(commands, logdir, timeout, **kwargs):
                    source, destination = map(Path, commands[0][-2:])
                    shutil.copytree(source, destination)
                    calls.append(commands)
                    if len(calls) == damaging_trial:
                        (source / "0" / "file").write_bytes(b"y")
                    return dict(ok=True, elapsed_seconds=1.0, exit_codes=[0], timed_out=False, commands=commands, logs=[])
                with mock.patch.object(run, "execute_commands", side_effect=execute), \
                        mock.patch.object(run, "environment", return_value={}), \
                        self.assertRaises(RuntimeError):
                    run.main(["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default",
                        "--bin-dir", str(binary), "--baseline-bin-dir", str(binary),
                        "--source-root", str(root), "--destination-root", str(root),
                        "--cache", "uncontrolled", "--no-timings", "--paired-seed", "7", "--repetitions", "2",
                        "--output", str(root / "out")])
                saved = report.parse_result((root / "out" / "results.json").read_text())
                self.assertEqual(saved["status"], "failed")
                self.assertFalse(saved["summaries"])
                self.assertEqual(pairs.comparisons(saved), [])
                self.assertEqual(sum(row["status"] == "ok" for row in saved["trials"]), damaging_trial)

    def test_completed_case_survives_a_later_case_failure(self):
        record = paired_record(("finished", "unfinished"))
        record.update(status="failed", trials=record["trials"][:5], summaries=record["summaries"][:2])
        record["trials"][-1].update(status="failed", exit_codes=[1], validation=dict(ok=False))
        report.validate_result(record)
        compared = pairs.comparisons(record)
        self.assertEqual(len(compared), 2)
        self.assertEqual({item["case_id"] for item in compared}, {"finished"})
        record["summaries"].pop()
        self.assertEqual(pairs.comparisons(record), [])
        with self.assertRaises(ValueError):
            pairs.validate(record)

    def test_imported_pairs_require_disabled_effective_timings(self):
        for mutate in (
                lambda value: value["context"].update(timing_request="automatic"),
                lambda value: value["context"]["timing_collection"].update({pairs.REFERENCE: "unsupported"}),
                lambda value: value["context"].pop("timing_collection"),
                lambda value: value["context"].pop("timing_request"),
                lambda value: [variant.update(args=["--timings=private-path"]) for variant in value["variants"]]):
            record = paired_record()
            mutate(record)
            with self.subTest(record=record["context"].get("timing_request")), self.assertRaises(ValueError):
                pairs.validate(record)
            with self.assertRaises(ValueError):
                report.validate_result(record)

    def test_failed_preflight_can_precede_timing_resolution(self):
        record = paired_record()
        record.update(status="failed", trials=[], summaries=[], variants=[], cases=[])
        record["context"].pop("timing_collection")
        record["context"].update(timing_request="automatic")
        report.validate_result(record)
        record["context"].pop("timing_request")
        report.validate_result(record)

    def test_large_plan_validation_visits_only_recorded_rows(self):
        for count in (0, 3):
            record = paired_record()
            record.update(status="failed", trials=record["trials"][:count], summaries=[])
            record["context"]["pairing"] = pairs.configuration(7, 10**12)
            original = pairs.order
            calls = []
            def bounded_order(*args):
                calls.append(args)
                self.assertLessEqual(len(calls), count)
                return original(*args)
            with self.subTest(rows=count), mock.patch.object(pairs, "order", side_effect=bounded_order):
                pairs.validate(record)
            self.assertEqual(len(calls), count)
            record["status"] = "complete"
            with mock.patch.object(pairs, "order", side_effect=AssertionError("must reject by row count")), self.assertRaises(ValueError):
                pairs.validate(record)

    def test_seed_contract_is_exact_in_json_and_javascript(self):
        for seed in (0, pairs.MAX_SEED):
            config = pairs.configuration(seed, 2)
            self.assertEqual(json.loads(json.dumps(config))["seed"], seed)
            self.assertEqual(int(float(seed)), seed)
            if shutil.which("node"):
                decoded = subprocess.run(["node", "-e", "process.stdout.write(JSON.stringify(JSON.parse(process.argv[1])))", json.dumps(config)], capture_output=True, text=True, check=True)
                self.assertEqual(json.loads(decoded.stdout), config)
        for seed in (True, -1, pairs.MAX_SEED + 1, 1152921504606846977, "1", 1.0, None):
            with self.subTest(seed=seed), self.assertRaises(ValueError):
                pairs.configuration(seed, 2)

    def test_malformed_imports_raise_value_error(self):
        for mutate in (
                lambda value: value["variants"][0].pop("tool"),
                lambda value: value["variants"][0].update(processes=True),
                lambda value: value["variants"][0].update(args=None),
                lambda value: value.update(variants=None),
                lambda value: value.update(cases=[None]),
                lambda value: value["cases"][0].update(id=[]),
                lambda value: value["context"].update(pairing=[]),
                lambda value: value["context"]["pairing"].update(block_pairs=2.0),
                lambda value: value["trials"][0].update(iteration=True),
                lambda value: value["trials"][0]["pairing"].update(position=False),
                lambda value: value["summaries"][0].update(case_id=[]),
                lambda value: value["summaries"][0].pop("variant_id")):
            record = paired_record()
            mutate(record)
            with self.assertRaises(ValueError):
                pairs.validate(record)


if __name__ == "__main__":
    unittest.main()
