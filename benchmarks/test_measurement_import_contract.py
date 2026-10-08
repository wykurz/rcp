"""Reject incomplete or ambiguous imported measurement evidence."""

import contextlib
import copy
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from benchmarks import measurements, report, run, sanitized
from benchmarks.test_measurements import build
from benchmarks.test_report import sample_run
from benchmarks.test_pair_boundaries import paired_record
from benchmarks.test_resource_boundaries import resource_record
from benchmarks.test_command_clocks import observed_record


def complete_resources():
    record = sample_run()
    record["context"].update(resource_record()["context"])
    record["phase_seconds"] = dict(generation=1, initial_verification=2, total=30)
    for row in record["trials"]:
        row.update(resource_record(status="ok")["trials"][0])
        row["phase_seconds"] = dict(preparation=1, verification=2, cleanup=3)
    return record


class MeasurementImportContractTests(unittest.TestCase):
    def reject_import_and_export(self, record, message):
        for consumer in (report.validate_result, lambda value: sanitized.project_run(value, [])):
            with self.subTest(consumer=consumer), self.assertRaisesRegex(ValueError, message):
                consumer(record)

    def test_complete_resources_require_both_qualifications(self):
        original = complete_resources()
        report.validate_result(original)
        exported = sanitized.project_run(original, [])
        self.assertTrue(all(row["exit_status_scope"] == "resource-supervisor" and row["payload_status"] == "succeeded" for row in exported["trials"]))
        for keys in (("exit_status_scope",), ("payload_status",), ("exit_status_scope", "payload_status")):
            record = copy.deepcopy(original)
            for key in keys:
                record["trials"][0].pop(key)
            with self.subTest(keys=keys):
                self.reject_import_and_export(record, "qualification")

    def test_execution_requires_one_nonempty_argv(self):
        for commands in ([], [["copy"], ["copy"]], [None], [[]], [[1]]):
            for failed in (False, True):
                record = complete_resources()
                row = record["trials"][0]
                row["commands"] = commands
                if failed:
                    record.update(status="failed", summaries=[])
                    row.update(status="failed", exit_codes=[7], payload_status="unconfirmed",
                               resources=dict(status="unavailable", metrics=None, reason="execution_failed"))
                    row["validation"] = dict(ok=False)
                with self.subTest(commands=commands, failed=failed):
                    self.reject_import_and_export(record, "exactly one recorded command")

    def test_prelaunch_failure_needs_no_execution_qualifications_or_phases(self):
        record = complete_resources()
        record.update(status="failed", summaries=[], trials=record["trials"][:1])
        row = record["trials"][0]
        row.update(status="failed", commands=[], exit_codes=[], validation=dict(ok=False),
                   resources=dict(status="unavailable", metrics=None))
        for key in ("exit_status_scope", "payload_status", "phase_seconds"):
            row.pop(key)
        report.validate_result(record)
        self.assertEqual(sanitized.project_run(record, [])["trials"][0]["resources"]["status"], "unavailable")

    def test_invalid_artifact_requires_success_and_raw_fingerprint(self):
        original = complete_resources()
        original.update(status="failed", summaries=[], trials=original["trials"][:1])
        row = original["trials"][0]
        row.update(status="failed", validation=dict(ok=False),
                   resources=dict(status="invalid", metrics=None, raw_sha256="a" * 64))
        report.validate_result(original)
        self.assertEqual(sanitized.project_run(original, [])["trials"][0]["resources"]["status"], "invalid")
        for digest in (None, "", "not-a-hash", "A" * 64):
            record = copy.deepcopy(original)
            record["trials"][0]["resources"]["raw_sha256"] = digest
            with self.subTest(digest=digest):
                self.reject_import_and_export(record, "raw fingerprint")
        missing = copy.deepcopy(original)
        missing["trials"][0]["resources"].pop("raw_sha256")
        self.reject_import_and_export(missing, "raw fingerprint")
        for code in (7, 127, 139):
            record = copy.deepcopy(original)
            record["trials"][0].update(exit_codes=[code], payload_status="unconfirmed")
            with self.subTest(code=code):
                self.reject_import_and_export(record, "successful command")

    def test_failed_execution_requires_withheld_metrics_and_failure_reason(self):
        for outcome in (dict(exit_codes=[7]), dict(exit_codes=[127]),
                        dict(exit_codes=[], timed_out=True), dict(exit_codes=[], launch_error="missing supervisor")):
            record = complete_resources()
            record.update(status="failed", summaries=[], trials=record["trials"][:1])
            row = record["trials"][0]
            row.update(status="failed", validation=dict(ok=False), payload_status="unconfirmed",
                       resources=dict(status="unavailable", metrics=None, reason="execution_failed"), **outcome)
            with self.subTest(outcome=outcome):
                report.validate_result(record)
                projected = sanitized.project_run(record, [])["trials"][0]
                self.assertEqual(projected["resources"]["reason"], "execution_failed")
                self.assertEqual(projected["payload_status"], "unconfirmed")
                for field in ("reason", "metrics"):
                    malformed = copy.deepcopy(record)
                    malformed["trials"][0]["resources"].pop(field)
                    self.reject_import_and_export(malformed, "execution_failed reason|null metrics")
                malformed = copy.deepcopy(record)
                malformed["trials"][0]["resources"]["metrics"] = {"user_seconds": 1}
                self.reject_import_and_export(malformed, "null metrics")
        record["trials"][0].update(exit_codes=[0], timed_out=False, launch_error=None, payload_status="succeeded")
        self.reject_import_and_export(record, "requires unsuccessful recorded execution")
        record["trials"][0]["resources"].pop("reason")
        report.validate_result(record)
        self.assertEqual(sanitized.project_run(record, [])["trials"][0]["payload_status"], "succeeded")

    def test_timeout_and_launch_failure_require_execution_evidence(self):
        prelaunch = complete_resources()
        prelaunch.update(status="failed", summaries=[], trials=prelaunch["trials"][:1])
        row = prelaunch["trials"][0]
        row.update(status="failed", commands=[], exit_codes=[], validation=dict(ok=False),
                   resources=dict(status="unavailable", metrics=None))
        for key in ("exit_status_scope", "payload_status", "phase_seconds"):
            row.pop(key)
        for timed_out in (False, None, 0, 1, "false"):
            record = copy.deepcopy(prelaunch)
            record["trials"][0]["timed_out"] = timed_out
            with self.subTest(timed_out=timed_out):
                if timed_out is False:
                    report.validate_result(record)
                    sanitized.project_run(record, [])
                else:
                    self.reject_import_and_export(record, "timed_out must be boolean")
        absent = copy.deepcopy(prelaunch)
        absent["trials"][0].pop("timed_out")
        report.validate_result(absent)
        for outcome in (dict(timed_out=True), dict(launch_error="cannot launch supervisor")):
            record = copy.deepcopy(prelaunch)
            row = record["trials"][0]
            row.update(outcome)
            with self.subTest(outcome=outcome):
                self.reject_import_and_export(record, "exactly one recorded command")
                row["commands"] = [["copy", "source", "destination"]]
                self.reject_import_and_export(record, "qualification")
                row.update(exit_status_scope=measurements.EXIT_STATUS_SCOPE, payload_status="unconfirmed")
                self.reject_import_and_export(record, "execution_failed reason")
                row["resources"]["reason"] = "execution_failed"
                report.validate_result(record)
                projected = sanitized.project_run(record, [])["trials"][0]
                self.assertEqual(projected["exit_status_scope"], measurements.EXIT_STATUS_SCOPE)
                self.assertEqual(projected["payload_status"], "unconfirmed")

    def test_complete_measured_trials_require_each_phase(self):
        original = complete_resources()
        for missing in (None, "preparation", "verification", "cleanup"):
            record = copy.deepcopy(original)
            if missing is None:
                record["trials"][0].pop("phase_seconds")
            else:
                record["trials"][0]["phase_seconds"].pop(missing)
            with self.subTest(missing=missing):
                self.reject_import_and_export(record, "phase costs")
            record.update(status="failed", summaries=[])
            # interruption after successful execution can leave the cleanup cost absent
            report.validate_result(record)
        paired_only = dict(status="complete", context=dict(pairing={}, measurement_environment={}), trials=[{}],
                           phase_seconds=dict(generation=0, initial_verification=0, total=0))
        with self.assertRaisesRegex(ValueError, "phase costs"):
            measurements.validate(paired_only)
        paired_only["trials"][0]["phase_seconds"] = dict(preparation=0, verification=0, cleanup=0)
        measurements.validate(paired_only)
        # existing runs without these opt-in modes still have no phase requirement
        report.validate_result(sample_run())

    def test_experiment_imports_require_environment_evidence(self):
        resource = complete_resources()
        provenance = sample_run()
        provenance["tools"]["rcp"]["sha256"] = "a" * 64
        provenance["context"].update(topology="local", measurement_environment={}, build_provenance=dict(
            qualification="caller-declared; executable hashes verified, source claims not attested",
            input_sha256="b" * 64, builds=dict(rcp=build("a" * 64))))
        for original in (resource, provenance, paired_record(), observed_record()):
            report.validate_result(original)
            record = copy.deepcopy(original)
            record["context"].pop("measurement_environment")
            self.reject_import_and_export(record, "measurement_environment evidence")
            record.update(status="failed", summaries=[])
            self.reject_import_and_export(record, "measurement_environment evidence")
        paired = dict(status="failed", context=dict(pairing={}), trials=[])
        with self.assertRaisesRegex(ValueError, "measurement_environment evidence"):
            measurements.validate(paired)
        paired["context"]["measurement_environment"] = {}
        measurements.validate(paired)
        # a failure before any experiment context is initialized remains readable
        measurements.validate(dict(status="failed", context={}, trials=[]))
        for key in ("local_resources", "build_provenance"):
            record = sample_run()
            record["context"].update({key: None, "measurement_environment": {}})
            with self.subTest(key=key):
                self.reject_import_and_export(record, "resource policy|provenance qualification")

    def test_complete_measured_runs_require_each_run_phase(self):
        original = complete_resources()
        for missing in (None, "generation", "initial_verification", "total"):
            record = copy.deepcopy(original)
            if missing is None:
                record.pop("phase_seconds")
            else:
                record["phase_seconds"].pop(missing)
            with self.subTest(missing=missing):
                self.reject_import_and_export(record, "completed measured run")
            record.update(status="failed", summaries=[])
            report.validate_result(record)
        paired = dict(status="complete", context=dict(pairing={}, measurement_environment={}), trials=[])
        with self.assertRaisesRegex(ValueError, "completed measured run"):
            measurements.validate(paired)
        paired["phase_seconds"] = dict(generation=0, initial_verification=0, total=0)
        measurements.validate(paired)

    def test_complete_total_covers_only_recorded_disjoint_phases(self):
        # three resource trials cost six seconds each; four paired trials cost twenty-four
        for original, minimum in ((complete_resources(), 21), (paired_record(), 27)):
            for total in (minimum, minimum + 1, minimum - .5e-6):
                record = copy.deepcopy(original)
                record["phase_seconds"]["total"] = total
                with self.subTest(minimum=minimum, total=total):
                    report.validate_result(record)
                    sanitized.project_run(record, [])
            for total in (0, minimum - 2e-6):
                record = copy.deepcopy(original)
                record["phase_seconds"]["total"] = total
                with self.subTest(minimum=minimum, total=total):
                    self.reject_import_and_export(record, "total must cover")
                record.update(status="failed", summaries=[])
                report.validate_result(record)
                sanitized.project_run(record, [])
        record = complete_resources()
        record["phase_seconds"].update(generation=100000000, total=100000020 - .05)
        report.validate_result(record)
        record["phase_seconds"]["total"] = 100000020 - .2
        self.reject_import_and_export(record, "total must cover")
        ordinary = sample_run()
        ordinary["phase_seconds"] = dict(generation=1, initial_verification=2, total=0)
        report.validate_result(ordinary)

    def test_finite_phase_values_cannot_overflow_their_total(self):
        record = complete_resources()
        record["phase_seconds"].update(generation=1e308, initial_verification=1e308, total=1e308)
        self.reject_import_and_export(record, "phase cost sum must be finite")

    def test_build_provenance_rejects_nonlocal_before_output_or_probes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "out"
            for extra in ([], ["--rtt-ms", "0"]):
                with self.subTest(extra=extra), mock.patch.object(run, "_tool") as probe, mock.patch.object(run, "_revision") as revision, contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    run.main(["--mode", "loopback", "--build-provenance", str(Path(directory) / "missing-build.json"),
                              "--output", str(output), *extra])
                self.assertEqual(error.exception.code, 2)
                probe.assert_not_called()
                revision.assert_not_called()
                self.assertFalse(output.exists())
            parsed = run._arguments(["--mode", "local", "--build-provenance", "build.json", "--output", str(output)])
            self.assertEqual(parsed.build_provenance, Path("build.json"))

    def test_build_provenance_imports_are_local_only(self):
        record = sample_run()
        record["tools"]["rcp"]["sha256"] = "a" * 64
        record["context"].update(topology="local", measurement_environment={}, build_provenance=dict(
            qualification="caller-declared; executable hashes verified, source claims not attested",
            input_sha256="b" * 64, builds=dict(rcp=build("a" * 64))))
        report.validate_result(record)
        sanitized.project_run(record, [])
        for status in ("complete", "failed"):
            remote = copy.deepcopy(record)
            remote["status"] = status
            remote["context"]["topology"] = "loopback"
            with self.subTest(status=status):
                self.reject_import_and_export(remote, "build provenance requires local mode")

    def test_run_and_trial_phase_scopes_do_not_mix(self):
        for trial_costs, run_costs in ((dict(generation=0), {}), ({}, dict(preparation=0))):
            record = dict(status="failed", context={}, trials=[dict(phase_seconds=trial_costs)], phase_seconds=run_costs)
            with self.subTest(trial_costs=trial_costs, run_costs=run_costs), self.assertRaisesRegex(ValueError, "phase costs"):
                measurements.validate(record)

    def test_source_revision_requires_exact_full_hash_length(self):
        declaration = build("a" * 64)
        tools = dict(rcp=dict(sha256="a" * 64))
        for length in (40, 64):
            measurements.validate_builds(dict(rcp=dict(declaration, source_revision="b" * length)), tools)
        for length in (0, 39, *range(41, 64), 65):
            with self.subTest(length=length), self.assertRaisesRegex(ValueError, "full commit ID"):
                measurements.validate_builds(dict(rcp=dict(declaration, source_revision="b" * length)), tools)


if __name__ == "__main__":
    unittest.main()
