"""Public diagnostics are bounded supplements, never replacement qualification evidence."""

import copy
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from . import production_diagnostics as diagnostics, production_export as export
from . import production_reports as reports, production_collection as collection
from . import test_production_publication as fixtures

FIELDS = diagnostics.METRICS


def inputs():
    metric = dict(user_seconds=2.14, system_seconds=930.34, max_rss_kib=93644,
        voluntary_switches=496054, involuntary_switches=70525, minor_faults=327566, major_faults=0, exit_code=0)
    trials = []
    for variant, samples in (("rcp-default", [621.653719, 621.398035]), ("rcp-baseline", [621.099793, 621.336719])):
        for index, elapsed in enumerate(samples, 1):
            trials.append(dict(case_id="bulk-1g-1tb", variant_id=variant, iteration=index,
                elapsed_seconds=elapsed, status="ok", ok=True, resources=dict(status="complete", metrics=copy.deepcopy(metric))))
    raw = dict(trials=trials, context=dict(environment=dict(
        cpu_model="Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz", effective_parallelism=96,
        kernel="7.0.0-1014-aws", architecture="x86_64", memory_limit="",
        hostname="PRIVATE_HOST", filesystem={side: dict(filesystem_type="ext4",
            mount_options=["rw", "nodev", "nosuid", "relatime", "stripe=256"],
            mount_source="/PRIVATE_DEVICE") for side in ("source", "destination")}),
        cache_policy="linux-drop-caches", verification=dict(policy="metadata-size-tree-v1"),
        pairing=dict(policy="adjacent-balanced-sha256-v1"), private_environment="PRIVATE_ENV"))
    worker = dict(storage=dict(layout="two-disjoint-raid0-ext4-v1",
        arrays={side: dict(members=["/PRIVATE_DISK_1", "/PRIVATE_DISK_2"], chunk_bytes=524288,
            member_models=["Amazon EC2 NVMe Instance Storage"] * 2, member_bytes=[900000000000] * 2)
            for side in ("source", "destination")},
        capacity={side: dict(block_size=4096, available_bytes=1704326160384) for side in ("source", "destination")}),
        private_key="PRIVATE_KEY")
    record = dict(run_key="repo-655073755-run-42458459014", retirement_sha256="a" * 64, benchmark_valid=True,
        measurements=[dict(case_id="bulk-1g-1tb", variant_id=variant,
            samples=[x["elapsed_seconds"] for x in trials if x["variant_id"] == variant])
            for variant in ("rcp-default", "rcp-baseline")])
    return record, raw, worker


def phase_summary():
    return dict(status="complete", samples=3, bytes=1234, artifact="system-telemetry.jsonl", reason=None, phases=[
        dict(phase="command", case_index=0, trial_index=0, status="observed", elapsed_seconds=10, samples=3,
            metrics=dict(cpu_usage_usec=dict(first=10, last=110, minimum=10, maximum=110, delta=100, status="available"),
                source_read_sectors=dict(first=0, last=2048, minimum=0, maximum=2048, delta=2048, status="available"),
                memory_current_bytes=dict(first=20, last=30, minimum=20, maximum=100, status="available"),
                cgroup_run_memory_peak_bytes=dict(first=200, last=200, minimum=200, maximum=200, status="available"),
                host_cpu_total_ticks=dict(first=100, last=200, minimum=100, maximum=200, delta=100, status="available"),
                host_cpu_idle_ticks=dict(first=50, last=100, minimum=50, maximum=100, delta=50, status="available"),
                host_cpu_iowait_ticks=dict(first=10, last=20, minimum=10, maximum=20, delta=10, status="available")))])


class DiagnosticProjectionTests(unittest.TestCase):
    def test_existing_cpu_rss_faults_and_switches_keep_units_and_exact_values(self):
        record, raw, worker = inputs()
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(value["status"], "complete")
        self.assertEqual(value["trials"][0]["metrics"]["max_rss_kib"], 93644)
        self.assertEqual(value["trials"][0]["metrics"]["system_seconds"], 930.34)
        self.assertEqual(set(value["trials"][0]["metrics"]), set(FIELDS))
        text = diagnostics.render(value, record)
        for label in ("Peak RSS KiB", "User CPU s", "System CPU s", "Voluntary switches", "Minor faults"):
            self.assertIn(label, text)
        self.assertIn("93,644", text)
        self.assertIn("CPU seconds may exceed wall time", text)
        self.assertIn("not host memory", text)

    def test_execution_limits_are_per_run_and_do_not_change_hardware_setup_identity(self):
        record, raw, worker = inputs()
        first = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(first["limits"], dict(allocation_seconds=None, command_timeout_seconds=None))
        worker.update(window=dict(runtime_seconds=28800), command_timeout_seconds=7200)
        second = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(second["limits"], dict(allocation_seconds=28800, command_timeout_seconds=7200))
        self.assertEqual(first["setup_id"], second["setup_id"])
        self.assertIn("28800 seconds", diagnostics.render(second, record))

    def test_private_paths_logs_environment_and_identifiers_are_not_projected(self):
        record, raw, worker = inputs()
        value = diagnostics.derive(record, b"archive", raw, worker)
        text = json.dumps(value)
        self.assertNotIn("PRIVATE", text)
        self.assertNotIn("/dev/", text)
        self.assertNotIn("mount_source", text)
        self.assertNotIn("private_key", text)
        self.assertIn("Amazon EC2 NVMe Instance Storage", text)
        self.assertEqual(value["setup"]["source"]["member_count"], 2)

    def test_old_guest_ram_stays_unknown_and_setup_groups_ignore_runtime_capacity(self):
        record, raw, worker = inputs()
        first = diagnostics.derive(record, b"archive", raw, worker)
        self.assertIsNone(first["setup"]["host_memory_total_bytes"])
        self.assertIn("not recorded", diagnostics.setup_html(first["setup"]))
        worker["storage"]["capacity"]["source"]["available_bytes"] += 4096
        second = diagnostics.derive(record, b"archive2", raw, worker)
        self.assertEqual(first["setup_id"], second["setup_id"])
        self.assertNotIn("192", diagnostics.setup_html(first["setup"]))

    def test_observed_new_ram_is_not_inferred_and_changes_setup_fingerprint(self):
        record, raw, worker = inputs()
        before = diagnostics.derive(record, b"archive", raw, worker)
        raw["system_setup"] = dict(host_memory_total_bytes=dict(status="available", value=200000000000),
                                  logical_cpu_count=dict(status="available", value=96))
        after = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(after["setup"]["host_memory_total_bytes"], 200000000000)
        self.assertNotEqual(before["setup_id"], after["setup_id"])

    def test_only_matching_frozen_copy_samples_get_backfilled(self):
        record, raw, worker = inputs()
        raw["trials"][0]["elapsed_seconds"] += 1
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual((value["status"], value["reason"]), ("unavailable", "not_matched"))
        self.assertEqual(value["trials"], [])
        self.assertIsNone(value["setup"])

    def test_failed_copy_resources_stay_unavailable_without_requalifying(self):
        record, raw, worker = inputs()
        record.update(benchmark_valid=False, measurements=[])
        raw["trials"][0].update(status="failed", ok=False)
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(value["status"], "partial")
        self.assertIsNone(value["trials"][0]["metrics"])
        self.assertFalse(record["benchmark_valid"])

    def test_invalid_process_metrics_are_not_zero_filled(self):
        for field in FIELDS:
            for bad in (-1, True, float("nan"), float("inf")):
                with self.subTest(field=field, bad=bad):
                    record, raw, worker = inputs()
                    raw["trials"][0]["resources"]["metrics"][field] = bad
                    value = diagnostics.derive(record, b"archive", raw, worker)
                    self.assertEqual(value["trials"][0]["status"], "unavailable")
                    self.assertIsNone(value["trials"][0]["metrics"])

    def test_public_tampering_private_fields_and_duplicate_trials_are_rejected(self):
        record, raw, worker = inputs()
        good = diagnostics.derive(record, b"archive", raw, worker)
        for change in (
            lambda v: v.update(retirement_sha256="b" * 64),
            lambda v: v["trials"][0]["metrics"].update(path="/PRIVATE"),
            lambda v: v["trials"].append(copy.deepcopy(v["trials"][0])),
            lambda v: v["setup"].update(hostname="PRIVATE"),
            lambda v: v.update(archive_sha256=None),
        ):
            value = copy.deepcopy(good)
            change(value)
            with self.assertRaises(ValueError):
                diagnostics.validate(value, record)

    def test_bounded_trial_count_and_identifier_privacy(self):
        record, raw, worker = inputs()
        raw["trials"] *= 33
        self.assertEqual(diagnostics.derive(record, b"archive", raw, worker)["reason"], "invalid_diagnostics")
        record, raw, worker = inputs()
        raw["trials"][0]["case_id"] = "/PRIVATE/credential"
        self.assertEqual(diagnostics.derive(record, b"archive", raw, worker)["reason"], "invalid_diagnostics")

    def test_system_summary_has_numeric_allowlists_scope_and_sector_units(self):
        record, raw, worker = inputs()
        raw["context"]["system_telemetry"] = diagnostics.TELEMETRY_DESCRIPTOR
        raw["system_telemetry"] = phase_summary()
        raw["system_telemetry"]["phases"][0]["metrics"]["PRIVATE_PATH"] = "/PRIVATE"
        raw["system_telemetry"]["artifact"] = "/PRIVATE"
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(value["system"]["status"], "complete")
        self.assertNotIn("PRIVATE", json.dumps(value))
        text = diagnostics.render(value, record)
        self.assertIn("40.00%", text)
        self.assertIn("1.000 MiB", text)
        self.assertIn("512-byte sectors", text)
        self.assertIn("not phase peaks", text)

    def test_counter_reset_and_missing_metrics_are_explicit(self):
        record, raw, worker = inputs()
        system = phase_summary()
        system["phases"][0]["metrics"]["source_read_sectors"].update(first=2048, last=1, minimum=1, delta=None, status="counter_reset")
        system["phases"][0]["metrics"]["read_bytes"] = dict(status="unavailable")
        raw["context"]["system_telemetry"] = diagnostics.TELEMETRY_DESCRIPTOR
        raw["system_telemetry"] = system
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertIsNone(value["system"]["phases"][0]["metrics"]["source_read_sectors"]["delta"])
        self.assertEqual(value["system"]["phases"][0]["metrics"]["read_bytes"], dict(status="unavailable"))

    def test_sampler_off_old_fields_unchanged_and_bad_sampler_is_optional(self):
        record, raw, worker = inputs()
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(value["system"], dict(status="unavailable", samples=0, phases=[]))
        raw["system_telemetry"] = dict(status="complete", samples=90000, phases=[])
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(value["system"]["status"], "unavailable")

    def test_rich_full_set_phase_hierarchy_fits_existing_json_and_companion_bounds(self):
        from benchmarks import telemetry
        record, raw, worker = inputs()
        cases = ("bulk-1g-1tb", "bulk-100m-1024gb", "bulk-1m-1024gb", "tiny-1k-10240k")
        trials, measurements, phases = [], [], []
        for case_index, case in enumerate(cases):
            for name in ("capacity", "generation", "initial_verification"):
                phases.append(dict(phase=name, case_index=case_index, trial_index=None))
            for trial in inputs()[1]["trials"]:
                trial = copy.deepcopy(trial)
                trial["case_id"] = case
                trial_index = len(trials)
                trials.append(trial)
                for name in ("preparation", "cache", "command", "timings", "destination",
                             "summary", "metadata", "source", "cleanup"):
                    phases.append(dict(phase=name, case_index=case_index, trial_index=trial_index))
            for item in record["measurements"]:
                measurements.append(dict(item, case_id=case))
        record["measurements"] = measurements
        self.assertEqual(len(phases), 156)
        phases += [dict(phase="unattributed", case_index=None, trial_index=None)
                   for _ in range(telemetry.MAX_PHASES - len(phases))]
        raw.update(trials=trials, cases=[dict(id=case, **diagnostics.CASE_SHAPES[case]) for case in cases])
        raw["context"]["system_telemetry"] = telemetry.descriptor()
        with tempfile.TemporaryDirectory() as temporary:
            sampler = telemetry.Sampler(temporary, temporary, temporary)
            sampler.closed = True
            sampler.partial = False
            sampler.reason = None
            sampler.phases = []
            for index, phase in enumerate(phases):
                metrics = {label: dict(first=0, last=(1 << 63) - 1,
                    minimum=0, maximum=(1 << 63) - 1, counter=counter, reset=False)
                    for _, _, label, counter in telemetry.SUMMARY_METRICS}
                sampler.phases.append(dict(phase, started=index, elapsed_seconds=1,
                    samples=2, status="observed", metrics=metrics))
            sampler.samples = 2 * len(phases)
            sampler.attach(raw)
        self.assertEqual(len(phases), 192)
        self.assertEqual(len(raw["system_telemetry"]["phases"]), 48)
        self.assertEqual(sum(len(t["system_telemetry"]["phases"]) for t in raw["trials"]), 144)
        self.assertEqual(raw["system_telemetry"]["samples"], 96)
        parsed = collection.results.json_value(collection.encoded(raw), collection.results.MAX_JSON)
        value = diagnostics.derive(record, b"archive", parsed, worker)
        self.assertEqual(len(value["trials"]), 16)
        self.assertIsNone(value["reason"])
        self.assertLessEqual(len(collection.encoded(value)), diagnostics.MAX_DOCUMENT)
        collection.results.json_value(collection.encoded(value), diagnostics.MAX_DOCUMENT)
        page = diagnostics.render(value, record)
        self.assertIn("Sampled run phases", page)
        self.assertEqual(page.count("<details><summary>Sampled phases:"), 16)

    def test_optional_phase_bulk_never_discards_existing_process_metrics_or_setup(self):
        record, raw, worker = inputs()
        raw["context"]["system_telemetry"] = diagnostics.TELEMETRY_DESCRIPTOR
        baseline = diagnostics.derive(record, b"archive", raw, worker)
        raw["system_telemetry"] = phase_summary()
        raw["system_telemetry"]["phases"] *= 30
        with patch.object(diagnostics, "MAX_DOCUMENT", len(collection.encoded(baseline)) + 200):
            value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(value["reason"], "phase_summary_budget")
        self.assertEqual(value["status"], "complete")
        self.assertEqual(value["trials"][0]["metrics"], baseline["trials"][0]["metrics"])
        self.assertEqual(value["setup"], baseline["setup"])
        self.assertIsNone(value["system"])
        self.assertIn("Per-copy process metrics and observed setup remain shown", diagnostics.render(value, record))

    def test_planned_cases_without_copy_trials_remain_visible_and_unknown(self):
        record, raw, worker = inputs()
        record.update(benchmark_valid=False, measurements=[])
        raw["cases"] = [dict(id=case, **shape) for case, shape in diagnostics.CASE_SHAPES.items()]
        raw["context"]["system_telemetry"] = diagnostics.TELEMETRY_DESCRIPTOR
        summary = phase_summary()
        summary["phases"][0].update(phase="generation", case_index=1, trial_index=None, status="interrupted")
        raw["system_telemetry"] = summary
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertEqual(len(value["cases"]), 4)
        self.assertEqual([case["copy_trials"] for case in value["cases"]], [4, 0, 0, 0])
        self.assertTrue(value["cases"][1]["phase_boundary_recorded"])
        self.assertFalse(value["cases"][2]["phase_boundary_recorded"])
        page = diagnostics.render(value, record)
        self.assertIn("No copy trial recorded; generation/attempt status unknown", page)
        self.assertIn("does not qualify individual cases", page)
        self.assertNotIn("not attempted", page)
        self.assertEqual(record["measurements"], [])

    def test_case_phase_boundary_requires_integer_index_and_recorded_sampler_contract(self):
        record, raw, worker = inputs()
        record.update(benchmark_valid=False, measurements=[])
        raw["cases"] = [dict(id=case, **shape) for case, shape in diagnostics.CASE_SHAPES.items()]
        summary = phase_summary()
        summary["phases"][0].update(phase="generation", case_index=True, trial_index=None)
        raw["system_telemetry"] = summary
        raw["context"]["system_telemetry"] = diagnostics.TELEMETRY_DESCRIPTOR
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertFalse(any(case["phase_boundary_recorded"] for case in value["cases"]))
        summary["phases"][0]["case_index"] = 1
        raw["context"].pop("system_telemetry")
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertFalse(any(case["phase_boundary_recorded"] for case in value["cases"]))
        raw["context"]["system_telemetry"] = dict(diagnostics.TELEMETRY_DESCRIPTOR, policy="future-contract")
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertFalse(any(case["phase_boundary_recorded"] for case in value["cases"]))
        raw["context"]["system_telemetry"] = diagnostics.TELEMETRY_DESCRIPTOR
        summary["phases"][0]["status"] = "malformed"
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertFalse(any(case["phase_boundary_recorded"] for case in value["cases"]))
        summary["phases"][0]["status"] = "observed"
        value = diagnostics.derive(record, b"archive", raw, worker)
        self.assertTrue(value["cases"][1]["phase_boundary_recorded"])

    def test_central_setup_explains_decimal_workloads_and_missing_ram(self):
        record, raw, worker = inputs()
        value = diagnostics.derive(record, b"archive", raw, worker)
        page = diagnostics.configuration_html([dict(run="42458459014-1", href="runs/a.html", setup=value["setup"])])
        self.assertIn("10,240,000", page)
        self.assertIn("exactly 100 times less", page)
        self.assertIn("decimal bytes", page)
        self.assertIn("not inferred", page)
        self.assertIn("gp3 EBS root", page)
        self.assertIn("local NVMe instance-store", page)
        self.assertIn("<th scope='row'>Observed host RAM</th><td>not recorded</td>", page)
        for section in ("machine", "datasets", "method", "observed"):
            self.assertIn("id='" + section + "'", page)
        self.assertNotIn("<script", page)

    def test_setup_rows_classify_fields_and_keep_unrecorded_values_unknown(self):
        record, raw, worker = inputs()
        setup = diagnostics.derive(record, b"archive", raw, worker)["setup"]
        listed = diagnostics.setup_rows(setup)
        rows = {(group, label): (text, kind) for group, label, text, kind in listed}
        self.assertEqual(len(rows), len(listed))
        self.assertEqual(rows[("Machine", "CPU model")],
                         ("Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz", "hardware"))
        self.assertEqual(rows[("Machine", "Logical CPUs")], (None, "hardware"))
        self.assertEqual(rows[("Machine", "Usable CPUs")], ("96", "hardware"))
        self.assertEqual(rows[("Machine", "Observed host RAM")], (None, "context"))
        self.assertEqual(rows[("System software", "Kernel")], ("7.0.0-1014-aws", "software"))
        self.assertEqual(rows[("Method", "Cache preparation")], ("linux-drop-caches", "software"))
        self.assertEqual(rows[("Source storage", "Filesystem")], ("ext4", "hardware"))
        self.assertEqual(rows[("Source storage", "Device sizes")],
                         ("900,000,000,000 bytes, 900,000,000,000 bytes", "hardware"))
        self.assertEqual(rows[("Destination storage", "Mount options")],
                         ("nodev, nosuid, relatime, rw, stripe=256", "software"))
        self.assertEqual({kind for _, kind in rows.values()}, {"hardware", "software", "context"})

    def test_central_setup_states_shared_values_once_and_lists_only_recorded_differences(self):
        record, raw, worker = inputs()
        first = diagnostics.derive(record, b"archive", raw, worker)["setup"]
        raw["system_setup"] = dict(logical_cpu_count=dict(status="available", value=96))
        second = diagnostics.derive(record, b"archive", raw, worker)["setup"]
        observed = [dict(run="1-1", href="runs/repo-1-run-1.html", setup=first),
                    dict(run="2-1", href="runs/repo-1-run-2.html", setup=second)]
        page = diagnostics.configuration_html(observed)
        self.assertEqual(page.count("Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz"), 1)
        self.assertNotIn("id='setup-differences'", page)
        # a field that only the newer run recorded is a value, not a difference
        self.assertIn("<th scope='row'>Logical CPUs</th><td>96 <small>(recorded by 1 of 2 runs)</small></td>", page)
        raw["context"]["environment"]["kernel"] = "7.0.0-1020-aws"
        third = diagnostics.derive(record, b"archive", raw, worker)["setup"]
        page = diagnostics.configuration_html(observed + [dict(run="3-1", href="runs/repo-1-run-3.html", setup=third)])
        self.assertIn("id='setup-differences'", page)
        self.assertIn("<td>7.0.0-1014-aws / 7.0.0-1020-aws <small>", page)
        self.assertIn("<a href='runs/repo-1-run-3.html'>3-1</a>", page)
        self.assertEqual(page.count("Intel(R) Xeon(R) Platinum 8275CL CPU @ 3.00GHz"), 1)

    def test_central_setup_merges_identical_source_and_destination_storage(self):
        record, raw, worker = inputs()
        setup = diagnostics.derive(record, b"archive", raw, worker)["setup"]
        page = diagnostics.configuration_html([dict(run="1-1", href="runs/a.html", setup=setup)])
        self.assertIn("Source and destination storage", page)
        self.assertEqual(page.count("<th scope='row'>Filesystem</th>"), 1)
        worker["storage"]["arrays"]["destination"]["chunk_bytes"] = 1048576
        setup = diagnostics.derive(record, b"archive", raw, worker)["setup"]
        page = diagnostics.configuration_html([dict(run="1-1", href="runs/a.html", setup=setup)])
        self.assertEqual(page.count("<th scope='row'>Filesystem</th>"), 2)

    def test_run_appendix_states_identical_storage_arrays_once(self):
        record, raw, worker = inputs()
        setup = diagnostics.derive(record, b"archive", raw, worker)["setup"]
        table = diagnostics.setup_html(setup)
        self.assertIn("<th colspan='2' scope='rowgroup'>Source and destination storage</th>", table)
        self.assertEqual(table.count("<th scope='row'>Filesystem</th>"), 1)
        worker["storage"]["arrays"]["destination"]["chunk_bytes"] = 1048576
        table = diagnostics.setup_html(diagnostics.derive(record, b"archive", raw, worker)["setup"])
        self.assertIn("<th colspan='2' scope='rowgroup'>Destination storage</th>", table)
        self.assertEqual(table.count("<th scope='row'>Filesystem</th>"), 2)

    def test_central_setup_without_observations_says_so_and_rejects_invalid_setups(self):
        self.assertIn("No observed setup was available in this snapshot.", diagnostics.configuration_html([]))
        with self.assertRaises(ValueError):
            diagnostics.configuration_html([dict(run="1-1", href="runs/a.html", setup=dict(cpu_model="<script>"))])

    def test_run_diagnostics_keep_the_observed_setup_in_a_collapsed_appendix(self):
        record, raw, worker = inputs()
        value = diagnostics.derive(record, b"archive", raw, worker)
        text = diagnostics.render(value, record)
        self.assertIn("id='resource-diagnostics'", text)
        self.assertIn("id='observed-setup'", text)
        self.assertIn("</summary><table><tbody><tr><th colspan='2' scope='rowgroup'>Machine</th></tr>"
                      "<tr><th scope='row'>CPU model</th>", text)
        self.assertNotIn("setups.html#setup-", text)
        self.assertNotIn("<script", text)
        missing = diagnostics.render(diagnostics.unavailable(record), record)
        self.assertIn("No observed setup was recorded for this run.", missing)


class RetainedDiagnosticReadTests(unittest.TestCase):
    setUp = fixtures.PublicationTests.setUp

    def test_read_only_companion_keeps_frozen_bytes_and_does_not_requalify(self):
        record = self.reader.public_record(self.key)
        saved = json.dumps(record, sort_keys=True)
        writes = len(self.fx.s3.writes)
        with patch.object(collection.results, "inspect", side_effect=AssertionError("no requalification")), \
                patch.object(collection.projection, "freeze", side_effect=AssertionError("no new frozen projection")):
            value, consumed = self.reader.public_diagnostics(self.key, record, maximum_bytes=100000000)
        self.assertGreater(consumed, 0)
        self.assertEqual(value["retirement_sha256"], record["retirement_sha256"])
        self.assertEqual(value["archive_sha256"], collection.digest(
            self.fx.s3.objects[self.key.replace("manifest.json", "results.zip")]["1"]))
        self.assertEqual(json.dumps(record, sort_keys=True), saved)
        self.assertEqual(len(self.fx.s3.writes), writes)

    def test_zero_budget_reads_nothing_and_returns_unavailable(self):
        record = self.reader.public_record(self.key)
        with patch.object(self.s3, "get_object", side_effect=AssertionError("no source budget")):
            value, consumed = self.reader.public_diagnostics(self.key, record, maximum_bytes=0)
        self.assertEqual(consumed, 0)
        self.assertEqual(value["reason"], "raw_budget")

    def test_manifest_budget_does_not_allow_an_over_budget_archive_read(self):
        record = self.reader.public_record(self.key)
        original = self.s3.get_object
        calls = []
        def read(**kwargs):
            calls.append(kwargs["Key"])
            self.assertNotIn("results.zip", kwargs["Key"])
            return original(**kwargs)
        with patch.object(self.s3, "get_object", side_effect=read):
            value, consumed = self.reader.public_diagnostics(self.key, record,
                maximum_bytes=2 * (collection.MAX_DOCUMENT + 2))
        self.assertEqual(value["reason"], "raw_budget")
        self.assertEqual(consumed, 2 * (collection.MAX_DOCUMENT + 2))
        self.assertEqual(len(calls), 2)

    def test_failed_raw_read_still_charges_its_full_reserved_body_budget(self):
        record = self.reader.public_record(self.key)
        prefix = self.key.removesuffix("manifest.json")
        manifest = json.loads(self.fx.s3.objects[self.key]["1"])
        archive_bytes = manifest["retained"]["results.zip"]["bytes"]
        original = self.reader.objects.referenced
        def failed(ref, key, limit):
            if key.endswith("results.zip"):
                self.assertEqual(limit, archive_bytes)
                raise OSError("body failed after a partial transfer")
            return original(ref, key, limit)
        with patch.object(self.reader.objects, "referenced", side_effect=failed):
            value, consumed = self.reader.public_diagnostics(self.key, record, maximum_bytes=100000000)
        self.assertEqual(value["reason"], "raw_unavailable")
        self.assertEqual(consumed, 2 * (collection.MAX_DOCUMENT + 2) + archive_bytes + 2)

    def test_expired_or_corrupt_archive_does_not_erase_frozen_timing_record(self):
        record = self.reader.public_record(self.key)
        key = self.key.replace("manifest.json", "results.zip")
        for mode in ("corrupt", "expired"):
            saved = self.fx.s3.objects.pop(key)
            if mode == "corrupt":
                self.fx.s3.objects[key] = {"1": b"untrusted private"}
            try:
                value, _ = self.reader.public_diagnostics(self.key, record, maximum_bytes=100000000)
                self.assertEqual(value["reason"], "raw_unavailable")
                self.assertEqual(self.reader.public_record(self.key), record)
                self.assertNotIn("private", json.dumps(value))
            finally:
                self.fx.s3.objects[key] = saved

    def test_changed_retirement_during_diagnostic_read_is_unavailable(self):
        record = self.reader.public_record(self.key)
        original = self.reader.record
        count = 0
        def changing(run):
            nonlocal count
            count += 1
            value, item = original(run)
            if count == 2:
                item = dict(item, revision={"N": "999"})
            return value, item
        with patch.object(self.reader, "record", side_effect=changing):
            value, _ = self.reader.public_diagnostics(self.key, record, maximum_bytes=100000000)
        self.assertEqual(value["reason"], "raw_unavailable")

    def test_actual_snapshot_v2_companion_is_bound_to_inventory_and_frozen_record(self):
        record = self.reader.public_record(self.key)
        self.db.get_item.side_effect = lambda **kw: {} if kw["Key"]["pk"]["S"] == "active" else {"Item": copy.deepcopy(self.item)}
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(reports, "REPOSITORY_ID", "123"), \
                patch.object(reports, "RUN", re.compile(r"repo-123-run-[1-9][0-9]{0,19}")):
            output = Path(temporary) / "snapshot"
            proof = reports.snapshot(self.reader, output, "a" * 40)
            records, index = reports.public_batch(output, "a" * 40, proof)
            self.assertEqual(index["schema"], reports.DIAGNOSTIC_SCHEMA)
            self.assertEqual(json.loads(records[record["run_key"]]), record)
            details = reports.public_diagnostics(output, index, records=records)
            self.assertEqual(set(details), {record["run_key"]})
            path = output / (record["run_key"] + ".diagnostics.json")
            changed = json.loads(path.read_bytes())
            changed["retirement_sha256"] = "b" * 64
            path.write_bytes(collection.encoded(changed))
            new_proof = reports.inventory(output)
            with self.assertRaises(ValueError):
                reports.public_batch(output, "a" * 40, new_proof)

    def test_optional_companion_cannot_displace_a_frozen_record_at_output_budget_edge(self):
        record = self.reader.public_record(self.key)
        self.db.get_item.side_effect = lambda **kw: {} if kw["Key"]["pk"]["S"] == "active" else {"Item": copy.deepcopy(self.item)}
        size = len(collection.encoded(record))
        with tempfile.TemporaryDirectory() as temporary, \
                patch.object(reports, "REPOSITORY_ID", "123"), \
                patch.object(reports, "RUN", re.compile(r"repo-123-run-[1-9][0-9]{0,19}")), \
                patch.object(reports.public, "MAX_PUBLIC_RECORD", size), \
                patch.object(reports, "MAX_HISTORY", size + reports.MAX_INDEX + 10):
            output = Path(temporary) / "snapshot"
            proof = reports.snapshot(self.reader, output, "a" * 40)
            records, index = reports.public_batch(output, "a" * 40, proof)
            self.assertEqual(json.loads(records[record["run_key"]]), record)
            self.assertEqual(index["schema"], reports.SCHEMA)
            self.assertTrue(reports.deployable(index))
            self.assertEqual({p.name for p in output.iterdir()}, {"index.json", record["run_key"] + ".json"})

    def test_export_companion_leaves_stage_and_old_renderer_unchanged(self):
        bundle = self.reader.download(self.key)
        record = export.project(bundle)
        before = export.render(record)
        details = export.diagnostic_companion(bundle)
        after = export.render(record, diagnostics=details)
        self.assertNotIn("Resource diagnostics", before)
        self.assertIn("Resource diagnostics", after)
        self.assertIn("id='observed-setup'", after)
        self.assertEqual(export.project(bundle), record)
        self.assertEqual(export.render(record), before)


class DiagnosticPriorityTests(unittest.TestCase):
    from .test_production_reports import ReportsTests as _Base
    setUp = _Base.setUp
    fake_reader = _Base.fake_reader
    batch = _Base.batch

    def two_records(self):
        second = copy.deepcopy(self.public)
        second["run_key"] = "repo-655073755-run-124"
        second["identity"].update(run="124-1",
            run_url="https://github.com/wykurz/rcp/actions/runs/124/attempts/1")
        return [self.public, second]

    def test_optional_raw_budget_cannot_defer_later_canonical_records(self):
        from .test_production_reports import REVISION
        values = self.two_records()
        reader = self.fake_reader({value["run_key"]: dict(state="ready", retired=True, record=value)
                                   for value in values})
        events = []
        observe = reader.observe_public.side_effect
        def canonical(key):
            events.append(("canonical", key))
            return observe(key)
        reader.observe_public.side_effect = canonical
        def optional(key, record, *, maximum_bytes):
            events.append(("diagnostic", key, maximum_bytes))
            consumed = reports.MAX_SOURCE_PER_RECORD if maximum_bytes >= reports.MAX_SOURCE_PER_RECORD else 0
            return diagnostics.unavailable(record, "raw_budget"), consumed
        reader.public_diagnostics = optional
        with patch.object(reports, "MAX_TOTAL", 2 * reports.MAX_SOURCE_PER_RECORD + 100):
            output, proof = self.batch(reader)
            records, index = reports.public_batch(output, REVISION, proof)
        self.assertEqual(set(records), {x["run_key"] for x in values})
        self.assertTrue(index["processing_complete"])
        self.assertTrue(reports.deployable(index))
        self.assertEqual([x[0] for x in events], ["canonical", "canonical", "diagnostic", "diagnostic"])
        self.assertTrue(events[2][1].endswith("run-124/collection/manifest.json"))
        self.assertEqual([x[2] for x in events if x[0] == "diagnostic"], [100, 100])

    def test_optional_output_cannot_displace_later_canonical_records(self):
        from .test_production_reports import REVISION
        values = self.two_records()
        reader = self.fake_reader({value["run_key"]: dict(state="ready", retired=True, record=value)
                                   for value in values})
        reader.public_diagnostics = lambda *args, **kwargs: self.fail("no diagnostic output budget")
        size = max(len(collection.encoded(value)) for value in values)
        with patch.object(reports.public, "MAX_PUBLIC_RECORD", size), \
                patch.object(reports, "MAX_HISTORY", 2 * size + reports.MAX_INDEX + 10):
            output, proof = self.batch(reader)
            records, index = reports.public_batch(output, REVISION, proof)
        self.assertEqual(set(records), {x["run_key"] for x in values})
        self.assertTrue(reports.deployable(index))
        self.assertNotIn("diagnostics", index)


if __name__ == "__main__":
    unittest.main()
