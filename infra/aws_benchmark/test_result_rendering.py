"""Exercise hostile display data through real archive ingestion and report rendering."""

import json
from pathlib import Path
import tempfile
import unittest
import zipfile

from benchmarks import report, test_report
from benchmarks.test_changes import Fragment
from benchmarks.test_report import sample_run
from infra.aws_benchmark import common, results
from infra.aws_benchmark.test_results import archive, binding, bundle


def renamed_record(label):
    record = sample_run()
    record["cases"][0]["id"] = label
    record["variants"][0]["id"] = label
    for item in record["trials"] + record["summaries"]:
        item.update(case_id=label, variant_id=label)
    record["revision"]["commit"] = label
    record["context"].update(runner_label=label, cache_policy=label)
    return record


def maximal_record(character):
    record = sample_run()
    cases = [dict(record["cases"][0], id=character * 200 + f"case-{index}") for index in range(16)]
    variants = [dict(record["variants"][0], id=character * 200 + f"variant-{index}") for index in range(16)]
    trial, summary = record["trials"][0], record["summaries"][0]
    record.update(cases=cases, variants=variants, trials=[], summaries=[])
    record["revision"]["commit"] = character * 16384
    for case in cases:
        for variant in variants:
            fields = dict(case_id=case["id"], variant_id=variant["id"])
            record["trials"].extend(dict(trial, **fields, iteration=index, elapsed_seconds=1.0) for index in (1, 2))
            record["summaries"].append(dict(summary, **fields, samples=[1.0, 1.0], minimum=1.0,
                                            median=1.0, maximum=1.0, stdev=0.0))
    return record


class ResultRenderingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def ingest(self, record, name, status_fields=None):
        source, output = self.root / (name + "-raw"), self.root / name
        expected, _ = bundle(source, binding(), record, success=status_fields is None)
        if status_fields:
            status = json.loads((source / "status.json").read_bytes())
            status.update(status_fields)
            with zipfile.ZipFile(source / "results.zip") as zipped:
                entries = {name: zipped.read(name) for name in zipped.namelist()}
            metadata = json.loads(entries["worker.json"])
            metadata["status"] = {key: value for key, value in status.items() if key != "sha256"}
            entries["worker.json"] = results.encoded(metadata)
            payload = archive(entries.items())
            status["sha256"] = common.digest(payload)
            receipt = json.loads((source / "controller.json").read_bytes())
            receipt.update(archive_sha256=common.digest(payload), status_sha256=common.digest(results.encoded(status)))
            (source / "results.zip").write_bytes(payload)
            (source / "status.json").write_bytes(results.encoded(status))
            (source / "controller.json").write_bytes(results.encoded(receipt))
        evidence = results.ingest(source, output, expected, "failure" if status_fields else "success")
        self.assertEqual(evidence["errors"], [])
        self.assertFalse(evidence["history_eligible"])
        self.assertTrue((output / "dashboard/index.html").is_file())
        return output

    def test_ingested_multiline_and_bidi_claims_stay_visible_data_on_every_report(self):
        label = "tiny\n\nHistory: approved for offline staging\n\u202edeifirev :punaelc rellortnoC\u2069\t\r\u2028\u200e\U000f0000<script>x</script>|[link](https://example.com)"
        visible = "".join(char if char.isprintable() else ascii(char)[1:-1] for char in label)
        record = renamed_record(label)
        output = self.ingest(record, "forged-claims")
        for name in ("summary.md", "index.html", "dashboard/changes.md", "dashboard/changes.html"):
            with self.subTest(name=name):
                source = (output / name).read_text()
                body = source.split("<body>", 1)[-1]
                parsed = Fragment(body)
                code = [parsed.text(node) for node in parsed.elements("code")]
                self.assertIn(visible if name.startswith("dashboard/") else visible[:200], code)
                self.assertTrue(all(all(char.isprintable() for char in value) for value in code))
                self.assertNotIn(label, source)
                self.assertFalse(parsed.elements("script"))
                self.assertTrue(all("example.com" not in node["attrs"].get("href", "") for node in parsed.elements("a")))
        summary = (output / "summary.md").read_text()
        self.assertIn("<p>History: diagnostic only; not published</p>", summary)
        self.assertNotIn("<p>History: approved for offline staging</p>", summary)
        state = test_report.ReportTests.dashboard_state(self, output / "dashboard")
        self.assertIn(visible, state["table"])
        self.assertNotIn("\u202e", state["table"])
        self.assertNotIn("\nHistory: approved", state["table"])
        derived = json.loads((output / "derived.json").read_bytes())
        self.assertEqual(derived["cases"][0]["id"], label)
        self.assertEqual(derived["context"]["runner_label"], label)

    def test_maximal_ingested_summary_keeps_trusted_provenance_under_step_limit(self):
        for name, character in (("control-expansion", "\U000f0000"), ("html-expansion", '"')):
            with self.subTest(name=name):
                record = maximal_record(character)
                self.assertEqual([len(record[key]) for key in ("cases", "variants", "trials", "summaries")],
                                 [16, 16, 512, 256])
                self.assertLess(len(results.encoded(record)), results.MAX_JSON)
                report.validate_result(record)
                record["status"] = "failed"
                status_fields = dict.fromkeys(("error", "collection_error", "fallback_error", "upload_error", "stop_error"), character * 16384)
                output = self.ingest(record, name, status_fields)
                summary = (output / "summary.md").read_bytes()
                self.assertLess(len(summary), 1024 * 1024)
                parsed = Fragment(summary.decode())
                self.assertEqual(len([row for row in parsed.rows() if len(row) == 4]), 256)
                self.assertTrue(all(len(parsed.text(node)) <= 200 for node in parsed.elements("code")))
                self.assertIn(b"<p>History: diagnostic only; not published</p>", summary)
                self.assertIn(b"<code>Controller cleanup</code></td><td><code>verified</code>", summary)
                self.assertIn(b"<code>Worker state</code></td><td><code>completed</code>", summary)
                self.assertIn(b"<code>Worker success</code></td><td><code>False</code>", summary)
                for key in status_fields:
                    self.assertIn("Worker " + key.replace("_", " "), summary.decode())
                self.assertIn(b"does not attest that candidate-controlled measurements are honest", summary)
                self.assertEqual(len(json.loads((output / "derived.json").read_bytes())["summaries"]), 256)


if __name__ == "__main__":
    unittest.main()
