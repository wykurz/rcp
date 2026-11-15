"""Operator evidence download: fixed names, bounded reads and digests bound to the retention manifest."""

import io
from pathlib import Path
import tempfile
import unittest

from . import evidence, production_collection as collection
from .common import digest

RUN_KEY = "repo-655073755-run-43075583972"
PREFIX = "production/runs/" + RUN_KEY + "/collection/"


class Missing(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class Bucket:
    """The versioned objects one collected run leaves behind."""

    def __init__(self, files=None):
        files = files or {"durable.json": b"{}", "controller.json": b'{"cleanup":"verified"}',
            "report.json": b'{"qualification_outcome":"failed","benchmark_valid":false}',
            "summary.html": b"<h1>AWS benchmark result</h1>", "status.json": b'{"state":"completed"}',
            "results.zip": b"PK-archive-bytes"}
        self.objects, retained = {}, {}
        for index, (name, data) in enumerate(sorted(files.items())):
            version = "version-" + str(index)
            self.objects[(PREFIX + name, version)] = data
            retained[name] = dict(key=PREFIX + name, version=version, sha256=digest(data), bytes=len(data))
        self.manifest = dict(schema=collection.SCHEMA, run_key=RUN_KEY, retained=retained, benchmark_valid=False)
        self.calls, self.bodies = [], []
        self.store()

    def store(self):
        self.objects[(PREFIX + "manifest.json", None)] = collection.encoded(self.manifest)

    def get_object(self, **request):
        self.calls.append(request)
        assert request["Bucket"] == "bucket"
        data = self.objects.get((request["Key"], request.get("VersionId")))
        if data is None:
            raise Missing()
        body = io.BytesIO(data)
        self.bodies.append(body)
        return dict(Body=body, ContentLength=len(data))


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.target = Path(temporary.name) / "evidence"

    def test_every_retained_file_is_saved_under_its_fixed_name_after_its_digest_matches(self):
        bucket = Bucket()
        manifest, saved = evidence.fetch(bucket, "bucket", RUN_KEY, self.target)
        self.assertEqual(sorted(path.name for path in self.target.iterdir()),
            ["controller.json", "durable.json", "manifest.json", "report.json", "results.zip", "status.json",
             "summary.html"])
        self.assertEqual((self.target / "results.zip").read_bytes(), b"PK-archive-bytes")
        self.assertEqual(saved["results.zip"], digest(b"PK-archive-bytes"))
        self.assertEqual(manifest["run_key"], RUN_KEY)
        # each file is read at the exact version the manifest names, never "the latest"
        self.assertEqual({call.get("VersionId") for call in bucket.calls if not call["Key"].endswith("manifest.json")},
                         {"version-" + str(index) for index in range(6)})
        # the archive is saved as it is: nothing in it is unpacked or run
        self.assertFalse(any(path.is_dir() for path in self.target.iterdir()))
        # evidence is not public: only its owner can read what was saved
        self.assertEqual(self.target.stat().st_mode & 0o777, 0o700)
        self.assertEqual({path.stat().st_mode & 0o777 for path in self.target.iterdir()}, {0o600})
        self.assertTrue(bucket.bodies and all(body.closed for body in bucket.bodies))

    def test_a_run_that_lost_its_worker_output_still_yields_its_report(self):
        bucket = Bucket({"durable.json": b"{}", "controller.json": b"{}", "report.json": b"{}", "summary.html": b"x"})
        _, saved = evidence.fetch(bucket, "bucket", RUN_KEY, self.target)
        self.assertEqual(sorted(saved), ["controller.json", "durable.json", "manifest.json", "report.json",
                                         "summary.html"])

    def test_bytes_that_differ_from_the_manifest_are_refused_and_nothing_is_left_behind(self):
        for change in (lambda bucket: bucket.objects.update({(PREFIX + "results.zip", "version-3"): b"PK-other-bytes!!"}),
                       lambda bucket: bucket.objects.update({(PREFIX + "report.json", "version-2"): b"{}"}),
                       lambda bucket: bucket.manifest["retained"]["status.json"].update(key=PREFIX + "../../other"),
                       lambda bucket: bucket.manifest["retained"].update({"extra.sh": bucket.manifest["retained"]["status.json"]}),
                       lambda bucket: bucket.manifest.update(run_key="repo-655073755-run-1"),
                       lambda bucket: bucket.manifest["retained"].pop("report.json"),
                       lambda bucket: bucket.manifest.update(schema="some-other-manifest"),
                       lambda bucket: bucket.manifest["retained"]["status.json"].update(version="null"),
                       lambda bucket: bucket.objects.update({(PREFIX + "status.json", "version-4"): b""}),
                       lambda bucket: bucket.manifest["retained"]["results.zip"].update(bytes=evidence.LIMITS["results.zip"] + 1)):
            bucket = Bucket()
            change(bucket)
            bucket.store()
            with self.subTest(change=change), self.assertRaises(ValueError):
                evidence.fetch(bucket, "bucket", RUN_KEY, self.target)
            self.assertFalse(self.target.exists())

    def test_the_directory_is_created_only_inside_an_existing_one(self):
        nested = self.target / "deeper" / "evidence"
        bucket = Bucket()
        with self.assertRaises(FileNotFoundError):
            evidence.fetch(bucket, "bucket", RUN_KEY, nested)
        self.assertFalse(self.target.exists())
        self.assertEqual(bucket.calls, [])

    def test_an_existing_directory_is_never_written_into(self):
        self.target.mkdir()
        (self.target / "notes.txt").write_text("mine")
        bucket = Bucket()
        with self.assertRaises(FileExistsError):
            evidence.fetch(bucket, "bucket", RUN_KEY, self.target)
        self.assertEqual([path.name for path in self.target.iterdir()], ["notes.txt"])
        self.assertEqual(bucket.calls, [])

    def test_a_run_without_retained_evidence_says_so(self):
        bucket = Bucket()
        del bucket.objects[(PREFIX + "manifest.json", None)]
        with self.assertRaisesRegex(ValueError, "no retained evidence"):
            evidence.fetch(bucket, "bucket", RUN_KEY, self.target)
        self.assertFalse(self.target.exists())

    def test_only_a_run_key_selects_what_is_read(self):
        for run_key in ("../other", "repo-1-run-2/..", "repo-655073755-run-43075583972/collection", ""):
            with self.subTest(run_key=run_key), self.assertRaisesRegex(ValueError, "run key"):
                evidence.fetch(Bucket(), "bucket", run_key, self.target)

    def test_the_summary_names_the_outcome_and_where_to_look(self):
        bucket = Bucket()
        manifest, saved = evidence.fetch(bucket, "bucket", RUN_KEY, self.target)
        text = evidence.summary(manifest, saved, self.target)
        self.assertIn("qualification: failed", text)
        self.assertIn("results.zip  sha256 " + digest(b"PK-archive-bytes"), text)
        # the manifest's own digest is the retirement digest printed in the run's public record
        self.assertIn("manifest.json  sha256 " + digest(collection.encoded(bucket.manifest)), text)
        self.assertIn("retirement_sha256", text)
        self.assertIn("summary.html", text)
        self.assertIn("untrusted", text)


if __name__ == "__main__":
    unittest.main()
