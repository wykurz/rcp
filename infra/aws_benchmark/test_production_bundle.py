"""Trust-boundary files between read-only download and offline export jobs."""

import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from . import production_bundle as codec, production_publish as publish
from . import test_production_publication as fixtures

REVISION = "c" * 40


class BundleTests(unittest.TestCase):
    setUp = fixtures.PublicationTests.setUp

    def saved(self, temporary):
        bundle = self.reader.download(self.key)
        root = Path(temporary) / "download"
        codec.save(bundle, root, REVISION)
        return bundle, root

    def test_round_trip_revalidates_retired_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            bundle, root = self.saved(temporary)
            loaded = codec.load(root, repository_id="123", reader_revision=REVISION)
            self.assertEqual(loaded, bundle)
            with self.assertRaises(FileExistsError):
                codec.save(bundle, root, REVISION)

    def test_embedded_record_is_consistency_only_external_inventory_detects_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            bundle, root = self.saved(temporary)
            expected = publish.inventory(root)
            path = root / "bundle.json"
            value = json.loads(path.read_bytes())
            value["record"]["revision"] += 1
            path.write_bytes(publish.encoded(value))
            # a self-consistent envelope is not authentication. only the separately
            # trusted predecessor digest detects this otherwise valid local rewrite.
            loaded = codec.load(root, repository_id="123", reader_revision=REVISION)
            self.assertEqual(loaded.record.revision, bundle.record.revision + 1)
            with self.assertRaisesRegex(ValueError, "inventory differs"):
                publish.verify_inventory(root, expected)

    def test_wrong_repository_and_reader_revision_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, root = self.saved(temporary)
            for repository, revision in (("124", REVISION), ("123", "d" * 40)):
                with self.assertRaises(ValueError):
                    codec.load(root, repository_id=repository, reader_revision=revision)

    def test_extra_missing_symlink_and_changed_files_fail(self):
        for mode in ("extra", "missing", "symlink", "changed"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temporary:
                _, root = self.saved(temporary)
                if mode == "extra":
                    (root / "extra").write_text("x")
                elif mode == "missing":
                    (root / "summary.html").unlink()
                elif mode == "symlink":
                    (root / "summary.html").unlink()
                    (root / "summary.html").symlink_to(root / "report.json")
                else:
                    (root / "summary.html").write_text("changed")
                with self.assertRaises((ValueError, OSError)):
                    codec.load(root, repository_id="123", reader_revision=REVISION)

    def test_traversal_and_duplicate_payload_names_fail(self):
        for names in (("../outside",), ("report.json", "report.json")):
            with self.subTest(names=names), tempfile.TemporaryDirectory() as temporary:
                _, root = self.saved(temporary)
                value = json.loads((root / "bundle.json").read_bytes())
                value["payloads"] = list(names)
                (root / "bundle.json").write_text(json.dumps(value))
                with self.assertRaises(ValueError):
                    codec.load(root, repository_id="123", reader_revision=REVISION)

    def test_unknown_entry_stops_directory_enumeration_immediately(self):
        class Entries:
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return False
            def __iter__(self):
                yield SimpleNamespace(name="unexpected")
                raise AssertionError("enumeration should stop at the first unexpected entry")
        with tempfile.TemporaryDirectory() as temporary:
            _, root = self.saved(temporary)
            with patch.object(codec.os, "scandir", return_value=Entries()):
                with self.assertRaisesRegex(ValueError, "unexpected files"):
                    codec.load(root, repository_id="123", reader_revision=REVISION)

    def test_symlink_root_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, root = self.saved(temporary)
            link = Path(temporary) / "alias"
            link.symlink_to(root, target_is_directory=True)
            with self.assertRaises(ValueError):
                codec.load(link, repository_id="123", reader_revision=REVISION)


if __name__ == "__main__":
    unittest.main()
