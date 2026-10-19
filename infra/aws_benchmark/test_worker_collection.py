"""Real supervisor archives retain good evidence when individual paths are omitted."""

import copy
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import Mock
import zipfile

from infra.aws_benchmark import common, results, worker
from infra.aws_benchmark.test_results import binding, bundle


class Mailbox:
    def __init__(self):
        self.objects = {}

    def put_object(self, *, Key, Body, **kwargs):
        self.objects[Key] = Body


class WorkerCollectionTests(unittest.TestCase):
    def test_individual_unsupported_paths_preserve_real_publish_and_ingest_evidence(self):
        paths = {
            "long-component": "x" * 201,
            "deep": "results/logs/case/variant/1/extra/bad.log",
            "long-path": "/".join(["x" * 199] * 6),
            "backslash": "back\\slash.log",
            "non-utf8": os.fsdecode(b"invalid-\xff.log"),
            "symlink": "link.log",
            "socket": "leftover.sock",
        }
        for label, name in paths.items():
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, output = root / "input", root / "worker-output"
                expected, _ = bundle(source, binding())
                with zipfile.ZipFile(source / "results.zip") as original:
                    metadata = json.loads(original.read("worker.json"))
                    raw = original.read(results.RESULT)
                (output / "results").mkdir(parents=True)
                (output / "results/results.json").write_bytes(raw)
                (output / "keep.log").write_bytes(b"retained diagnostic")
                forbidden = output / name
                forbidden.parent.mkdir(parents=True, exist_ok=True)
                if label == "symlink":
                    target = root / "outside"
                    target.write_bytes(b"outside data must not enter archive")
                    forbidden.symlink_to(target)
                elif label == "socket":
                    with socket.socket(socket.AF_UNIX) as endpoint:
                        endpoint.bind(str(forbidden))
                else:
                    forbidden.write_bytes(b"omitted diagnostic")
                mailbox = Mailbox()
                status = common.completed_status(expected["run"], True)
                worker.publish_report(mailbox, "test-bucket", output, copy.deepcopy(metadata), status, Mock())
                archive = mailbox.objects[common.RESULTS_KEY]
                terminal = mailbox.objects[common.STATUS_KEY]
                self.assertTrue(json.loads(terminal)["success"])
                self.assertNotIn("collection_error", json.loads(terminal))
                self.assertEqual(json.loads(terminal)["sha256"], common.digest(archive))
                receipt = json.loads((source / "controller.json").read_bytes())
                receipt.update(archive_sha256=common.digest(archive), status_sha256=common.digest(terminal))
                (source / "results.zip").write_bytes(archive)
                (source / "status.json").write_bytes(terminal)
                (source / "controller.json").write_bytes(results.encoded(receipt))
                evidence = results.ingest(source, root / "report", expected, "success")
                self.assertEqual(evidence["errors"], [])
                self.assertEqual(evidence["members"]["benchmark/keep.log"], common.digest(b"retained diagnostic"))
                self.assertEqual(evidence["members"][results.RESULT], common.digest(raw))
                self.assertEqual(set(evidence["members"]), {"worker.json", results.RESULT, "benchmark/keep.log"})
                self.assertEqual(evidence["worker"]["harness_revision"], metadata["harness_revision"])
                self.assertEqual(evidence["worker"]["request"], metadata["request"])
                self.assertEqual(evidence["worker"]["omitted_or_truncated_count"], 1)
                omissions = evidence["worker"]["omitted_or_truncated"]
                self.assertEqual(len(omissions), 1)
                self.assertEqual(omissions[0]["archived_bytes"], 0)
                expected_omission = "benchmark/" + (str(forbidden.parent.relative_to(output)) if label == "deep" else name)
                expected_omission = expected_omission.encode("utf-8", errors="backslashreplace").decode("utf-8")[:512]
                self.assertEqual(omissions[0]["name"], expected_omission)
                self.assertTrue((root / "report/derived.json").exists())
                self.assertEqual((root / "report/raw/results.zip").read_bytes(), archive)
                self.assertNotIn(b"outside data must not enter archive", archive)


if __name__ == "__main__":
    unittest.main()
