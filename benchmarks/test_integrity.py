import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from benchmarks import report, run


class IntegrityTests(unittest.TestCase):
    def test_nonfinite_timeout_is_rejected_before_loading_inputs(self):
        for timeout in ("nan", "inf", "-inf"):
            with self.subTest(timeout=timeout), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source = root / "source"
                destination = root / "destination"
                source.mkdir()
                destination.mkdir()
                output = root / "out"
                with self.assertRaisesRegex(ValueError, "timeout.*finite"):
                    run.main([f"--timeout={timeout}", "--manifest", str(root / "missing.json"), "--source-root", str(source), "--destination-root", str(destination), "--output", str(output)])
                self.assertEqual(list(source.iterdir()), [])
                self.assertEqual(list(destination.iterdir()), [])
                result = report.parse_result((output / "results.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["trials"], [])

    def test_final_copy_source_damage_fails_case_and_keeps_prior_summaries(self):
        for damage in ("change", "remove"):
            with self.subTest(damage=damage), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                binary = root / "bin"
                binary.mkdir()
                filegen = binary / "filegen"
                filegen.write_text("#!/usr/bin/env python3\nimport pathlib,sys\nif '--version' in sys.argv: print('filegen 1'); sys.exit(0)\np=pathlib.Path(sys.argv[1])/'filegen'/'dir'; p.mkdir(parents=True); (p/'file').write_bytes(b'x')\n")
                filegen.chmod(0o755)
                copier = binary / "rsync"
                copier.write_text("#!/usr/bin/env python3\nimport pathlib,shutil,sys\nif '--version' in sys.argv: print('rsync 1'); sys.exit(0)\nsource=pathlib.Path(sys.argv[-2]); destination=pathlib.Path(sys.argv[-1])\nshutil.copytree(source,destination,dirs_exist_ok=True)\nif source.parent.name == 'later' and destination.name == '2':\n" + (" (source/'dir'/'file').write_bytes(b'y')\n" if damage == "change" else " shutil.rmtree(source)\n"))
                copier.chmod(0o755)
                manifest = root / "manifest.json"
                manifest.write_text(json.dumps({"schema_version": 1, "cases": [{"id": case, "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1} for case in ("first", "later")], "variants": [{"id": "copy", "tool": "rsync", "args": ["-a"], "processes": 1}]}))
                output = root / "out"
                with mock.patch.object(run.shutil, "which", return_value=str(copier)), self.assertRaisesRegex(RuntimeError, "source.*changed.*later"):
                    run.main(["--manifest", str(manifest), "--case", "first", "--case", "later", "--variant", "copy", "--bin-dir", str(binary), "--cache", "uncontrolled", "--repetitions", "2", "--source-root", str(root), "--destination-root", str(root), "--output", str(output)])
                result = report.parse_result((output / "results.json").read_text())
                self.assertEqual(result["status"], "failed")
                self.assertEqual([summary["case_id"] for summary in result["summaries"]], ["first"])
                self.assertEqual([trial["validation"]["ok"] for trial in result["trials"]], [True] * 4)
                scratch = Path(result["context"]["failure_artifacts"]["source_scratch"])
                self.assertFalse((scratch / "first").exists())
                self.assertTrue((scratch / "later").is_dir())
                if damage == "change":
                    self.assertEqual((scratch / "later" / "filegen" / "dir" / "file").read_bytes(), b"y")
                self.assertTrue((output / "logs" / "later" / "copy" / "2" / "0.stdout.log").is_file())


if __name__ == "__main__":
    unittest.main()
