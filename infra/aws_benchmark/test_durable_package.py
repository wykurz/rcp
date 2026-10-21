"""Packaging must exclude local configuration and reject linked handler sources."""

from pathlib import Path
import tempfile
import unittest

from infra.aws_benchmark.durable_package import SOURCES, source_files


class PackageSourcesTests(unittest.TestCase):
    def test_only_handler_closure_is_packaged_even_with_local_python_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in SOURCES:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("# reviewed source")
            private = root / "infra/aws_benchmark/local_credentials.py"
            private.write_text("# private local configuration")
            self.assertNotIn(private, list(source_files(root)))
            handler = root / "infra/aws_benchmark/durable_aws.py"
            handler.unlink()
            handler.symlink_to(private)
            with self.assertRaisesRegex(ValueError, "ordinary reviewed file"):
                list(source_files(root))

    def test_missing_handler_dependency_fails_instead_of_building_partial_package(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "ordinary reviewed file"):
                list(source_files(Path(directory)))


if __name__ == "__main__":
    unittest.main()
