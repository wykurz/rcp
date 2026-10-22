"""Operator files cannot silently override a bound or identity with duplicate keys."""

from pathlib import Path
import tempfile
import unittest

from infra.aws_benchmark.durable_operator import read_json


class OperatorJsonTests(unittest.TestCase):
    def test_duplicate_budget_and_nested_identity_keys_are_rejected(self):
        for text in ('{"cap_usd":80,"cap_usd":800}',
                     '{"pins":{"ami":"ami-one","ami":"ami-two"}}'):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "input.json"
                path.write_text(text)
                with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
                    read_json(path)

    def test_valid_bounded_files_are_read_and_nonfinite_numbers_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.json"
            path.write_text('{"cap_usd":80}')
            self.assertEqual(read_json(path), {"cap_usd": 80})
            for value in ("NaN", "Infinity", "1e999"):
                path.write_text('{"cap_usd":' + value + '}')
                with self.assertRaises(ValueError):
                    read_json(path)
            path.write_text(" " * 16385)
            with self.assertRaisesRegex(ValueError, "exceeds bound"):
                read_json(path)
