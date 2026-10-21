"""Capacity tuning changes admission estimates, never the bounded fixture contract."""

import unittest

from benchmarks import capacity


class CapacityPolicyTests(unittest.TestCase):
    case = dict(id="small", mode="fresh", directory_widths=[1], files_per_leaf=1,
                file_size_bytes=3)

    def test_independent_margins_round_up_without_changing_fixture_counts(self):
        base = capacity.requirements(self.case, 4096, byte_percent=0, inode_percent=0,
                                     byte_reserve=0, inode_reserve=0)
        changed = capacity.requirements(self.case, 4096, byte_percent=1, inode_percent=1,
                                        byte_reserve=7, inode_reserve=9)
        self.assertEqual(changed["required_bytes"], base["required_bytes"] +
                         (base["required_bytes"] + 99) // 100 + 7)
        self.assertEqual(changed["required_inodes"], base["required_inodes"] + 1 + 9)
        for field in ("logical_bytes", "data_block_bytes", "fixture_inodes"):
            self.assertEqual(changed[field], base[field])

    def test_defaults_and_invalid_options_fail_before_inventory(self):
        self.assertEqual(capacity.policy(byte_percent=None), capacity.DEFAULT_POLICY)
        for key in capacity.DEFAULT_POLICY:
            for value in (-1, True, 1.5, 2 ** 63):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    capacity.policy(**{key: value})
        with self.assertRaises(ValueError):
            capacity.policy(unknown=1)
        with self.assertRaises(ValueError):
            capacity.requirements(dict(self.case, file_size_bytes=capacity.MAX_FILE_BYTES + 1),
                                  4096, byte_percent=0, inode_percent=0,
                                  byte_reserve=0, inode_reserve=0)


class StructuralAdmissionTests(unittest.TestCase):
    def test_impossible_tree_shapes_fail_before_count_or_storage_work(self):
        from unittest import mock
        for shape in (dict(directory_widths=[5000], files_per_leaf=1),
                      dict(directory_widths=[1], files_per_leaf=4097),
                      dict(directory_widths=[1] * 129, files_per_leaf=1),
                      dict(directory_widths=[4096], files_per_directory=1)):
            with self.subTest(shape=shape), mock.patch.object(capacity, "expected_counts") as counts:
                with self.assertRaisesRegex(ValueError, "verification depth or directory entry"):
                    capacity.validate_contract(dict(id="impossible", file_size_bytes=100_000_000, **shape))
                counts.assert_not_called()

    def test_verifier_boundaries_are_admitted_without_generating_fixtures(self):
        for shape in (dict(directory_widths=[4096], files_per_leaf=1),
                      dict(directory_widths=[1], files_per_leaf=4096),
                      dict(directory_widths=[1] * 128, files_per_leaf=1),
                      dict(directory_widths=[4095], files_per_directory=1)):
            with self.subTest(shape=shape):
                counts = capacity.validate_contract(dict(id="boundary", file_size_bytes=1, **shape))
                self.assertGreater(counts["files"], 0)
