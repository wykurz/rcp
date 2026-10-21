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
