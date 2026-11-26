"""Linux mountstats command windows on the NFS destination of an rlink trial."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks import nfs_rpc


MOUNT = "/bench/nfs destination\\copy"
ESCAPED = r"/bench/nfs\040destination\134copy"
DEVICE = "127.0.0.1:/exports/destination"
INFO = f"42 17 0:58 / {ESCAPED} rw,relatime - nfs4 {DEVICE} rw,vers=4.2,proto=tcp\n"
BEFORE = """  NULL: 0 0 0 0 0 0 0 0 0
  LOOKUP: 10 11 0 2400 4500 5 57 60 0
  GETATTR: 2 2 0 210 320 0 10 10 0
  READDIR: 0 0 0 0 0 0 0 0 0
  LINK: 3 3 0 720 1250 1 16 18 0
  SEQUENCE: 1 1 0 145 310 0 5 5 0
"""
AFTER = """  NULL: 0 0 0 0 0 0 0 0 0
  LOOKUP: 18 22 0 5200 9000 15 110 125 0
  GETATTR: 2 2 0 210 320 0 10 10 0
  READDIR: 2 2 0 320 1500 1 11 12 0
  LINK: 7 7 0 1660 2750 2 37 40 0
  SEQUENCE: 3 4 0 480 910 2 22 25 0
"""


def section(rows, *, target=ESCAPED, device=DEVICE, age=80, version="4.2"):
    return (f"device {device} mounted on {target} with fstype nfs4 statvers=1.1\n"
            f"\topts: rw,vers={version},rsize=1048576,proto=tcp\n\tage: {age}\n"
            "\tRPC iostats version: 1.1  p/v: 100003/4 (nfs)\n\txprt: tcp 731 0 1 0 0 18 18 0 0 0\n"
            "\tper-op statistics\n" + rows)


def counters(completed, transmissions, retransmissions, rtt_milliseconds):
    return dict(completed=completed, transmissions=transmissions, retransmissions=retransmissions,
                rtt_milliseconds=rtt_milliseconds)


class MountStatsTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.proc = Path(temp.name)

    def point(self, data, info=INFO, mount=MOUNT):
        (self.proc / "mountinfo").write_text(info)
        (self.proc / "mountstats").write_text(data)
        return nfs_rpc.snapshot(mount, proc=self.proc)

    def test_only_the_named_mount_contributes_and_total_includes_other_procedures(self):
        first = section(AFTER, target="/bench/nfs-source")
        last = section(BEFORE, target="/bench/nfs-destination-old")
        before = self.point(first + section(BEFORE) + last, mount=Path(MOUNT))
        after = self.point(section(BEFORE, target="/bench/nfs-source") + section(AFTER, age=95) + last)
        self.assertEqual(nfs_rpc.difference(before, after), dict(status="available",
            operations=dict(LOOKUP=counters(8, 11, 3, 53), GETATTR=counters(0, 0, 0, 0),
                            READDIR=counters(2, 2, 0, 11), LINK=counters(4, 4, 0, 21)),
            total=counters(16, 20, 4, 102)))

    def test_an_unreported_procedure_is_distinct_from_one_with_no_activity(self):
        rows = BEFORE.replace("  READDIR: 0 0 0 0 0 0 0 0 0\n", "")
        point = self.point(section(rows))
        delta = nfs_rpc.difference(point, point)
        self.assertNotIn("READDIR", delta["operations"])
        self.assertEqual(delta["operations"]["LOOKUP"], counters(0, 0, 0, 0))
        self.assertEqual(delta["total"], counters(0, 0, 0, 0))
        # a total cannot span two different sets of reported procedures
        self.assertEqual(nfs_rpc.difference(point, self.point(section(BEFORE))), dict(status="unavailable"))

    def test_a_destination_below_a_mount_uses_its_deepest_mount_at_both_boundaries(self):
        nested = ESCAPED + "/nested"
        child_info = f"77 42 0:79 / {nested} rw - nfs4 {DEVICE} rw,vers=4.2\n"
        inventory = INFO + child_info
        root = MOUNT + "/nested/bench"
        before = self.point(section(AFTER) + section(BEFORE, target=nested), inventory, root)
        after = self.point(section(BEFORE) + section(AFTER, target=nested), inventory, root)
        self.assertEqual(nfs_rpc.difference(before, after).get("total"), counters(16, 20, 4, 102))
        # the parent still exists after the child mount disappears
        moved = self.point(section(AFTER), INFO, root)
        self.assertEqual(nfs_rpc.difference(after, moved), dict(status="mount_changed"))
        # a non-NFS child also shadows its NFS parent; a sibling with a shared string prefix does not
        covered = child_info.replace("nfs4 " + DEVICE, "tmpfs tmpfs")
        self.assertIsNone(self.point(section(AFTER), INFO + covered, root))
        sibling = self.point(section(AFTER) + section(BEFORE, target=nested), inventory, MOUNT + "/nested-other")
        self.assertEqual(nfs_rpc.difference(self.point(section(BEFORE)), sibling).get("total"), counters(16, 20, 4, 102))

    def test_a_parent_overmount_can_hide_an_nfs_mount_still_listed_in_proc(self):
        # both mounts have parent 17; the NFS mount is hidden below the tmpfs overmount
        cover = "80 17 0:99 / /bench rw - tmpfs tmpfs rw\n"
        self.assertIsNone(self.point(section(AFTER), INFO + cover))

    def test_a_replaced_mount_never_contributes_a_delta_even_if_its_counters_increased(self):
        before = self.point(section(BEFORE))
        for data, info in ((section(AFTER), INFO.replace("42 17", "43 17")),
                           (section(AFTER, device="127.0.0.1:/new"), INFO.replace(DEVICE, "127.0.0.1:/new")),
                           (section(AFTER, age=2), INFO)):
            with self.subTest(info=info, data=data):
                self.assertEqual(nfs_rpc.difference(before, self.point(data, info)), dict(status="mount_changed"))

    def test_a_configured_root_alias_can_move_while_the_old_mount_remains(self):
        a, b, alias = (self.proc / name for name in ("a", "b", "destination"))
        for mount in (a, b):
            (mount / "bench").mkdir(parents=True)
        alias.symlink_to(a / "bench")
        info = INFO.replace(ESCAPED, str(a)) + INFO.replace(ESCAPED, str(b)).replace("42 17", "43 17")
        stats = section(BEFORE, target=a) + section(AFTER, target=b)
        self.point(stats, info, a)
        prepared = nfs_rpc.prepare_root(alias, proc=self.proc)
        before = nfs_rpc.snapshot(prepared, proc=self.proc)
        # any requests made by resolving the configured root after rlink fall outside its counter window
        realpath = os.path.realpath
        def resolve(path, **options):
            (self.proc / "mountstats").write_text(section(AFTER, target=a) + section(AFTER, target=b))
            return realpath(path, **options)
        with patch("os.path.realpath", side_effect=resolve):
            same = nfs_rpc.snapshot(alias, proc=self.proc, resolve_after=True)
        self.assertEqual(nfs_rpc.difference(before, same)["total"], counters(0, 0, 0, 0))
        alias.unlink()
        alias.symlink_to(b / "bench")
        after = nfs_rpc.snapshot(alias, proc=self.proc, resolve_after=True)
        self.assertEqual(nfs_rpc.difference(before, after), dict(status="mount_changed"))

    def test_a_literal_mountpoint_needs_no_path_lookup_on_the_nfs_client(self):
        self.point(section(BEFORE))
        with patch("os.path.realpath", side_effect=AssertionError("unnecessary root lookup")):
            before = nfs_rpc.snapshot(nfs_rpc.prepare_root(MOUNT, proc=self.proc), proc=self.proc)
            after = nfs_rpc.snapshot(MOUNT, proc=self.proc, resolve_after=True)
        self.assertEqual(nfs_rpc.difference(before, after)["status"], "available")

    def test_a_reset_in_one_procedure_cannot_be_hidden_by_increases_in_other_procedures(self):
        before = self.point(section(BEFORE))
        for old, new in (("3 4 0 480 910 2 22 25 0", "0 0 0 0 0 0 0 0 0"),
                         ("18 22 0 5200 9000 15 110", "18 22 0 5200 9000 15 50"),
                         ("18 22 0", "18 18 0")):
            with self.subTest(change=new):
                after = self.point(section(AFTER.replace(old, new)))
                self.assertEqual(nfs_rpc.difference(before, after), dict(status="counter_reset"))

    def test_unreadable_ambiguous_and_incomplete_observations_remain_unavailable(self):
        before = self.point(section(BEFORE))
        missing = section(BEFORE, target="/bench/nfs-different")
        malformed = (section(BEFORE).replace("100003/4", "100003/3"), section(BEFORE, version="4.1"),
                     section(""), section(BEFORE + "  GETATTR: 0 0 0 0 0 0 0 0 0\n"),
                     section(BEFORE.replace("10 11 0", "10 11 secret")), section(BEFORE + "  WRITE: 1 1 0\n"),
                     section(BEFORE + "  WRITE: -1 1 0 0 0 0 5 5\n"), section(BEFORE) * 2, missing,
                     section(BEFORE) + "x" * nfs_rpc.READ_BYTES)
        for data, info in [*((data, INFO) for data in malformed), (section(BEFORE), ""),
                           (section(BEFORE), INFO * 2), (section(BEFORE), INFO.replace(DEVICE, "other:/export"))]:
            with self.subTest(size=len(data), info=info):
                self.assertEqual(nfs_rpc.difference(before, self.point(data, info)), dict(status="unavailable"))
        (self.proc / "mountstats").unlink()
        self.assertEqual(nfs_rpc.difference(nfs_rpc.snapshot(MOUNT, proc=self.proc), before),
                         dict(status="unavailable"))

    def test_an_interruption_during_a_read_still_cancels_the_run(self):
        with patch.object(Path, "open", side_effect=InterruptedError("run stopped")), self.assertRaises(InterruptedError):
            nfs_rpc.snapshot(MOUNT, proc=self.proc)


if __name__ == "__main__":
    unittest.main()
