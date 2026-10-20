"""No block devices or commands: synthetic inventory/sysfs and a recording executor."""

from contextlib import ExitStack
import json
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from infra.aws_benchmark import large_storage as storage


def inventory():
    return dict(blockdevices=[
        dict(path=f"/dev/nvme{index}n1", type="disk", size=900_000_000_000,
             model=storage.MODEL, mountpoints=[None], fstype=None, ro=False,
             serial=f"disk{index}", **{"maj:min": f"259:{index}"})
        for index in range(1, 5)
    ] + [dict(path="/dev/nvme0n1", model=storage.EBS_MODEL, mountpoints=["/"])])


class SelectionTests(unittest.TestCase):
    def test_only_exactly_four_blank_unique_instance_store_disks_are_selected(self):
        self.assertEqual([disk["serial"] for disk in storage.select(inventory())],
                         ["disk1", "disk2", "disk3", "disk4"])
        changes = (dict(children=[{}]), dict(mountpoints=["/"]), dict(mountpoints=None),
                   dict(fstype="ext4"), dict(ro=True), dict(type="part"), dict(size=1),
                   dict(size=True), dict(serial=""), dict(path="/dev/sda"), dict(model="unknown"))
        for change in changes:
            tree = inventory()
            tree["blockdevices"][0].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                storage.select(tree)
        for field in ("path", "serial", "maj:min"):
            tree = inventory()
            tree["blockdevices"][1][field] = tree["blockdevices"][0][field]
            with self.subTest(duplicate=field), self.assertRaises(ValueError):
                storage.select(tree)
        for count in (3, 5):
            tree = inventory()
            tree["blockdevices"] = tree["blockdevices"][:3] if count == 3 else tree["blockdevices"] + [tree["blockdevices"][0]]
            with self.assertRaises(ValueError):
                storage.select(tree)

    def test_open_pins_a_nofollow_block_descriptor_and_closes_it_on_failure(self):
        with mock.patch.object(storage.os, "open", return_value=23) as opened, \
                mock.patch.object(storage.os, "close") as closed, \
                mock.patch.object(storage.os, "fstat", return_value=SimpleNamespace(
                    st_mode=stat.S_IFREG, st_rdev=os.makedev(259, 1))):
            with self.assertRaisesRegex(ValueError, "block"), ExitStack() as stack:
                storage.open_block("/dev/nvme1n1", stack)
            self.assertEqual(opened.call_args.args[1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            closed.assert_called_once_with(23)


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.data = self.root / "data"
        self.budget = object()
        self.tree = inventory()
        self.calls = []
        self.signature_fd = None
        self.fail_command = None
        self.capacity_error = False
        self.case = dict(directory_widths=[10], files_per_leaf=100, file_size_bytes=1_000_000_000)
        self.write("proc/swaps", "Filename\tType\tSize\tUsed\tPriority\n")
        self.write("proc/self/mountinfo", "1 0 259:0 / / rw - ext4 /dev/nvme0n1 rw\n")
        for index in range(1, 5):
            prefix = f"sys/dev/block/259:{index}"
            self.write(prefix + "/device/model", storage.MODEL)
            self.write(prefix + "/device/serial", f"disk{index}")
            self.write(prefix + "/size", str(900_000_000_000 // 512))
            self.write(prefix + "/ro", "0")
            (self.root / prefix / "holders").mkdir()
        for index in range(2):
            prefix = f"sys/dev/block/9:{index}"
            for name, value in {"md/level": "raid0", "md/raid_disks": "2", "md/metadata_version": "1.2",
                                "md/chunk_size": str(storage.CHUNK_BYTES), "ro": "0",
                                "size": str(1_799_000_000_000 // 512)}.items():
                self.write(prefix + "/" + name, value)
            for member in range(1 + index * 2, 3 + index * 2):
                self.write(prefix + f"/slaves/nvme{member}n1/dev", f"259:{member}")
            (self.root / prefix / "holders").mkdir()
        self.stack = self.enterContext(ExitStack())
        def mapped(value):
            text = str(value)
            if text.startswith(("/sys/", "/proc/", "/dev/")):
                return self.root / text.lstrip("/")
            return Path(value)
        self.stack.enter_context(mock.patch.object(storage, "Path", side_effect=mapped))
        self.stack.enter_context(mock.patch.object(storage, "open_block", side_effect=self.open_block))
        self.stack.enter_context(mock.patch.object(storage.os, "chown"))
        self.capacity = self.stack.enter_context(mock.patch.object(storage.capacity, "check_pair", return_value={"checked": True}))
        original_stat = Path.stat
        def mounted_stat(path, *args, **kwargs):
            if path in [self.data / side for side in storage.SIDES]:
                return SimpleNamespace(st_dev=os.makedev(9, storage.SIDES.index(path.name)))
            return original_stat(path, *args, **kwargs)
        self.stack.enter_context(mock.patch.object(Path, "stat", mounted_stat))

    def write(self, name, text):
        target = self.root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def open_block(self, path, stack):
        if str(path).startswith("/dev/nvme"):
            index = int(Path(path).name[4:-2])
            return 10 + index, f"259:{index}"
        index = storage.SIDES.index(Path(path).name.removeprefix("rcp-"))
        return 20 + index, f"9:{index}"

    def execute(self, argv, budget, **kwargs):
        self.assertIs(budget, self.budget)
        self.calls.append((argv, kwargs))
        if self.fail_command and self.fail_command(argv):
            raise TimeoutError("injected command/deadline failure")
        if argv[0] == "lsblk":
            return SimpleNamespace(stdout=json.dumps(self.tree))
        if argv[0] == "wipefs":
            fd = int(argv[-1].rsplit("/", 1)[1])
            self.assertEqual(kwargs["pass_fds"], (fd,))
            signatures = [dict(type="ext4", offset="0x438")] if fd == self.signature_fd else []
            return SimpleNamespace(stdout=json.dumps(dict(signatures=signatures)))
        if argv[0] == "mdadm":
            self.assertNotIn("--force", argv)
            path = Path(argv[2])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
            self.assertEqual(argv[-2:], [f"/proc/self/fd/{fd}" for fd in kwargs["pass_fds"]])
        if argv[0] == "blkid":
            return SimpleNamespace(stdout="ext4\n")
        return SimpleNamespace(stdout="")

    def prepare(self):
        return storage.prepare(self.data, self.case, self.budget, 1000, 1000, self.execute)

    def commands(self):
        return [argv[0] for argv, _ in self.calls]

    def test_all_member_checks_precede_creation_and_both_arrays_precede_formatting(self):
        result = self.prepare()
        commands = self.commands()
        self.assertEqual(commands[:5], ["lsblk"] + ["wipefs"] * 4)
        self.assertEqual(commands[5:9], ["mdadm", "wipefs", "mdadm", "wipefs"])
        self.assertEqual(commands.count("mkfs.ext4"), 2)
        self.capacity.assert_called_once_with(self.data / "source", self.data / "destination", self.case)
        self.assertEqual(result["arrays"]["source"]["members"], ["/dev/nvme1n1", "/dev/nvme2n1"])
        self.assertEqual(result["arrays"]["destination"]["members"], ["/dev/nvme3n1", "/dev/nvme4n1"])
        for argv, kwargs in self.calls:
            if argv[0] == "mkfs.ext4":
                self.assertNotIn("-F", argv)
                self.assertIn("16000000", argv)
                self.assertEqual(argv[-1], f"/proc/self/fd/{kwargs['pass_fds'][0]}")
            if argv[0] == "mount" and "ext4" in argv:
                self.assertIn("--no-canonicalize", argv)
                self.assertIn("--internal-only", argv)
                self.assertEqual(argv[-2], f"/proc/self/fd/{kwargs['pass_fds'][0]}")
        self.assertEqual(result["layout"], "two-disjoint-raid0-ext4-v1")

    def test_last_disk_signature_refuses_all_mutating_commands(self):
        self.signature_fd = 14
        with self.assertRaisesRegex(ValueError, "signature"):
            self.prepare()
        self.assertNotIn("mdadm", self.commands())
        self.assertNotIn("mkfs.ext4", self.commands())

    def test_root_mount_swap_holders_and_changed_identity_refuse_creation(self):
        for defect in ("mount", "swap", "holder", "serial", "partition", "size"):
            with self.subTest(defect=defect):
                # each defect is removed before checking the next
                target = None
                old = None
                if defect == "mount":
                    target = self.root / "proc/self/mountinfo"
                    old = target.read_text()
                    target.write_text("1 0 259:4 / / rw - ext4 /dev/nvme4n1 rw\n")
                elif defect == "swap":
                    target = self.root / "proc/swaps"
                    old = target.read_text()
                    target.write_text(old + "/swap file 1 0 -2\n")
                elif defect in ("holder", "partition"):
                    target = self.root / ("sys/dev/block/259:4/holders/md0" if defect == "holder" else "sys/dev/block/259:4/partition")
                    target.touch()
                else:
                    target = self.root / ("sys/dev/block/259:4/device/serial" if defect == "serial" else "sys/dev/block/259:4/size")
                    old = target.read_text()
                    target.write_text("changed" if defect == "serial" else "1")
                try:
                    with self.assertRaises(ValueError):
                        self.prepare()
                    self.assertNotIn("mdadm", self.commands())
                finally:
                    target.write_text(old) if old is not None else target.unlink()
                    self.calls.clear()

    def test_wrong_second_array_members_refuse_all_formatting(self):
        self.write("sys/dev/block/9:1/slaves/nvme3n1/dev", "259:1")
        with self.assertRaisesRegex(ValueError, "members"):
            self.prepare()
        self.assertEqual(self.commands().count("mdadm"), 2)
        self.assertNotIn("mkfs.ext4", self.commands())

    def test_format_deadline_and_capacity_failures_never_prepare_output(self):
        self.fail_command = lambda argv: argv[0] == "mkfs.ext4"
        with self.assertRaises(TimeoutError):
            self.prepare()
        self.capacity.assert_not_called()
        self.assertFalse((self.data / "output").exists())

    def test_postformat_capacity_failure_aborts_before_output(self):
        self.capacity.side_effect = ValueError("insufficient free bytes or inodes")
        with self.assertRaisesRegex(ValueError, "insufficient"):
            self.prepare()
        self.assertEqual(self.commands().count("mkfs.ext4"), 2)
        self.assertFalse((self.data / "output").exists())

    def test_existing_target_is_never_reused(self):
        target = self.root / "dev/md/rcp-source"
        target.parent.mkdir(parents=True)
        target.symlink_to("/nonexistent")
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.prepare()
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
