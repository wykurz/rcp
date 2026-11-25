"""No server, mount or qdisc: a recording executor and a synthetic /proc, /sys and /run."""

from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest import mock

from infra.aws_benchmark import other_storage as storage


class Budget:
    def left(self, reserve=0):
        return 3600


def mountinfo(data, *, kind="nfs4", sources=None, also=()):
    """The mount inventory after a preparation that went as intended, and `also` these mount points."""
    sources = sources or {side: f"127.0.0.1:{data / side}" for side in storage.SIDES}
    lines = [f"{90 + index} 30 0:{50 + index} / {data / storage.NFS_MOUNTS[side]} rw,relatime shared:1 - "
             f"{kind} {sources[side]} rw,vers=4.2,rsize=1048576,wsize=1048576,hard,proto=tcp\n"
             for index, side in enumerate(storage.SIDES)]
    lines += [f"{70 + index} 30 0:{40 + index} / {target} rw,relatime - pseudo none rw\n"
              for index, target in enumerate(also)]
    return "".join(lines)


def daemon(command):
    """What a transient unit runs: the program and its arguments."""
    start = next(index for index, word in enumerate(command) if word.startswith("/usr/sbin/"))
    return command[start:]


class Fixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.data = self.root / "mnt"
        for side in storage.SIDES:
            (self.data / side).mkdir(parents=True)
        self.state = self.root / "run/rcp-benchmark"
        self.state.mkdir(parents=True)
        # what the kernel shows: the switch for the same-host shortcut, one entry per mounted
        # filesystem, and the server's own files
        self.kernel = storage.Kernel(localio=self.root / "sys/module/nfs/parameters/localio_enabled",
                                     clients=self.root / "sys/fs/nfs", server=self.root / "proc/fs/nfsd",
                                     pipes=self.root / "run/rpc_pipefs")
        self.kernel.localio.parent.mkdir(parents=True)
        self.kernel.localio.write_text("Y\n")
        for name in ("0:50", "0:51"):
            (self.kernel.clients / name).mkdir(parents=True)
            (self.kernel.clients / name / "localio").write_text("0\n")
        self.kernel.server.mkdir(parents=True)
        (self.kernel.server / "threads").write_text("0\n")
        (self.kernel.server / "v4_end_grace").write_text("Y\n")
        self.commands = []
        # units that systemd would report as not running
        self.dead = set()
        self.inventory = mountinfo(self.data)

    def execute(self, argv, budget, **kwargs):
        self.commands.append(list(argv))
        if argv[:3] == ["systemctl", "is-active", "--quiet"] and argv[3] in self.dead:
            raise subprocess.CalledProcessError(3, argv)

    def ran(self, *programs):
        """The recorded commands of these programs."""
        return [command for command in self.commands if command[0] in programs]

    def prepare(self, *, again=True, execute=None, **changes):
        """Prepare the NFS mounts; `again` first takes away what an earlier call made."""
        if again:
            for path in (*(self.data / name for name in storage.NFS_MOUNTS.values()), self.state / "nfsdcld"):
                if path.is_dir():
                    path.rmdir()
            self.commands.clear()
        places = dict(kernel=self.kernel, inventory=lambda: self.inventory, round_trip=lambda: 0.0052,
                      pause=lambda seconds: None)
        places.update(changes)
        return storage.prepare_nfs(self.data, self.state, Budget(), execute or self.execute, **places)


class NfsTests(Fixture):
    def test_the_arrays_are_served_on_the_loopback_and_mounted_back_with_a_delay(self):
        result = self.prepare()
        programs = [command[0] for command in self.commands]
        # the server is up before anything is mounted, and so is the delay
        self.assertLess(programs.index("rpc.nfsd"), programs.index("mount.nfs"))
        self.assertLess(programs.index("tc"), programs.index("mount.nfs"))
        self.assertLess(programs.index("modprobe"), programs.index("rpc.nfsd"))
        self.assertEqual(self.ran("rpc.nfsd"), [["rpc.nfsd", "--host", "127.0.0.1", "--no-nfs-version", "3", "256"]])
        self.assertEqual(self.ran("exportfs"),
                         [["exportfs", "-o", "rw,async,no_subtree_check", f"127.0.0.1:{self.data / side}"]
                          for side in storage.SIDES])
        self.assertEqual(self.ran("tc"), [["tc", "qdisc", "add", "dev", "lo", "root", "netem", "delay", "2500us",
                                           "limit", "100000"]])
        self.assertEqual(self.ran("mount.nfs"),
                         [["mount.nfs", f"127.0.0.1:{self.data / side}", str(self.data / storage.NFS_MOUNTS[side]),
                           "-o", "vers=4.2"] for side in storage.SIDES])
        for side in storage.SIDES:
            self.assertTrue((self.data / storage.NFS_MOUNTS[side]).is_dir())
        self.assertEqual(result["layout"], "nfs-loopback-v1")
        self.assertEqual((result["version"], result["threads"], result["delay_microseconds"]), ("4.2", 256, 2500))
        self.assertEqual(result["round_trip_seconds"], 0.0052)
        self.assertEqual(result["mounts"], {side: str(self.data / storage.NFS_MOUNTS[side]) for side in storage.SIDES})

    def test_the_same_host_shortcut_is_switched_off_before_anything_is_mounted(self):
        # with it on, file data passes the loopback by and no delay reaches it
        seen = []

        def execute(argv, budget, **kwargs):
            if argv[0] == "mount.nfs":
                seen.append(self.kernel.localio.read_text())
            self.execute(argv, budget, **kwargs)
        self.prepare(execute=execute)
        self.assertEqual(seen, ["N\n", "N\n"])

    def test_a_kernel_without_the_shortcut_needs_no_switch(self):
        self.kernel.localio.unlink()
        for name in ("0:50", "0:51"):
            (self.kernel.clients / name / "localio").unlink()
        self.assertEqual(self.prepare()["layout"], "nfs-loopback-v1")
        self.assertFalse(self.kernel.localio.exists())

    def test_the_kernel_filesystems_of_the_server_are_mounted_where_its_programs_look(self):
        self.prepare()
        # Ubuntu's nfs.conf points the daemons at /run/rpc_pipefs, and nothing else mounts it
        # once the packaged units are masked
        mounts = [["mount", "-t", "nfsd", "nfsd", str(self.kernel.server)],
                  ["mount", "-t", "rpc_pipefs", "sunrpc", str(self.kernel.pipes)]]
        self.assertEqual(self.ran("mount"), mounts)
        self.assertTrue(self.kernel.pipes.is_dir())
        self.assertLess(self.commands.index(mounts[1]), self.commands.index(self.ran("systemd-run")[0]))
        # what is mounted already stays as it is
        self.inventory = mountinfo(self.data, also=(self.kernel.server, self.kernel.pipes))
        self.prepare()
        self.assertEqual(self.ran("mount"), [])

    def test_delegations_are_off_and_the_client_tracker_runs_before_the_server(self):
        self.prepare()
        self.assertIn(["sysctl", "--quiet", "--write", "fs.leases-enable=0"], self.commands)
        tracking = self.state / "nfsdcld"
        # mountd opens no socket of its own when the two old protocol versions are off
        self.assertEqual([daemon(command) for command in self.ran("systemd-run")],
                         [["/usr/sbin/nfsdcld", "--foreground", "--storagedir", str(tracking)],
                          ["/usr/sbin/rpc.mountd", "--foreground", "--no-nfs-version", "2", "--no-nfs-version", "3"]])
        # without the tracker the first create waits out the server's grace period
        order = [daemon(command)[0] if command[0] == "systemd-run" else command[0] for command in self.commands]
        self.assertLess(order.index("/usr/sbin/nfsdcld"), order.index("rpc.nfsd"))

    def test_the_client_tracker_starts_from_a_record_of_its_own(self):
        # a client recorded by an earlier boot of the same image would be waited for
        self.prepare()
        tracking = self.state / "nfsdcld"
        self.assertEqual(list(tracking.iterdir()), [])
        self.assertEqual(stat.S_IMODE(tracking.stat().st_mode), 0o700)
        # one that is there already is not taken over
        for name in storage.NFS_MOUNTS.values():
            (self.data / name).rmdir()
        self.commands.clear()
        with self.assertRaisesRegex(ValueError, "exists"):
            self.prepare(again=False)
        self.assertEqual(self.commands, [])

    def test_a_server_that_is_already_running_is_refused(self):
        # it would keep its listeners, its protocol versions and its exports
        (self.kernel.server / "threads").write_text("8\n")
        with self.assertRaisesRegex(ValueError, "already running"):
            self.prepare()
        self.assertEqual(self.ran("systemd-run", "exportfs", "rpc.nfsd", "tc", "mount.nfs"), [])

    def test_a_daemon_that_did_not_stay_up_is_refused_before_anything_is_exported(self):
        for unit in ("rcp-nfsdcld.service", "rcp-nfs-mountd.service"):
            self.dead = {unit}
            with self.subTest(unit=unit), self.assertRaises(subprocess.CalledProcessError) as raised:
                self.prepare()
            self.assertEqual(raised.exception.cmd, ["systemctl", "is-active", "--quiet", unit])
            self.assertEqual(self.ran("exportfs", "rpc.nfsd", "tc", "mount.nfs"), [])

    def test_a_daemon_that_died_while_the_server_started_is_refused(self):
        def execute(argv, budget, **kwargs):
            self.execute(argv, budget, **kwargs)
            if argv[0] == "rpc.nfsd":
                self.dead.add("rcp-nfsdcld.service")
        with self.assertRaises(subprocess.CalledProcessError):
            self.prepare(execute=execute)
        self.assertEqual(len(self.ran("mount.nfs")), 2)

    def test_a_server_in_its_grace_period_is_refused(self):
        # it found no tracker, or a client to wait for: the first create would take 90 seconds
        (self.kernel.server / "v4_end_grace").write_text("N\n")
        with self.assertRaisesRegex(ValueError, "grace"):
            self.prepare()

    def test_a_mount_that_still_takes_the_shortcut_is_refused(self):
        (self.kernel.clients / "0:51" / "localio").write_text("1\n")
        with self.assertRaisesRegex(ValueError, "shortcut"):
            self.prepare()

    def test_a_mount_that_is_not_the_loopback_export_is_refused(self):
        for inventory in (mountinfo(self.data, kind="ext4"),
                          mountinfo(self.data, sources=dict(source="10.0.0.9:/elsewhere",
                                                            destination=f"127.0.0.1:{self.data / 'destination'}")),
                          ""):
            self.inventory = inventory
            with self.subTest(inventory=inventory[:40]), self.assertRaisesRegex(ValueError, "mount"):
                self.prepare()

    def test_a_round_trip_far_from_the_intended_one_is_refused(self):
        # twice the delay, plus what the machine itself needs
        for seconds in (0.0002, 0.0049, 0.0081, 1.5):
            with self.subTest(seconds=seconds), self.assertRaisesRegex(ValueError, "round trip"):
                self.prepare(round_trip=lambda seconds=seconds: seconds)
        for seconds in (0.0050, 0.0052, 0.0065):
            self.assertEqual(self.prepare(round_trip=lambda seconds=seconds: seconds)["round_trip_seconds"], seconds)

    def test_existing_mount_points_are_refused_before_any_command(self):
        (self.data / storage.NFS_MOUNTS["source"]).mkdir()
        with self.assertRaisesRegex(ValueError, "exists"):
            self.prepare(again=False)
        self.assertEqual(self.commands, [])


class MemoryTests(Fixture):
    def test_two_roots_in_memory_are_mounted_for_the_benchmark_account(self):
        with mock.patch.object(storage.os, "chown") as chown:
            result = storage.prepare_memory(self.data, Budget(), 1234, 1235, self.execute)
        self.assertEqual(self.commands, [
            ["mount", "-t", "tmpfs", "-o", "size=64g,nr_inodes=24000000,nosuid,nodev,mode=0755", "tmpfs",
             str(self.data / storage.MEMORY_MOUNTS[side])] for side in storage.SIDES])
        self.assertEqual([call.args for call in chown.call_args_list],
                         [(self.data / storage.MEMORY_MOUNTS[side], 1234, 1235) for side in storage.SIDES])
        self.assertEqual(result, dict(layout="tmpfs-pair-v1", size_bytes=64 * 1024 ** 3, inodes=24_000_000,
                                      mounts={side: str(self.data / storage.MEMORY_MOUNTS[side])
                                              for side in storage.SIDES}))

    def test_existing_roots_are_refused_before_any_command(self):
        (self.data / storage.MEMORY_MOUNTS["destination"]).mkdir()
        with self.assertRaisesRegex(ValueError, "exists"):
            storage.prepare_memory(self.data, Budget(), 1, 1, self.execute)
        self.assertEqual(self.commands, [])


if __name__ == "__main__":
    unittest.main()
