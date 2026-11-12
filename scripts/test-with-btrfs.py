#!/usr/bin/env python3
"""Exercise the Btrfs wrapper's lifetime rules without privileged mounts."""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest


WRAPPER = Path(__file__).resolve().with_name("with-btrfs.sh")

# only privileged filesystem operations are simulated; the wrapper creates and
# removes real temporary files, and its child is a real unprivileged process
FAKE_TOOL = r'''
import json
import os
from pathlib import Path
import sys

state = Path(os.environ["BTRFS_TEST_STATE"])
name = Path(sys.argv[0]).name
args = sys.argv[1:]
with (state / "calls").open("a") as log:
    log.write(json.dumps([name, *args]) + "\n")

def fail(operation):
    if os.environ.get("BTRFS_TEST_FAIL") == operation:
        print("failed " + operation, file=sys.stderr)
        sys.exit(42)

if name == "id":
    print(os.environ.get("BTRFS_TEST_UID", "1000"))
elif name == "mkfs.btrfs":
    image = Path(args[-1])
    assert image.is_file()
    (state / "fixture").write_text(str(image.parent))
    fail("mkfs")
elif name == "sudo":
    assert args[0] == "-n", args
    args = args[1:]
    fail(args[0])
    if args[0] == "mount":
        (state / "mounted").write_text(args[-1])
        fail("mounted")
    elif args[0] == "chown":
        assert args[1] == "1000:1000", args
    elif args[0] == "umount":
        assert args[-1] == (state / "mounted").read_text()
        (state / "mounted").unlink()
    else:
        raise AssertionError(args)
elif name == "stat":
    assert Path(args[-1]).is_dir()
    print(os.environ.get("BTRFS_TEST_FS", "btrfs"))
elif name == "mountpoint":
    fail("mountpoint")
    sys.exit(0 if (state / "mounted").exists() else 32)
else:
    raise AssertionError(name)
'''


class BtrfsWrapperTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="btrfs-wrapper-test-")
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.bin = self.state / "bin"
        self.bin.mkdir()
        for name in ["sudo", "mkfs.btrfs", "stat", "mountpoint", "id"]:
            tool = self.bin / name
            tool.write_text(f"#!{sys.executable}\n" + FAKE_TOOL)
            tool.chmod(0o755)
        self.env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}",
                        BTRFS_TEST_STATE=str(self.state))
        self.addCleanup(self.remove_fixture)

    def remove_fixture(self):
        fixture = self.state / "fixture"
        if fixture.exists():
            shutil.rmtree(fixture.read_text(), ignore_errors=True)

    def run_wrapper(self, source, *args, **environment):
        source = (
            "import os; from pathlib import Path; "
            "Path(os.environ['BTRFS_TEST_STATE'], 'invoked').touch(); " + source
        )
        return subprocess.run(
            ["bash", str(WRAPPER), sys.executable, "-c", source, *args],
            env=dict(self.env, **environment), text=True, capture_output=True, timeout=10,
        )

    def test_runs_command_with_literal_arguments_on_short_tmpdir(self):
        result = self.run_wrapper(
            "import json, os, sys; "
            "print(json.dumps([os.environ['TMPDIR'], sys.argv[1:]]))",
            "one two", "$(must stay literal)", "", TMPDIR="/a/long/unusable/parent" * 8,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        tmpdir, args = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertLess(len(tmpdir), 50)
        self.assertTrue(tmpdir.startswith("/tmp/rcp-btrfs."), tmpdir)
        self.assertEqual(args, ["one two", "$(must stay literal)", ""])
        self.assertFalse(Path(tmpdir).exists())
        self.assertFalse((self.state / "mounted").exists())

    def test_preserves_command_failure_after_cleanup(self):
        result = self.run_wrapper("import sys; sys.exit(7)")
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertTrue((self.state / "invoked").exists())
        self.assertFalse(Path((self.state / "fixture").read_text()).exists())
        self.assertFalse((self.state / "mounted").exists())

    def test_setup_failures_never_run_the_command(self):
        for operation in ["mkfs", "mount", "mounted", "chown"]:
            with self.subTest(operation=operation):
                result = self.run_wrapper("pass", BTRFS_TEST_FAIL=operation)
                self.assertEqual(result.returncode, 42, result.stderr)
                self.assertFalse((self.state / "invoked").exists())
                self.assertFalse(Path((self.state / "fixture").read_text()).exists())
                self.assertFalse((self.state / "mounted").exists())

    def test_refuses_to_run_on_the_wrong_filesystem(self):
        result = self.run_wrapper("pass", BTRFS_TEST_FS="ext2/ext3")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("filesystem", result.stderr)
        self.assertFalse((self.state / "invoked").exists())
        self.assertFalse(Path((self.state / "fixture").read_text()).exists())
        self.assertFalse((self.state / "mounted").exists())

    def test_refuses_root_before_creating_a_fixture(self):
        result = self.run_wrapper("pass", BTRFS_TEST_UID="0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("non-root", result.stderr)
        self.assertFalse((self.state / "invoked").exists())
        self.assertFalse((self.state / "fixture").exists())

    def assert_signal_stops_the_command_tree(self, signum, status):
        ready = self.state / "ready"
        source = (
            "import json, os, subprocess, sys, time; from pathlib import Path; "
            "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
            "state = Path(os.environ['BTRFS_TEST_STATE']); "
            "(state / 'ready.tmp').write_text(json.dumps([os.getpid(), child.pid])); "
            "(state / 'ready.tmp').rename(state / 'ready'); time.sleep(60)"
        )
        process = subprocess.Popen(
            ["bash", str(WRAPPER), sys.executable, "-c", source],
            env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        completed = False
        try:
            deadline = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < deadline:
                self.assertIsNone(process.poll(), "wrapper exited before the child was ready")
                time.sleep(0.01)
            self.assertTrue(ready.exists(), "child did not become ready")
            process.send_signal(signum)
            # descendants inherit the output pipes, so communicate cannot finish
            # while the child or grandchild remains running
            stdout, stderr = process.communicate(timeout=5)
            completed = True
            self.assertEqual(process.returncode, status, stdout + stderr)
            self.assertFalse(Path((self.state / "fixture").read_text()).exists())
            self.assertFalse((self.state / "mounted").exists())
        finally:
            if not completed and ready.exists():
                for pid in json.loads(ready.read_text()):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)

    def test_sigint_stops_the_command_tree_before_cleanup(self):
        self.assert_signal_stops_the_command_tree(signal.SIGINT, 130)

    def test_sigterm_stops_the_command_tree_before_cleanup(self):
        self.assert_signal_stops_the_command_tree(signal.SIGTERM, 143)

    def test_failed_unmount_preserves_image_and_contents(self):
        for command_status in [0, 7]:
            with self.subTest(command_status=command_status):
                result = self.run_wrapper(
                    "import sys; Path(os.environ['TMPDIR'], 'keep').touch(); "
                    f"sys.exit({command_status})", BTRFS_TEST_FAIL="umount",
                )
                if command_status:
                    self.assertEqual(result.returncode, 7, result.stderr)
                else:
                    self.assertNotEqual(result.returncode, 0)
                self.assertIn("unmount", result.stderr)
                mount = Path((self.state / "mounted").read_text())
                self.assertTrue((mount / "keep").exists())
                fixture = Path((self.state / "fixture").read_text())
                self.assertTrue(any(path.is_file() for path in fixture.iterdir()))
                self.remove_fixture()
                (self.state / "mounted").unlink()

    def test_unknown_mount_state_preserves_the_fixture(self):
        result = self.run_wrapper(
            "Path(os.environ['TMPDIR'], 'keep').touch()", BTRFS_TEST_FAIL="mountpoint",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mount state", result.stderr)
        mount = Path((self.state / "mounted").read_text())
        self.assertTrue((mount / "keep").exists())

    def test_requires_a_command(self):
        result = subprocess.run(["bash", str(WRAPPER)], env=self.env,
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("usage", result.stderr.lower())
        self.assertFalse((self.state / "fixture").exists())


if __name__ == "__main__":
    unittest.main()
