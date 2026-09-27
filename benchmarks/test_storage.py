import contextlib
import copy
import io
import unittest

from benchmarks import run


class StorageIdentityTests(unittest.TestCase):
    def setUp(self):
        self.case = {"id": "tiny", "directory_widths": [1], "files_per_leaf": 1, "file_size_bytes": 1}
        self.variant = {"id": "rcp", "tool": "rcp", "args": ["--summary"], "processes": 1}
        self.environment = {"filesystem": {
            "source": {"filesystem_type": "ext4", "mount_source": "/dev/fast", "mountpoint": "/fast", "mount_options": ["rw", "noatime"]},
            "destination": {"filesystem_type": "ext4", "mount_source": "/dev/slow", "mountpoint": "/slow", "mount_options": ["rw", "noatime"]},
        }}
        self.profiles = {"source": "source-storage", "destination": "destination-storage"}

    def series(self, environment=None, profiles=None):
        arguments = (self.case, self.variant, "source-warm", "local", "runner", environment or self.environment, {"rsync": {"version": "3.4", "sha256": "a" * 64}})
        return run.series_id(*arguments) if profiles is None else run.series_id(*arguments, storage_ids=profiles)

    def test_different_devices_and_swapped_endpoints_split_series(self):
        original = self.series()
        changed = copy.deepcopy(self.environment)
        changed["filesystem"]["source"]["mount_source"] = "/dev/other"
        self.assertNotEqual(original, self.series(changed))
        swapped = {"filesystem": {"source": self.environment["filesystem"]["destination"], "destination": self.environment["filesystem"]["source"]}}
        self.assertNotEqual(original, self.series(swapped))

    def test_different_mountpoints_split_observed_storage(self):
        changed = copy.deepcopy(self.environment)
        changed["filesystem"]["source"]["mountpoint"] = "/another-mount"
        self.assertNotEqual(self.series(), self.series(changed))

    def test_explicit_profiles_ignore_allocation_but_preserve_raw_observations(self):
        changed = copy.deepcopy(self.environment)
        for mount in changed["filesystem"].values():
            mount["mount_source"] = "/dev/loop77"
            mount["mountpoint"] = "/job-77"
        before = copy.deepcopy(changed)
        self.assertEqual(self.series(profiles=self.profiles), self.series(changed, self.profiles))
        self.assertEqual(changed, before)

    def test_either_endpoint_profile_changes_series(self):
        original = self.series(profiles=self.profiles)
        for side in ("source", "destination"):
            with self.subTest(side=side):
                profiles = {**self.profiles, side: "different-storage"}
                self.assertNotEqual(original, self.series(profiles=profiles))

    def test_profiles_preserve_filesystem_and_semantic_option_changes(self):
        original = self.series(profiles=self.profiles)
        for key, value in (("filesystem_type", "xfs"), ("mount_options", ["rw", "relatime"])):
            with self.subTest(key=key):
                changed = copy.deepcopy(self.environment)
                changed["filesystem"]["source"][key] = value
                self.assertNotEqual(original, self.series(changed, self.profiles))

    def test_explicit_and_observed_identities_are_distinct(self):
        profiles = {side: mount["mount_source"] for side, mount in self.environment["filesystem"].items()}
        self.assertNotEqual(self.series(), self.series(profiles=profiles))

    def test_cli_rejects_blank_storage_ids(self):
        for option in ("--source-storage-id", "--destination-storage-id"):
            with self.subTest(option=option), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    run._arguments(["--output", "/tmp/unused-benchmark-result", option, "  "])

    def test_cli_accepts_explicit_storage_ids(self):
        arguments = run._arguments(["--output", "/tmp/unused-benchmark-result", "--source-storage-id", "source", "--destination-storage-id", "destination"])
        self.assertEqual(arguments.source_storage_id, "source")
        self.assertEqual(arguments.destination_storage_id, "destination")

    def test_cp_reference_version_and_digest_split_series_but_path_does_not(self):
        arguments = (self.case, self.variant, "source-warm", "local", "runner", self.environment)
        reference = {"version": "cp 9.11", "sha256": "a" * 64, "path": "/usr/bin/cp"}
        original = run.series_id(*arguments, {"cp": reference})
        relocated = run.series_id(*arguments, {"cp": {**reference, "path": "/nix/store/cp"}})
        self.assertEqual(original, relocated)
        for change in ({"version": "cp 9.12"}, {"sha256": "b" * 64}):
            with self.subTest(change=change):
                self.assertNotEqual(original, run.series_id(*arguments, {"cp": {**reference, **change}}))

    def test_baseline_build_changes_split_series_while_candidate_changes_do_not(self):
        arguments = (self.case, self.variant, "source-warm", "loopback", "runner", self.environment)
        tools = {tool: {"version": "0.41.0", "sha256": "a" * 64, "path": f"/build/{tool}"} for tool in ("rcp", "rcpd", "rcp-baseline", "rcpd-baseline")}
        original = run.series_id(*arguments, tools)
        for tool in ("rcp-baseline", "rcpd-baseline"):
            for change in ({"version": "0.42.0"}, {"sha256": "b" * 64}):
                with self.subTest(tool=tool, change=change):
                    changed = {**tools, tool: {**tools[tool], **change}}
                    self.assertNotEqual(original, run.series_id(*arguments, changed))
        relocated = {tool: {**identity, "path": f"/another-build/{tool}"} for tool, identity in tools.items()}
        self.assertEqual(original, run.series_id(*arguments, relocated))
        candidate_changes = {**tools, **{tool: {**tools[tool], "version": "0.42.0", "sha256": "b" * 64} for tool in ("rcp", "rcpd")}}
        self.assertEqual(original, run.series_id(*arguments, candidate_changes))

    def test_loopback_ssh_client_and_transport_profile_split_series(self):
        arguments = (self.case, self.variant, "source-warm", "loopback", "runner", self.environment)
        ssh = {"version": "OpenSSH_9.6", "sha256": "a" * 64, "path": "/usr/bin/ssh"}
        original = run.series_id(*arguments, {"ssh": ssh}, ssh_transport_profile="ci-v1")
        for changed in ({"version": "OpenSSH_9.7"}, {"sha256": "b" * 64}):
            with self.subTest(changed=changed):
                self.assertNotEqual(original, run.series_id(*arguments, {"ssh": {**ssh, **changed}}, ssh_transport_profile="ci-v1"))
        self.assertNotEqual(original, run.series_id(*arguments, {"ssh": ssh}, ssh_transport_profile="ci-v2"))
        self.assertEqual(original, run.series_id(*arguments, {"ssh": {**ssh, "path": "/nix/store/ssh"}}, ssh_transport_profile="ci-v1"))
        local = (self.case, self.variant, "source-warm", "local", "runner", self.environment)
        self.assertEqual(run.series_id(*local, {"ssh": ssh}), run.series_id(*local, {"ssh": {**ssh, "sha256": "b" * 64}}))


if __name__ == "__main__":
    unittest.main()
