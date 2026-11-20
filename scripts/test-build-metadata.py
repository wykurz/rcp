#!/usr/bin/env python3
"""Exercise Git identity and Cargo freshness with the real build script, offline."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class BuildMetadataTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="rcp build metadata ")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.repo = self.root / "checkout"
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith("GIT_")}
        self.env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                        CARGO_TARGET_DIR=str(self.root / "target"))
        for name in ("CARGO_BUILD_TARGET", "CARGO_ENCODED_RUSTFLAGS", "RUSTFLAGS",
                     "RUSTDOCFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER"):
            self.env.pop(name, None)
        self.write("Cargo.toml", '[workspace]\nmembers = ["common", "app"]\nresolver = "3"\n')
        self.write(".gitignore", "/target\n/Cargo.lock\n")
        self.write("README.md", "fixture\n")
        self.write("common/Cargo.toml", '[package]\nname = "metadata-common"\n'
                   'version = "0.1.0"\nedition = "2024"\n')
        self.write("common/build.rs", (ROOT / "remote/build.rs").read_text())
        self.write("common/src/lib.rs", 'pub fn identity() -> (&\'static str, &\'static str) {\n'
                   '    (option_env!("RCP_GIT_HASH").unwrap_or("unknown"),\n'
                   '     option_env!("RCP_GIT_DESCRIBE").unwrap_or("unknown"))\n}\n')
        self.write("app/Cargo.toml", '[package]\nname = "metadata-app"\n'
                   'version = "0.1.0"\nedition = "2024"\n'
                   '[dependencies]\nmetadata-common = { path = "../common" }\n')
        self.write("app/src/main.rs", 'fn main() { let (hash, describe) = metadata_common::identity();\n'
                   'println!("{hash}\\n{describe}"); }\n')
        self.git("init", "-b", "main")
        self.commit_fixture()

    def commit_fixture(self):
        self.git("config", "user.name", "Build metadata test")
        self.git("config", "user.email", "build-metadata@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("add", ".")
        self.git("commit", "-m", "Initial fixture")

    def write(self, relative, contents):
        path = self.repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)

    def run_command(self, *args, cwd=None):
        result = subprocess.run(args, cwd=cwd or self.repo, env=self.env,
                                capture_output=True, text=True, errors="surrogateescape", timeout=60)
        self.assertEqual(result.returncode, 0, f"{args}:\n{result.stdout}\n{result.stderr}")
        return result.stdout.strip()

    def git(self, *args):
        return self.run_command("git", *args)

    def build(self, *, fresh=False, identity=None):
        output = self.run_command(os.environ.get("CARGO", "cargo"), "build", "--offline",
                                  "--workspace", "--message-format=json")
        artifacts = [json.loads(line) for line in output.splitlines()]
        artifacts = [item for item in artifacts if item["reason"] == "compiler-artifact"]
        self.assertTrue(artifacts)
        if fresh:
            self.assertTrue(all(item["fresh"] for item in artifacts),
                            "unchanged build recompiled: " + ", ".join(
                                item["target"]["name"] for item in artifacts if not item["fresh"]))
        binary = next(item["executable"] for item in artifacts
                      if item["target"]["name"] == "metadata-app")
        if identity is None:
            identity = [self.git("rev-parse", "HEAD"),
                        self.git("describe", "--tags", "--long", "--always", "--dirty")]
        self.assertEqual(self.run_command(binary).splitlines(), identity)
        return artifacts

    def linked_worktree(self):
        linked = self.root / "linked checkout"
        self.git("worktree", "add", "-b", "feature/linked", str(linked))
        self.repo = linked

    def test_unchanged_checkout_stays_fresh(self):
        self.build()
        self.build(fresh=True)

    def test_unchanged_linked_worktree_stays_fresh(self):
        self.linked_worktree()
        self.build()
        self.build(fresh=True)

    def test_unrelated_branch_changes_do_not_rebuild(self):
        self.build()
        self.git("branch", "unrelated")
        self.build(fresh=True)

    def test_linked_commits_and_detached_checkouts_update_identity(self):
        self.linked_worktree()
        original = self.git("rev-parse", "HEAD")
        self.build()
        self.git("commit", "--allow-empty", "-m", "Commit without source changes")
        self.build()
        self.build(fresh=True)
        self.git("checkout", "--detach", original)
        self.build()
        self.build(fresh=True)

    def test_loose_and_packed_tags_update_identity(self):
        self.build()
        self.git("tag", "v1")
        self.build()
        self.git("pack-refs", "--all", "--prune")
        self.build()
        self.build(fresh=True)
        self.git("tag", "-d", "v1")
        self.build()
        self.build(fresh=True)

    def test_packed_refs_work_in_linked_worktrees(self):
        self.linked_worktree()
        self.git("tag", "v1")
        self.git("pack-refs", "--all", "--prune")
        self.build()
        self.build(fresh=True)
        self.git("commit", "--allow-empty", "-m", "Advance packed branch")
        self.build()
        self.build(fresh=True)

    def test_dirty_initial_build_stays_fresh(self):
        self.linked_worktree()
        self.write("README.md", "modified fixture\n")
        self.build()
        self.build(fresh=True)

    def test_source_archive_has_no_git_identity(self):
        shutil.rmtree(self.repo / ".git")
        self.build(identity=["unknown", "unknown"])
        self.build(fresh=True, identity=["unknown", "unknown"])

    def test_archive_does_not_borrow_parent_repository_identity(self):
        outer = self.repo
        archive = outer / "source archive"
        shutil.copytree(outer, archive, ignore=shutil.ignore_patterns(".git", "source archive"))
        self.repo = archive
        self.build(identity=["unknown", "unknown"])
        self.build(fresh=True, identity=["unknown", "unknown"])

    def test_invalid_git_marker_does_not_borrow_parent_identity(self):
        archive = self.repo / "source archive"
        shutil.copytree(self.repo, archive,
                        ignore=shutil.ignore_patterns(".git", "source archive"))
        self.repo = archive
        (archive / ".git").mkdir()
        self.build(identity=["unknown", "unknown"])
        self.build(fresh=True, identity=["unknown", "unknown"])

    def test_non_utf8_branch_updates_identity(self):
        self.git("checkout", "-b", os.fsdecode(b"topic-\xff"))
        self.build()
        self.git("commit", "--allow-empty", "-m", "Advance non-UTF-8 branch")
        self.build()
        self.build(fresh=True)

    def test_retargeting_symbolic_branch_updates_identity(self):
        self.git("branch", "other")
        self.git("commit", "--allow-empty", "-m", "Advance main")
        self.git("symbolic-ref", "refs/heads/alias", "refs/heads/main")
        self.git("symbolic-ref", "HEAD", "refs/heads/alias")
        self.build()
        self.git("symbolic-ref", "refs/heads/alias", "refs/heads/other")
        self.build()
        self.build(fresh=True)

    def test_git_executable_is_optional(self):
        artifacts = self.build()
        script = next(item["filenames"][0] for item in artifacts
                      if item["target"]["kind"] == ["custom-build"])
        result = subprocess.run([script], cwd=self.repo / "common",
                                env={**self.env, "PATH": str(self.root / "no executables")},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("cargo:rustc-env=", result.stdout)

    def test_linked_reftables_track_shared_tags_and_private_head(self):
        reftable = self.root / "reftable checkout"
        result = subprocess.run(["git", "init", "-b", "main", "--ref-format=reftable", str(reftable)],
                                env=self.env, capture_output=True, text=True, timeout=10)
        if result.returncode and "unknown option" in result.stderr:
            self.skipTest("installed Git predates reftable support")
        self.assertEqual(result.returncode, 0, result.stderr)
        shutil.copytree(self.repo, reftable, dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns(".git"))
        self.repo = reftable
        self.commit_fixture()
        self.linked_worktree()
        original = self.git("rev-parse", "HEAD")
        self.build()
        self.git("tag", "v1")
        self.build()
        self.build(fresh=True)
        self.git("commit", "--allow-empty", "-m", "Advance reftable branch")
        self.build()
        self.git("checkout", "--detach", original)
        self.build()
        self.build(fresh=True)

    def test_deepening_shallow_history_updates_description(self):
        self.git("tag", "v1")
        self.git("commit", "--allow-empty", "-m", "Shallow boundary")
        shallow = self.root / "shallow checkout"
        self.git("clone", "--no-local", "--depth=1", str(self.repo), str(shallow))
        self.repo = shallow
        self.git("fetch", "origin", "refs/tags/v1:refs/tags/v1")
        self.build()
        self.git("fetch", "--unshallow")
        self.build()
        self.build(fresh=True)


if __name__ == "__main__":
    unittest.main()
