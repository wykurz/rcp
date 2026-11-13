#!/usr/bin/env python3
"""Exercise lightweight validation with real Git history and workflow gates."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("release_refs", ROOT / "scripts/test-release-refs.py")
REFS = importlib.util.module_from_spec(spec)
spec.loader.exec_module(REFS)


class GitFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="rcp-validation-scope-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
        self.git("init", "-q", "--initial-branch=main")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.invalid")
        self.write("CHANGELOG.md", "# Changelog\n")
        self.write("Cargo.toml", "version = '1.0.0'\n")
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, env=self.env, text=True).strip()

    def write(self, path, content="changed\n"):
        destination = self.root / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content)

    def commit(self):
        self.git("add", "--all")
        self.git("commit", "-qm", "fixture", "--no-gpg-sign")
        return self.git("rev-parse", "HEAD")

    def classify(self, base=None, head=None):
        return subprocess.run([sys.executable, str(ROOT / "scripts/validation-scope.py"),
                               base or self.base, head or self.git("rev-parse", "HEAD")],
                              cwd=self.root, env=self.env, text=True, capture_output=True)

    def assert_scope(self, expected, **kwargs):
        result = self.classify(**kwargs)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, expected + "\n")


class ScopeTests(GitFixture):
    def test_changelog_content_edit_is_lightweight(self):
        self.write("CHANGELOG.md", "# Changelog\n\nRelease notes.\n")
        self.commit()
        self.assert_scope("docs")

    def test_documentation_and_assets_are_recognized_without_file_allowlists(self):
        for path in ("README.md", "docs/new-guide.md", ".github/workflows/README.md",
                     "common/README.MD", "CONTRIBUTING.rst", "notes.adoc", "odd\nname.md",
                     "docs/assets/new-diagram.svg", "docs/reports/new-figure.png",
                     "assets/demo.gif", "assets/site-theme.html"):
            with self.subTest(path=path):
                self.git("reset", "--hard", self.base)
                self.write(path)
                self.commit()
                self.assert_scope("docs")

    def test_renaming_documentation_is_lightweight(self):
        (self.root / "CHANGELOG.md").rename(self.root / "HISTORY.md")
        self.commit()
        self.assert_scope("docs")

    def test_empty_diff_runs_full_validation(self):
        self.assert_scope("full")

    def test_other_paths_including_version_bumps_run_full_validation(self):
        for path in ("Cargo.toml", "Cargo.lock", "src/main.rs", "flake.nix", "requirements.txt",
                     ".github/workflows/validate.yml", "scripts/release.sh", "CHANGELOG.md.bak",
                     "docs/example.rs", "odd\nCargo.toml", "tests/fixture.svg", "assets/build.py"):
            with self.subTest(path=path):
                self.git("reset", "--hard", self.base)
                self.write("CHANGELOG.md", "Release notes.\n")
                self.write(path)
                self.commit()
                self.assert_scope("full")

    def test_checks_all_pr_commits_not_only_the_latest(self):
        self.write("src/main.rs")
        self.commit()
        self.write("CHANGELOG.md", "Release notes.\n")
        self.commit()
        self.assert_scope("full")

    def test_base_branch_advancement_does_not_count_as_pr_changes(self):
        self.git("checkout", "-qb", "feature")
        self.write("CHANGELOG.md", "Release notes.\n")
        head = self.commit()
        self.git("checkout", "-q", "main")
        self.write("src/main.rs")
        base = self.commit()
        self.assert_scope("docs", base=base, head=head)

    def test_deleting_documentation_is_lightweight(self):
        (self.root / "CHANGELOG.md").unlink()
        self.commit()
        self.assert_scope("docs")

    def test_adding_documentation_is_lightweight(self):
        (self.root / "CHANGELOG.md").unlink()
        base = self.commit()
        self.write("CHANGELOG.md")
        self.commit()
        self.assert_scope("docs", base=base)

    def test_renaming_a_file_to_changelog_runs_full_validation(self):
        (self.root / "CHANGELOG.md").unlink()
        (self.root / "Cargo.toml").rename(self.root / "CHANGELOG.md")
        self.commit()
        self.assert_scope("full")

    def test_changelog_mode_or_type_change_runs_full_validation(self):
        for mode in ("executable", "symlink"):
            with self.subTest(mode=mode):
                self.git("reset", "--hard", self.base)
                changelog = self.root / "CHANGELOG.md"
                if mode == "executable":
                    changelog.chmod(0o755)
                else:
                    changelog.unlink()
                    changelog.symlink_to("Cargo.toml")
                self.commit()
                self.assert_scope("full")

    def test_unavailable_or_invalid_commits_fail_instead_of_exempting(self):
        for base in ("0" * 40, "--help", "main"):
            with self.subTest(base=base):
                result = self.classify(base=base)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn("docs", result.stdout)

    def test_scope_job_uses_the_pr_commits_and_publishes_its_decision(self):
        flow = REFS.workflow("validate.yml")
        job = flow["jobs"]["scope"]
        checkout = next(s for s in job["steps"] if s.get("uses", "").startswith("actions/checkout@"))
        self.assertEqual(checkout["with"]["fetch-depth"], "0", "the complete PR history is needed")
        step = next(s for s in job["steps"] if s.get("id") == "changes")
        self.write("CHANGELOG.md", "Release notes.\n")
        for expected in ("docs", "full"):
            if expected == "full":
                self.write("Cargo.toml", "version = '1.1.0'\n")
            head = self.commit()
            # the fixture has no workflow helper in its history; supply the real helper after commit
            helper = self.root / "scripts/validation-scope.py"
            helper.parent.mkdir(exist_ok=True)
            helper.write_bytes((ROOT / "scripts/validation-scope.py").read_bytes())
            values = REFS.WorkflowContext({"github.event_name": "pull_request", "inputs.ref": "",
                                           "github.event.pull_request.base.sha": self.base,
                                           "github.event.pull_request.head.sha": head})
            self.assertTrue(REFS.Condition(checkout["if"], values, []).result())
            with tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "output"
                env = dict(self.env, GITHUB_OUTPUT=str(output),
                           **{k: REFS.expression(v, values) for k, v in step["env"].items()})
                result = subprocess.run(REFS.shell_command(flow, job, step), input=step["run"],
                                        cwd=self.root, env=env, text=True, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                output_value = output.read_text().strip().split("=", 1)[1]
                values["steps.changes.outputs.mode"] = output_value
                self.assertEqual(REFS.expression(job["outputs"]["mode"], values), expected)
            helper.unlink()


class VersionTests(GitFixture):
    def setUp(self):
        super().setUp()
        self.write("Cargo.toml", '''[workspace]
members = ["common", "tool"]
resolver = "3"
[workspace.package]
version = "1.0.0"
[workspace.dependencies]
commonlib = { package = "rcp-tools-common", path = "common", version = "1.0.0" }
external = "2.0.0"
''')
        self.write("common/Cargo.toml", '''[package]
name = "rcp-tools-common"
version.workspace = true
[dependencies]
external.workspace = true
''')
        self.write("tool/Cargo.toml", '''[package]
name = "rcp-tools-tool"
version.workspace = true
[dependencies]
commonlib.workspace = true
''')
        self.write("Cargo.lock", '''version = 4
[[package]]
name = "rcp-tools-common"
version = "1.0.0"
dependencies = ["external"]
[[package]]
name = "rcp-tools-tool"
version = "1.0.0"
dependencies = ["rcp-tools-common"]
[[package]]
name = "external"
version = "2.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
''')
        self.base = self.commit()

    def replace(self, path, before, after):
        original = (self.root / path).read_text()
        self.assertIn(before, original, "fixture replacement must change its input")
        self.write(path, original.replace(before, after))

    def bump(self, version="1.1.0", paths=("Cargo.toml", "Cargo.lock")):
        for path in paths:
            self.replace(path, "1.0.0", version)

    def test_consistent_workspace_version_bump_is_lightweight(self):
        self.bump()
        self.commit()
        self.assert_scope("version")

    def test_version_bump_can_include_documentation_and_toml_formatting(self):
        self.bump()
        self.write("docs/new.md")
        self.replace("Cargo.toml", 'version = "1.1.0"\n', "version = '1.1.0' # new release\n")
        self.commit()
        self.assert_scope("version")

    def test_workspace_members_are_discovered_from_manifests(self):
        self.replace("Cargo.toml", '["common", "tool"]', '["common", "tool", "new-member"]')
        self.write("new-member/Cargo.toml", '[package]\nname = "another-local-package"\nversion.workspace = true\n')
        with (self.root / "Cargo.lock").open("a") as stream:
            stream.write('[[package]]\nname = "another-local-package"\nversion = "1.0.0"\n')
        self.base = self.commit()
        self.bump()
        self.commit()
        self.assert_scope("version")

    def test_partial_or_inconsistent_bumps_run_full_validation(self):
        for paths in (("Cargo.toml",), ("Cargo.lock",)):
            with self.subTest(paths=paths):
                self.git("reset", "--hard", self.base)
                self.bump(paths=paths)
                self.commit()
                self.assert_scope("full")
        for path, before, after in (
            ("Cargo.toml", 'path = "common", version = "1.1.0"', 'path = "common", version = "1.0.0"'),
            ("Cargo.lock", 'name = "rcp-tools-tool"\nversion = "1.1.0"', 'name = "rcp-tools-tool"\nversion = "1.0.0"'),
        ):
            with self.subTest(path=path):
                self.git("reset", "--hard", self.base)
                self.bump()
                self.replace(path, before, after)
                self.commit()
                self.assert_scope("full")

    def test_unrelated_manifest_or_lockfile_changes_run_full_validation(self):
        changes = (
            ("Cargo.toml", 'external = "2.0.0"', 'external = "2.1.0"'),
            ("Cargo.toml", 'resolver = "3"', 'resolver = "2"'),
            ("Cargo.toml", 'path = "common"', 'path = "elsewhere"'),
            ("Cargo.toml", 'package = "rcp-tools-common"', 'features = ["extra"], package = "rcp-tools-common"'),
            ("Cargo.lock", 'version = "2.0.0"', 'version = "2.1.0"'),
            ("Cargo.lock", 'checksum = "aaaa', 'checksum = "bbbb'),
            ("Cargo.lock", 'dependencies = ["external"]', 'dependencies = []'),
            ("Cargo.lock", 'name = "rcp-tools-tool"', 'name = "rcp-tools-other"'),
        )
        for path, before, after in changes:
            with self.subTest(path=path, change=before):
                self.git("reset", "--hard", self.base)
                self.bump()
                self.replace(path, before, after)
                self.commit()
                self.assert_scope("full")

    def test_new_or_removed_packages_do_not_hide_in_a_version_bump(self):
        self.bump()
        with (self.root / "Cargo.lock").open("a") as stream:
            stream.write('[[package]]\nname = "new-dependency"\nversion = "1.0.0"\n')
        self.commit()
        self.assert_scope("full")
        self.git("reset", "--hard", self.base)
        self.bump()
        self.replace("Cargo.lock", '[[package]]\nname = "rcp-tools-tool"\nversion = "1.1.0"\ndependencies = ["rcp-tools-common"]\n', '')
        self.commit()
        self.assert_scope("full")

    def test_registry_package_with_workspace_name_is_not_a_local_package(self):
        with (self.root / "Cargo.lock").open("a") as stream:
            stream.write('''[[package]]
name = "rcp-tools-common"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
''')
        self.base = self.commit()
        self.bump()
        self.commit()
        self.assert_scope("full")

    def test_toml_value_type_changes_are_not_version_only(self):
        with (self.root / "Cargo.toml").open("a") as stream:
            stream.write("[profile.release]\ndebug = 1\n")
        self.base = self.commit()
        for path, before, after in (
            ("Cargo.toml", "debug = 1", "debug = true"),
            ("Cargo.toml", "debug = 1", "debug = 1.0"),
            ("Cargo.lock", "version = 4", "version = 4.0"),
        ):
            with self.subTest(path=path, change=after):
                self.git("reset", "--hard", self.base)
                self.bump()
                self.replace(path, before, after)
                self.commit()
                self.assert_scope("full")

    def test_unchanged_versions_and_unsupported_layouts_use_full_validation(self):
        for before, after in (
            ('members = ["common", "tool"]', 'members = ["*"]'),
            ('members = ["common", "tool"]', 'members = []'),
            ('members = ["common", "tool"]', 'members = ["../common", "tool"]'),
        ):
            with self.subTest(layout=after):
                original_base = self.base
                self.git("reset", "--hard", original_base)
                self.replace("Cargo.toml", before, after)
                base = self.commit()
                self.bump()
                self.commit()
                self.assert_scope("full", base=base)
        self.git("reset", "--hard", self.base)
        for path in ("Cargo.toml", "Cargo.lock"):
            with (self.root / path).open("a") as stream:
                stream.write("# formatting only\n")
        self.commit()
        self.assert_scope("full")

    def test_invalid_versions_or_metadata_run_full_validation(self):
        for version in ("01.1.0", "1.1", "1.1.0-01", "garbage"):
            with self.subTest(version=version):
                self.git("reset", "--hard", self.base)
                self.bump(version)
                self.commit()
                self.assert_scope("full")
        self.git("reset", "--hard", self.base)
        self.bump()
        self.write("Cargo.toml", "not TOML")
        self.commit()
        self.assert_scope("full")

    def test_prerelease_and_build_metadata_versions_are_supported(self):
        self.bump("1.1.0-rc.1+build.5")
        self.commit()
        self.assert_scope("version")

    def test_compares_versions_against_the_merge_base(self):
        self.git("checkout", "-qb", "feature")
        self.bump()
        head = self.commit()
        self.git("checkout", "-q", "main")
        self.replace("Cargo.toml", 'resolver = "3"', 'resolver = "2"')
        base = self.commit()
        self.assert_scope("version", base=base, head=head)


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.flow = REFS.workflow("validate.yml")

    def values(self, mode="full", result="success", event="pull_request", ref=""):
        values = REFS.WorkflowContext({"github.event_name": event, "inputs.ref": ref,
                                       "needs.scope.result": result,
                                       "needs.scope.outputs.mode": mode})
        outputs = self.flow["jobs"]["lint-and-docs"].get("outputs", {})
        values["needs.lint-and-docs.outputs.mode"] = REFS.expression(
            outputs.get("mode", ""), values)
        return values

    def job_runs(self, name, mode, result="success"):
        job = self.flow["jobs"][name]
        states = {"scope": result, "lint-and-docs": "success"}
        dependencies = [states[n] for n in REFS.ancestors(self.flow["jobs"], name)]
        return REFS.Condition(job.get("if", "true"), self.values(mode, result), dependencies).result()

    def steps(self, mode):
        return [s for s in self.flow["jobs"]["lint-and-docs"]["steps"]
                if REFS.Condition(s.get("if", "true"), self.values(mode), []).result()]

    def test_expensive_jobs_skip_for_docs_and_verified_versions(self):
        for name in ("msrv", "test-musl-debug", "test-musl-release", "test-docker", "build-glibc", "nix"):
            with self.subTest(job=name):
                self.assertFalse(self.job_runs(name, "docs"))
                self.assertFalse(self.job_runs(name, "version"))
                self.assertTrue(self.job_runs(name, "full"))

    def test_lint_keeps_lightweight_checks_and_skips_builds(self):
        steps = self.steps("docs")
        names = {s.get("name") for s in steps}
        self.assertLessEqual({"Check markdown formatting", "Test release source identity",
                              "Check action pins", "Check package metadata",
                              "Check report publishing, links, and preserved measurement hashes"}, names)
        for name in ("Install Nix", "Setup Rust", "Format", "Lint", "Documentation",
                     "Test benchmark harness", "Test AWS benchmark safety offline"):
            self.assertNotIn(name, names)
        full_names = {s.get("name") for s in self.steps("full")}
        self.assertLessEqual({"Install Nix", "Setup Rust", "Format", "Lint", "Documentation"}, full_names)
        self.assertEqual(steps, self.steps("version"))

    def test_reports_always_run_but_only_full_validation_installs_the_aws_sdk(self):
        for mode in ("docs", "version", "full"):
            with self.subTest(mode=mode):
                setup = next(s for s in self.steps(mode)
                             if s.get("uses") == "./.depot/actions/setup-report-python")
                self.assertEqual(REFS.expression(setup["with"]["aws-benchmark-tests"],
                                                 self.values(mode)),
                                 "true" if mode == "full" else "false")

    def test_required_lint_rejects_failed_or_missing_scope(self):
        job = self.flow["jobs"]["lint-and-docs"]
        guard = job["steps"][0]
        self.assertIn("scope", REFS.dependencies(job))
        for result, mode, success in (("success", "docs", True), ("success", "full", True),
                                         ("success", "version", True),
                                         ("failure", "docs", False), ("cancelled", "docs", False),
                                         ("skipped", "", False), ("success", "", False),
                                         ("success", "unexpected", False)):
            with self.subTest(result=result, mode=mode):
                self.assertTrue(self.job_runs("lint-and-docs", mode, result))
                values = self.values(mode, result)
                env = dict(os.environ, **{k: REFS.expression(v, values) for k, v in guard["env"].items()})
                run = subprocess.run(REFS.shell_command(self.flow, job, guard), input=guard["run"],
                                     text=True, capture_output=True, env=env)
                self.assertEqual(run.returncode == 0, success, run.stdout + run.stderr)

    def test_release_and_non_pr_calls_never_inspect_source_scope(self):
        scope = self.flow["jobs"].get("scope")
        self.assertIsNotNone(scope, "validation needs a scope job")
        checkout = next(s for s in scope["steps"] if s.get("uses", "").startswith("actions/checkout@"))
        decision = next(s for s in scope["steps"] if s.get("id") == "changes")
        for event, ref in (("push", ""), ("workflow_dispatch", ""), ("pull_request", "pinned-commit")):
            with self.subTest(event=event, ref=ref), tempfile.TemporaryDirectory() as temporary:
                values = self.values(event=event, ref=ref)
                values.update({"github.event.pull_request.base.sha": "", "github.event.pull_request.head.sha": ""})
                self.assertFalse(REFS.Condition(checkout.get("if", "true"), values, []).result())
                output = Path(temporary) / "output"
                env = dict(os.environ, GITHUB_OUTPUT=str(output),
                           **{k: REFS.expression(v, values) for k, v in decision["env"].items()})
                run = subprocess.run(REFS.shell_command(self.flow, scope, decision), input=decision["run"],
                                     cwd=temporary, text=True, capture_output=True, env=env)
                self.assertEqual(run.returncode, 0, run.stderr)
                self.assertEqual(output.read_text(), "mode=full\n")


if __name__ == "__main__":
    unittest.main()
