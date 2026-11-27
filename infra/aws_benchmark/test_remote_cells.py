"""Remote matrix cells need a little more of the worker and its image than local ones."""

import contextlib
import io
from pathlib import Path
import subprocess
import unittest
from unittest import mock

from benchmarks import matrix
from . import common, production_projection as projection, remote_check, worker
from . import test_matrix_results
from .test_matrix_results import matrix_record

ROOT = Path(__file__).resolve().parents[2]


def properties(argv):
    return dict(item[len("--property="):].split("=", 1) for item in argv if item.startswith("--property="))


class RemoteCellWorkerTests(unittest.TestCase):
    def test_only_suites_with_remote_cells_may_open_ipv4_sockets(self):
        for suite, cells in common.MATRIX_SUITES.items():
            with self.subTest(suite=suite):
                remote = any(scenario == "remote" for _, scenario in cells)
                # a suite of the catalog says so in its name; a weekly suite is named after what it is for
                self.assertEqual(remote, "remote" in suite or suite in common.MATRIX_WEEKLY_SUITES)
                sandbox = properties(worker.command(dict(suite=suite, baseline="a" * 40), 25000))
                # loopback only: the unit keeps its private network, and no netlink or IPv6
                self.assertEqual(sandbox["RestrictAddressFamilies"], "AF_UNIX AF_INET" if remote else "AF_UNIX")
                self.assertEqual(sandbox["PrivateNetwork"], "yes")
        for suite in ("bulk-1g-metadata", "all-filesets-metadata"):
            self.assertEqual(properties(worker.command(dict(suite=suite, baseline="a" * 40), 600))
                             ["RestrictAddressFamilies"], "AF_UNIX")

    def test_nothing_else_about_the_sandbox_differs_for_a_remote_suite(self):
        local = properties(worker.command(dict(suite="matrix-all-local", baseline="a" * 40), 25000))
        remote = properties(worker.command(dict(suite="matrix-all-remote", baseline="a" * 40), 25000))
        self.assertEqual({key for key in local if local[key] != remote[key]}, {"RestrictAddressFamilies"})
        self.assertEqual(set(local), set(remote))
        self.assertEqual((remote["User"], remote["NoNewPrivileges"], remote["CapabilityBoundingSet"],
                          remote["RestrictNamespaces"], remote["ProtectSystem"]),
                         ("rcp-benchmark", "yes", "", "yes", "strict"))

    def test_a_remote_suite_tells_the_runner_which_openssh_to_use(self):
        argv = worker.command(dict(suite="matrix-commission-local+remote", baseline="a" * 40), 9000)
        args = matrix._arguments(argv[argv.index("benchmarks.matrix") + 1:])
        self.assertEqual((args.sshd, args.ssh, args.ssh_keygen),
                         (Path("/usr/sbin/sshd"), Path("/usr/bin/ssh"), Path("/usr/bin/ssh-keygen")))
        self.assertEqual(args.cells, ["commission-64m:local", "commission-64m:remote",
                                      "commission-10k:local", "commission-10k:remote"])
        # a suite without remote cells gets the command it always had
        local = worker.command(dict(suite="matrix-commission-local", baseline="a" * 40), 9000)
        self.assertFalse({"--sshd", "--ssh", "--ssh-keygen"} & set(local))

    def test_the_image_check_logs_in_under_the_sandbox_of_a_remote_suite(self):
        argv = remote_check.command()
        self.assertEqual(argv[:2], ["/usr/bin/systemd-run", "--unit=rcp-remote-check"])
        self.assertTrue({"--wait", "--collect"} <= set(argv))
        check = properties(argv)
        expected = properties(worker.command(dict(suite="matrix-all-remote", baseline="a" * 40), 120))
        # the data mounts exist only on a worker, and the build log must say why a check failed
        del expected["ReadWritePaths"]
        expected.update(StandardOutput="journal+console", StandardError="journal+console")
        self.assertEqual(check, expected)
        self.assertEqual(check["RestrictAddressFamilies"], "AF_UNIX AF_INET")
        self.assertIn("--setenv=PYTHONPATH=/opt/rcp-benchmark", argv)
        self.assertEqual(argv[-10:], ["/usr/bin/python3", "-B", "-m", "benchmarks.private_sshd", "--sshd",
                                      "/usr/sbin/sshd", "--ssh", "/usr/bin/ssh", "--ssh-keygen",
                                      "/usr/bin/ssh-keygen"])

    def test_the_image_check_fails_the_build_when_the_login_fails(self):
        with mock.patch.object(remote_check.os, "geteuid", return_value=0), \
                mock.patch.object(remote_check.subprocess, "run") as run, contextlib.redirect_stdout(io.StringIO()):
            run.return_value.returncode = 0
            remote_check.main()
            self.assertEqual(run.call_args.args[0], remote_check.command())
            run.return_value.returncode = 1
            with self.assertRaisesRegex(SystemExit, "cannot serve remote benchmark cells"):
                remote_check.main()
            # a check that hangs fails the build as well, and says that it hung
            run.side_effect = subprocess.TimeoutExpired(remote_check.command(), 180)
            with self.assertRaisesRegex(SystemExit, "cannot serve remote benchmark cells: .* timed out"):
                remote_check.main()
        with mock.patch.object(remote_check.os, "geteuid", return_value=1000), \
                self.assertRaisesRegex(SystemExit, "requires root"):
            remote_check.main()

    def test_the_image_installs_openssh_and_an_account_that_can_run_a_command(self):
        installer = (ROOT / "infra/aws_benchmark/install-worker.sh").read_text()
        packages = next(line for line in installer.splitlines() if "apt-get install" in line)
        self.assertIn(" openssh-client ", packages + " ")
        self.assertIn(" openssh-server ", packages + " ")
        # sshd runs a command through the account's shell and starts it in the account's home:
        # a shell that refuses logins or a home that does not exist would break every remote cell
        self.assertIn("useradd --system --no-create-home --home-dir /opt/rcp-benchmark/home --shell /bin/sh "
                      "rcp-benchmark\n", installer)
        self.assertIn("/opt/rcp-benchmark/home\n", installer.split("useradd", 1)[1].split("progress", 1)[0])
        self.assertNotIn("nologin", installer)
        copied = installer.split("cp infra/aws_benchmark/{", 1)[1].split("}", 1)[0].split(",")
        self.assertIn("remote_check", copied)
        # the check runs on the installed tree, after it is owned by root and readable
        after = installer.split("progress verify-unprivileged", 1)[1]
        self.assertIn("progress verify-remote-cells\npython3 -B -m infra.aws_benchmark.remote_check\n", after)
        self.assertLess(after.index("verify-remote-cells"), after.index("progress output-contract"))


class RemoteCellCollectionTests(unittest.TestCase):
    """The collector and the public record take remote cells as they take local ones."""

    collect = test_matrix_results.MatrixCollectionTests.collect

    def test_local_and_remote_cells_of_one_run_are_published_side_by_side(self):
        suite = "matrix-commission-local+remote"
        unreachable = ("the private ssh endpoint stopped and could not be started again before copy 2: "
                       "sshd rejected its configuration: no output")
        record = matrix_record(suite, {3: dict(status="failed", stage="transport", message=unreachable)})
        result, report, public = self.collect(suite, record)
        self.assertEqual((result["state"], result["retired"], report["benchmark_valid"]), ("success", True, True))
        self.assertEqual(public["schema"], "rcp-production-public-report-v4")
        self.assertEqual([(cell["case_id"], cell["scenario"], cell["outcome"]) for cell in public["cells"]], [
            ("commission-64m", "local", "complete"), ("commission-64m", "remote", "complete"),
            ("commission-10k", "local", "complete"), ("commission-10k", "remote", "failed")])
        self.assertEqual((public["cells"][3]["stage"], public["cells"][3]["message"]), ("transport", unreachable))
        self.assertEqual([(item["case_id"], item["scenario"]) for item in public["measurements"]], [
            ("commission-64m", "local"), ("commission-64m", "remote"), ("commission-10k", "local")])
        # a file set's local and remote copies are different series, each joined to its own past
        self.assertEqual(len({item["series_id"] for item in public["measurements"]}), 3)
        self.assertEqual(projection.outcome_label(projection.bind(public, "a" * 64)), "3 of 4 measurements completed")
        self.assertEqual(public["validation"],
                         "Not verified: a copy counts when rcp exits cleanly and reports the expected file count.")

    def test_every_cell_of_the_two_scenario_suite_fits_one_record(self):
        suite = "matrix-all-local+remote"
        record = matrix_record(suite, {index: dict(status="not_run", stage=None,
                                                   message="allocation ended before this cell") for index in (6, 7)})
        result, report, public = self.collect(suite, record, remaining=28300, runtime_seconds=28800,
                                              capacity_seconds=7200)
        self.assertEqual((result["state"], report["benchmark_valid"]), ("success", True), public["diagnostics"])
        self.assertEqual([cell["outcome"] for cell in public["cells"]], ["complete"] * 6 + ["not_run"] * 2)
        self.assertEqual([item["scenario"] for item in public["measurements"]], ["local", "remote"] * 3)

    def test_cells_under_another_scenario_are_not_the_suite(self):
        record = matrix_record("matrix-commission-remote")
        result, report, public = self.collect("matrix-commission-local", record)
        self.assertEqual((report["benchmark_valid"], public["cells"], public["measurements"]), (False, [], []))
        self.assertIn("unexpected matrix measurement contract", " ".join(public["diagnostics"]))


if __name__ == "__main__":
    unittest.main()
