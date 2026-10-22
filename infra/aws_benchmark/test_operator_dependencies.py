"""Operator dependencies stay separate from the worker and Lambda SDK closure."""

from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from infra.aws_benchmark import durable_package

ROOT = Path(__file__).resolve().parents[2]


class OperatorDependencyTests(unittest.TestCase):
    def test_operator_addition_has_exact_version_and_wheel_hashes(self):
        content = (ROOT / "infra/aws_benchmark/operator-requirements.txt").read_text()
        lines = [line.strip() for line in content.replace("\\\n", "").splitlines()
                 if line.strip() and not line.startswith("#")]
        self.assertEqual(lines.pop(0), "-r requirements.txt")
        self.assertEqual(len(lines), 1)
        self.assertRegex(lines[0], r"^PyYAML==[0-9]+(?:\.[0-9]+)*\s+(?:--hash=sha256:[0-9a-f]{64}\s*)+$")
        hashes = re.findall(r"--hash=sha256:([0-9a-f]{64})", lines[0])
        self.assertEqual(len(hashes), len(set(hashes)))
        self.assertNotIn("yaml", (ROOT / "infra/aws_benchmark/requirements.txt").read_text().lower())

    def test_operator_import_template_parse_and_help_need_no_cloud_access(self):
        program = """
import pathlib, runpy, sys
sys.path.insert(0, sys.argv[1])
def no_network(event, args):
    if event in {"socket.connect", "socket.getaddrinfo"}:
        raise AssertionError("operator help attempted network access")
sys.addaudithook(no_network)
import yaml
template = yaml.safe_load((pathlib.Path(sys.argv[1]) / "infra/aws_benchmark/stack.yaml").read_text())
assert isinstance(template["Resources"]["LaunchTemplate"]["Properties"]["LaunchTemplateData"], dict)
module = sys.argv[2]
sys.argv = [module, "--help"]
runpy.run_module(module, run_name="__main__")
"""
        with tempfile.TemporaryDirectory() as directory:
            for module in ("infra.aws_benchmark.operator", "infra.aws_benchmark.durable_operator"):
                with self.subTest(module=module):
                    result = subprocess.run([sys.executable, "-I", "-c", program, str(ROOT), module],
                                            cwd=directory, capture_output=True, text=True, timeout=20)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("usage:", result.stdout)

    def test_runtime_installers_keep_the_sdk_only_requirement_file(self):
        with patch.object(durable_package.subprocess, "run") as run:
            durable_package.install_dependencies(Path("/unused/stage"), Path("/unused/wheels"))
        command = run.call_args.args[0]
        self.assertEqual(command[command.index("-r") + 1], str(ROOT / "infra/aws_benchmark/requirements.txt"))
        installer = (ROOT / "infra/aws_benchmark/install-worker.sh").read_text()
        installs = [line for line in installer.splitlines() if "/pip install " in line]
        self.assertEqual(len(installs), 1)
        self.assertTrue(installs[0].endswith("--require-hashes -r infra/aws_benchmark/requirements.txt"))


if __name__ == "__main__":
    unittest.main()
