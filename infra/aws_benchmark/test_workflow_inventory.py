"""Inventory tests write unevaluated workflow references, never credential values."""

from pathlib import Path
import tempfile
import unittest

from . import history_contract
from .history_contract import CANARY_WORKFLOW, ENVIRONMENT, PUBLICATION_WORKFLOW
from .workflow_inventory import check_workflows

# this literal is a public configuration name; the contract-binding test prevents fixture drift
PUBLIC_REFERENCE_NAME = "RCP_AWS_HISTORY_APP_PRIVATE_KEY"


class WorkflowInventoryTests(unittest.TestCase):
    def test_public_reference_fixture_matches_the_workflow_contract(self):
        self.assertEqual(PUBLIC_REFERENCE_NAME, history_contract.WRITER_SECRET_NAME)

    def test_both_suffixes_case_and_dynamic_environment_cannot_hide_a_consumer(self):
        for suffix, environment, expression in (("yml", ENVIRONMENT, "secrets." + PUBLIC_REFERENCE_NAME),
                ("yaml", ENVIRONMENT, "secrets." + PUBLIC_REFERENCE_NAME),
                ("yaml", ENVIRONMENT.upper(), "secrets." + PUBLIC_REFERENCE_NAME),
                ("yml", "${{ inputs.target }}", "toJSON(secrets)"),
                ("yaml", ENVIRONMENT.upper(), "secrets." + PUBLIC_REFERENCE_NAME.lower()),
                ("yaml", "other", "secrets[format(\"RCP_AWS_{0}\", \"HISTORY_APP_PRIVATE_KEY\")]")):
            with self.subTest(suffix=suffix, environment=environment), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for name, job in ((PUBLICATION_WORKFLOW, "publish"), (CANARY_WORKFLOW, "verify")):
                    (root / name).write_text("jobs:\n  " + job + ":\n    environment: " + ENVIRONMENT +
                        "\n    steps:\n      - run: echo ${{ secrets." + PUBLIC_REFERENCE_NAME + " }}\n")
                self.assertEqual(len(check_workflows(root)), 2)
                (root / ("rogue." + suffix)).write_text("jobs:\n  rogue:\n    environment: '" + environment +
                    "'\n    steps:\n      - run: echo ${{ " + expression + " }}\n")
                with self.assertRaises(ValueError):
                    check_workflows(root)

    def test_environment_and_key_sets_must_both_be_exact(self):
        for missing in ("environment", "key"):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for name, job in ((PUBLICATION_WORKFLOW, "publish"), (CANARY_WORKFLOW, "verify")):
                    (root / name).write_text("jobs:\n  " + job + ":\n" +
                        ("    environment: " + ENVIRONMENT + "\n" if missing != "environment" else "") +
                        ("    steps:\n      - run: echo ${{ secrets." + PUBLIC_REFERENCE_NAME + " }}\n" if missing != "key" else "    steps: []\n"))
                with self.assertRaises(ValueError):
                    check_workflows(root)

    def test_global_secret_expression_cannot_hide_outside_jobs(self):
        for expression in ("secrets." + PUBLIC_REFERENCE_NAME, "toJSON(secrets)"):
            with self.subTest(expression=expression), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for name, job in ((PUBLICATION_WORKFLOW, "publish"), (CANARY_WORKFLOW, "verify")):
                    (root / name).write_text("jobs:\n  " + job + ":\n    environment: " + ENVIRONMENT +
                        "\n    steps:\n      - run: echo ${{ secrets." + PUBLIC_REFERENCE_NAME + " }}\n")
                (root / "rogue.yaml").write_text("env:\n  VALUE: ${{ " + expression + " }}\njobs: {}\n")
                with self.assertRaises(ValueError):
                    check_workflows(root)

    def test_dynamic_secret_lookup_holds_even_without_literal_key_name(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, job in ((PUBLICATION_WORKFLOW, "publish"), (CANARY_WORKFLOW, "verify")):
                (root / name).write_text("jobs:\n  " + job + ":\n    environment: " + ENVIRONMENT +
                    "\n    steps:\n      - run: echo ${{ secrets." + PUBLIC_REFERENCE_NAME + " }}\n")
            (root / "rogue.yaml").write_text("jobs:\n  rogue:\n    steps:\n      - run: |\n          echo ${{ secrets[format('RCP_{0}', inputs.suffix)] }}\n")
            with self.assertRaisesRegex(ValueError, "explicit named secret"):
                check_workflows(root)

    def test_multiline_expressions_are_checked_without_repr_escaping(self):
        for expression in ("secrets\n[format('RCP_{0}', inputs.suffix)]", "toJSON(\nsecrets)"):
            with self.subTest(expression=expression), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for name, job in ((PUBLICATION_WORKFLOW, "publish"), (CANARY_WORKFLOW, "verify")):
                    (root / name).write_text("jobs:\n  " + job + ":\n    environment: " + ENVIRONMENT +
                        "\n    steps:\n      - run: echo ${{ secrets." + PUBLIC_REFERENCE_NAME + " }}\n")
                (root / "rogue.yaml").write_text("jobs:\n  rogue:\n    steps:\n      - run: |\n          echo ${{ " +
                    expression.replace("\n", "\n          ") + " }}\n")
                with self.assertRaisesRegex(ValueError, "secret"):
                    check_workflows(root)

    def test_broad_secret_context_is_rejected_in_the_approved_job(self):
        for expression in ("toJSON(secrets)", "toJSON((secrets))", "join(secrets.*)",
                "toJSON(secrets.*)", "fromJSON(toJSON(secrets.*))", "(secrets).OTHER_TOKEN",
                "secrets['OTHER_TOKEN']", "secrets[inputs.name]", "SECRETS . *",
                "format('{0}', secrets)", "secrets", "toJSON(\nsecrets\n)"):
            with self.subTest(expression=expression), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.write_consumers(root, "${{ " + expression + " }}")
                with self.assertRaisesRegex(ValueError, "explicit named secret"):
                    check_workflows(root)

    def test_named_secrets_and_literal_text_remain_allowed(self):
        for value in ("${{ secrets.OTHER_TOKEN }}", "${{ toJSON(secrets.OTHER_TOKEN) }}",
                "${{ secrets . OTHER_TOKEN }}", "${{ SECRETS.other_token }}",
                "${{ inputs.secrets }}", "${{ steps.secrets.outputs.value }}",
                "${{ format('secrets.* and secrets[inputs.name]', secrets.OTHER_TOKEN) }}",
                "${{ format('}} and ''secrets'' and ${{ secrets }}', secrets.OTHER_TOKEN) }}",
                "${{ 'toJSON((secrets))' }}", "literal toJSON(secrets) and secrets[inputs.name]"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.write_consumers(root, value)
                self.assertEqual(len(check_workflows(root)), 2)

    def test_invalid_expression_boundaries_are_rejected(self):
        for value in ("${{ secrets.OTHER_TOKEN", "${{ 'unterminated }}"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.write_consumers(root, value)
                with self.assertRaisesRegex(ValueError, "unterminated workflow expression"):
                    check_workflows(root)

    def write_consumers(self, root, extra):
        for name, job in ((PUBLICATION_WORKFLOW, "publish"), (CANARY_WORKFLOW, "verify")):
            text = ("jobs:\n  " + job + ":\n    environment: " + ENVIRONMENT +
                "\n    steps:\n      - with:\n          private-key: ${{ secrets." + PUBLIC_REFERENCE_NAME + " }}\n")
            if name == PUBLICATION_WORKFLOW:
                text += "      - run: |\n          " + extra.replace("\n", "\n          ") + "\n"
            (root / name).write_text(text)


if __name__ == "__main__":
    unittest.main()
