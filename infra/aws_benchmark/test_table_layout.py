"""Tables of figures keep their identifiers whole, and a wide one takes the page's margin before it scrolls."""

from pathlib import Path
import re
import unittest

from . import production_export as export, production_history as history
from .test_matrix_pages import matrix_example
from .test_production_history import split
from .test_time_use import RUN_LEVEL, marked, phase

ASSETS = Path(__file__).resolve().parents[2] / "scripts/report-site"
STYLE = (ASSETS / "aws.css").read_text()
# what the navigation leaves of the page's width
BESIDE = "100cqw - var(--navigation-width) - var(--navigation-gap)"


def rule(selector, style=STYLE):
    """The declarations of the first rule whose selector starts as given."""
    return style[style.index(selector):].split("}", 1)[0]


class IdentifierTests(unittest.TestCase):
    def test_an_identifier_in_a_table_of_figures_is_never_broken(self):
        # the shared stylesheet lets an identifier in any table break, and anywhere: a table that is too wide then
        # squeezes the column of its file sets until their names stand two letters to a line
        shared = (ASSETS / "report.css").read_text()
        self.assertIn("white-space: normal", rule("table code {", shared))
        self.assertIn("overflow-wrap: anywhere", rule("article code {", shared))
        self.assertIn("white-space: nowrap", rule(".aws table.data code {"))

    def test_the_name_of_a_suite_breaks_between_its_parts_and_never_inside_one(self):
        declarations = rule(".aws table.data td.suite code {")
        self.assertIn("white-space: normal", declarations)
        self.assertIn("overflow-wrap: normal", declarations)
        # the cell wraps too, and both rules stand after the ones they are exceptions to
        self.assertIn("white-space: normal", rule(".aws table.data td.suite {"))
        self.assertLess(STYLE.index(".aws table.data code {"), STYLE.index(".aws table.data td.suite code {"))
        self.assertLess(STYLE.index(".aws table.data td {"), STYLE.index(".aws table.data td.suite {"))


    def test_the_copies_of_a_point_stand_one_to_a_line_in_the_data_table_of_a_trend(self):
        # a tool's point holds its runs after the local and after the remote copies: side by side, the two would
        # make the widest column of the table, now that an identifier is not broken to fit
        records, companions = split([matrix_example("matrix-1k-local+tools+remote+tools")])
        html = history.render(records, companions, "rcmp")
        self.assertRegex(html, r"<td><code>local-rcmp</code> <code>[0-9a-f]{12}</code><br>"
                               r"<code>remote-rcmp</code> <code>[0-9a-f]{12}</code></td>")


class WidthTests(unittest.TestCase):
    def test_a_table_wider_than_the_text_takes_the_margin_on_the_right_before_it_scrolls(self):
        # the page says how wide it is, and a table may be as wide as what the navigation leaves of that
        self.assertIn("container-type: inline-size", rule(".page-layout {"))
        box = rule(".aws .table-scroll {")
        self.assertIn("max-width: calc(" + BESIDE + ")", box)
        # only a table that cannot be narrower takes the margin: its box is as wide as the text, or as wide as
        # the table is with every heading wrapped. A box as wide as the table on one line would push a table
        # into the margin that fits the text with its headings on two lines
        self.assertIn(" width: min-content;", box)
        self.assertIn("min-width: 100%", box)
        self.assertNotIn("max-content", STYLE)
        # a table inside a block inside a block starts further right, and ends where the others do
        self.assertIn("max-width: calc(" + BESIDE + " - 1rem)", rule(".aws details details .table-scroll {"))

    def test_a_page_with_its_navigation_on_top_keeps_a_table_within_its_content(self):
        narrow = STYLE[STYLE.index("@media (max-width: 960px) {", STYLE.index(".aws .table-scroll {")):]
        narrow = narrow[:narrow.index("\n}")]
        self.assertIn(".aws .table-scroll, .aws details details .table-scroll { max-width: 100%; }", narrow)
        # the shared stylesheet puts the navigation on top at the same width
        shared = (ASSETS / "report.css").read_text()
        self.assertIn("@media (max-width: 960px) {\n  .page-layout { grid-template-columns: minmax(0, 1fr);", shared)

    def test_the_tables_of_many_columns_have_narrower_cells(self):
        self.assertIn("padding-inline: .55rem", rule(".aws table.dense th, .aws table.dense td {"))
        record, extra = marked("matrix-1g-local", RUN_LEVEL, lambda trial: [phase("command", 100.0)])
        html = export.render(record, diagnostics=extra)
        # the figures of each process, and the phases of the run and of each of its three copies
        self.assertIn("<table class='data dense'><thead><tr><th>File set</th><th>Scenario</th><th>Copy</th>", html)
        self.assertEqual(html.count("<table class='data dense'><thead><tr><th>Phase</th><th>Case index</th>"), 4)
        # the tables of few columns keep the cells of every other table
        self.assertEqual(len(re.findall(r"<table class='data dense'>", html)), 5)
        self.assertIn("<table class='data cells'>", html)

    def test_the_table_of_requests_to_an_nfs_server_has_narrower_cells_too(self):
        from . import production_diagnostics as diagnostics
        record, extra = matrix_example("matrix-all-nfs+tools")
        window = dict(status="unavailable")
        for trial in extra["trials"]:
            if trial["scenario"] == "nfs-rlink":
                trial["nfs_rpc"] = dict(window)
        extra["setup"]["instrumentation"] = dict(diagnostics.STORAGE_TELEMETRY_DESCRIPTOR)
        extra["setup_id"] = diagnostics.setup_id(extra["setup"])
        diagnostics.validate(extra, record)
        html = export.render(record, diagnostics=extra)
        self.assertIn("<table class='data dense'><thead><tr><th>File set</th><th>Run</th><th>Hard links created</th>",
                      html)
