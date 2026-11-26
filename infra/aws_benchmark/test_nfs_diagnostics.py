"""Publication of mount-wide RPC deltas over an individual rlink command window."""

import copy
import unittest

from . import production_diagnostics as diagnostics
from .json_bytes import encoded
from .test_matrix_diagnostics import public, WORKER
from .test_matrix_results import matrix_record


SUITE = "matrix-commission-nfs+tools"
DESCRIPTOR = diagnostics.STORAGE_TELEMETRY_DESCRIPTOR
LINK = dict(completed=10_000, transmissions=10_001, retransmissions=1, rtt_milliseconds=50_504)
ZERO = dict(completed=0, transmissions=0, retransmissions=0, rtt_milliseconds=0)
TOTAL = dict(completed=22_000, transmissions=22_007, retransmissions=7, rtt_milliseconds=114_170)
OBSERVATION = dict(status="available", operations=dict(LINK=LINK, GETATTR=ZERO), total=TOTAL)


def fixture(outcomes=None, *, sampled=True):
    raw = matrix_record(SUITE, outcomes, **(dict(system_telemetry=DESCRIPTOR) if sampled else {}))
    cell = next(cell for cell in raw["cells"] if cell["scenario"] == "nfs-rlink")
    trial = cell["copies"][0]
    trial.update(nfs_rpc=copy.deepcopy(OBSERVATION), tool_summary={"hard_links_created": 10_000})
    return public(raw), raw, trial


def rlink_trial(extra):
    return next(item for item in extra["trials"] if item["scenario"] == "nfs-rlink")


def derive(record, raw):
    return diagnostics.derive(record, b"archive", raw, WORKER)


class NfsDiagnosticsTests(unittest.TestCase):
    def test_only_numeric_rpc_deltas_and_a_checked_denominator_reach_the_run_page(self):
        record, raw, trial = fixture()
        trial["nfs_rpc"].update(mount="/private/mount", hard_links_created=666)
        trial["nfs_rpc"]["total"]["server"] = "private-host"
        trial["nfs_rpc"]["operations"]["private-name"] = TOTAL
        doc = derive(record, raw)
        self.assertEqual(rlink_trial(doc)["nfs_rpc"], dict(OBSERVATION, hard_links_created=10_000))
        self.assertEqual(diagnostics.validate(doc, record), doc)
        self.assertNotIn(b"private", encoded(doc))
        html = diagnostics.render(doc, record)
        self.assertIn("NFS client RPCs during rlink", html)
        self.assertIn("destination mount", html)
        self.assertIn("Cumulative RTT (ms)", html)
        self.assertIn("RPCs per hard link", html)
        self.assertIn("<td>All procedures</td><td>22,000</td><td>22,007</td><td>7</td>"
                      "<td>114,170</td><td>5.190</td><td>2.200</td>", html)
        self.assertIn("<td>LINK</td><td>10,000</td><td>10,001</td><td>1</td>"
                      "<td>50,504</td><td>5.050</td><td>1.000</td>", html)
        self.assertIn("<td>READDIR</td><td colspan='6'>unavailable</td>", html)
        self.assertIn("<td>GETATTR</td><td>0</td><td>0</td><td>0</td><td>0</td><td>—</td><td>0.000</td>", html)

    def test_limits_reject_incoherent_or_unbounded_counters_without_losing_other_diagnostics(self):
        for edit in (lambda rpc: rpc.update(status=[]), lambda rpc: rpc.update(total=None),
                     lambda rpc: rpc["operations"]["LINK"].update(rtt_milliseconds=True),
                     lambda rpc: rpc["total"].update(retransmissions=6),
                     lambda rpc: rpc["operations"]["LINK"].update(completed=-1),
                     lambda rpc: rpc["total"].update(rtt_milliseconds=2**63),
                     lambda rpc: rpc["operations"].update(READDIR=TOTAL)):
            record, raw, trial = fixture()
            edit(trial["nfs_rpc"])
            doc = derive(record, raw)
            with self.subTest(edit=edit):
                self.assertEqual(rlink_trial(doc)["nfs_rpc"], dict(status="unavailable"))
                self.assertEqual(doc["status"], "complete")
        for status in ("counter_reset", "mount_changed", "unavailable"):
            record, raw, trial = fixture()
            trial["nfs_rpc"] = dict(status=status, info="/private")
            self.assertEqual(rlink_trial(derive(record, raw))["nfs_rpc"], dict(status=status))

    def test_partial_or_unverified_link_work_never_supplies_the_per_link_denominator(self):
        for links in (None, True, 10_000.0, 0, -4, 3, 2**64, "10000"):
            record, raw, trial = fixture()
            trial["tool_summary"] = {"hard_links_created": links} if links is not None else None
            with self.subTest(links=links):
                self.assertEqual(rlink_trial(derive(record, raw))["nfs_rpc"], dict(OBSERVATION, hard_links_created=None))
        failure = {2: dict(status="failed", stage="command", message="rlink failed")}
        record, raw, _ = fixture(failure)
        self.assertEqual(rlink_trial(derive(record, raw))["nfs_rpc"], dict(OBSERVATION, hard_links_created=None))

    def test_earlier_companions_and_cells_outside_the_instrumented_scope_keep_their_shape(self):
        for sampled in (True, False):
            record, raw, trial = fixture(sampled=sampled)
            if sampled:
                del trial["nfs_rpc"]
            raw["cells"][0]["copies"][0]["nfs_rpc"] = OBSERVATION
            earlier = derive(record, raw)
            self.assertTrue(all("nfs_rpc" not in trial for trial in earlier["trials"]))
            self.assertNotIn("NFS client RPCs during rlink", diagnostics.render(earlier, record))
            again = copy.deepcopy(earlier)
            self.assertEqual(diagnostics.validate(again, record), earlier)
            self.assertEqual(encoded(again), encoded(earlier))

    def test_the_sealed_public_companion_rejects_extra_fields_or_misplaced_diagnostics(self):
        record, raw, _ = fixture()
        doc = derive(record, raw)
        for edit in (lambda rpc: rpc.update(mount="/private"),
                     lambda rpc: rpc["operations"].update(OPEN=LINK),
                     lambda rpc: rpc["total"].update(server="private"),
                     lambda rpc: rpc["total"].update(transmissions=True),
                     lambda rpc: rpc.update(hard_links_created=10000.0),
                     lambda rpc: rpc.update(hard_links_created=9999),
                     lambda rpc: rpc.pop("hard_links_created")):
            broken = copy.deepcopy(doc)
            edit(rlink_trial(broken)["nfs_rpc"])
            with self.subTest(edit=edit), self.assertRaises(ValueError):
                diagnostics.validate(broken, record)
        broken = copy.deepcopy(doc)
        broken["trials"][0]["nfs_rpc"] = rlink_trial(broken)["nfs_rpc"]
        with self.assertRaises(ValueError):
            diagnostics.validate(broken, record)
        record, raw, _ = fixture(sampled=False)
        uninstrumented = derive(record, raw)
        rlink_trial(uninstrumented)["nfs_rpc"] = rlink_trial(doc)["nfs_rpc"]
        with self.assertRaises(ValueError):
            diagnostics.validate(uninstrumented, record)


if __name__ == "__main__":
    unittest.main()
