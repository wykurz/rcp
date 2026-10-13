#!/usr/bin/env python3
"""Audit the sealed packet and render exploratory performance figures.

Usage: python3 analyze.py [packet-directory-or-archive]
Requires matplotlib and numpy. Defaults to the preserved raw/ packet.
All statistics are recomputed from raw records; inputs are never modified.
"""

if not __debug__:
    raise SystemExit("The measurement audit requires enabled assertions; run without -O, -OO, or PYTHONOPTIMIZE.")

import argparse
from collections import Counter
import copy
from datetime import datetime
import hashlib
import io
import json
import math
import os
from pathlib import Path
import shlex
import statistics
import tarfile
import tempfile

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "rcp-perf-matplotlib"))
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.colors import LinearSegmentedColormap, Normalize
from matplotlib.ticker import FuncFormatter, LogFormatterMathtext, LogLocator, MaxNLocator, NullFormatter
import numpy as np


CASES = ["10x100x1G", "10x1024x100M", "10x10x10x1024x1M", "10x10x10x10x1024x1K"]
CASE_LABELS = ["1 GB / file\n1,000 files\n1.000 TB total", "100 MB / file\n10,240 files\n1.024 TB total", "1 MB / file\n1.024M files\n1.024 TB total", "1 kB / file\n10.24M files\n10.24 GB total"]
SHORT_LABELS = ["1 GB", "100 MB", "1 MB", "1 kB"]
COLORS = {"rcp": "#111111", "dcp": "#D62728", "cp": "#16833B", "rsync": "#E0B500"}
TOOL_MARKERS = {"rcp": "o", "dcp": "s", "cp": "^", "rsync": "D"}
BASELINE_STYLES = {
    "cp-1x": {"label": "cp", "color": COLORS["cp"], "face": COLORS["cp"], "line": "-"},
    "cp-10x": {"label": "cp · scripted ×10", "color": "#555555", "face": "white", "line": "--"},
    "rsync-1x": {"label": "rsync", "color": COLORS["rsync"], "face": COLORS["rsync"], "line": "-"},
    "rsync-10x": {"label": "rsync · scripted ×10", "color": "#7928A1", "face": "#7928A1", "line": "--"},
}
FS_LABELS = {"lustre": "Lustre", "weka": "Weka", "nfs": "NFS", "zfs": "ZFS", "xfs": "XFS"}
OVERVIEW_FILESYSTEMS = {"network": ["lustre", "weka", "nfs"], "local": ["xfs", "zfs"]}
METRICS = ["copy_seconds", "completion_seconds"]
METRIC_LABELS = {"completion_seconds": "Copy + destination sync + validation (s)", "copy_seconds": "Copy command wall time (s)"}
ZFS_ARC_CAPTION = (
    "ZFS: ARC effects could not be fully excluded. The latest equivalent 32-rank dcp set also had the lower mean (all 3 runs).\n"
    "Block cloning is expected to reduce rcp's sensitivity to source-data caching; metadata caching can still affect timings."
)
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.titleweight": "bold", "axes.titlesize": 12,
                     "figure.facecolor": "white", "axes.facecolor": "white", "svg.fonttype": "none",
                     "svg.hashsalt": "rcp-performance"})


def digest(data):
    return hashlib.sha256(data).hexdigest()


def stats(values):
    return {"mean": statistics.mean(values), "median": statistics.median(values),
            "min": min(values), "max": max(values)}


def audit_packet(archive):
    blobs = {}
    archive_sha256 = None
    if archive.is_dir():
        for path in sorted(archive.rglob("*")):
            if path.is_symlink():
                raise ValueError(f"Unexpected packet symlink: {path}")
            if path.is_file():
                blobs[path.relative_to(archive).as_posix()] = path.read_bytes()
    else:
        raw = archive.read_bytes()
        archive_sha256 = digest(raw)
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
            for member in tar.getmembers():
                name = Path(member.name)
                if name.is_absolute() or ".." in name.parts or not (member.isfile() or member.isdir()):
                    raise ValueError(f"Unexpected archive member: {member.name}")
                if member.isfile():
                    relative = str(name.relative_to("rcp-results-clean"))
                    if relative in blobs:
                        raise ValueError(f"Duplicate archive member: {member.name}")
                    blobs[relative] = tar.extractfile(member).read()
    seals = blobs["SHA256SUMS"].decode().splitlines()
    sealed_paths = set()
    for line in seals:
        expected, relative = line.split(None, 1)
        relative = relative.lstrip("*")
        if relative in sealed_paths or digest(blobs[relative]) != expected:
            raise ValueError(f"Packet checksum mismatch or duplicate: {relative}")
        sealed_paths.add(relative)
    if sealed_paths != blobs.keys() - {"SHA256SUMS"}:
        raise ValueError("Packet inventory differs from SHA256SUMS")
    results = []
    checked_stats = 0
    checked_logs = 0
    censored = []
    measurement_keys = set()
    campaign_counts = Counter()
    for relative, blob in sorted(blobs.items()):
        if not relative.endswith(".json") or not relative.startswith(("local/", "remote/")):
            continue
        document = json.loads(blob)
        item = {k: document[k] for k in ["scope", "filesystem", "fileset", "measurement_contexts"]}
        item["source_path"] = relative
        item["variants"] = []
        for variant in document["variants"]:
            samples = variant["samples"]
            assert len(samples) == variant["summary"]["n"] == 3
            assert sorted(s["record"]["iteration"] for s in samples) == [1, 2, 3]
            campaign_counts[variant["source_campaign"]] += len(samples)
            normalized = []
            for sample in samples:
                record = sample["record"]
                key = (relative, variant["variant"], record["iteration"])
                assert key not in measurement_keys, key
                measurement_keys.add(key)
                assert record["status"] == "ok" and record["exit_code"] == 0
                assert record["source_files"] == record["destination_files"] == item["fileset"]["files"]
                assert record["source_bytes"] == record["destination_bytes"] == item["fileset"]["apparent_bytes"]
                for log_key in ["command_output", "gnu_time_output"]:
                    log = sample[log_key]
                    assert digest(log["text"].encode()) == log["sha256"], (relative, log_key)
                    checked_logs += 1
                end_field = "copy_plus_sync_seconds" if variant["source_campaign"] == "lustre_weka" else "sync_seconds"
                assert record[end_field] >= record["copy_seconds"] > 0
                normalized.append({"iteration": record["iteration"], "recorded_utc": record["recorded_utc"],
                                   "copy_seconds": record["copy_seconds"], "completion_seconds": record[end_field],
                                   "record_sha256": sample["source_record_sha256"],
                                   "source_variant": variant["variant"], "command": record["command"],
                                   "source": record["source"], "destination": record["destination"],
                                   "measurements": {metric: record[end_field] if metric == "completion_seconds" else record[metric]
                                                    for metric, value in variant["summary"].items() if metric != "n" and value is not None}})
                if "rcp_runtime_stats" in sample:
                    normalized[-1]["rcp_runtime_stats"] = sample["rcp_runtime_stats"]
            recalculated = {}
            for metric, reported in variant["summary"].items():
                if metric == "n" or reported is None:
                    continue
                if metric in METRICS:
                    values = [s[metric] for s in normalized]
                else:
                    values = [s["record"][metric] for s in samples]
                recalculated[metric] = stats(values)
                for statistic, value in recalculated[metric].items():
                    assert math.isclose(value, reported[statistic], rel_tol=1e-10, abs_tol=1e-9), (relative, metric, statistic)
                    checked_stats += 1
            item["variants"].append({"variant": variant["variant"], "campaign": variant["source_campaign"],
                                     "n": len(samples), "statistics": recalculated, "samples": normalized,
                                     "command_example": samples[0]["record"]["command"]})
        for outcome in document["right_censored_outcomes"]:
            record = outcome["record"]
            censored.append({"source_path": relative, "variant": record["variant"],
                             "outcome": "timed_out", "time_limit_seconds": 43200,
                             "recorded_utc": record["recorded_utc"], "iteration": record["iteration"],
                             "record_sha256": outcome["source_record_sha256"], "command": record["command"],
                             "copy_lower_bound_seconds": record["copy_seconds"],
                             "destination_files": record["destination_files"], "source_files": record["source_files"],
                             "n": 1, "interpretation": outcome["interpretation"]})
        results.append(item)
    audit = {"archive_sha256": archive_sha256, "packet_checksums_sha256": digest(blobs["SHA256SUMS"]),
             "archive_files_verified": len(seals),
             "workload_documents": len(results), "variants": sum(len(d["variants"]) for d in results),
             "successful_samples": sum(v["n"] for d in results for v in d["variants"]),
             "summary_statistics_verified": checked_stats, "embedded_log_hashes_verified": checked_logs,
             "censored_outcomes": len(censored)}
    source_inventory = json.loads(blobs["SOURCES.json"])
    expected_counts = {name: source["selected_success_rows"] for name, source in source_inventory.items()}
    assert dict(campaign_counts) == expected_counts, (dict(campaign_counts), expected_counts)
    audit["campaign_sample_counts_verified"] = dict(campaign_counts)
    return results, censored, audit


def audit_coverage(results, censored):
    """Account for the comparison grid, including documented exclusions and timeouts."""
    rcp = ["rcp-default", "rcp-8", "rcp-32", "rcp-128"]
    dcp = ["dcp-default", "dcp-8", "dcp-32", "dcp-128"]
    serial = ["cp-1x", "rsync-1x"]
    recipes = serial + ["cp-10x", "rsync-10x"]
    expected = {}
    for fs in FS_LABELS:
        for case in CASES:
            path = f"local/{fs}/{case}.json"
            for name in rcp + dcp + recipes:
                reason = None
                if fs in {"lustre", "weka"} and name == "dcp-128":
                    reason = "128-rank MPI excluded by the campaign operator after the resource probe."
                elif fs in {"lustre", "weka"} and name in serial and (case == CASES[-1] or (fs == "weka" and case == CASES[-2])):
                    reason = "Serial recipe excluded by the documented Lustre/Weka campaign policy."
                expected[path, name] = reason
    for case in CASES:
        for name in rcp + ["rsync-1x", "rsync-10x"]:
            expected[f"remote/{case}.json", name] = None
    for name in ["rcp-default", "dcp-default", "cp-1x", "rsync-1x"]:
        expected["local/xfs/linux-source.json", name] = None
    for name in ["rcp-default", "rsync-1x"]:
        expected["remote/linux-source.json", name] = None
    actual = {(d["source_path"], v["variant"]): v for d in results for v in d["variants"]}
    assert len(actual) == sum(len(d["variants"]) for d in results)
    rows = []
    for (path, name), reason in sorted(expected.items()):
        row = {"source_path": path, "variant": name}
        if (path, name) in actual:
            row.update(status="complete", successful_samples=actual[path, name]["n"])
        else:
            outcomes = [c for c in censored if c["source_path"] == path and c["variant"] == name]
            if outcomes:
                row.update(status="timed_out", attempts=len(outcomes), time_limit_seconds=outcomes[0]["time_limit_seconds"],
                           note="One 12-hour attempt; repetitions 2 and 3 intentionally omitted.")
            elif reason:
                row.update(status="documented_exclusion", reason=reason)
            else:
                row.update(status="missing")
        rows.append(row)
    return {"basis": "Generated local grid: 5 filesystems × 4 filesets; generated remote grid: 4 filesets; Linux source local and remote. Internal dcp aliases are counted separately here before selection.",
            "status_counts": dict(Counter(row["status"] for row in rows)),
            "unexplained_missing": [row for row in rows if row["status"] == "missing"],
            "additional_configurations": [{"source_path": path, "variant": name} for path, name in sorted(actual.keys() - expected.keys())],
            "configurations": rows}


def select_latest_equivalent(results):
    """Collapse equivalent 32-rank dcp labels, retaining the latest sample per repeat."""
    selected = copy.deepcopy(results)
    superseded = []
    for document in selected:
        vv = variants(document)
        if not {"dcp-default", "dcp-32"} <= vv.keys():
            continue
        earlier, explicit = vv["dcp-default"], vv["dcp-32"]
        assert earlier["campaign"] == explicit["campaign"]
        groups = {}
        for variant in [earlier, explicit]:
            for sample in variant["samples"]:
                groups.setdefault(sample["iteration"], []).append(sample)
        retained = []
        for iteration, samples in sorted(groups.items()):
            commands = {tuple(token.replace(s["destination"], "{destination}") for token in shlex.split(s["command"])) for s in samples}
            assert len(commands) == 1, (document["source_path"], iteration, commands)
            assert len({s["source"] for s in samples}) == 1
            command = next(iter(commands))
            assert command[command.index("-np") + 1] == "32"
            newest = max(samples, key=lambda s: datetime.fromisoformat(s["recorded_utc"]))
            retained.append(newest)
            superseded.append({"source_path": document["source_path"], "canonical_variant": "dcp-32",
                               "iteration": iteration, "normalized_command": list(command),
                               "kept": newest, "superseded": [s for s in samples if s is not newest]})
        explicit["samples"] = retained
        explicit["n"] = len(retained)
        explicit["statistics"] = {metric: stats([s["measurements"][metric] for s in retained]) for metric in explicit["statistics"]}
        explicit["command_example"] = retained[0]["command"]
        explicit["equivalent_labels"] = ["dcp-default", "dcp-32"]
        document["variants"] = [v for v in document["variants"] if v["variant"] != "dcp-default"]
    return selected, superseded


def comparable(document, candidate, reference):
    if candidate["campaign"] == reference["campaign"]:
        return True
    if document["filesystem"] != "xfs" or document["fileset"]["name"] not in CASES:
        return False
    updated_rcp = "remote_october" if document["scope"] == "remote" else "xfs_corrective"
    return {candidate["campaign"], reference["campaign"]} == {updated_rcp, "compute_september"}


def find(results, scope, filesystem, case):
    return next(d for d in results if (d["scope"], d["filesystem"], d["fileset"]["name"]) == (scope, filesystem, case))


def variants(document):
    return {v["variant"]: v for v in document["variants"]}


def save(fig, output, name, png_output=None):
    fig.savefig(output / f"{name}.svg", bbox_inches="tight", metadata={"Date": None})
    if png_output is not None:
        fig.savefig(png_output / f"{name}.png", dpi=160, bbox_inches="tight")
    plt.close(fig)


def theme_axis(ax, logarithmic=False):
    if logarithmic:
        ax.set_yscale("log", base=10)
        ax.set_ylim(bottom=1, top=10 ** math.ceil(math.log10(ax.dataLim.y1)))
        ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0,), numticks=8))
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10, labelOnlyBase=True))
        ax.yaxis.set_minor_locator(LogLocator(base=10, subs=np.arange(2, 10)))
        ax.yaxis.set_minor_formatter(NullFormatter())
    else:
        ax.set_yscale("linear")
        ax.set_ylim(bottom=0)
        formatter = FuncFormatter(lambda value, _: f"{value:,.0f}" if float(value).is_integer() else f"{value:,.1f}")
        ax.yaxis.set_major_formatter(formatter)
        ax.yaxis.set_major_locator(MaxNLocator(nbins=5, min_n_ticks=3))
    ax.grid(axis="y", alpha=0.18, which="major")
    ax.set_axisbelow(True)


def error_point(ax, x, variant, metric):
    name = variant["variant"]
    tool = name.split("-", 1)[0]
    s = variant["statistics"][metric]
    baseline = BASELINE_STYLES.get(name)
    color = baseline["color"] if baseline else COLORS[tool]
    face = baseline["face"] if baseline else color
    default = name in {"rcp-default", "dcp-default", "dcp-32"}
    ax.errorbar(x, s["mean"], yerr=[[s["mean"] - s["min"]], [s["max"] - s["mean"]]],
                fmt=TOOL_MARKERS[tool], color=color, markerfacecolor=face, markeredgecolor="#333333",
                markeredgewidth=0.6, markersize=8 if default else 5,
                capsize=2.5, lw=1, zorder=5 if default else 4)


def tool_legend(tools=("rcp", "dcp")):
    return [Line2D([], [], color=COLORS[tool], marker=TOOL_MARKERS[tool], markeredgecolor="#333333",
                   markeredgewidth=0.6, linestyle="-", label=tool) for tool in tools]


def baseline_legend(names):
    return [Line2D([], [], color=BASELINE_STYLES[name]["color"],
                   markerfacecolor=BASELINE_STYLES[name]["face"], markeredgecolor="#333333",
                   marker=TOOL_MARKERS[name.split("-")[0]], linestyle=BASELINE_STYLES[name]["line"],
                   label=BASELINE_STYLES[name]["label"]) for name in BASELINE_STYLES if name in names]


def timeout_point(ax, x, outcome):
    """Show the censoring limit without implying a measured completion time."""
    tool = outcome["variant"].split("-")[0]
    color = COLORS[tool]
    limit = outcome["time_limit_seconds"]
    ax.scatter(x, limit, marker=TOOL_MARKERS[tool], s=40, color=color, edgecolors="#333333", linewidths=0.6)
    ax.annotate("", xy=(x, limit * 1.12), xytext=(x, limit),
                arrowprops={"arrowstyle": "->", "color": color})


def local_overview(results, output, metric, group, png_output=None):
    network = group == "network"
    filesystems = OVERVIEW_FILESYSTEMS[group]
    fig, axes = plt.subplots(2 if network else 1, 2, figsize=(16, 9.6 if network else 7.4))
    axes = axes.ravel()
    positions = {"rcp-default": -0.35, "rcp-8": -0.25, "rcp-32": -0.15, "rcp-128": -0.05,
                 "dcp-8": 0.15, "dcp-32": 0.25, "dcp-128": 0.35}
    for ax, fs in zip(axes, filesystems):
        for i, case in enumerate(CASES):
            document = find(results, "local", fs, case)
            present = variants(document)
            for name, offset in positions.items():
                if name in present:
                    error_point(ax, i + offset, present[name], metric)
        ax.axvspan(2.55, 3.55, color="#f0eee8", alpha=0.7, zorder=0)
        ax.axvline(2.55, color="#bbbbbb", lw=0.8, linestyle=":")
        ax.set_xlim(-0.53, 3.53)
        ax.set_xticks(range(4), CASE_LABELS, fontsize=9)
        ax.tick_params(axis="x", which="major", pad=8, length=0)
        theme_axis(ax, logarithmic=network)
        ax.set_ylabel(METRIC_LABELS[metric])
        ax.set_title(FS_LABELS[fs], loc="left")
    guide = (
        "Within each fileset, left to right: rcp auto, 8, 32, 128; dcp 8, 32, 128, where available.\n"
        "Larger points mark automatic rcp and the benchmark's 32-rank dcp baseline.\n"
        "rcp settings limit files in flight; dcp settings select MPI ranks."
    )
    caption = (
        "Points = means of 3 runs; whiskers = observed min–max.\n"
        "Shaded 1 kB workload contains 10.24 GB, versus ~1 TB elsewhere. Sizes are decimal. Recipes: plain rcp; dcp -p."
    )
    if network:
        upper = max(ax.get_ylim()[1] for ax in axes[:len(filesystems)])
        for ax in axes[:len(filesystems)]:
            ax.set_ylim(1, upper)
        axes[-1].axis("off")
        axes[-1].text(0.02, 0.96, "1 MB files · elapsed time", fontsize=13, weight="bold", va="top")
        axes[-1].text(0.02, 0.78, "1.024 million files · 1.024 TB total", fontsize=10, va="top")
        rows = []
        for fs in filesystems:
            present = variants(find(results, "local", fs, CASES[2]))
            rcp = present["rcp-default"]["statistics"][metric]["mean"]
            dcp = present["dcp-32"]["statistics"][metric]["mean"]
            rows.append([FS_LABELS[fs], f"{rcp / 60:.1f} min", f"{dcp / 60:.1f} min", f"{dcp / rcp:.1f}×"])
        table = axes[-1].table(cellText=rows, colLabels=["Filesystem", "rcp auto", "dcp 32", "dcp / rcp"],
                              cellLoc="left", colLoc="left", bbox=(0.02, 0.10, 0.96, 0.57), edges="horizontal")
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        for (row, col), cell in table.get_celld().items():
            cell.set_edgecolor("#dddddd")
            cell.set_linewidth(0.6)
            if row == 0 or col == 3:
                cell.get_text().set_weight("bold")
        fig.text(0.075, 0.91, "Logarithmic wall time: each major tick is a 10× increase.",
                 fontsize=12, weight="bold")
        fig.subplots_adjust(top=0.84, bottom=0.24, left=0.075, right=0.985, wspace=0.25, hspace=0.65)
        caption = guide + "\n" + caption
        caption_y = 0.15
    else:
        fig.subplots_adjust(top=0.85, bottom=0.45, left=0.075, right=0.985, wspace=0.25)
        caption = (guide + "\n" + caption + "\n"
                   "XFS: tools were measured on equivalent hosts. Reflinking is included as a default feature.\n"
                   + ZFS_ARC_CAPTION)
        caption_y = 0.31
    fig.suptitle(f"Local copies on {group} filesystems", x=0.075, ha="left", fontsize=20, weight="bold")
    fig.legend(handles=tool_legend(), loc="upper right", bbox_to_anchor=(0.985, 0.988), ncol=2, frameon=False)
    fig.text(0.075, caption_y, caption, fontsize=9, va="top", linespacing=1.6)
    save(fig, output, f"local-overview-{group}-{metric.removesuffix('_seconds')}", png_output)


def scaling(results, censored, output, scope, fs, metric, png_output=None):
    fig, axes = plt.subplots(1, 4, figsize=(17, 6.3))
    remote = scope == "remote"
    numeric = {"8": 0, "32": 1, "128": 2}
    offsets = {"rcp": -0.025, "dcp": 0.025}
    baseline_names = set()
    tuning_tools = set()
    for ax, case, label in zip(axes, CASES, CASE_LABELS):
        document = find(results, scope, fs, case)
        selected = document["variants"]
        for tool in ("rcp", "dcp"):
            group = [v for v in selected if v["variant"].startswith(tool + "-")]
            if group:
                tuning_tools.add(tool)
            numbered = sorted([v for v in group if not v["variant"].endswith("default")], key=lambda v: numeric[v["variant"].split("-")[1]])
            if numbered:
                x = [numeric[v["variant"].split("-")[1]] + offsets[tool] for v in numbered]
                y = [v["statistics"][metric]["mean"] for v in numbered]
                ax.plot(x, y, color=COLORS[tool], lw=1, alpha=0.8)
            for v in group:
                setting = v["variant"].split("-")[1]
                x = (3.1 if setting == "default" else numeric[setting]) + offsets[tool]
                error_point(ax, x, v, metric)
        for v in selected:
            name = v["variant"]
            if name not in BASELINE_STYLES:
                continue
            baseline_names.add(name)
            style = BASELINE_STYLES[name]
            ax.axhline(v["statistics"][metric]["mean"], color=style["color"], linestyle=style["line"], linewidth=1.2, zorder=2)
            error_point(ax, 3.65, v, metric)
        outcomes = [c for c in censored if c["source_path"] == document["source_path"]]
        for offset, outcome in enumerate(outcomes):
            baseline_names.add(outcome["variant"])
            timeout_point(ax, 3.48 + 0.2 * offset, outcome)
        if outcomes:
            ax.text(0.03, 0.98, "cp / rsync: timed out at 12 h", transform=ax.transAxes,
                    ha="left", va="top", fontsize=8)
        ax.axvline(2.55, color="#aaaaaa", lw=0.8, linestyle=":")
        ax.axvspan(2.55, 3.4, color="#eeeeee", zorder=0)
        ax.set_xlim(-0.25, 3.85)
        ax.set_xticks([0, 1, 2, 3.1], ["8", "32", "128", "rcp auto"], fontsize=9)
        theme_axis(ax)
        if outcomes:
            ax.set_ylim(top=54000)
        ax.set_title(label, fontsize=11, linespacing=1.5)
    axes[0].set_ylabel(METRIC_LABELS[metric])
    cohort = "Remote XFS" if remote else "Local " + FS_LABELS[fs]
    fig.suptitle(cohort + ": parallelism and copy recipes", x=0.06, ha="left", fontsize=19, weight="bold")
    handles = tool_legend([t for t in ("rcp", "dcp") if t in tuning_tools]) + baseline_legend(baseline_names)
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.53, 0.94), ncol=6, frameon=False, fontsize=10)
    fig.subplots_adjust(top=0.70, bottom=0.30, left=0.06, right=0.985, wspace=0.31)
    controls = "Curves: rcp copy concurrency settings. Horizontal lines: separate rsync recipes; dashed = scripted 10-process copy." if remote else "Curves: rcp file limits and dcp MPI ranks. Horizontal lines: separate cp/rsync recipes; dashed = scripted 10-process copies."
    caution = "Tools were measured on equivalent hosts. Automatic rcp limits were not recorded." if remote else "Larger points identify automatic rcp and the benchmark's 32-rank dcp default. Marker shape identifies the tool, not parallelism."
    if remote:
        additional = "The scripted rsync recipe divides the generated top-level directories among 10 processes; this is not a built-in rsync option."
    elif fs == "zfs":
        additional = ZFS_ARC_CAPTION
    elif fs == "xfs":
        additional = "XFS: tools were measured on equivalent hosts. Reflinking benefits rcp and cp; dcp copies file data."
    elif fs == "nfs":
        additional = "NFS: cp and rsync each timed out at 12 h on the 1 kB workload; arrows mark unfinished copies, excluded from means."
    else:
        additional = "Single-process cp and rsync were excluded on the 1 kB workload" + (" and Weka's 1 MB workload." if fs == "weka" else ".")
    fig.text(0.06, 0.215, "Points = means of 3 runs; whiskers = observed min–max.\n"
             + controls + "\n" + caution + "\n" + additional + "\n"
             "Settings on the x-axis are categories; automatic rcp is shown separately. Sizes are decimal; the 1 kB workload has ~100× fewer bytes.",
             fontsize=9, va="top", linespacing=1.6)
    save(fig, output, f"parallelism-{scope}-{fs}-{metric.removesuffix('_seconds')}", png_output)


def linux_source(results, output, png_output=None):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.4), sharex=True)
    metric = "copy_seconds"
    for ax, scope in zip(axes, ["local", "remote"]):
        data = variants(find(results, scope, "xfs", "linux-source"))
        order = [name for name in ["rcp-default", "cp-1x", "dcp-default", "rsync-1x"] if name in data]
        labels = []
        for y, name in enumerate(order):
            v = data[name]
            tool = name.split("-")[0]
            labels.append({"rcp-default": "rcp · default", "cp-1x": "cp", "dcp-default": "dcp · 32 ranks", "rsync-1x": "rsync"}[name])
            s = v["statistics"][metric]
            ax.barh(y, s["mean"], height=0.5, color=COLORS[tool], edgecolor="#333333", linewidth=0.6)
            ax.errorbar(s["mean"], y, xerr=[[s["mean"] - s["min"]], [s["max"] - s["mean"]]], fmt="none", color="#303030", capsize=4, lw=1)
            ax.text(s["max"] + 0.18, y, f"{s['mean']:.2f} s", va="center", fontsize=11)
        ax.set_yticks(range(len(order)), labels)
        ax.set_ylim(-0.6, len(order) - 0.4)
        ax.invert_yaxis()
        ax.set_xlim(0, 10.3)
        ax.set_xlabel("Copy command wall time (s)")
        ax.grid(axis="x", alpha=0.18)
        ax.set_axisbelow(True)
        ax.set_title("Local XFS" if scope == "local" else "Two hosts · XFS → XFS", loc="left", pad=14)
    fig.suptitle("Linux source tree: local and remote copies", x=0.09, ha="left", fontsize=19, weight="bold")
    fig.text(0.09, 0.90, "94,775 regular files · 6,204 reported directories · 99 symlinks · 1.62 GB apparent tree size", fontsize=11)
    fig.subplots_adjust(top=0.78, bottom=0.27, left=0.12, right=0.98, wspace=0.40)
    fig.text(0.09, 0.16, "Bars = mean command wall time over 3 runs; whiskers = observed min–max.\n"
             "Source-host caches were dropped before each copy. Local XFS reflinking benefits rcp and cp; dcp copies file data.\n"
             "Recipes: plain rcp, cp -a --reflink=auto, dcp -p, rsync -a. Preservation flags differ.",
             fontsize=9, va="top", linespacing=1.6)
    save(fig, output, "linux-source", png_output)


def default_ratios(comparison_rows, output, png_output=None):
    cohorts = [("local", fs) for fs in ["lustre", "weka", "nfs", "xfs", "zfs"]] + [("remote", "xfs")]
    fig, ax = plt.subplots(figsize=(10, 7.7))
    cmap = LinearSegmentedColormap.from_list("default_overhead", ["#ffffff", "#555555"])
    norm = Normalize(vmin=0, vmax=1.31)
    metric = "copy_seconds"
    by_path = {row["source_path"]: row["default_over_best_tested_rcp"][metric] for row in comparison_rows}
    values = []
    best_names = []
    for scope, fs in cohorts:
        row = []
        names = []
        for case in CASES:
            path = f"local/{fs}/{case}.json" if scope == "local" else f"remote/{case}.json"
            comparison = by_path[path]
            row.append(comparison["ratio"] - 1)
            names.append(comparison["best_variant"].removeprefix("rcp-"))
        values.append(row)
        best_names.append(names)
    ax.imshow(values, cmap=cmap, norm=norm, aspect="auto")
    for i, row in enumerate(values):
        for j, value in enumerate(row):
            ax.text(j, i, f"+{100*value:.1f}%\nbest: {best_names[i][j]}", ha="center", va="center", fontsize=10,
                    color="white" if value > 0.8 else "black", linespacing=1.5)
    ax.set_xticks(range(4), SHORT_LABELS)
    ax.set_yticks(range(6), ["Local " + FS_LABELS[fs] if scope == "local" else "Remote XFS" for scope, fs in cohorts])
    ax.set_title("Command wall time", loc="left")
    ax.set_xticks(np.arange(-0.5, 4, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, 6, 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=2)
    ax.tick_params(which="minor", bottom=False, left=False)
    fig.suptitle("How close are rcp defaults to the best tested setting?", x=0.04, ha="left", fontsize=17, weight="bold")
    fig.text(0.10, 0.90, "Extra elapsed time = (default mean / smallest tested rcp mean − 1) × 100; 0% means default had the lowest mean.", fontsize=10)
    fig.subplots_adjust(top=0.81, bottom=0.25, left=0.18, right=0.95)
    fig.text(0.04, 0.16, "Best is selected from automatic, 8, 32 and 128 for each cell; it is an observed minimum, not a proven optimum.\n"
             "Three successful runs per setting; this view shows descriptive ratios, not uncertainty or statistical significance.\n"
             "The first three columns contain ~1 TB; 1 kB contains 10.24 GB. Linux source has no tuning sweep and is excluded.\n"
             "ZFS ARC effects could not be fully excluded, including metadata caching.",
             fontsize=9, va="top", linespacing=1.6)
    save(fig, output, "default-vs-best-tested", png_output)


def comparisons(results):
    output = []
    for d in results:
        vv = variants(d)
        default = vv["rcp-default"]
        item = {"source_path": d["source_path"], "rcp_default_mean": {}, "comparator_over_rcp": {}, "default_over_best_tested_rcp": {}}
        for metric in METRICS:
            base = default["statistics"][metric]["mean"]
            item["rcp_default_mean"][metric] = base
            item["comparator_over_rcp"][metric] = {name: v["statistics"][metric]["mean"] / base for name, v in vv.items()
                                                     if not name.startswith("rcp-") and comparable(d, v, default)}
            best = min((v for name, v in vv.items() if name.startswith("rcp-")), key=lambda v: v["statistics"][metric]["mean"])
            item["default_over_best_tested_rcp"][metric] = {"best_variant": best["variant"], "ratio": base / best["statistics"][metric]["mean"]}
        output.append(item)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path, nargs="?", default=Path(__file__).resolve().parent / "raw",
                        help="preserved packet directory or original .tar.gz archive")
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--figures-output", type=Path,
                        default=Path(__file__).resolve().parents[2] / "docs/reports/figures/performance")
    parser.add_argument("--png-output", type=Path, help="optionally export PNG copies to a separate directory")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    figure_dir = args.figures_output
    figure_dir.mkdir(parents=True, exist_ok=True)
    if args.png_output is not None:
        args.png_output.mkdir(parents=True, exist_ok=True)
    recorded_results, censored, audit = audit_packet(args.archive)
    coverage = audit_coverage(recorded_results, censored)
    results, superseded = select_latest_equivalent(recorded_results)
    policy = {
        "purpose": "Practical performance guide for HPC administrators, SREs and software developers, to live in the repository.",
        "selection": "Use the latest successful sample per equivalent command configuration and repetition; never choose by speed.",
        "dcp_aliases": "dcp-default and dcp-32 both mean 32 MPI ranks; select latest after verifying commands and sources match.",
        "remote_comparability": {"campaigns": ["remote_october", "compute_september"],
                                 "basis": "Experiment owner confirmed equivalent hosts and authorized updated rcp results against existing baselines.",
                                 "qualification": "The comparison uses equivalent hosts. Measurement dates and build identifiers remain in provenance; the report and figures omit measurement chronology."},
        "local_xfs_comparability": {"campaigns": ["xfs_corrective", "compute_september"],
                                    "basis": "Experiment owner confirmed equivalent hosts for the local rcp rerun and supplied the missing baseline results.",
                                    "qualification": "The comparison uses equivalent hosts. Measurement dates and build identifiers remain in provenance; the report and figures omit measurement chronology."},
        "reflink": "XFS cloning used by rcp and cp is a fair, intended default feature. Dcp copies data. Elapsed time is measured, not physical byte throughput.",
        "zfs_arc": {
            "caveat": "We could not fully eliminate ZFS ARC effects from these measurements.",
            "dcp_selection": "For each ZFS fileset, the latest of two equivalent 32-rank dcp trial sets also had the lower mean command wall time. All three repetitions from that set are retained; selection is by recency, not speed, and no individual minimum or different rank count is substituted.",
            "rcp_expectation": "ZFS block cloning is the reflink equivalent: cloned files share existing data blocks. When cloning succeeds, rcp avoids reading and copying the file payload and is expected to be less sensitive to source-data ARC state. Metadata caching can still affect timings.",
            "evidence_limit": "Cache effects were not isolated experimentally. The supplied archive does not include clone-success counters or syscall traces; the block-cloning explanation is consistent with the copy path and observed timings.",
            "figure_caption": ZFS_ARC_CAPTION,
            "references": [
                "https://openzfs.github.io/openzfs-docs/Basic%20Concepts/Data%20Storage/Block%20Cloning.html",
                "https://openzfs.github.io/openzfs-docs/Basic%20Concepts/Pool%20Structure/Caching.html",
            ],
        },
        "timeouts": "Experiment owner confirmed the two NFS serial 1 kB attempts timed out at 12 hours. Plot as right-censored, not as completed-copy times.",
        "metadata": "Same fresh-destination tree-copy task; CLI preservation flags differ. Reference source and a local rcp 0.41.0 experiment show plain rcp does not preserve original timestamps; exact private benchmark source is unavailable.",
        "future_companion_topics": ["--auto-meta-throttle", "TOCTOU prevention"]}
    presentation = {
        "primary_metric": "copy_seconds",
        "wall_time_axes": "The network-filesystem overview uses matching base-10 logarithmic axes for Lustre, Weka and NFS, starting at 1 second (10^0), with an explicit log-scale subtitle. The local-filesystem overview and other wall-time plots use linear axes starting at zero.",
        "overview_tools": ["rcp", "dcp"],
        "overview_filesystem_groups": OVERVIEW_FILESYSTEMS,
        "overview_elapsed_time_context": "The network overview also lists mean elapsed times in minutes and dcp/rcp ratios for the 1 MB fileset, keeping absolute durations visible alongside logarithmic axes.",
        "colors": COLORS,
        "marker_meaning": "Tool identity only; larger markers identify default settings. Parallelism is labeled explicitly.",
        "baseline_recipes": BASELINE_STYLES,
        "scripted_parallelism": "Ten-process cp and rsync recipes partition the generated directory tree. They are independent baselines, not native parallelism options.",
        "reader_labels": "Figures use descriptive filesystem and workload names. Internal campaign IDs remain only in provenance data.",
        "measurement_chronology": "Report prose and figure captions omit measurement dates and campaign history. Timestamped records remain in provenance; reproducibility paths retain their actual filenames.",
        "remote_concurrency": "Label explicit remote settings as copy concurrency. Keep the exact max-files-in-flight and max-connections flag pair in the report's reproducibility appendix and raw commands.",
        "linux_source_metric": "copy_seconds only",
    }
    audit["selected_variants"] = sum(len(d["variants"]) for d in results)
    audit["selected_successful_samples"] = sum(v["n"] for d in results for v in d["variants"])
    audit["superseded_equivalent_samples"] = sum(len(s["superseded"]) for s in superseded)
    payload = {"audit": audit, "aggregation": "Arithmetic mean; median and observed min/max also retained; n=3 per successful variant.",
               "completion_definition": "Copy start through destination sync and count/size validation, including harness overhead.",
               "copy_definition": "Complete copy command invocation wall time, including command startup.",
               "selection_policy": policy, "presentation": presentation, "coverage": coverage, "superseded_equivalent_samples": superseded,
               "censored": censored, "results": results, "comparisons": comparisons(results)}
    (args.output / "analysis.json").write_text(json.dumps(payload, indent=2) + "\n")
    metric = "copy_seconds"
    for group in OVERVIEW_FILESYSTEMS:
        local_overview(results, figure_dir, metric, group, args.png_output)
    for fs in ["lustre", "weka", "nfs", "xfs", "zfs"]:
        scaling(results, censored, figure_dir, "local", fs, metric, args.png_output)
    scaling(results, censored, figure_dir, "remote", "xfs", metric, args.png_output)
    linux_source(results, figure_dir, args.png_output)
    default_ratios(payload["comparisons"], figure_dir, args.png_output)
    print(json.dumps(audit, indent=2))
    print("Coverage:", json.dumps(coverage["status_counts"]))
    print(f"Wrote {len(list(figure_dir.glob('*.svg')))} SVG figures to {figure_dir}")


if __name__ == "__main__":
    main()
