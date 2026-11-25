"""Bounded presentation-only history of rcp performance from frozen public records.

The history follows the build under test with default settings: one trend per
file set for each kind of copy and for each of the other tools, joined across
code versions. A tool's trend takes the runs of its cells after either kind of
copy. The first page shows the duration of the copies of every file set at
once, and of the other tools on the file set with the most files; each trend
has a page of its own with every metric. Frozen records and series IDs are
read, never rewritten; every emitted string is escaped and no script is
produced.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
import math
import sys

from . import production_diagnostics as diagnostics
from . import production_projection as projection
from .common import require
from .results import utc

# newest qualified runs plotted for each file set
MAX_PLOT_RUNS = 32
# the build under test with default settings, and the same command run with a reference build
CURRENT, REFERENCE = "rcp-default", "rcp-baseline"
# the two kinds of copy on the arrays, and the two on other storage: between two mounts of a
# network filesystem, and between two filesystems in memory
LOCAL, REMOTE = "local", "remote"
NFS, MEMORY = "nfs", "memory"
COPIES = (LOCAL, REMOTE, NFS, MEMORY)
# the other tools, each timed on the copies of a run, in the order they run, and what each does
TOOLS = ("rcmp", "rlink", "rrm")
TOOL_TITLES = {"rcmp": "Compare with rcmp", "rlink": "Hard-link with rlink", "rrm": "Remove with rrm"}
# what a cell's scenario means to a reader. A tool cell is named after the copies it follows and its tool
_AFTER = {LOCAL: "a local copy", REMOTE: "a remote copy", NFS: "a copy between NFS mounts", MEMORY: "a copy in memory"}
_tool_cells = lambda *kinds: {copies + "-" + tool: TOOL_TITLES[tool] + ", after " + _AFTER[copies]
                              for copies in kinds for tool in TOOLS}
# the cells of the arrays first: they came first, and the tables that list cells keep their order
SCENARIOS = {LOCAL: "Local copy", REMOTE: "Remote copy over loopback", **_tool_cells(LOCAL, REMOTE),
             NFS: "Copy between NFS mounts", MEMORY: "Copy in memory", **_tool_cells(NFS, MEMORY)}
TOOL_CELLS = frozenset(SCENARIOS) - set(COPIES)
# what has a trend of its own: each kind of copy, and each tool whichever copies it followed
TRENDS = (*COPIES, *TOOLS)
# what a tool does with a file set, after "files of … each"
ACTIONS = {"rcmp": "compared with their source by rcmp, by file size,", "rlink": "hard-linked into a new tree by rlink",
           "rrm": "removed by rrm"}
CASES = {
    "bulk-1g-1tb": "1 GB files",
    "bulk-100m-1024gb": "100 MB files",
    "bulk-1m-1024gb": "1 MB files",
    "tiny-1k-10240k": "1 KB files",
}
# the file sets of other storage. Their files are those of a set of the arrays, so each is named
# with its storage. The copies of one kind measure each, and the tools after those copies
OTHER_CASES = {"tiny-1k-700x1024": "1 KB files on NFS", "tiny-1k-10240k-memory": "1 KB files in memory"}
# what every file set with a trend is called
NAMES = {**CASES, **OTHER_CASES}
METRICS = (
    ("duration", "Copy duration", "s"),
    ("cpu", "CPU time", "s"),
    ("memory", "Peak memory", "MiB"),
)
# a tool run is one process, like a local copy
TOOL_METRICS = (
    ("duration", "Duration", "s"),
    ("cpu", "CPU time", "s"),
    ("memory", "Peak memory", "MiB"),
)
# a remote copy is three processes: the daemons do the work and are plotted, each on its own
REMOTE_METRICS = (
    ("duration", "Copy duration", "s"),
    ("source_cpu", "Source rcpd CPU time", "s"),
    ("destination_cpu", "Destination rcpd CPU time", "s"),
    ("source_memory", "Source rcpd peak memory", "MiB"),
    ("destination_memory", "Destination rcpd peak memory", "MiB"),
)
# table columns: every plotted metric, then the rcp command's own figures for a remote copy
COLUMNS = {
    LOCAL: (("duration", "Duration s: median (range)"), ("cpu", "CPU s"), ("memory", "Peak RSS MiB")),
    REMOTE: (("duration", "Duration s: median (range)"), ("source_cpu", "Source rcpd CPU s"),
             ("destination_cpu", "Destination rcpd CPU s"), ("source_memory", "Source rcpd peak RSS MiB"),
             ("destination_memory", "Destination rcpd peak RSS MiB"), ("cpu", "rcp CPU s"),
             ("memory", "rcp peak RSS MiB")),
}
# a copy on other storage is one process, as a local copy and a tool run are
COLUMNS.update({trend: COLUMNS[LOCAL] for trend in (NFS, MEMORY, *TOOLS)})
# each scenario's title where its trends are listed, the page with every metric, and what that page adds
ROWS = {
    LOCAL: ("Local copies", "local.html", "CPU time, peak memory and data tables"),
    REMOTE: ("Remote copies", "remote.html", "CPU time and peak memory of each rcpd, and data tables"),
    NFS: ("Copies between NFS mounts", "nfs.html", "CPU time, peak memory and data tables"),
    MEMORY: ("Copies in memory", "memory.html", "CPU time, peak memory and data tables"),
}
ROWS.update({tool: (TOOL_TITLES[tool], tool + ".html", "CPU time, peak memory and data tables") for tool in TOOLS})
# the file sets each page plots: the copies on other storage their own, and a tool every set
# where it takes real time. What a tool does with a thousand files takes milliseconds: that is
# noise, not a trend. The file sets of other storage have a section once a run has a cell there
PLOTTED = {NFS: ("tiny-1k-700x1024",), MEMORY: ("tiny-1k-10240k-memory",),
           "rcmp": ("bulk-1m-1024gb", "tiny-1k-10240k", *OTHER_CASES),
           "rlink": ("bulk-1m-1024gb", "tiny-1k-10240k", *OTHER_CASES), "rrm": (*CASES, *OTHER_CASES)}
# the file set whose tool trends the first page shows: the one with the most files
MOST_FILES = "tiny-1k-10240k"
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
DAY = 86400


@dataclass(frozen=True)
class Frame:
    """Chart geometry in viewBox units: the size of the drawing and the margins around its plot area."""
    width: int
    height: int
    left: int
    right: int
    top: int
    bottom: int
    # a plot with no room for a date a day names the days of its first and last run instead
    ends: bool = False

    @property
    def plot_width(self):
        return self.width - self.left - self.right

    @property
    def plot_height(self):
        return self.height - self.top - self.bottom

    @property
    def base(self):
        return self.height - self.bottom


# a plot on the page of its scenario, and the smaller one that stands four abreast on the first page:
# drawn small rather than scaled down, so that its labels keep their size
DETAIL, COMPACT = Frame(360, 200, 46, 12, 14, 26), Frame(232, 150, 42, 8, 14, 24, ends=True)
TIP_WIDTH, TIP_HEIGHT = 196, 62
# the width of one character of a date label, to keep a label inside the drawing and off its neighbours
TICK_CHARACTER = 6.2


def _summary(values, *, frozen=None):
    """Summarize copies as median and range; every summary value stays finite."""
    require(values and all(math.isfinite(x) and x >= 0 for x in values), "invalid history metric")
    ordered = sorted(values)
    lower, upper = ordered[(len(ordered) - 1) // 2], ordered[len(ordered) // 2]
    # two large samples are halved before they are added, so their midpoint cannot overflow
    middle = (lower + upper) / 2 if math.isfinite(lower + upper) else lower / 2 + upper / 2
    return dict(value=middle if frozen is None else frozen, low=ordered[0], high=ordered[-1], samples=len(ordered))


def _day(value):
    return f"{value.day} {MONTHS[value.month - 1]}"


def moment(value):
    """Format a UTC time for reading; callers keep the ISO form in the markup."""
    return f"{_day(value)} {value.year} {value:%H:%M} UTC"


def time_html(text):
    """Show a validated ISO time readably while keeping its exact form in the markup."""
    return "<time datetime='" + escape(text) + "'>" + escape(moment(utc(text))) + "</time>"


def cells(trend):
    """The cell scenarios whose runs a trend holds: a kind of copy its own, a tool its cells after every kind."""
    return tuple(copies + "-" + trend for copies in COPIES) if trend in TOOLS else (trend,)


def present(records):
    """The trends with a page, in page order: local copies, and each other one that some run has a cell of.

    A cell counts whether it was measured or not, so a page appears with the first run that tried.
    """
    seen = {cell["scenario"] for value in records.values() for cell in value.get("cells", ())}
    return tuple(trend for trend in TRENDS if trend == LOCAL or seen.intersection(cells(trend)))


def has_remote(records):
    """Whether any run has a remote cell, measured or not: then remote copies have their page."""
    return REMOTE in present(records)


def panels(scenario):
    """The metrics plotted for a scenario, as (key, title, unit)."""
    return (METRICS if scenario in (LOCAL, NFS, MEMORY) else TOOL_METRICS if scenario in TOOLS else
            REMOTE_METRICS)


def layout(scenario):
    """The class of the grid that holds a scenario's tiles.

    A remote copy has a tile for each process. The stylesheet stands them in a column per measure,
    the destination under the source, by their position in `REMOTE_METRICS`.
    """
    return "metrics processes" if scenario == REMOTE else "metrics"


def samples(scenario):
    """What one sample of a trend is called, as (one, many): a copy, or a run of a tool.

    A point is one benchmark run, so a run of a tool is never called just a run.
    """
    return ("tool run", "tool runs") if scenario in TOOLS else ("copy", "copies")


def plotted(scenario):
    """The file sets with a trend under a scenario, in page order."""
    return PLOTTED.get(scenario, tuple(CASES))


def _points(value, extra, scenario=LOCAL):
    """Summarize the build under test for each file set of one qualified record, on one trend."""
    started, identity = utc(value["timestamp"]), value["identity"]
    # identical binaries make the reference copies further samples of the same build
    roles = (CURRENT, REFERENCE) if value["comparison"] == projection.IDENTICAL_BINARIES else (CURRENT,)
    pooled = cells(scenario)
    if scenario in TOOLS:
        # a tool cell is measured under what its tool ran, as its record names it
        variants = projection.PUBLIC_CONTRACTS[projection.STORAGE_SCHEMA]["tool_variants"]
        roles = tuple({variants[cell] for cell in pooled})
    # which cell a measurement or a trial is of: a record without scenarios predates the matrix
    of = lambda item: (item.get("scenario", LOCAL), item["variant_id"])
    selected, outside, seen = {}, 0, set()
    for measurement in value["measurements"]:
        # a record without scenarios predates the matrix: all of its copies are local
        key = (measurement["case_id"], measurement.get("scenario", LOCAL), measurement["variant_id"])
        require(key not in seen, "duplicate history case and variant")
        seen.add(key)
        if key[1] in pooled:
            if key[0] in plotted(scenario) and key[2] in roles:
                selected.setdefault(key[0], []).append(measurement)
            elif key[0] not in NAMES or key[2] not in roles:
                # a file set that a tool's page leaves out on purpose is not one it failed to place
                outside += 1
        elif key[1] not in SCENARIOS and scenario == LOCAL:
            # a scenario without a page is reported once, next to the local trends; a measurement
            # of a scenario that has one is that page's to plot or to count
            outside += 1
    points = {}
    for case, items in selected.items():
        trials = [] if extra is None else [row for row in extra["trials"] if row["case_id"] == case and
                                           any(of(row) == of(item) for item in items)]
        # resources count only when every plotted copy has them; a subset would cherry-pick repeats
        counted = all(sum(of(row) == of(item) for row in trials) == len(item["samples"]) for item in items)
        complete = counted and all(row["status"] == "complete" and row["metrics"] is not None for row in trials)
        points[case] = dict(
            run_key=value["run_key"], run=identity["run"], revision=identity["candidate"],
            timestamp=started.isoformat().replace("+00:00", "Z"), at=started.timestamp(),
            date=moment(started), day=f"{_day(started)} {started.year}",
            collected_at=value.get("collected_at"), noun=samples(scenario),
            pooled=len(items) > 1, series_contract=value["series_contract"],
            # a tool's runs after both kinds of copy are one variant in two cells, so they name their cell
            copies=[dict(variant=item["variant_id"], series_id=item["series_id"],
                         **(dict(cell=item["scenario"]) if scenario in TOOLS else {})) for item in items],
            setup=None if extra is None else extra["setup"],
            setup_id=None if extra is None else extra["setup_id"],
            duration=_summary([x for item in items for x in item["samples"]],
                              frozen=items[0]["median"] if len(items) == 1 else None),
            cpu=_summary([row["metrics"]["user_seconds"] + row["metrics"]["system_seconds"]
                          for row in trials]) if complete else None,
            memory=_summary([row["metrics"]["max_rss_kib"] / 1024 for row in trials]) if complete else None)
        if scenario == REMOTE:
            # each daemon is measured on its own, and has a value only when every plotted copy has it
            for role in diagnostics.ROLES:
                figures = [(row.get("roles") or {}).get(role) for row in trials]
                ready = complete and all(item is not None for item in figures)
                points[case][role + "_cpu"] = _summary(
                    [item["user_seconds"] + item["system_seconds"] for item in figures]) if ready else None
                points[case][role + "_memory"] = _summary(
                    [item["max_rss_kib"] / 1024 for item in figures]) if ready else None
    return points, outside


def _link(points):
    """Assign trend segments and the recorded setup changes between runs, in time order.

    A value is compared only with the last recorded value of the same field, so
    a run that did not record a field neither splits nor marks the trend.
    """
    known, contract, segment = {}, None, 0
    for point in points:
        recorded = {} if point["setup"] is None else {
            (group + ": " + label if group.endswith(" storage") else label): (text, kind)
            for group, label, text, kind in diagnostics.setup_rows(point["setup"])
            if text is not None and kind != "context"}
        differing = [(name, known[name], text, kind) for name, (text, kind) in recorded.items()
                     if name in known and known[name] != text]
        changes = [(name, before, after) for name, before, after, _ in differing]
        starts = any(kind == "hardware" for *_, kind in differing)
        if contract is not None and point["series_contract"] != contract:
            changes.insert(0, ("Measurement contract", contract, point["series_contract"]))
            starts = True
        if starts:
            # later runs are compared with the new machine, not the one before it
            segment += 1
            known = {}
        known.update({name: text for name, (text, _) in recorded.items()})
        contract = point["series_contract"]
        point.update(segment=segment, changes=changes, starts_segment=starts)


def model(records, companions, scenario=LOCAL):
    """Summarize each qualified run per file set under one scenario and link the runs in time order."""
    require(isinstance(records, dict) and len(records) <= projection.MAX_PUBLIC_HISTORY_RUNS and
            isinstance(companions, dict) and set(companions) <= set(records),
            "history input exceeds its bounded record set")
    # a file set of other storage has its section on a tool's page once a run has a cell on it
    tried = {cell["case_id"] for value in records.values() for cell in value.get("cells", ())}
    cases, outside = {case: [] for case in plotted(scenario)
                      if case in CASES or scenario not in TOOLS or case in tried}, 0
    for run_key, value in sorted(records.items()):
        projection.validate_public(value)
        require(value["run_key"] == run_key, "history record identity differs")
        extra = companions.get(run_key)
        if extra is not None:
            diagnostics.validate(extra, value)
        if not value["benchmark_valid"]:
            continue
        points, skipped = _points(value, extra, scenario)
        outside += skipped
        for case, item in points.items():
            cases[case].append(item)
    for points in cases.values():
        points.sort(key=lambda item: (item["at"], item["run_key"]))
        _link(points)
    return cases, outside


def point(value, extra, scenario=LOCAL):
    """Summarize one record per file set exactly as its history point is plotted."""
    return _points(value, extra, scenario)[0] if value["benchmark_valid"] else {}


def rounded(value):
    """Round for reading; exact values stay in the public JSON."""
    if value == 0:
        return "0"
    size = abs(value)
    if size >= 1e6 or size < 1e-3:
        return format(value, ".3g")
    return format(value, ",.1f" if size >= 10 else ".2f" if size >= 1 else ".3f")


def exact(value):
    """Keep three decimals for tables without printing hundreds of digits for absurd values."""
    return format(value, ".3f" if abs(value) < 1e9 else ".6g")


def plural(count, one, many):
    return f"{count} {one if count == 1 else many}"


def _value_axis(points, metric):
    """Fit the value axis to the data, never tighter than ±5% of its centre."""
    # a frozen median is not required to lie between its samples, so it is part of the fit
    low = min(min(item[metric]["low"], item[metric]["value"]) for item in points)
    high = max(max(item[metric]["high"], item[metric]["value"]) for item in points)
    centre = low / 2 + high / 2
    half = max((high - low) / 2 * 1.15, centre * .05)
    if not half > 1e-300:
        # a zero or denormal span has no usable scale of its own
        half = 1.0
    bottom, top = max(0, centre - half), min(centre + half, sys.float_info.max)
    # the largest round step that fits at least three times gives three to six ticks
    third = (top - bottom) / 3
    magnitude = 10.0 ** math.floor(math.log10(third))
    step = max((multiple * magnitude for multiple in (1, 2, 2.5, 5) if multiple * magnitude <= third),
               default=magnitude)
    first = math.ceil(bottom / step)
    ticks = [value for value in ((first + index) * step for index in range(8))
             if math.isfinite(value) and value <= top]
    if step >= 1e-4 and top < 1e7:
        decimals = next((digits for digits in range(6) if abs(round(step, digits) - step) < step * 1e-9), 6)
        labels = [format(value, f",.{decimals}f") for value in ticks]
    else:
        # enough significant digits that no tick is rounded onto its neighbour
        digits = min(15, int(math.log10(top / step)) + 3)
        labels = [format(value, f".{digits}g") for value in ticks]
    return bottom, top, list(zip(ticks, labels))


def _time_axis(points):
    """Pad the time axis to at least two days and tick it at whole UTC days."""
    first, last = points[0]["at"], points[-1]["at"]
    pad = max((last - first) * .04, DAY - (last - first) / 2)
    start, end = first - pad, last + pad
    days = (end - start) / DAY
    step = next((size for size in (1, 2, 7, 14, 28, 91, 182, 365) if days / size <= 5), math.ceil(days / 4))
    ticks = []
    for index in range(math.ceil(start / DAY / step), math.floor(end / DAY / step) + 1):
        try:
            day = datetime.fromtimestamp(index * step * DAY, timezone.utc)
        except (OverflowError, OSError, ValueError):
            # a worker-reported start at the edge of the calendar keeps its point, without a label
            continue
        ticks.append((index * step * DAY, _day(day) if step < 91 else f"{MONTHS[day.month - 1]} {day.year}"))
    return start, end, ticks


def _describe(item, metric, unit):
    data = item[metric]
    return (f"{rounded(data['value'])} {unit} · {item['date']} · run {item['run']} · commit {item['revision'][:7]}"
            f" · range {rounded(data['low'])}–{rounded(data['high'])} {unit} · " +
            plural(data["samples"], *item["noun"]) +
            ("" if not item["changes"] else " · recorded setup change: " +
             ", ".join(name for name, _, _ in item["changes"])))


def _end_ticks(points):
    """Name the days of the last and the first run, in that order; one run, or two of one day, has one name."""
    first, last = points[0], points[-1]
    # across a new year a date needs its year
    plain = first["day"].rsplit(" ", 1)[1] == last["day"].rsplit(" ", 1)[1]
    name = lambda item: item["day"].rsplit(" ", 1)[0] if plain else item["day"]
    return [(last["at"], name(last))] + ([(first["at"], name(first))] if name(first) != name(last) else [])


def _date_labels(ticks, x, frame):
    """Centre each date on its tick, in order of preference, and return the ones that fit, left to right.

    A label that would cross the right edge is pulled inside the drawing; one that would then run
    into a label already placed is left out.
    """
    placed = []
    for at, label in ticks:
        half = len(label) * TICK_CHARACTER / 2
        centre = min(x(at), frame.width - half - .5)
        if all(abs(centre - other) >= half + extent for other, extent, _, _ in placed):
            placed.append((centre, half, at, label))
    return sorted(placed)


def _tip_top(frame, height):
    """Place the tooltip of a point at `height` above or below it, never over it."""
    if frame.plot_height >= 2 * (TIP_HEIGHT + 10):
        # one side of any point has room for it inside the plot
        return frame.top + 4 if height > frame.top + TIP_HEIGHT + 10 else frame.base - TIP_HEIGHT - 4
    # a plot lower than two tooltips lends them its margins
    return 2 if height >= TIP_HEIGHT + 10 else frame.height - TIP_HEIGHT - 2


def _chart(points, metric, title, unit, frame=DETAIL):
    """Draw one metric as a script-free line chart with a CSS-only hover layer."""
    available = [item for item in points if item[metric] is not None]
    bottom, top, value_ticks = _value_axis(available, metric)
    start, end, day_ticks = _time_axis(points)
    edge, base = frame.width - frame.right, frame.base
    x = lambda at: frame.left + (at - start) / (end - start) * frame.plot_width
    y = lambda value: frame.top + frame.plot_height * (1 - (value - bottom) / (top - bottom))
    # the name is an attribute: a title element would also pop up as a native tooltip over the styled one
    svg = (f"<svg class='chart' xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {frame.width} {frame.height}' "
           "role='group' aria-label='" + escape(f"{title} over worker-reported run start, in {unit}") + "'>")
    for value, label in value_ticks:
        svg += (f"<line class='grid' x1='{frame.left}' x2='{edge}' y1='{y(value):.2f}' y2='{y(value):.2f}'/>"
                f"<text class='ytick' x='{frame.left - 6}' y='{y(value) + 3.5:.2f}' text-anchor='end'>"
                f"{escape(label)}</text>")
    svg += f"<line class='axis' x1='{frame.left}' x2='{edge}' y1='{base}' y2='{base}'/>"
    labels = _date_labels(_end_ticks(points) if frame.ends else day_ticks, x, frame)
    # every whole day keeps its tick; a run is ticked where it is named
    for at in [at for _, _, at, _ in labels] if frame.ends else [at for at, _ in day_ticks]:
        svg += f"<line class='axis' x1='{x(at):.2f}' x2='{x(at):.2f}' y1='{base}' y2='{base + 4}'/>"
    for centre, _, _, label in labels:
        svg += (f"<text class='xtick' x='{centre:.2f}' y='{frame.height - 8}' text-anchor='middle'>"
                f"{escape(label)}</text>")
    occupied = frame.left
    for item in points:
        if not item["changes"]:
            continue
        position = x(item["at"])
        svg += f"<line class='marker' x1='{position:.2f}' x2='{position:.2f}' y1='{frame.top}' y2='{base}'/>"
        label = "new line" if item["starts_segment"] else "setup change"
        width = len(label) * 5.4
        # the label is centred on its line and pulled inside the plot at either edge
        left = min(max(position - width / 2, frame.left), edge - width)
        if left >= occupied:
            # a label is skipped rather than drawn over the previous one
            svg += f"<text class='marker-label' x='{left:.2f}' y='{frame.top - 4}'>{label}</text>"
            occupied = left + width + 6
    joined = []
    for item in [*points, None]:
        if item is None or item[metric] is None or (joined and item["segment"] != joined[-1]["segment"]):
            if len(joined) > 1:
                svg += ("<polyline class='line' points='" + " ".join(
                    f"{x(p['at']):.2f},{y(p[metric]['value']):.2f}" for p in joined) + "' fill='none'/>")
            joined = []
        if item is not None and item[metric] is not None:
            joined.append(item)
    for item in available:
        data, position = item[metric], x(item["at"])
        svg += (f"<line class='range' x1='{position:.2f}' x2='{position:.2f}' y1='{y(data['low']):.2f}' "
                f"y2='{y(data['high']):.2f}'/>"
                f"<circle class='dot' cx='{position:.2f}' cy='{y(data['value']):.2f}' r='4'/>")
    # the hover layer comes last so a revealed tooltip is never painted over by a later mark
    for index, item in enumerate(available):
        data, position, height = item[metric], x(item["at"]), y(item[metric]["value"])
        left = frame.left if index == 0 else (x(available[index - 1]["at"]) + position) / 2
        right = edge if index == len(available) - 1 else (position + x(available[index + 1]["at"])) / 2
        tip_x = min(max(position - TIP_WIDTH / 2, 2), frame.width - TIP_WIDTH - 2)
        tip_y = _tip_top(frame, height)
        svg += ("<a class='pt' href='runs/" + escape(item["run_key"], quote=True) + ".html' aria-label='" +
                escape(_describe(item, metric, unit), quote=True) + "'>"
                f"<rect class='hit' x='{left:.2f}' y='{frame.top}' width='{max(right - left, 1):.2f}' "
                f"height='{frame.plot_height}' fill='none' pointer-events='all'/>"
                "<g class='hover' visibility='hidden' pointer-events='none'>"
                f"<line class='guide' x1='{position:.2f}' x2='{position:.2f}' y1='{frame.top}' y2='{base}'/>"
                f"<circle class='ring' cx='{position:.2f}' cy='{height:.2f}' r='6'/>"
                f"<g class='tip' transform='translate({tip_x:.2f},{tip_y})'>"
                f"<rect width='{TIP_WIDTH}' height='{TIP_HEIGHT}' rx='4'/>"
                "<text x='10' y='21'><tspan class='tip-value'>" + escape(f"{rounded(data['value'])} {unit}") +
                "</tspan><tspan dx='6'>" + escape(f"{rounded(data['low'])}–{rounded(data['high'])}") + "</tspan></text>"
                "<text x='10' y='38'>" + escape(item["date"]) + "</text>"
                "<text x='10' y='53'>" + escape(f"commit {item['revision'][:7]} · " +
                                                 plural(data["samples"], *item["noun"])) + "</text>"
                "</g></g></a>")
    return svg + "</svg>"


def _change(percent):
    """Format a relative change to two decimals; one that rounds to nothing has no minus sign."""
    return f"{round(percent, 2) + 0.0:+.2f}%"


def _opening(figure_id, attribute, heading, row=None):
    """Open a stat tile and its caption; the tile is named by its heading, after its row's title if it has one."""
    return ("<figure class='metric' " + attribute + " aria-labelledby='" + (row + " " if row else "") + figure_id +
            "'><figcaption><h3 id='" + figure_id + "'>" + heading + "</h3>")


def _panel(case, points, metric, title, unit, *, frame=DETAIL, opening=None, notes=()):
    """Pair a stat tile (latest value and change) with the metric's chart.

    A tile of the first page brings its own `opening`, named after its file set instead of its
    metric; `notes` are further lines under the change.
    """
    available = [item for item in points if item[metric] is not None]
    html = opening or _opening("history-" + case + "-" + metric, "data-history-metric='" + metric + "'",
                               escape(title))
    if not available:
        return (html + "<p class='value'>unavailable</p><p class='delta'>No recorded value for these runs; "
                "nothing is filled in.</p></figcaption></figure>")
    latest = available[-1]
    earlier = [item for item in available[:-1] if item["segment"] == latest["segment"]]
    if not earlier:
        change = "first run on this line"
    else:
        previous = earlier[-1][metric]["value"]
        percent = (latest[metric]["value"] / previous - 1) * 100 if previous else math.inf
        change = (_change(percent) + " vs previous point" if math.isfinite(percent) else
                  f"previous point {rounded(previous)} {unit}")
    html += ("<p class='value'>" + escape(rounded(latest[metric]["value"])) + " <span class='unit'>" + unit +
             "</span></p><p class='delta'>" + escape(latest["day"]) + " · " + escape(change) + "</p>" +
             "".join("<p class='delta'>" + escape(note) + "</p>" for note in notes) +
             "</figcaption>" + _chart(points, metric, title, unit, frame))
    missing = len(points) - len(available)
    if missing:
        html += ("<p class='gap'>" + plural(missing, "run has", "runs have") + " no recorded value; " +
                 ("the gap is" if missing == 1 else "the gaps are") + " not filled.</p>")
    return html + "</figure>"


def _decimal(size):
    unit, name = next(((scale, name) for scale, name in ((10 ** 12, "TB"), (10 ** 9, "GB"), (10 ** 6, "MB"),
                                                        (10 ** 3, "KB")) if size >= scale), (1, "bytes"))
    return format(size / unit, "g") + " " + name


def _reading(scenario):
    """One line under a set of plots: what a point and its bar are, and where the rest is explained."""
    each, of = samples(scenario)
    return ("<p class='reading'>Each point is one run: the median of its " + of + ", with a bar from the fastest "
            "to the slowest " + each + ". <a href='setups.html#reading'>How to read these plots</a>.</p>")


def _file_set(case, points, scenario):
    shape = diagnostics.MATRIX_CASE_SHAPES[case]
    files = math.prod(shape["directory_widths"]) * shape["files_per_leaf"]
    how = ("copied locally by rcp with default settings" if scenario == LOCAL else
           "copied by rcp with default settings between two mounts of a network filesystem, where a request to "
           f"the server costs {2 * diagnostics.CONFIGURED_STORAGE['nfs_delay_microseconds'] / 1000:g} ms"
           if scenario == NFS else
           "copied by rcp with default settings between two filesystems in memory" if scenario == MEMORY else
           ACTIONS[scenario] + " after each copy" if scenario in TOOLS else
           "copied by rcp through a source and a destination rcpd over loopback, with default settings")
    html = ("<section id='history-" + case + "' aria-labelledby='history-" + case + "-title'>"
        "<h2 id='history-" + case + "-title'>" + NAMES[case] + "</h2><p class='dataset'>" +
        f"{files:,} files of {_decimal(shape['file_size_bytes'])} each ({_decimal(files * shape['file_size_bytes'])}), "
        + how + " · " + plural(len(points), "run", "runs") +
        "</p><div class='" + layout(scenario) + "'>" +
        "".join(_panel(case, points, metric, title, unit) for metric, title, unit in panels(scenario)) +
        "</div>" + _reading(scenario))
    changed = [item for item in points if item["changes"]]
    if changed:
        html += "<ul class='changes'>"
        for item in changed:
            html += ("<li><time datetime='" + escape(item["timestamp"], quote=True) + "'>" +
                escape(item["day"]) + "</time>, run <a href='runs/" + escape(item["run_key"], quote=True) +
                ".html'>" + escape(item["run"]) + "</a>: " + "; ".join(
                    escape(name) + " changed from <code>" + escape(before) + "</code> to <code>" +
                    escape(after) + "</code>" for name, before, after in item["changes"]) + "." +
                (" The trend starts a new line here." if item["starts_segment"] else "") + "</li>")
        html += "</ul>"
    html += ("<details><summary>Data table</summary><div class='table-scroll'><table class='data'><thead><tr>"
        "<th>Run start (worker-reported)</th><th>Run</th><th>Commit</th><th>" + samples(scenario)[1].capitalize() +
        "</th>" +
        "".join("<th>" + header + "</th>" for _, header in COLUMNS[scenario]) + "<th>Collected</th>"
        "<th>Plotted copies and frozen series</th><th>Observed setup</th></tr></thead><tbody>")
    for item in reversed(points):
        page = "runs/" + escape(item["run_key"], quote=True) + ".html"
        values = ["unavailable" if item[metric] is None else
                  f"{exact(item[metric]['value'])} ({exact(item[metric]['low'])}–{exact(item[metric]['high'])})"
                  for metric, _ in COLUMNS[scenario]]
        html += ("<tr><td>" + time_html(item["timestamp"]) + "</td><td><a href='" + page + "'>" +
            escape(item["run"]) + "</a></td><td><code>" + escape(item["revision"][:7]) + "</code></td><td>" +
            str(item["duration"]["samples"]) + "</td>" + "".join("<td>" + escape(v) + "</td>" for v in values) +
            "<td>" + ("unavailable (legacy record)" if item["collected_at"] is None else
                      time_html(item["collected_at"])) + "</td><td>" +
            ", ".join("<code>" + escape(copy.get("cell", copy["variant"])) + "</code> <code>" +
                      escape(copy["series_id"][:12]) +
                      "</code>" for copy in item["copies"]) +
            "</td><td>" + ("not recorded" if item["setup_id"] is None else
                           "<a href='" + page + "#observed-setup'>recorded</a>") + "</td></tr>")
    return html + "</tbody></table></div></details></section>"


def _render(cases, outside, limit, unmeasured, scenario, several):
    html = "<section id='history' aria-label='Performance history'>"
    html += "".join(_file_set(case, points, scenario) for case, points in cases.items() if points)
    empty = [NAMES[case] for case in unmeasured]
    if empty:
        # once there are two kinds of run, a file set without this kind may well have the other
        html += ("<p class='empty'>No qualified " + (scenario + " " if several else "") +
                 "runs yet for " + (", ".join(empty[:-1]) + " and " if len(empty) > 1 else "") + empty[-1] + ".</p>")
    if limit is not None:
        html += _window(limit, "<a href='index.html#runs'>list of runs</a>")
    # only a tool leaves file sets of the arrays out: a kind of copy plots every set of its storage
    left_out = [case for case in CASES if case not in plotted(scenario)] if scenario in TOOLS else []
    if left_out:
        html += _left_out(scenario, left_out)
    if outside:
        html += ("<p class='window'>" + plural(outside, "qualified measurement is", "qualified measurements are") +
                 " outside these trends (reference builds, other variants, scenarios or file sets); "
                 "see the run pages.</p>")
    return html + "</section>"


def _left_out(scenario, cases):
    """Say which file sets a tool's page does not plot, and why: there the tool takes no measurable time."""
    counts = []
    for case in cases:
        shape = diagnostics.CASE_SHAPES[case]
        counts.append(f"{math.prod(shape['directory_widths']) * shape['files_per_leaf']:,}" +
                      (" files" if not counts else "") + " of the " + CASES[case].replace(" files", "") + " set")
    return ("<p class='window'>" + scenario + " is timed on every file set, but on the " + " and the ".join(counts) +
            " it takes milliseconds: those runs are on the run pages and are not plotted.</p>")


def _window(limit, where):
    """Say that older runs are not plotted, and where every run is still listed."""
    # the list of runs is on the first page, under the plots of every scenario
    return ("<p class='window'>The plots show each file set's newest " +
            plural(limit, "qualified run", "qualified runs") +
            ". Older runs are not plotted; every run stays in the " + where + ".</p>")


def _fit(models, build):
    """Plot each file set's newest qualified runs, and fewer of them while the page exceeds its bound.

    `models` holds the runs of each scenario by file set; `build` renders their plotted windows.
    """
    limit = MAX_PLOT_RUNS
    while True:
        # each file set keeps its own newest runs: one measured weekly is not pushed off the
        # page by one measured every few days
        windows = {scenario: {case: points[max(len(points) - limit, 0):] for case, points in cases.items()}
                   for scenario, cases in models.items()}
        for cases in windows.values():
            for points in cases.values():
                # markers and breaks always relate two plotted runs
                _link(points)
        cut = any(len(points) > limit for cases in models.values() for points in cases.values())
        html = build(windows, limit if cut else None)
        if len(html.encode()) <= projection.MAX_PUBLIC_HTML:
            return html
        require(limit, "AWS history plots exceed public HTML bound")
        limit -= 1


def render(records, companions, scenario=LOCAL):
    """Plot each file set's newest qualified runs under one scenario, with every metric.

    The inventory and frozen records stay unbounded by this.
    """
    cases, outside = model(records, companions, scenario)
    # a file set is unmeasured only when no run ever measured it, whatever the plots show
    unmeasured = [case for case, points in cases.items() if not points]
    several = scenario != LOCAL or len(present(records)) > 1
    return _fit({scenario: cases}, lambda windows, limit: _render(
        windows[scenario], outside, limit, unmeasured, scenario, several))


def _tile(case, points, scenario, page, beside, row=None):
    """One file set's copy duration under one scenario; `beside` holds its local copies, run by run.

    `row` is the title of the row the tile stands in, when that is not the row of its own scenario.
    """
    name = NAMES[case]
    # both rows have a tile of this name: its link and its plot say which copy they are, for a reader
    # who meets them in a list instead of in their row
    link = ("<a href='" + page + "#history-" + case + "' aria-label='" + name + ", " + scenario + " copies'>" +
            name + "</a>")
    opening = _opening("overview-" + scenario + "-" + case, "data-overview='" + scenario + "'",
                       link if points else name, row or "overview-" + scenario + "-title")
    if not points:
        # the tile keeps its place, so that a column is the same file set in every row
        return opening + "</figcaption><p class='delta'>No qualified run yet.</p></figure>"
    notes = []
    if scenario == REMOTE:
        # how the remote path compares with a local copy is read within one run: same build, same machine
        latest = points[-1]
        local = next((item for item in beside if item["run_key"] == latest["run_key"]), None)
        if local is not None and local["duration"]["value"] > 0:
            percent = (latest["duration"]["value"] / local["duration"]["value"] - 1) * 100
            if math.isfinite(percent):
                notes.append(_change(percent) + " vs local copy")
    return _panel(case, points, "duration", name + ", " + scenario + " copy duration", "s", frame=COMPACT,
                  opening=opening, notes=notes)


def _tool_tile(scenario, points):
    """One tool's duration on the file set with the most files, leading to that tool's page."""
    title, page, _ = ROWS[scenario]
    link = ("<a href='" + page + "#history-" + MOST_FILES + "' aria-label='" + title + ", " + CASES[MOST_FILES] +
            "'>" + title + "</a>")
    opening = _opening("overview-" + scenario + "-" + MOST_FILES, "data-overview='" + scenario + "'",
                       link if points else title, "overview-tools-title")
    if not points:
        # the tile keeps its place, so that the row always names the same tools in the same order
        return opening + "</figcaption><p class='delta'>No qualified run yet.</p></figure>"
    return _panel(MOST_FILES, points, "duration", title + ", duration", "s", frame=COMPACT, opening=opening)


def _tools_row(windows):
    """The row of the other tools: each on the one file set where it has the most to do."""
    shape = diagnostics.CASE_SHAPES[MOST_FILES]
    files = math.prod(shape["directory_widths"]) * shape["files_per_leaf"]
    return ("<section id='overview-tools' aria-labelledby='overview-tools-title'>"
            "<div class='overview-title'><h2 id='overview-tools-title'>Other tools</h2><span>" +
            f"{files:,} files of {_decimal(shape['file_size_bytes'])}, after each copy" + "</span></div>"
            "<div class='overview'>" + "".join(
                _tool_tile(scenario, windows[scenario][MOST_FILES]) for scenario in TOOLS if scenario in windows) +
            "</div></section>")


def _storage_row(windows):
    """The row of the copies on other storage: each kind on its one file set, leading to its page."""
    tiles = "".join(_tile(case, points, scenario, ROWS[scenario][1], (), row="overview-storage-title")
                    for scenario in (NFS, MEMORY) if scenario in windows
                    for case, points in windows[scenario].items())
    return ("<section id='overview-storage' aria-labelledby='overview-storage-title'>"
            "<div class='overview-title'><h2 id='overview-storage-title'>Copies on other storage</h2>"
            "<span>between two NFS mounts, and between two filesystems in memory</span></div>"
            "<div class='overview'>" + tiles + "</div></section>")


def _overview(windows, limit, several, local):
    html = "<section id='history' aria-label='Copy duration of every file set'>"
    for scenario, cases in windows.items():
        if scenario in (NFS, MEMORY, *TOOLS):
            # the copies on other storage share one row after the copies on the arrays, and the
            # tools one after that
            continue
        title, page, more = ROWS[scenario]
        measured = any(cases.values())
        # one line heads a row, its title and the way to its page: both rows are meant to fit one screen
        html += ("<section id='overview-" + scenario + "' aria-labelledby='overview-" + scenario + "-title'>"
                 "<div class='overview-title'><h2 id='overview-" + scenario + "-title'>" + title + "</h2>" +
                 ("<a href='" + page + "'>" + more + "</a>" if measured else "") + "</div>")
        if measured:
            html += ("<div class='overview'>" + "".join(
                _tile(case, points, scenario, page, local[case]) for case, points in cases.items()) + "</div>")
        else:
            # once there are two kinds of run, a row without this kind may well have the other
            html += "<p class='empty'>No qualified " + (scenario + " " if several else "") + "runs yet.</p>"
        html += "</section>"
    if NFS in windows or MEMORY in windows:
        html += _storage_row(windows)
    tools = any(scenario in TOOLS for scenario in windows)
    if tools:
        html += _tools_row(windows)
    if any(points for cases in windows.values() for points in cases.values()):
        html += ("<p class='reading'>Each point is one run: the median of its copies" +
                 (" or of a tool's runs" if tools else "") + ", with a bar from the fastest "
                 "to the slowest" + ("" if tools else " copy") +
                 ". <a href='setups.html#reading'>How to read these plots</a>.</p>")
    if limit is not None:
        html += _window(limit, "list below")
    return html + "</section>"


def overview(records, companions):
    """Show the copy duration of every file set at once: a row of small plots for each kind of copy.

    The other metrics and the data tables are on the page of each scenario. A row for remote
    copies appears with the first run that has a remote cell, one for the copies on other storage
    with the first run that has a cell there, and a row for the other tools with the first run
    that has a cell of one.
    """
    shown = present(records)
    several = len(shown) > 1
    models = {scenario: model(records, companions, scenario)[0] for scenario in shown if scenario not in TOOLS}
    # the other tools appear with the first run that has a cell of one, on the file set with the most files
    models.update({scenario: {MOST_FILES: model(records, companions, scenario)[0][MOST_FILES]}
                   for scenario in shown if scenario in TOOLS})
    # a remote plot is compared with the local copy of its own run, whether or not that copy is still plotted
    return _fit(models, lambda windows, limit: _overview(windows, limit, several, models[LOCAL]))


def storage_html(run, href, rows):
    """Tabulate what the arrays and the processors did during one run's copies, in the order of the trends.

    `rows` are those of `diagnostics.storage_use`; file sets without a trend are left out, and so is
    the whole table when nothing is left.
    """
    rows = sorted((row for row in rows if row["case_id"] in CASES and row["scenario"] in SCENARIOS),
                  key=lambda row: (list(CASES).index(row["case_id"]), list(SCENARIOS).index(row["scenario"])))
    if not rows:
        return ""
    rate = lambda value: "unavailable" if value is None else f"{value / 10 ** 9:.2f} GB/s"
    share = lambda value: "unavailable" if value is None else f"{value * 100:.0f}%"
    # a table that also has rows of the other tools says so; one of copies alone reads as it always did
    tools = any(row["scenario"] in TOOL_CELLS for row in rows)
    html = ("<p>What the arrays and the processors did during the copies " + ("and the tool runs " if tools else "") +
            "of run <a href='" +
            escape(href, quote=True) + "'>" + escape(run) + "</a>, the newest that recorded it. Each figure is the "
            "median of the copies" + (", or of the runs of a tool," if tools else "") + " of that file set.</p>"
            "<div class='table-scroll'><table class='data'><thead><tr><th>File set</th><th>" +
            ("Scenario" if tools else "Copy") + "</th>"
            "<th>Written to the destination</th><th>Destination busy</th><th>Read from the source</th>"
            "<th>Source busy</th><th>CPU busy</th></tr></thead><tbody>")
    for row in rows:
        cells = (CASES[row["case_id"]], SCENARIOS[row["scenario"]], rate(row["written"]),
                 share(row["destination_busy"]), rate(row["read"]), share(row["source_busy"]), share(row["cpu"]))
        html += "<tr>" + "".join("<td>" + escape(cell) + "</td>" for cell in cells) + "</tr>"
    return (html + "</tbody></table></div><p class='reading'>The figures come from the kernel's counters for "
            "each array as a whole, not its drives. They are read every ten seconds, so they cover each copy from "
            "its first reading to its last, up to ten seconds less than the whole" +
            ("; a tool run shorter than that has no figures" if tools else "") + ". Busy is the share of that time "
            "during which the array had a request in progress. CPU busy is the share of all processors' time that "
            "was neither idle nor waiting for I/O.</p>")


def reading_html(remote=False, tools=False):
    """Explain once how points, lines and gaps are derived.

    `remote` when remote copies have a page, `tools` when the other tools have theirs.
    """
    other = ("" if not tools else
        "<li><strong>Other tools.</strong> A run can also time <code>rcmp</code>, <code>rlink</code> and "
        "<code>rrm</code> on its copies: comparing a copy with its source, hard-linking it into a new tree, and "
        "removing it. Each tool has a page of its own (" + ", ".join(
            '<a href="' + ROWS[tool][1] + '">' + tool + "</a>" for tool in TOOLS) + "), where a point is the median "
        "of the tool's runs in one benchmark run: three after the local copies of a file set and, when the run's "
        "suite has them, three after its remote copies. CPU time and peak memory are the tool's own. The AWS "
        "benchmarks page shows each tool on the file set with the most files. A tool's page leaves out the file "
        "sets on which it takes milliseconds.</li>")
    scope = (
        "<li><strong>Scope.</strong> The plots show local copies on one host. The AWS benchmarks page shows "
        'the copy duration of every file set; <a href="local.html">Local copies</a> adds CPU time, peak memory '
        "and the data tables. A run counts for every file set it measured; a file set it did not finish has no "
        "point for that run, and its run page says why. Failed and unavailable runs stay in the run list "
        "without performance points.</li>" if not remote else
        "<li><strong>Scope.</strong> The AWS benchmarks page shows the copy duration of every file set, for "
        "local copies on one host and for remote copies over loopback. Each kind has a page of its own with CPU "
        'time, peak memory and the data tables: <a href="local.html">local copies</a> and '
        '<a href="remote.html">remote copies</a>. A run counts for every file set and scenario it measured; one '
        "it did not finish has no point for that run, and its run page says why. Failed and unavailable runs "
        "stay in the run list without performance points.</li>"
        "<li><strong>Remote copies.</strong> A remote copy is three processes: the <code>rcp</code> command, "
        "and the source and the destination <code>rcpd</code> that it starts over SSH and that move the data. "
        "Copy duration is the wall time of the <code>rcp</code> command. Each process reports its own CPU "
        "time and peak memory: they are plotted for each daemon, and the command's own figures are in each "
        "data table.</li>"
        "<li><strong>Against the local copy.</strong> On the AWS benchmarks page, the plot of a remote copy "
        "also gives its latest run's median relative to the same run's local copy of that file set: the same "
        "build on the same machine, copied both ways. The comparison is left out when that run made no local "
        "copy of the file set.</li>"
        "<li><strong>Dates.</strong> A plot of the AWS benchmarks page names the days of its first and its "
        "last run; the plots of the other pages are ticked at whole days.</li>")
    return ("<section aria-labelledby='reading'><h2 id='reading'>Reading the plots</h2><ul>"
        "<li><strong>Point.</strong> One qualified run, summarizing the copies of one file set made by the "
        "build under test with rcp's default settings: three copies in a matrix run, four in a paired run. "
        "In a paired run all four count when its baseline and candidate binaries are identical; otherwise "
        "only the candidate's two do.</li>"
        "<li><strong>Value and bar.</strong> The value is the median of those copies. The bar spans the fastest "
        "and slowest copy: repeat minima and maxima, not confidence intervals. A few copies do not establish "
        "a performance change.</li>"
        "<li><strong>Time axis.</strong> The benchmark start recorded by the worker, before fixture "
        "preparation; not per-copy start, collection or retirement time.</li>"
        "<li><strong>Value axis.</strong> Fitted to the data and never narrower than 5% either side of its "
        "centre, so it does not start at zero and small run-to-run noise stays flat.</li>"
        "<li><strong>Copy duration.</strong> Copy wall time. Fixture generation, cache preparation, validation "
        "and cleanup are outside it. For the terabyte file sets it cannot be shorter than the destination array "
        "allows: see <a href='#storage'>What the storage allows</a>.</li>"
        "<li><strong>CPU time.</strong> User plus system seconds, added for each copy before the median is "
        "taken; not a sum of repetitions.</li>"
        "<li><strong>Peak memory.</strong> Peak command RSS, converted from KiB to MiB. CPU and RSS cover the "
        "copy command with all its threads; they are not whole-host totals or simultaneous summed process "
        "memory. Matrix runs record what rcp reports about itself in its summary, read from the kernel's "
        "accounting of its own process. Paired runs are measured from outside with GNU <code>time</code>.</li>"
        "<li><strong>Gaps.</strong> A run without complete resource metrics for every plotted copy has no CPU "
        "or memory point. Missing values stay unavailable, never zero, and no line is drawn across them.</li>"
        "<li><strong>Lines.</strong> Runs of a file set are joined in time order across code versions, harness "
        "revisions and machine instances. The trend starts a new line when the measurement contract changes "
        "or a recorded hardware value differs from the last recorded one: CPU model or count, architecture, "
        "storage devices, layout or filesystem. Any other recorded change, such as the kernel, is marked on "
        "the line. A field that an older run did not record is not a change.</li>"
        + scope + other + "</ul></section>")
