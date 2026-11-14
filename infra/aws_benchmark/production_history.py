"""Bounded presentation-only history of rcp performance from frozen public records.

The history follows the build under test with default settings: one trend per
file set, joined across code versions. Frozen records and series IDs are read,
never rewritten; every emitted string is escaped and no script is produced.
"""
from datetime import datetime, timezone
from html import escape
import math
import sys

from . import production_diagnostics as diagnostics
from . import production_projection as projection
from .common import require
from .results import utc

MAX_PLOT_RUNS = 32
# the build under test with default settings, and the same command run with a reference build
CURRENT, REFERENCE = "rcp-default", "rcp-baseline"
CASES = {
    "bulk-1g-1tb": "1 GB files",
    "bulk-100m-1024gb": "100 MB files",
    "bulk-1m-1024gb": "1 MB files",
    "tiny-1k-10240k": "1 KB files",
}
METRICS = (
    ("duration", "Copy duration", "s"),
    ("cpu", "CPU time", "s"),
    ("memory", "Peak memory", "MiB"),
)
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
DAY = 86400
# chart geometry in viewBox units; the plot area is 302 by 160
WIDTH, HEIGHT, LEFT, RIGHT, TOP, BOTTOM = 360, 200, 46, 12, 14, 26
PLOT_WIDTH, PLOT_HEIGHT, BASE = WIDTH - LEFT - RIGHT, HEIGHT - TOP - BOTTOM, HEIGHT - BOTTOM
TIP_WIDTH, TIP_HEIGHT = 196, 62


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


def _points(value, extra):
    """Summarize the build under test for each file set of one qualified record."""
    started, identity = utc(value["timestamp"]), value["identity"]
    # identical binaries make the reference copies further samples of the same build
    roles = (CURRENT, REFERENCE) if value["comparison"] == projection.IDENTICAL_BINARIES else (CURRENT,)
    selected, outside, seen = {}, 0, set()
    for measurement in value["measurements"]:
        key = (measurement["case_id"], measurement["variant_id"])
        require(key not in seen, "duplicate history case and variant")
        seen.add(key)
        if key[0] in CASES and key[1] in roles:
            selected.setdefault(key[0], []).append(measurement)
        else:
            outside += 1
    points = {}
    for case, items in selected.items():
        trials = [] if extra is None else [row for row in extra["trials"] if row["case_id"] == case and
                                           any(row["variant_id"] == item["variant_id"] for item in items)]
        # resources count only when every plotted copy has them; a subset would cherry-pick repeats
        complete = all(sum(row["variant_id"] == item["variant_id"] for row in trials) == len(item["samples"])
                       for item in items) and all(row["status"] == "complete" and row["metrics"] is not None
                                                  for row in trials)
        points[case] = dict(
            run_key=value["run_key"], run=identity["run"], revision=identity["candidate"],
            timestamp=started.isoformat().replace("+00:00", "Z"), at=started.timestamp(),
            date=moment(started), day=f"{_day(started)} {started.year}",
            collected_at=value.get("collected_at"),
            pooled=len(items) > 1, series_contract=value["series_contract"],
            copies=[dict(variant=item["variant_id"], series_id=item["series_id"]) for item in items],
            setup=None if extra is None else extra["setup"],
            setup_id=None if extra is None else extra["setup_id"],
            duration=_summary([x for item in items for x in item["samples"]],
                              frozen=items[0]["median"] if len(items) == 1 else None),
            cpu=_summary([row["metrics"]["user_seconds"] + row["metrics"]["system_seconds"]
                          for row in trials]) if complete else None,
            memory=_summary([row["metrics"]["max_rss_kib"] / 1024 for row in trials]) if complete else None)
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


def model(records, companions):
    """Summarize each qualified run per file set and link the runs in time order."""
    require(isinstance(records, dict) and len(records) <= projection.MAX_PUBLIC_HISTORY_RUNS and
            isinstance(companions, dict) and set(companions) <= set(records),
            "history input exceeds its bounded record set")
    cases, outside = {case: [] for case in CASES}, 0
    for run_key, value in sorted(records.items()):
        projection.validate_public(value)
        require(value["run_key"] == run_key, "history record identity differs")
        extra = companions.get(run_key)
        if extra is not None:
            diagnostics.validate(extra, value)
        if not value["benchmark_valid"]:
            continue
        points, skipped = _points(value, extra)
        outside += skipped
        for case, item in points.items():
            cases[case].append(item)
    for points in cases.values():
        points.sort(key=lambda item: (item["at"], item["run_key"]))
        _link(points)
    return cases, outside


def point(value, extra):
    """Summarize one record per file set exactly as its history point is plotted."""
    return _points(value, extra)[0] if value["benchmark_valid"] else {}


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
            plural(data["samples"], "copy", "copies") +
            ("" if not item["changes"] else " · recorded setup change: " +
             ", ".join(name for name, _, _ in item["changes"])))


def _chart(points, metric, title, unit, figure_id):
    """Draw one metric as a script-free line chart with a CSS-only hover layer."""
    available = [item for item in points if item[metric] is not None]
    bottom, top, value_ticks = _value_axis(available, metric)
    start, end, day_ticks = _time_axis(points)
    x = lambda at: LEFT + (at - start) / (end - start) * PLOT_WIDTH
    y = lambda value: TOP + PLOT_HEIGHT * (1 - (value - bottom) / (top - bottom))
    # the name is an attribute: a title element would also pop up as a native tooltip over the styled one
    svg = (f"<svg class='chart' xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {WIDTH} {HEIGHT}' "
           "role='group' aria-label='" + escape(f"{title} over worker-reported run start, in {unit}") + "'>")
    for value, label in value_ticks:
        svg += (f"<line class='grid' x1='{LEFT}' x2='{WIDTH - RIGHT}' y1='{y(value):.2f}' y2='{y(value):.2f}'/>"
                f"<text class='ytick' x='{LEFT - 6}' y='{y(value) + 3.5:.2f}' text-anchor='end'>{escape(label)}</text>")
    svg += f"<line class='axis' x1='{LEFT}' x2='{WIDTH - RIGHT}' y1='{BASE}' y2='{BASE}'/>"
    for at, label in day_ticks:
        svg += (f"<line class='axis' x1='{x(at):.2f}' x2='{x(at):.2f}' y1='{BASE}' y2='{BASE + 4}'/>"
                f"<text class='xtick' x='{x(at):.2f}' y='{HEIGHT - 8}' text-anchor='middle'>{escape(label)}</text>")
    occupied = LEFT
    for item in points:
        if not item["changes"]:
            continue
        position = x(item["at"])
        svg += f"<line class='marker' x1='{position:.2f}' x2='{position:.2f}' y1='{TOP}' y2='{BASE}'/>"
        label = "new line" if item["starts_segment"] else "setup change"
        width = len(label) * 5.4
        # the label is centred on its line and pulled inside the plot at either edge
        left = min(max(position - width / 2, LEFT), WIDTH - RIGHT - width)
        if left >= occupied:
            # a label is skipped rather than drawn over the previous one
            svg += f"<text class='marker-label' x='{left:.2f}' y='{TOP - 4}'>{label}</text>"
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
        left = LEFT if index == 0 else (x(available[index - 1]["at"]) + position) / 2
        right = WIDTH - RIGHT if index == len(available) - 1 else (position + x(available[index + 1]["at"])) / 2
        tip_x = min(max(position - TIP_WIDTH / 2, 2), WIDTH - TIP_WIDTH - 2)
        tip_y = TOP + 4 if height > TOP + TIP_HEIGHT + 10 else BASE - TIP_HEIGHT - 4
        svg += ("<a class='pt' href='runs/" + escape(item["run_key"], quote=True) + ".html' aria-label='" +
                escape(_describe(item, metric, unit), quote=True) + "'>"
                f"<rect class='hit' x='{left:.2f}' y='{TOP}' width='{max(right - left, 1):.2f}' "
                f"height='{PLOT_HEIGHT}' fill='none' pointer-events='all'/>"
                "<g class='hover' visibility='hidden' pointer-events='none'>"
                f"<line class='guide' x1='{position:.2f}' x2='{position:.2f}' y1='{TOP}' y2='{BASE}'/>"
                f"<circle class='ring' cx='{position:.2f}' cy='{height:.2f}' r='6'/>"
                f"<g class='tip' transform='translate({tip_x:.2f},{tip_y})'>"
                f"<rect width='{TIP_WIDTH}' height='{TIP_HEIGHT}' rx='4'/>"
                "<text x='10' y='21'><tspan class='tip-value'>" + escape(f"{rounded(data['value'])} {unit}") +
                "</tspan><tspan dx='6'>" + escape(f"{rounded(data['low'])}–{rounded(data['high'])}") + "</tspan></text>"
                "<text x='10' y='38'>" + escape(item["date"]) + "</text>"
                "<text x='10' y='53'>" + escape(f"commit {item['revision'][:7]} · " +
                                                 plural(data["samples"], "copy", "copies")) + "</text>"
                "</g></g></a>")
    return svg + "</svg>"


def _panel(case, points, metric, title, unit):
    """Pair a stat tile (latest value and change) with the metric's chart."""
    figure_id = "history-" + case + "-" + metric
    available = [item for item in points if item[metric] is not None]
    html = ("<figure class='metric' data-history-metric='" + metric + "' aria-labelledby='" + figure_id +
            "'><figcaption><h3 id='" + figure_id + "'>" + escape(title) + "</h3>")
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
        change = (f"{percent:+.2f}% vs previous point" if math.isfinite(percent) else
                  f"previous point {rounded(previous)} {unit}")
    html += ("<p class='value'>" + escape(rounded(latest[metric]["value"])) + " <span class='unit'>" + unit +
             "</span></p><p class='delta'>" + escape(latest["day"]) + " · " + escape(change) +
             "</p></figcaption>" + _chart(points, metric, title, unit, figure_id))
    missing = len(points) - len(available)
    if missing:
        html += ("<p class='gap'>" + plural(missing, "run has", "runs have") + " no recorded value; " +
                 ("the gap is" if missing == 1 else "the gaps are") + " not filled.</p>")
    return html + "</figure>"


def _decimal(size):
    unit, name = next(((scale, name) for scale, name in ((10 ** 12, "TB"), (10 ** 9, "GB"), (10 ** 6, "MB"),
                                                        (10 ** 3, "KB")) if size >= scale), (1, "bytes"))
    return format(size / unit, "g") + " " + name


def _file_set(case, points):
    shape = diagnostics.CASE_SHAPES[case]
    files = math.prod(shape["directory_widths"]) * shape["files_per_leaf"]
    html = ("<section id='history-" + case + "' aria-labelledby='history-" + case + "-title'>"
        "<h2 id='history-" + case + "-title'>" + CASES[case] + "</h2><p class='dataset'>" +
        f"{files:,} files of {_decimal(shape['file_size_bytes'])} each ({_decimal(files * shape['file_size_bytes'])}), "
        "copied locally by rcp with default settings · " + plural(len(points), "run", "runs") +
        "</p><div class='metrics'>" +
        "".join(_panel(case, points, metric, title, unit) for metric, title, unit in METRICS) +
        "</div><p class='reading'>Each point is one run: the median of its copies, with a bar from the fastest "
        "to the slowest copy. <a href='setups.html#reading'>How to read these plots</a>.</p>")
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
        "<th>Run start (worker-reported)</th><th>Run</th><th>Commit</th><th>Copies</th>"
        "<th>Duration s: median (range)</th><th>CPU s</th><th>Peak RSS MiB</th><th>Collected</th>"
        "<th>Plotted copies and frozen series</th><th>Observed setup</th></tr></thead><tbody>")
    for item in reversed(points):
        page = "runs/" + escape(item["run_key"], quote=True) + ".html"
        values = ["unavailable" if item[metric] is None else
                  f"{exact(item[metric]['value'])} ({exact(item[metric]['low'])}–{exact(item[metric]['high'])})"
                  for metric, _, _ in METRICS]
        html += ("<tr><td>" + time_html(item["timestamp"]) + "</td><td><a href='" + page + "'>" +
            escape(item["run"]) + "</a></td><td><code>" + escape(item["revision"][:7]) + "</code></td><td>" +
            str(item["duration"]["samples"]) + "</td>" + "".join("<td>" + escape(v) + "</td>" for v in values) +
            "<td>" + ("unavailable (legacy record)" if item["collected_at"] is None else
                      time_html(item["collected_at"])) + "</td><td>" +
            ", ".join("<code>" + escape(copy["variant"]) + "</code> <code>" + escape(copy["series_id"][:12]) +
                      "</code>" for copy in item["copies"]) +
            "</td><td>" + ("not recorded" if item["setup_id"] is None else
                           "<a href='" + page + "#observed-setup'>recorded</a>") + "</td></tr>")
    return html + "</tbody></table></div></details></section>"


def _render(cases, outside, omitted):
    html = "<section id='history' aria-label='Performance history'>"
    html += "".join(_file_set(case, points) for case, points in cases.items() if points)
    empty = [CASES[case] for case, points in cases.items() if not points]
    if empty:
        html += ("<p class='empty'>No qualified runs yet for " +
                 (", ".join(empty[:-1]) + " and " if len(empty) > 1 else "") + empty[-1] + ".</p>")
    if omitted:
        html += ("<p class='window'>The plots show the newest qualified runs. " +
                 plural(omitted, "older qualified run is", "older qualified runs are") +
                 " not plotted; every run stays in the list below.</p>")
    if outside:
        html += ("<p class='window'>" + plural(outside, "qualified measurement is", "qualified measurements are") +
                 " outside these trends (reference builds, other variants or other file sets); "
                 "see the run pages.</p>")
    return html + "</section>"


def render(records, companions):
    """Plot the newest qualified runs; the complete inventory and frozen records stay unbounded by this."""
    cases, outside = model(records, companions)
    started = {item["run_key"]: item["at"] for points in cases.values() for item in points}
    selected = sorted(started, key=lambda key: (started[key], key), reverse=True)[:MAX_PLOT_RUNS]
    while True:
        chosen = set(selected)
        window = {case: [item for item in points if item["run_key"] in chosen] for case, points in cases.items()}
        for points in window.values():
            # markers and breaks always relate two plotted runs
            _link(points)
        html = _render(window, outside, len(started) - len(selected))
        if len(html.encode()) <= projection.MAX_PUBLIC_HTML:
            return html
        require(selected, "AWS history plots exceed public HTML bound")
        selected.pop()


def reading_html():
    """Explain once how points, lines and gaps are derived."""
    return ("<section aria-labelledby='reading'><h2 id='reading'>Reading the plots</h2><ul>"
        "<li><strong>Point.</strong> One qualified run, summarizing the copies made by the build under test "
        "with rcp's default settings. When a run's baseline and candidate binaries are identical, all of its "
        "copies count; otherwise only the candidate's copies do.</li>"
        "<li><strong>Value and bar.</strong> The value is the median of those copies. The bar spans the fastest "
        "and slowest copy: repeat minima and maxima, not confidence intervals. A few copies do not establish "
        "a performance change.</li>"
        "<li><strong>Time axis.</strong> The frozen, worker-reported benchmark start, recorded before fixture "
        "preparation; not per-copy start, collection or retirement time. It is not independently attested.</li>"
        "<li><strong>Value axis.</strong> Fitted to the data and never narrower than 5% either side of its "
        "centre, so it does not start at zero and small run-to-run noise stays flat.</li>"
        "<li><strong>Copy duration.</strong> Copy wall time. Fixture generation, cache preparation, validation "
        "and cleanup are outside it.</li>"
        "<li><strong>CPU time.</strong> User plus system seconds, added for each copy before the median is "
        "taken; not a sum of repetitions.</li>"
        "<li><strong>Peak memory.</strong> Peak command RSS, converted from KiB to MiB. CPU and RSS cover the "
        "command, its threads and waited descendants; they are not whole-host totals or simultaneous summed "
        "process memory.</li>"
        "<li><strong>Gaps.</strong> A run without complete resource metrics for every plotted copy has no CPU "
        "or memory point. Missing values stay unavailable, never zero, and no line is drawn across them.</li>"
        "<li><strong>Lines.</strong> Runs of a file set are joined in time order across code versions, harness "
        "revisions and machine instances. The trend starts a new line when the measurement contract changes "
        "or a recorded hardware value differs from the last recorded one: CPU model or count, architecture, "
        "storage devices, layout or filesystem. Any other recorded change, such as the kernel, is marked on "
        "the line. A field that an older run did not record is not a change.</li>"
        "<li><strong>Scope.</strong> Current <code>aws-local-ext4-v1</code> runs are local copies; future "
        "remote modes require their own validated contract. Only qualified runs are plotted. Failed "
        "and unavailable runs stay in the run list without performance points.</li>"
        "</ul></section>")
