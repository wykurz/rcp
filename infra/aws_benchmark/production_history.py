"""Bounded presentation-only history plots from frozen public records."""
from datetime import timezone
from html import escape
import math
from statistics import median

from . import production_diagnostics as diagnostics
from . import production_projection as projection
from .common import require
from .results import utc

CONTRACT = "rcp-public-history-plots-v1"
MAX_PLOT_RUNS = 32
MAX_PLOT_POINTS = 256
CASES = {
    "bulk-1g-1tb": "1 GB files",
    "bulk-100m-1024gb": "100 MB files",
    "bulk-1m-1024gb": "1 MB files",
    "tiny-1k-10240k": "1 KB files",
}
METRICS = (
    ("duration", "Copy duration", "seconds"),
    ("cpu", "Command CPU: user + system", "seconds"),
    ("memory", "Peak command RSS", "MiB"),
)
COLORS = ("#397cc3", "#c25c24", "#398267", "#9856a5", "#b23d58", "#807128", "#52708c", "#8c6245")


def _summary(values, *, frozen=None):
    require(values and all(math.isfinite(x) and x >= 0 for x in values), "invalid history metric")
    return dict(value=median(values) if frozen is None else frozen,
                low=min(values), high=max(values), samples=len(values))


def model(records, companions):
    """Keep duration independent of optional resources; never pool unknown setups."""
    require(isinstance(records, dict) and len(records) <= 512 and
            isinstance(companions, dict) and set(companions) <= set(records),
            "history input exceeds its bounded record set")
    cases = {case: [] for case in CASES}
    unsupported = 0
    for run_key, value in sorted(records.items()):
        projection.validate_public(value)
        require(value["run_key"] == run_key, "history record identity differs")
        extra = companions.get(run_key)
        if extra is not None:
            diagnostics.validate(extra, value)
        if not value["benchmark_valid"]:
            continue
        started = utc(value["timestamp"]).astimezone(timezone.utc)
        require(started.tzinfo is not None, "history time needs a UTC offset")
        normalized = started.isoformat().replace("+00:00", "Z")
        setup = extra["setup_id"] if extra is not None else None
        schema = extra["schema"] if extra is not None else "resources-unavailable"
        scope = extra["process_scope"] if extra is not None else "scope-unavailable"
        seen = set()
        for measurement in value["measurements"]:
            case, variant = measurement["case_id"], measurement["variant_id"]
            require((case, variant) not in seen, "duplicate history case and variant")
            seen.add((case, variant))
            if case not in cases:
                unsupported += 1
                continue
            trials = [] if extra is None else [
                row for row in extra["trials"] if row["case_id"] == case and row["variant_id"] == variant]
            complete = (len(trials) == len(measurement["samples"]) and
                        all(row["status"] == "complete" and row["metrics"] is not None for row in trials))
            cpu = _summary([row["metrics"]["user_seconds"] + row["metrics"]["system_seconds"]
                            for row in trials]) if complete else None
            memory = _summary([row["metrics"]["max_rss_kib"] / 1024 for row in trials]) if complete else None
            # frozen series IDs include the measured experiment identity. Supplementary
            # setup and metric contracts split traces without rewriting those IDs.
            key = (CONTRACT, value["series_contract"], variant, measurement["series_id"],
                   setup if setup is not None else "unobserved-" + run_key, schema, scope)
            cases[case].append(dict(
                run_key=run_key, run=value["identity"]["run"], variant=variant,
                timestamp=normalized, at=started.timestamp(), date=normalized[:16].replace("T", " "),
                collected_at=projection.collected_time(value), group=key,
                series_id=measurement["series_id"], series_contract=value["series_contract"],
                setup_id=setup,
                duration=_summary(measurement["samples"], frozen=measurement["median"]),
                cpu=cpu, memory=memory))
    for points in cases.values():
        points.sort(key=lambda point: (point["at"], point["run_key"], point["variant"]))
    return cases, unsupported


def _number(value):
    return format(value, ".6g")


def _label(point):
    setup = point["setup_id"][:12] if point["setup_id"] is not None else "not recorded (" + point["run"] + ")"
    return (point["variant"] + " · " + point["series_contract"] + " · series " +
            point["series_id"][:12] + " · setup " + setup)


def _svg(points, metric, title, unit, figure_id):
    available = [point for point in points if point[metric] is not None]
    if not available:
        return "<p>No recorded qualified " + escape(title.lower()) + " samples for this file set.</p>"
    width, height, left, right, top, bottom = 800, 270, 78, 20, 18, 58
    plot_width, plot_height = width - left - right, height - top - bottom
    xmin, xmax = min(p["at"] for p in points), max(p["at"] for p in points)
    if xmin == xmax:
        xmin, xmax = xmin - 3600, xmax + 3600
    ymax = max(max(p[metric]["value"], p[metric]["high"]) for p in available)
    ymax = ymax if ymax else 1
    x = lambda point: left + (point["at"] - xmin) / (xmax - xmin) * plot_width
    y = lambda value: top + plot_height * (1 - .95 * (value / ymax))
    html = ("<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 800 270' "
            "role='img' aria-labelledby='" + figure_id + "-svg-title' "
            "style='width:100%;height:auto'><title id='" + figure_id + "-svg-title'>" +
            escape(title + " over worker-reported run start time; " + unit) + "</title>")
    for step in range(5):
        value = ymax * (step / 4)
        yp = y(value)
        html += (f"<line x1='{left}' x2='{width-right}' y1='{yp:.3f}' y2='{yp:.3f}' "
                 "stroke='var(--border)'/><text x='68' y='" + f"{yp+4:.3f}" +
                 "' text-anchor='end' fill='var(--text)' font-size='12'>" +
                 escape(format(value, ".3g")) + "</text>")
    html += (f"<line x1='{left}' x2='{width-right}' y1='{top+plot_height}' y2='{top+plot_height}' "
             "stroke='var(--text)'/>")
    extrema = [min(points, key=lambda p: p["at"]), max(points, key=lambda p: p["at"])]
    ticks = list({p["at"]: p for p in extrema}.values())
    for index, point in enumerate(ticks):
        anchor = "middle" if len(ticks) == 1 else ("start" if index == 0 else "end")
        html += (f"<text x='{x(point):.3f}' y='{height-34}' text-anchor='{anchor}' "
                 "fill='var(--text)' font-size='11'>" + escape(point["date"]) + "</text>")
    html += (f"<text x='{left+plot_width/2:.3f}' y='{height-10}' text-anchor='middle' "
             "fill='var(--text)' font-size='12'>Worker-reported run start (UTC)</text>")
    groups = sorted({p["group"] for p in points})
    for index, group in enumerate(groups):
        color = COLORS[index % len(COLORS)]
        segment = []
        def flush():
            if len(segment) < 2:
                return ""
            coordinates = " ".join(f"{x(p):.3f},{y(p[metric]['value']):.3f}" for p in segment)
            return "<polyline points='" + coordinates + "' fill='none' stroke='" + color + "' stroke-width='2'/>"
        for point in [p for p in points if p["variant"] == group[2]]:
            data = point[metric]
            if point["group"] != group or data is None:
                html += flush()
                segment = []
                continue
            segment.append(point)
        html += flush()
        for point in [p for p in points if p["group"] == group and p[metric] is not None]:
            data = point[metric]
            description = (point["timestamp"] + " · run " + point["run"] + " · " + _label(point) +
                " · " + _number(data["value"]) + " " + unit + " · repeat range " +
                _number(data["low"]) + "–" + _number(data["high"]) + " · n=" + str(data["samples"]))
            html += (f"<line x1='{x(point):.3f}' x2='{x(point):.3f}' "
                     f"y1='{y(data['low']):.3f}' y2='{y(data['high']):.3f}' "
                     "stroke='" + color + "' stroke-width='2'/>"
                     "<a href='runs/" + point["run_key"] + ".html' aria-label='" +
                     escape(description, quote=True) + "'>"
                     f"<circle cx='{x(point):.3f}' cy='{y(data['value']):.3f}' r='4' "
                     "fill='" + color + "'><title>" + escape(description) + "</title></circle></a>")
    return html + "</svg>"


def _render(cases, unsupported, omitted):
    html = ("<section aria-labelledby='history-plots'><h1 id='history-plots'>Benchmark history plots</h1>"
        "<p>" + escape(projection.MEASUREMENT_CAVEAT) + "</p><p>" + escape(projection.VALIDATION) + "</p>"
        "<p>Each point is one qualified run and variant. The x-axis uses the frozen, worker-reported "
        "benchmark start timestamp, recorded before fixture preparation, not per-copy start, collection "
        "or retirement time. It is not independently attested.</p>"
        "<p>Duration is the frozen median copy wall time. CPU is the median of user plus system seconds "
        "calculated for each matching copy, not a sum of repetitions. Memory is the median peak command "
        "RSS, converted from KiB to MiB. CPU and RSS cover the command, its threads and waited descendants; "
        "they are not whole-host totals or simultaneous summed process memory. Fixture generation, "
        "validation and cleanup are outside copy measurement.</p>"
        "<p>Whiskers show repeat minima and maxima, not confidence intervals. A few A/A samples do not "
        "establish a performance improvement. Missing resource coverage stays unavailable, never zero; "
        "lines break across missing samples. Different frozen series, observed setups and metric versions "
        "are separate traces. Unknown setup evidence is never pooled across runs. Current "
        "<code>aws-local-ext4-v1</code> series are local copies; future remote modes require their own "
        "validated contract.</p><p>Only still-retained qualified records are plotted. Failed, unavailable "
        "and pending runs remain visible in the run inventory, without performance points. "
        "This is not permanent history.</p>")
    html += ("<p>The plots show the newest qualified runs, up to " + str(MAX_PLOT_RUNS) +
        " runs and " + str(MAX_PLOT_POINTS) + " case/variant points within the page size limit. " +
        str(omitted) + " older qualified runs are omitted from these plots. " +
        "<a href='index.html'>The complete retained run inventory</a> and individual reports remain available.</p>")
    for case, points in cases.items():
        html += "<section id='history-" + case + "'><h2>" + CASES[case] + "</h2>"
        groups = sorted({p["group"] for p in points})
        if groups:
            html += "<ul aria-label='Trace legend'>"
            for index, group in enumerate(groups):
                point = next(p for p in points if p["group"] == group)
                html += ("<li><span aria-hidden='true' style='color:" + COLORS[index % len(COLORS)] +
                         "'>●</span> " + escape(_label(point)) + "</li>")
            html += "</ul>"
        for metric, title, unit in METRICS:
            figure_id = "history-" + case + "-" + metric
            html += ("<figure data-history-metric='" + metric + "' aria-labelledby='" + figure_id +
                     "'><h3 id='" + figure_id + "'>" + escape(title + " (" + unit + ")") + "</h3>" +
                     _svg(points, metric, title, unit, figure_id))
            missing = sum(p[metric] is None for p in points)
            if missing:
                html += "<figcaption>" + str(missing) + " run/variant points unavailable; no zero fill or interpolation.</figcaption>"
            html += "</figure>"
        if points:
            html += ("<details><summary>Source values and provenance</summary><div class='table-scroll'>"
                "<table><thead><tr><th>Run start (worker UTC)</th><th>Run</th><th>Variant</th>"
                "<th>Duration s</th><th>CPU s</th><th>Peak RSS MiB</th><th>Collected at</th>"
                "<th>Series</th><th>Observed setup</th></tr></thead><tbody>")
            for point in points:
                values = [_number(point[m]["value"]) if point[m] is not None else "unavailable"
                          for m, _, _ in METRICS]
                setup = ("<a href='setups.html#setup-" + point["setup_id"] + "'>" +
                         point["setup_id"][:12] + "</a>") if point["setup_id"] else "not recorded"
                html += ("<tr><td>" + escape(point["timestamp"]) + "</td><td><a href='runs/" +
                         point["run_key"] + ".html'>" + escape(point["run"]) + "</a></td><td>" +
                         escape(point["variant"]) + "</td>" +
                         "".join("<td>" + escape(v) + "</td>" for v in values) +
                         "<td>" + escape(point["collected_at"]) + "</td><td><code>" +
                         escape(point["series_id"]) + "</code></td><td>" + setup + "</td></tr>")
            html += "</tbody></table></div></details>"
        html += "</section>"
    if unsupported:
        html += "<p>" + str(unsupported) + " qualified case/variant records are outside this chart catalog; see their individual reports.</p>"
    html += "</section>"
    return html


def render(records, companions):
    """Bound plots, never the complete retained inventory or frozen source records."""
    cases, unsupported = model(records, companions)
    runs = {}
    for points in cases.values():
        for point in points:
            started, count = runs.get(point["run_key"], (point["at"], 0))
            runs[point["run_key"]] = (started, count + 1)
    selected, count = [], 0
    for run in sorted(runs, key=lambda key: (runs[key][0], key), reverse=True):
        if len(selected) == MAX_PLOT_RUNS or count + runs[run][1] > MAX_PLOT_POINTS:
            break
        selected.append(run)
        count += runs[run][1]
    while True:
        window = {case: [point for point in points if point["run_key"] in selected]
                  for case, points in cases.items()}
        html = _render(window, unsupported, len(runs) - len(selected))
        if len(html.encode()) <= projection.MAX_PUBLIC_HTML:
            return html
        require(selected, "AWS history plots exceed public HTML bound")
        selected.pop()
