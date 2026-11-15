"""Performance-only benchmark matrix: independent cells, hang-only timeouts, no verification.

A cell is one file set copied under one scenario. Each cell records its own
outcome, so a failed or hung cell never stops the others, and a cell that the
allocation leaves no time for is reported as not run rather than failed. A copy
counts when the command exits zero and its own summary reports the expected
transfer counts; trees are never walked or hashed. Correctness is covered
elsewhere; this runner only measures.

The `local` scenario copies between two directories. The `remote` scenario makes
the same copy through rcp's remote path on one host: rcp starts a source and a
destination rcpd over SSH, on a private sshd that this runner owns and that
listens on IPv4 loopback only (benchmarks/private_sshd.py). The time measured is
the rcp command's, as for a local copy.

CPU time, peak memory, context switches and page faults are what rcp reports
about itself: each copy is asked for its summary as JSON (--output-format=json),
which carries exact figures for the rcp process and, after a remote copy, for
the source and the destination rcpd. No wrapper runs the commands, and only the
copy's duration is measured from outside. A build from before rcp had the option
is timed all the same: its transfer counts are read from the text summary, and
it has no figures.

The results are saved after every step, so a run that is stopped keeps the cells
it finished. Once the allocation has ended nothing more is removed: clearing
millions of files could outlast the time that is left, and the worker's storage
goes away with the machine. Command logs are cut to a bounded head and tail, so
one failing copy cannot fill a small output directory and take the results of
the other cells with it.
"""

import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

from benchmarks import cache, capacity, measurements, operations, private_sshd, run, strict_json, telemetry
from benchmarks.signals import SignalCancellation
from benchmarks.verification import SUMMARY_POLICY

SCHEMA = "rcp-benchmark-matrix-v1"
POLICY = SUMMARY_POLICY
VARIANT = "rcp-default"
SCENARIOS = ("local", "remote")
# how each scenario reaches its data: directly, or through rcpd on this host's loopback
TOPOLOGY = {"local": "local", "remote": "loopback"}
PURPOSES = ("performance", "commissioning")
CACHE_POLICIES = ("linux-drop-caches", "uncontrolled")
RUN_STATUSES = ("running", "complete", "partial", "failed")
CELL_OUTCOMES = ("complete", "failed", "timed_out", "interrupted", "not_run")
COPY_STATUSES = ("ok", "failed", "timed_out", "interrupted")
GENERATION_STATUSES = ("ok", "failed", "timed_out", "interrupted", "not_run")
# seconds kept before the worker deadline for cleanup and for persisting the results
DEADLINE_MARGIN = 120
# a command is not started when the allocation has less than this left for it
MINIMUM_COMMAND = 60
MAX_CELLS = 64
MAX_COPIES = 16
# bytes kept of each command log, as its head and its tail. A worker archives at most 1 MiB of a
# log, and its output directory holds 128 MiB for every cell together.
LOG_LIMIT = 1024 * 1024 - 4096
# how a copy is asked for its summary, and where its figures come from
SUMMARY_FORMAT = "--output-format=json"
RESOURCE_POLICY = "rcp-output-json-v1"
# seconds a build gets to say whether it knows the option, as it gets to print its version
PROBE_SECONDS = 10
ROLES = ("source", "destination")
# what a process reports about itself: the name it is recorded under, its key in rcp's summary
# and the divisor that turns the reported unit into the recorded one
STATISTICS = (("user_seconds", "cpu_time_user_ms", 1000), ("system_seconds", "cpu_time_kernel_ms", 1000),
              ("max_rss_kib", "peak_rss_bytes", 1024), ("voluntary_switches", "voluntary_context_switches", None),
              ("involuntary_switches", "involuntary_context_switches", None),
              ("minor_faults", "minor_page_faults", None), ("major_faults", "major_page_faults", None))
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?\+00:00")


class Allocation:
    """Time left before the worker's deadline; unlimited when the worker set none."""

    def __init__(self, environment):
        value = environment.get(cache.WORKER_DEADLINE_ENV)
        if value is not None and (not value.isascii() or not value.isdecimal() or len(value) > 19):
            raise ValueError("invalid worker deadline")
        self.deadline = None if value is None else int(value)

    def limit(self, timeout):
        """Return (seconds, clipped) for the next command, or None once the allocation has ended.

        The fixed timeout only catches hangs. When the allocation ends first the
        command is cut early, which is reported as an interruption, not a failure.
        """
        if self.deadline is None:
            return timeout, False
        left = (self.deadline - time.monotonic_ns()) / 1_000_000_000 - DEADLINE_MARGIN
        if left < MINIMUM_COMMAND:
            return None
        return (timeout, False) if timeout <= left else (left, True)


def _arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--cell", action="append", dest="cells", metavar="CASE:SCENARIO",
                        help="one file set under one scenario; repeat in the order to run")
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--destination-root", type=Path, required=True)
    parser.add_argument("--source-storage-id")
    parser.add_argument("--destination-storage-id")
    parser.add_argument("--cache", choices=CACHE_POLICIES, default="linux-drop-caches")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=3600,
                        help="per-command limit that only catches hangs; a slow copy must still finish")
    parser.add_argument("--check-capacity", action="store_true", help="require free blocks and inodes before generating")
    parser.add_argument("--system-telemetry", action="store_true", help="sample bounded host and cgroup diagnostics")
    parser.add_argument("--sshd", type=Path, help="sshd for remote cells; by default the one on PATH or in /usr/sbin")
    parser.add_argument("--ssh", type=Path, help="ssh client for remote cells; by default the one on PATH")
    parser.add_argument("--ssh-keygen", type=Path, help="ssh-keygen for remote cells; by default the one on PATH")
    parser.add_argument("--runner-label", default="local")
    parser.add_argument("--purpose", choices=PURPOSES, default="performance")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def selection(manifest, cells):
    """Resolve CASE:SCENARIO selections, in order, before any work starts."""
    if not cells:
        raise ValueError("select at least one cell")
    cases = {case["id"]: case for case in manifest["cases"]}
    selected = []
    for cell in cells:
        case_id, separator, scenario = cell.partition(":")
        if not separator or not case_id or not scenario:
            raise ValueError(f"cell must be CASE:SCENARIO: {cell!r}")
        if case_id not in cases:
            raise ValueError(f"unknown file set: {case_id}")
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario: {scenario}")
        if (case_id, scenario) in selected:
            raise ValueError(f"duplicate cell: {cell}")
        if cases[case_id].get("mode", "fresh") != "fresh":
            raise ValueError(f"matrix cells copy fresh fixtures only: {case_id}")
        selected.append((case_id, scenario))
    if len(selected) > MAX_CELLS:
        raise ValueError("too many cells")
    return [(cases[case_id], scenario) for case_id, scenario in selected]


def cell_id(case, scenario, variant, context):
    """Identify the same cell across runs: its definition, never the code, machine or time."""
    identity = [SCHEMA, {key: value for key, value in case.items() if key != "description"}, scenario,
                variant["args"], context["cache_policy"], context["verification"], context["runner_label"],
                context["storage_ids"]]
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _persist(output, record, sampler=None):
    if sampler is not None:
        record["system_setup"] = copy.deepcopy(sampler.system_setup)
        record["system_telemetry"] = sampler.snapshot(non_trial_only=True)
        for item in (item for cell in record["cells"] for item in cell["copies"]):
            item["system_telemetry"] = sampler.snapshot(item["trial_index"])
    temporary = output / "results.json.tmp"
    temporary.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    temporary.replace(output / "results.json")
    lines = [f"# Benchmark matrix {record['run_id']}", "", f"Status: **{record['status']}**", "",
             "Copies are not verified: a copy counts when the command exits zero and reports the expected "
             "transfer counts.", ""]
    if record.get("error"):
        lines += [f"Error: {record['error']}", ""]
    lines += ["| File set | Scenario | Outcome | Median (s) | Min–max (s) | Copies | Note |",
              "| --- | --- | --- | ---: | ---: | ---: | --- |"]
    for cell in record["cells"]:
        summary = cell["summary"]
        numbers = (f"{summary['median']:.3f} | {summary['minimum']:.3f}–{summary['maximum']:.3f}"
                   if summary else "— | —")
        note = (cell["outcome"]["message"] or "").replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {cell['case_id']} | {cell['scenario']} | {cell['outcome']['status']} | {numbers} | "
                     f"{sum(item['status'] == 'ok' for item in cell['copies'])} | {note} |")
    (output / "summary.md").write_text("\n".join(lines) + "\n")


def _out_of_time(error):
    """Whether cache preparation ran out of time, as opposed to failing.

    The cache step wraps what went wrong in its own error and records the original type.
    """
    kind = (error.observation.get("error_type") if isinstance(error, cache.PreparationError)
            else type(error).__name__)
    return kind in ("TimeoutExpired", "TimeoutError")


def _remove(path, timeout):
    """Free a fixture or destination within a time limit: "removed", "failed" or "timed_out".

    A separate process does the work, so it can be cut when its time is up; millions of files
    take minutes. A stop signal raises here as anywhere else and ends the run.
    """
    try:
        done = subprocess.run(["rm", "-rf", "--", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                              timeout=timeout)
    except subprocess.TimeoutExpired:
        return "timed_out"
    return "removed" if done.returncode == 0 else "failed"


def _end_daemons(endpoint, item, successful):
    """End any daemon of a remote copy that is still running, and record how many there were.

    This is housekeeping: a failure here is not the copy's.
    """
    try:
        # daemons of a good copy are on their way out; anything else is ended at once
        leftover = endpoint.reap(private_sshd.EXIT_SECONDS if successful else 0)
        if leftover:
            item["leftover_processes"] = leftover
    except InterruptedError:
        raise
    except OSError:
        pass


def _reports_json(rcp):
    """Whether this build of rcp can print its summary as JSON.

    A build that knows the option goes on to print its version; an older one refuses the
    option. Nothing is read from the command, so a stop signal is not lost in a pipe.
    """
    return subprocess.run([rcp, SUMMARY_FORMAT, "--version"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL, timeout=PROBE_SECONDS).returncode == 0


def _json_summary(text):
    """The summary object that is the last line of a copy's output, or None.

    Lines end at a newline only: JSON may carry other characters that look like line ends
    inside its strings. Duplicate keys and numbers that are not finite are refused, since
    either would leave it open which figure was meant.
    """
    try:
        value = strict_json.parse_json(text.rstrip().rpartition("\n")[2])
    except (ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _figures(process):
    """What one process reported about itself, in the units of the results.

    Figures are a measuring aid: when one is missing or is not a count, the process has none.
    Nor does it when its peak memory is zero, which no process that measured itself reports:
    a figure that was not read is unavailable, never a zero.
    """
    if not isinstance(process, dict) or all(key not in process for _, key, _ in STATISTICS):
        return dict(status="unavailable", metrics=None, error="the summary has no statistics for this process")
    metrics = {}
    for name, key, divisor in STATISTICS:
        value = process.get(key)
        if type(value) is not int or not 0 <= value < 2 ** 63:
            return dict(status="unavailable", metrics=None, error=f"{key} is missing or is not a count")
        metrics[name] = value if divisor is None else value / divisor if divisor == 1000 else value // divisor
    if process["peak_rss_bytes"] == 0:
        return dict(status="unavailable", metrics=None,
                    error="peak_rss_bytes is zero: the process did not measure itself")
    # the summary is printed by a process that finished its work
    return dict(status="complete", metrics=dict(metrics, exit_code=0))


def _bound_logs(directory):
    """Cut every command log directly in a directory to a head and a tail, in place.

    Rewriting in place needs no free space, so it still works on a full output directory.
    """
    try:
        logs = sorted(Path(directory).glob("*.log"))
    except OSError:
        return
    for log in logs:
        try:
            with log.open("r+b") as stream:
                size = stream.seek(0, os.SEEK_END)
                marker = b"\n[... %d bytes omitted by the benchmark runner ...]\n"
                half = (LOG_LIMIT - len(marker) - 20) // 2
                if size <= LOG_LIMIT or half <= 0:
                    continue
                stream.seek(size - half)
                tail = stream.read(half)
                stream.seek(half)
                stream.write(marker % (size - 2 * half) + tail)
                stream.truncate()
        except InterruptedError:
            raise
        except OSError:
            # an unreadable log costs nothing but the log
            continue


def main(argv=None):
    args = _arguments(argv)
    manifest = run.load_manifest(args.manifest)
    planned = selection(manifest, args.cells)
    variant = next((item for item in manifest["variants"] if item["id"] == VARIANT), None)
    if variant is None or not operations.summary_supported(variant):
        raise ValueError(f"the manifest needs a single-process {VARIANT} variant with an exact transfer summary")
    if args.repetitions <= 0 or args.repetitions > MAX_COPIES or not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError(f"repetitions must be 1..{MAX_COPIES} and timeout finite and positive")
    for root in (args.source_root, args.destination_root):
        if not root.is_dir():
            raise ValueError(f"scratch parent does not exist: {root}")
    allocation = Allocation(os.environ)
    run_started = time.monotonic()
    output = args.output.resolve()
    context = dict(runner_label=args.runner_label, purpose=args.purpose, cache_policy=args.cache,
        repetitions=args.repetitions, timeout_seconds=args.timeout, verification=dict(policy=POLICY),
        source=str(args.source_root.resolve()), destination=str(args.destination_root.resolve()),
        storage_ids=dict(source=args.source_storage_id, destination=args.destination_storage_id),
        repository=os.environ.get("GITHUB_REPOSITORY", ""), run_url=os.environ.get("BENCHMARK_RUN_URL", ""))
    file_sets = []
    for case, _ in planned:
        if all(item["case"]["id"] != case["id"] for item in file_sets):
            file_sets.append(dict(case=case, expected_counts=operations.expected_counts(case),
                                  generation=dict(status="not_run")))
    with SignalCancellation():
        # reading the revision tolerates a missing git; a stop signal must not be tolerated with it
        revision = run._revision()
    record = dict(schema=SCHEMA, run_id=uuid.uuid4().hex, timestamp=datetime.now(timezone.utc).isoformat(),
        status="running", revision=revision, context=context, tools={}, file_sets=file_sets,
        cells=[dict(case_id=case["id"], scenario=scenario, cell_id=cell_id(case, scenario, variant, context),
                    variant=variant, outcome=dict(status="not_run", stage=None, message="not started"),
                    copies=[], summary=None) for case, scenario in planned])
    # a selection or label that the results document cannot hold is refused before any work
    validate(record)
    output.mkdir(parents=True, exist_ok=False)
    (output / "logs").mkdir()
    sampler = endpoint = None
    # why remote cells cannot be measured, once the private ssh endpoint is known to be unusable
    unreachable = None
    source_scratch = destination_scratch = None
    # ended: the allocation leaves no time for another command; finished: the plan ran to its end
    ended = finished = False
    _persist(output, record)
    try:
        tools = {name: run._tool(args.bin_dir.resolve() / name) for name in ("filegen", "rcp", "rcpd")}
        record["tools"] = tools
        paths = {name: tool["path"] for name, tool in tools.items()}
        # a build from before rcp could print its summary as JSON is timed all the same
        reports = _reports_json(paths["rcp"])
        context["summary_format"] = "json" if reports else "text"
        if reports:
            context["resources"] = dict(policy=RESOURCE_POLICY)
        context["environment"] = run.environment(args.source_root, args.destination_root)
        if args.system_telemetry:
            context["system_telemetry"] = telemetry.descriptor()
            sampler = telemetry.Sampler(output, args.source_root, args.destination_root)
            with SignalCancellation():
                sampler.start()
        source_scratch = Path(tempfile.mkdtemp(prefix="rcp-matrix-source-", dir=args.source_root)).resolve()
        destination_scratch = Path(tempfile.mkdtemp(prefix="rcp-matrix-destination-", dir=args.destination_root)).resolve()
        _persist(output, record, sampler)
        trial_index = 0

        def remote(cell):
            return TOPOLOGY[cell["scenario"]] == "loopback"

        if any(remote(cell) for cell in record["cells"]):
            if allocation.limit(args.timeout) is None:
                ended = True
            else:
                endpoint = private_sshd.Endpoint(log=output / "logs" / "sshd.log", sshd=args.sshd, ssh=args.ssh,
                                                 keygen=args.ssh_keygen)
                try:
                    endpoint.start()
                    context["remote"] = endpoint.descriptor()
                except InterruptedError:
                    raise
                except (private_sshd.EndpointError, OSError, subprocess.SubprocessError) as error:
                    # the cells that need no endpoint are still measured
                    unreachable = f"the private ssh endpoint could not be started: {error}"

        def mark(phase, case_index, trial=None):
            if sampler is not None:
                sampler.mark(phase, case_index, trial)

        def save():
            """Save the results. A save that fails is made up for by the next one: a full output
            directory must not end a run whose later cells can still be measured and saved."""
            try:
                _persist(output, record, sampler)
            except InterruptedError:
                raise
            except OSError:
                pass

        def free(path, case_index, trial=None):
            """Remove data whose space later cells need, within what is left of the allocation."""
            nonlocal ended
            limit = None if ended else allocation.limit(args.timeout)
            if limit is None:
                ended = True
                return
            mark("cleanup", case_index, trial)
            outcome = _remove(path, limit[0])
            if outcome != "removed":
                record["removal_failures"] = record.get("removal_failures", 0) + 1
                # cut by the end of the allocation: nothing more can start
                ended = outcome == "timed_out" and limit[1]

        def settle(cells, status, stage, message):
            for cell in cells:
                cell["outcome"] = dict(status=status, stage=stage, message=message)

        def generate(case_index, file_set, cells, fixture):
            """Create one file set; return its tree, or None once its cells are settled as not measurable."""
            nonlocal ended
            case, generation = file_set["case"], file_set["generation"]
            limit = allocation.limit(args.timeout)
            if limit is None:
                ended = True
                settle(cells, "not_run", None, "allocation ended before this cell")
                return None
            try:
                if args.check_capacity:
                    mark("capacity", case_index)
                    file_set["capacity_admission"] = capacity.check_pair(
                        args.source_root, args.destination_root, case, **capacity.policy())
            except InterruptedError:
                # a stop signal is an OSError too; it ends the run instead of failing this file set
                raise
            except (OSError, ValueError) as error:
                generation.update(status="failed", message=str(error))
                settle(cells, "failed", "capacity", str(error))
                return None
            fixture.mkdir()
            leaf_files = "files_per_leaf" in case
            command = [paths["filegen"], str(fixture), ",".join(map(str, case["directory_widths"])),
                       str(case["files_per_leaf" if leaf_files else "files_per_directory"]), str(case["file_size_bytes"])]
            if leaf_files:
                command.append("--leaf-files")
            command.append(f"--bufsize={min(case['file_size_bytes'], 1048576)}")
            mark("generation", case_index)
            started = time.monotonic()
            log = output / "logs" / case["id"]
            log.mkdir()
            # what a stop finds on disk: generation was under way
            generation.update(status="interrupted", command=command)
            save()
            try:
                with (log / "filegen.stdout.log").open("wb") as stdout, (log / "filegen.stderr.log").open("wb") as stderr:
                    code = subprocess.run(command, stdout=stdout, stderr=stderr, timeout=limit[0]).returncode
                state, message = (("ok", None) if code == 0 else
                                  ("failed", f"file set generation failed with exit code {code}; "
                                             f"see logs/{case['id']}/filegen.stderr.log"))
            except subprocess.TimeoutExpired:
                code = None
                state, message = (("interrupted", "allocation ended during generation") if limit[1] else
                                  ("timed_out", f"file set generation did not finish within {args.timeout:g} s"))
            finally:
                _bound_logs(log)
            generation.update(status=state, elapsed_seconds=time.monotonic() - started, exit_code=code,
                              command=command, message=message)
            if state != "ok":
                ended = state == "interrupted"
                settle(cells, "not_run" if ended else state, "generation", message)
                free(fixture, case_index)
                return None
            return fixture / "filegen"

        def measure(case_index, cell, source, expected):
            """Copy one cell's file set the configured number of times and settle the cell's outcome."""
            nonlocal ended, trial_index, unreachable
            copies, loopback = args.repetitions, remote(cell)
            if loopback and unreachable is not None:
                settle([cell], "failed", "transport", unreachable)
                return
            for iteration in range(1, copies + 1):
                limit = None if ended else allocation.limit(args.timeout)
                if limit is None:
                    ended = True
                    settle([cell], "not_run" if iteration == 1 else "interrupted", "copy" if iteration > 1 else None,
                           "allocation ended before this cell" if iteration == 1 else
                           f"allocation ended before copy {iteration} of {copies}")
                    return
                trial = Path(cell["case_id"]) / cell["scenario"] / str(iteration)
                destination = destination_scratch / trial
                destination.parent.mkdir(parents=True, exist_ok=True)
                commands = run.plan_commands(variant, source, destination, paths, "local")
                if reports:
                    # the option goes before the two operands
                    commands = [[*command[:-2], SUMMARY_FORMAT, *command[-2:]] for command in commands]
                restarted = False
                if loopback:
                    commands = [endpoint.command(command, paths["rcpd"]) for command in commands]
                    try:
                        restarted = endpoint.ensure()
                    except InterruptedError:
                        raise
                    except (private_sshd.EndpointError, OSError, subprocess.SubprocessError) as error:
                        # later remote cells fail at once instead of generating a file set for nothing
                        unreachable = f"the private ssh endpoint stopped and could not be started again: {error}"
                        settle([cell], "failed", "transport", "the private ssh endpoint stopped and could not be "
                               f"started again before copy {iteration}: {error}")
                        return
                # until a step says otherwise the copy is under way: that is what a stop finds on disk
                item = dict(iteration=iteration, trial_index=trial_index, status="interrupted", command=commands[0])
                if restarted:
                    item["endpoint_restarted"] = True
                trial_index += 1
                cell["copies"].append(item)
                settle([cell], "interrupted", "cache",
                       f"run stopped during cache preparation before copy {iteration} of {copies}")
                save()
                mark("cache", case_index, item["trial_index"])
                prepared = time.monotonic()
                try:
                    diagnostics = item.setdefault("cache_preparation", {}) if args.cache == "linux-drop-caches" else None
                    run._prepare_cache(args.cache, source, limit[0], diagnostics=diagnostics, cache_timeout=limit[0])
                except InterruptedError:
                    # a stop signal ends the run; it is not this cell's failure
                    raise
                except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
                    slow = _out_of_time(error)
                    # a step that fails just as a clipped limit runs out is the allocation ending
                    ended = limit[1] and (slow or allocation.limit(args.timeout) is None)
                    item["status"] = "interrupted" if ended else "timed_out" if slow else "failed"
                    settle([cell], item["status"], "cache",
                           f"allocation ended during cache preparation before copy {iteration} of {copies}"
                           if ended else f"cache preparation before copy {iteration} failed: {error}")
                    return
                measurements.add_cost(item, "preparation", prepared)
                # the cache step took its share: the copy gets what is left now, not what was left before it
                limit = allocation.limit(args.timeout)
                if limit is None:
                    ended = True
                    settle([cell], "interrupted", "cache",
                           f"allocation ended after cache preparation before copy {iteration} of {copies}")
                    return
                settle([cell], "interrupted", "copy", f"run stopped during copy {iteration} of {copies}")
                save()
                mark("command", case_index, item["trial_index"])
                options = dict(environment=endpoint.environment()) if loopback else {}
                try:
                    outcome = run.execute_commands(commands, output / "logs" / trial, limit[0],
                                                   stable_summary_locale=True, **options)
                finally:
                    _bound_logs(output / "logs" / trial)
                item.update({key: outcome[key] for key in ("elapsed_seconds", "exit_codes", "timed_out", "logs")})
                succeeded = outcome["ok"]
                if loopback:
                    _end_daemons(endpoint, item, succeeded)
                if outcome["timed_out"]:
                    ended = limit[1]
                    item["status"] = "interrupted" if ended else "timed_out"
                    settle([cell], item["status"], "copy",
                           f"allocation ended during copy {iteration} of {copies}" if ended else
                           f"copy {iteration} did not finish within {args.timeout:g} s")
                elif not succeeded:
                    codes = outcome["exit_codes"]
                    item["status"] = "failed"
                    settle([cell], "failed", "copy",
                           f"copy {iteration} could not be started: {outcome['launch_error']}"
                           if outcome["launch_error"] is not None else
                           f"copy {iteration} failed with exit " +
                           (f"code {codes[0]}" if len(codes) == 1 else f"codes {codes}") +
                           # rcp reports what went wrong on its output; its argument parser on stderr
                           f"; see 0.stdout.log and 0.stderr.log in logs/{trial}")
                else:
                    mark("summary", case_index, item["trial_index"])
                    # output is read as text with stand-ins for stray bytes: only the summary matters
                    text = run._read(outcome["logs"][0]["stdout"])
                    reported = (_json_summary(text) if reports else
                                operations.summary_proof(variant, text, expected)[0])
                    if reported is None:
                        item["status"] = "failed"
                        settle([cell], "failed", "summary", f"copy {iteration} printed no JSON summary as its "
                               f"last line of output; see logs/{trial}/0.stdout.log")
                    else:
                        # only the counts that the summary was checked for are recorded
                        counts = {key: reported[key] for key in ("files_copied", "files_unchanged")
                                  if type(reported.get(key)) is int}
                        item["copy_summary"] = counts
                        if reports:
                            # the copy stands on its exit status and these counts: figures that are
                            # missing leave CPU and memory unavailable and are not a failure of rcp
                            item["resources"] = _figures(reported)
                            if loopback:
                                daemons = reported.get("remote")
                                daemons = daemons if isinstance(daemons, dict) else {}
                                item["role_resources"] = {role: _figures(daemons.get(role)) for role in ROLES}
                        wrong = [f"{key} is {counts.get(key, 'missing')}; expected {expected[key]}"
                                 for key in ("files_copied", "files_unchanged") if counts.get(key) != expected[key]]
                        if wrong:
                            item["status"] = "failed"
                            settle([cell], "failed", "summary",
                                   f"copy {iteration} reported an unexpected summary: " + "; ".join(wrong))
                        else:
                            item["status"] = "ok"
                if item["status"] != "ok":
                    save()
                    free(destination, case_index, item["trial_index"])
                    return
                if iteration == copies:
                    samples = [item["elapsed_seconds"] for item in cell["copies"]]
                    # in one step: a stop between two would leave a summary on an unfinished cell
                    cell.update(summary=dict(unit="seconds", median=statistics.median(samples), minimum=min(samples),
                                             maximum=max(samples), samples=samples),
                                outcome=dict(status="complete", stage=None, message=None))
                else:
                    settle([cell], "interrupted", "copy", f"run stopped after copy {iteration} of {copies}")
                # the measurement is on disk before the removal that a stop could interrupt
                save()
                cleaned = time.monotonic()
                free(destination, case_index, item["trial_index"])
                measurements.add_cost(item, "cleanup", cleaned)

        def fail(cells, error):
            """Charge an error nobody foresaw to the cells it interrupted; finished cells keep their result."""
            for cell in cells:
                if cell["outcome"]["status"] != "complete":
                    cell.update(summary=None, outcome=dict(status="failed", stage="harness",
                        message=f"unexpected {type(error).__name__}: {error}"))

        for case_index, file_set in enumerate(file_sets):
            case = file_set["case"]
            cells = [cell for cell in record["cells"] if cell["case_id"] == case["id"]]
            if ended:
                settle(cells, "not_run", None, "allocation ended before this cell")
                continue
            if unreachable is not None and all(remote(cell) for cell in cells):
                # nothing here can be measured: the file set is not generated for it
                settle(cells, "failed", "transport", unreachable)
                save()
                continue
            fixture = source_scratch / case["id"]
            # an error nobody foresaw costs the file set or the cell it happened in, never the
            # ones after it. A stop signal is an exception too, and still ends the run.
            try:
                source = generate(case_index, file_set, cells, fixture)
                expected = operations.transfer_counts("fresh", file_set["expected_counts"])
            except InterruptedError:
                raise
            except Exception as error:
                if file_set["generation"]["status"] in ("not_run", "interrupted"):
                    file_set["generation"].update(status="failed",
                                                  message=f"unexpected {type(error).__name__}: {error}")
                fail(cells, error)
                source = None
                free(fixture, case_index)
            save()
            if source is None:
                continue
            for cell in cells:
                if ended:
                    settle([cell], "not_run", None, "allocation ended before this cell")
                    continue
                try:
                    measure(case_index, cell, source, expected)
                except InterruptedError:
                    raise
                except Exception as error:
                    fail([cell], error)
                    if endpoint is not None and remote(cell):
                        _end_daemons(endpoint, {}, False)
                    _bound_logs(output / "logs" / case["id"] / cell["scenario"] / str(len(cell["copies"])))
                    free(destination_scratch / case["id"] / cell["scenario"], case_index)
                save()
            free(fixture, case_index)
        complete = sum(cell["outcome"]["status"] == "complete" for cell in record["cells"])
        record["status"] = ("complete" if complete == len(record["cells"]) else "partial" if complete else "failed")
        finished = True
    except BaseException as error:
        record["status"] = "failed"
        record["error"] = str(error) or type(error).__name__
        for cell in record["cells"]:
            if cell["outcome"]["message"] == "not started":
                cell["outcome"] = dict(status="not_run", stage=None, message="the run stopped before this cell")
        raise
    finally:
        # save first; a stop signal that arrives now waits until the results are on disk
        with SignalCancellation():
            try:
                measurements.add_cost(record, "total", run_started)
                if sampler is not None:
                    sampler.finish(record["status"] != "failed")
                _persist(output, record, sampler)
            finally:
                if endpoint is not None:
                    # sshd runs in a session of its own and would outlive this process: it and the
                    # keys go with the run, whatever ended it, a save that failed included
                    endpoint.stop()
                    _bound_logs(output / "logs")
        # a stopped run must exit within seconds and an ended allocation has no time left to spend
        for scratch in (source_scratch, destination_scratch):
            limit = allocation.limit(args.timeout) if finished and not ended and scratch is not None else None
            if limit is not None:
                _remove(scratch, limit[0])
    return record


class Invalid(ValueError):
    """A results document that fails validation. Messages name fields and never quote the document."""


def _check(condition, message):
    if not condition:
        raise Invalid(message)


def _number(value, name, *, positive=False):
    _check(type(value) in (int, float) and math.isfinite(value) and (value > 0 if positive else value >= 0),
           f"{name} must be a finite {'positive' if positive else 'non-negative'} number")
    return value


def _text(value, limit=256):
    return isinstance(value, str) and 0 < len(value) <= limit


def _identifier(value):
    return isinstance(value, str) and len(value) <= 64 and run.ID_PATTERN.fullmatch(value) is not None


def validate(record):
    """Check a results document before anything is concluded from it.

    Covers what qualification and the public record read: status, time, revision, context,
    tools, file sets, cells, copies and summaries. Environment, setup, per-copy resources and
    sampled telemetry are diagnostics that their readers check field by field. The only
    exception raised is Invalid.
    """
    try:
        return _validate(record)
    except Invalid:
        raise
    except (ArithmeticError, AttributeError, LookupError, TypeError, ValueError) as error:
        raise Invalid("malformed benchmark matrix result") from error


def _validate(record):
    _check(isinstance(record, dict) and record.get("schema") == SCHEMA, "not a benchmark matrix result")
    _check(record.get("status") in RUN_STATUSES, "invalid matrix status")
    _check(isinstance(record.get("run_id"), str) and re.fullmatch(r"[0-9a-f]{32}", record["run_id"]),
           "run_id must be 32 hexadecimal digits")
    stamp = record.get("timestamp")
    # exactly the spelling this runner writes: parsers differ on everything looser, and a time
    # that one reader accepts and the next rejects would stop a result from being collected
    try:
        moment = datetime.fromisoformat(stamp) if isinstance(stamp, str) and TIMESTAMP.fullmatch(stamp) else None
    except ValueError:
        moment = None
    _check(moment is not None, "timestamp must be an ISO 8601 UTC time")
    revision = record.get("revision")
    # no further keys: consumers copy the revision as it is
    _check(isinstance(revision, dict) and set(revision) == {"commit", "branch", "dirty"}
           and all(revision[key] is None or _text(revision[key]) for key in ("commit", "branch"))
           and (revision["dirty"] is None or type(revision["dirty"]) is bool), "invalid revision")
    context = record.get("context")
    _check(isinstance(context, dict) and context.get("verification") == dict(policy=POLICY), "invalid matrix context")
    repetitions = context.get("repetitions")
    _check(type(repetitions) is int and 1 <= repetitions <= MAX_COPIES, "invalid matrix repetitions")
    _number(context.get("timeout_seconds"), "context.timeout_seconds", positive=True)
    _check(context.get("purpose") in PURPOSES and context.get("cache_policy") in CACHE_POLICIES
           and _text(context.get("runner_label"))
           and all(isinstance(context.get(key), str) and len(context[key]) <= 4096
                   for key in ("repository", "run_url", "source", "destination")), "invalid matrix context")
    storage = context.get("storage_ids")
    _check(isinstance(storage, dict) and set(storage) == {"source", "destination"}
           and all(value is None or _text(value) for value in storage.values()), "invalid storage identities")
    tools = record.get("tools")
    # a run that stops before it has read its tools still reports, with none recorded
    _check(isinstance(tools, dict) and set(tools) <= {"filegen", "rcp", "rcpd"} and all(
        isinstance(tool, dict) and isinstance(tool.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", tool["sha256"])
        and _text(tool.get("path"), 4096) and _text(tool.get("version"), 4096) for tool in tools.values()),
        "invalid tools")
    cells = record.get("cells")
    _check(isinstance(cells, list) and 1 <= len(cells) <= MAX_CELLS, "matrix result needs 1..64 cells")
    file_sets = record.get("file_sets")
    _check(isinstance(file_sets, list) and len(file_sets) <= MAX_CELLS and all(
        isinstance(item, dict) and isinstance(item.get("case"), dict) and _identifier(item["case"].get("id"))
        and isinstance(item.get("expected_counts"), dict) and isinstance(item.get("generation"), dict)
        and item["generation"].get("status") in GENERATION_STATUSES for item in file_sets), "invalid matrix file sets")
    known = [item["case"]["id"] for item in file_sets]
    _check(len(set(known)) == len(known), "duplicate matrix file set")
    seen = set()
    for index, cell in enumerate(cells):
        name = f"cells[{index}]"
        _check(isinstance(cell, dict) and _identifier(cell.get("case_id")) and cell["case_id"] in known
               and _identifier(cell.get("scenario")), f"{name} has an invalid file set or scenario")
        key = (cell["case_id"], cell["scenario"])
        _check(key not in seen, f"{name} duplicates a cell")
        seen.add(key)
        _check(isinstance(cell.get("cell_id"), str) and re.fullmatch(r"[0-9a-f]{64}", cell["cell_id"]),
               f"{name}.cell_id must be a SHA256 hex digest")
        variant = cell.get("variant")
        _check(isinstance(variant, dict) and variant.get("id") == VARIANT and isinstance(variant.get("args"), list)
               and all(isinstance(argument, str) for argument in variant["args"]), f"{name}.variant is invalid")
        outcome = cell.get("outcome")
        _check(isinstance(outcome, dict) and outcome.get("status") in CELL_OUTCOMES
               and all(field in outcome and (outcome[field] is None or isinstance(outcome[field], str))
                       for field in ("stage", "message")), f"{name}.outcome is invalid")
        copies = cell.get("copies")
        _check(isinstance(copies, list) and len(copies) <= repetitions, f"{name}.copies is invalid")
        for position, item in enumerate(copies, 1):
            label = f"{name}.copies[{position - 1}]"
            _check(isinstance(item, dict) and type(item.get("iteration")) is int and item["iteration"] == position
                   and item.get("status") in COPY_STATUSES and type(item.get("trial_index")) is int
                   and item["trial_index"] >= 0, f"{label} is invalid")
            if item["status"] == "ok":
                _number(item.get("elapsed_seconds"), f"{label}.elapsed_seconds")
                _check(item.get("exit_codes") == [0] and item.get("timed_out") is False,
                       f"{label} is ok without a clean exit")
            elif item.get("elapsed_seconds") is not None:
                _number(item["elapsed_seconds"], f"{label}.elapsed_seconds")
        summary = cell.get("summary")
        if outcome["status"] == "complete":
            _check(len(copies) == repetitions and all(item["status"] == "ok" for item in copies),
                   f"{name} is complete without every copy")
            samples = [item["elapsed_seconds"] for item in copies]
            _check(isinstance(summary, dict) and summary.get("unit") == "seconds"
                   and isinstance(summary.get("samples"), list), f"{name}.summary is invalid")
            # numbers by type: True equals 1, so a comparison alone would let booleans through
            for key in ("median", "minimum", "maximum"):
                _number(summary.get(key), f"{name}.summary.{key}")
            for sample in summary["samples"]:
                _number(sample, f"{name}.summary.samples")
            _check(summary["samples"] == samples and summary["minimum"] == min(samples)
                   and summary["maximum"] == max(samples) and summary["median"] == statistics.median(samples),
                   f"{name}.summary does not match its copies")
        else:
            _check(summary is None, f"{name} has a summary without completing")
    complete = sum(cell["outcome"]["status"] == "complete" for cell in cells)
    expected = "complete" if complete == len(cells) else "partial" if complete else "failed"
    # a stopped run reports "running" or "failed" whatever its finished cells say
    _check(record["status"] in ("running", "failed", expected), "matrix status differs from its cells")
    _check(record.get("error") is None or isinstance(record["error"], str), "error must be a string")
    _check(type(record.get("removal_failures", 0)) is int and record.get("removal_failures", 0) >= 0,
           "removal_failures must be a count")
    _check(isinstance(record.get("phase_seconds", {}), dict), "phase_seconds must be an object")
    return record


if __name__ == "__main__":
    def _sigterm(_signum, _frame):
        raise InterruptedError("SIGTERM")
    signal.signal(signal.SIGTERM, _sigterm)
    try:
        main()
    except Exception as exc:
        print(f"benchmark matrix failed: {exc}", file=sys.stderr)
        sys.exit(1)
