"""Small real fixtures and realistic summary logs for harness integration tests."""

import contextlib
import io
import json
from pathlib import Path
import shutil
from unittest import mock

from benchmarks import report, run, streaming
from benchmarks.test_timings import TimingTests


def run_fixture(root, *, policy=streaming.POLICY, scan_fault=None, copy_fault=None,
                outcome=None, summary=True, streaming_verification=True, cache="linux-drop-caches",
                summary_text=None, extra_args=()):
    binary, manifest = TimingTests()._fixture(root)
    if summary:
        contents = json.loads(manifest.read_text())
        contents["variants"][0]["args"] = ["--summary"]
        manifest.write_text(json.dumps(contents))
    events = []
    copied_source = None
    original_scan = streaming.scan_tree if streaming_verification else run.scan_tree
    original_cache = run._prepare_cache
    def scan(path, *args, **kwargs):
        stage = "initial" if copied_source is None else "source" if path == copied_source else "destination"
        events.append("verify")
        if scan_fault:
            scan_fault(stage, path)
        return original_scan(path, *args, **kwargs)
    def prepare_cache(*args):
        events.append("drop")
        return original_cache(*args) if cache == "source-verified" else None
    def execute(commands, logdir, timeout, **kwargs):
        nonlocal copied_source
        events.append("copy")
        copied_source, destination = map(Path, commands[0][-2:])
        shutil.copytree(copied_source, destination)
        if copy_fault:
            copy_fault(copied_source, destination)
        logdir.mkdir(parents=True)
        stdout, stderr = logdir / "0.stdout.log", logdir / "0.stderr.log"
        stdout.write_text(summary_text if summary_text is not None else
            "directories copied: 2\nfiles copied: 1\nfiles unchanged: 0\nsymlinks copied: 0\n")
        stderr.write_text("")
        return dict(ok=True, elapsed_seconds=1.0, exit_codes=[0], timed_out=False,
                    commands=commands, logs=[dict(stdout=str(stdout), stderr=str(stderr))]) | (outcome or {})
    args = ["--manifest", str(manifest), "--case", "tiny", "--variant", "rcp-default",
            "--bin-dir", str(binary), "--baseline-bin-dir", str(binary), "--source-root", str(root),
            "--destination-root", str(root), "--cache", cache, "--no-timings", "--paired-seed", "7",
            "--repetitions", "2", "--output", str(root / "out")]
    if streaming_verification:
        args += ["--streaming-verification", "--verification-mode",
                 "metadata-size" if policy == streaming.METADATA_POLICY else "full-content"]
    args += list(extra_args)
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(run, "environment", return_value={}))
        stack.enter_context(mock.patch.object(run, "_prepare_cache", side_effect=prepare_cache))
        stack.enter_context(mock.patch.object(run, "execute_commands", side_effect=execute))
        stack.enter_context(mock.patch.object(streaming if streaming_verification else run, "scan_tree", side_effect=scan))
        stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        if policy == streaming.METADATA_POLICY:
            stack.enter_context(mock.patch.object(streaming.os, "fdopen", side_effect=AssertionError("payload read")))
        try:
            run.main(args)
        except (RuntimeError, run.operations.VerificationError):
            pass
    record = report.parse_result((root / "out/results.json").read_text())
    return record, events, (root / "out/summary.md").read_text()
