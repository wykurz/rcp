#!/usr/bin/env python3
"""Model-check the teardown contract and validate a fresh Rust event trace."""
import argparse
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tlc_jar", type=lambda path: Path(path).resolve(strict=True))
    parser.add_argument("trace", type=Path)
    args = parser.parse_args()
    events = args.trace.read_text(encoding="utf-8").splitlines()
    if any(not re.fullmatch(r"[A-Za-z]+", event) for event in events):
        parser.error("expected one protocol event name per line")

    model_dir = Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory(prefix="rcp-teardown-") as tmp:
        work = Path(tmp)
        for name in ("RemoteTeardown.tla", "RemoteTeardown.cfg"):
            shutil.copyfile(model_dir / name, work / name)

        def tlc(module):
            subprocess.run(
                ["java", "-cp", args.tlc_jar, "tlc2.TLC", "-workers", "1", module],
                cwd=work,
                check=True,
            )

        tlc("RemoteTeardown")
        sequence = ", ".join(json.dumps(event) for event in events)
        (work / "TeardownTrace.tla").write_text(
            "---- MODULE TeardownTrace ----\n"
            "EXTENDS RemoteTeardown\n"
            "VARIABLE position\n"
            f"Trace == <<{sequence}>>\n"
            "Start == TraceInit(position)\n"
            "Step == TraceNext(position, Trace)\n"
            "Accept == TraceComplete(position, Trace)\n"
            "====\n",
            encoding="utf-8",
        )
        (work / "TeardownTrace.cfg").write_text(
            "INIT Start\nNEXT Step\nINVARIANT Accept\n",
            encoding="utf-8",
        )
        tlc("TeardownTrace")


if __name__ == "__main__":
    main()
