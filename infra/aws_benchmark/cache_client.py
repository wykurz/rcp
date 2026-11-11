#!/usr/bin/python3
"""Accept only the harness's fixed cache-drop invocation; never retry a request."""

import json
import os
import socket
import sys

from benchmarks import cache

EXPECTED = ["-n", "sh", "-c", "printf 3 > /proc/sys/vm/drop_caches"]
SOCKET = "/run/rcp-benchmark/cache.sock"


def main():
    if sys.argv[1:] != EXPECTED:
        raise SystemExit("only the fixed cache-drop operation is supported")
    stage = "request"
    try:
        raw = os.environ.get(cache.DEADLINE_ENV, "")
        if not raw.isascii() or not raw.isdecimal() or len(raw) > 19:
            raise ValueError("missing or invalid cache deadline")
        deadline = int(raw)
        cache.remaining(deadline)
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(cache.remaining(deadline))
            connection.connect(SOCKET)
            cache.send(connection, {"version": cache.VERSION, "operation": "drop",
                "deadline_ns": deadline}, deadline, cache.MAX_REQUEST)
            connection.shutdown(socket.SHUT_WR)
            stage = "reply"
            result = cache.response(cache.receive(connection, deadline, cache.MAX_REPLY))
            if not result["ok"]:
                print(json.dumps(result, sort_keys=True), file=sys.stderr)
                return 1
            print(json.dumps(result, sort_keys=True))
            return 0
    except (OSError, ValueError) as error:
        print(json.dumps({"version": cache.VERSION, "ok": False, "stage": stage,
            "error_type": type(error).__name__, "completion": "unknown" if stage == "reply" else "not-confirmed"},
            sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
