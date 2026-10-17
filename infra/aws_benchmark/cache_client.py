#!/usr/bin/python3
"""Accept only the existing harness's fixed sudo cache-drop invocation."""

import socket
import sys

EXPECTED = ["-n", "sh", "-c", "printf 3 > /proc/sys/vm/drop_caches"]
SOCKET = "/run/rcp-benchmark/cache.sock"


def main():
    if sys.argv[1:] != EXPECTED:
        raise SystemExit("only the fixed cache-drop operation is supported")
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(30)
        connection.connect(SOCKET)
        connection.sendall(b"drop\n")
        connection.shutdown(socket.SHUT_WR)
        if connection.recv(16) != b"ok\n":
            raise SystemExit("cache drop failed")


if __name__ == "__main__":
    main()
