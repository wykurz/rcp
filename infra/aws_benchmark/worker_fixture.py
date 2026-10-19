"""Shared test adapter for the actual supervisor command."""

from benchmarks import run as harness
from infra.aws_benchmark import worker


def parsed_command(expected):
    command = worker.command(expected, 600)
    arguments = harness._arguments(command[command.index("benchmarks.run") + 1:])
    environment = dict(value.removeprefix("--setenv=").split("=", 1)
                       for value in command if value.startswith("--setenv="))
    return arguments, environment
