"""Shared assets for the benchmark dashboard and maintained report pages."""

from functools import cache
from pathlib import Path


@cache
def read_theme():
    return (Path(__file__).resolve().parent.parent / "assets/site-theme.html").read_text(encoding="utf-8")
