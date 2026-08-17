"""Lightweight, zero-overhead-when-off sub-stage profiler for the build's hot functions.

The build already times whole STAGES via `build.py::_tick` (load / graph / border / registry / …).
This adds a finer breakdown INSIDE a stage so a slow stage (e.g. the ~60-min `build_registry` or the
~28-min `border`) can be attributed to a specific sub-phase instead of guessed at. Off by default —
wrap sub-phases in `prof.phase(...)` and they cost nothing unless profiling is enabled.

Enable per-run with an env var (no code change):

    PIPELINE_PROFILE=1 PYTHONPATH="$PWD" .venv/bin/python -m pipeline.build --full --out output/v2/full

or explicitly: `build_registry(graph, prof=Profiler(enabled=True))`.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager


def _env_on() -> bool:
    return os.environ.get("PIPELINE_PROFILE", "").lower() not in ("", "0", "false", "no")


class Profiler:
    """Accumulates (label -> seconds) for sub-phases and prints a breakdown once. A no-op unless
    enabled (env `PIPELINE_PROFILE`, or `enabled=True`), so it's safe to leave wired into hot loops."""

    def __init__(self, enabled: bool | None = None):
        self.enabled = _env_on() if enabled is None else enabled
        self.spans: list[tuple[str, float]] = []

    @contextmanager
    def phase(self, label: str):
        if not self.enabled:
            yield
            return
        t = time.perf_counter()
        try:
            yield
        finally:
            self.spans.append((label, time.perf_counter() - t))

    def add(self, label: str, seconds: float) -> None:
        if self.enabled:
            self.spans.append((label, seconds))

    def report(self, title: str = "") -> None:
        if not self.enabled or not self.spans:
            return
        total = sum(dt for _, dt in self.spans)
        head = f"  [profile: {title}]" if title else "  [profile]"
        print(head)
        for label, dt in self.spans:
            pct = 100 * dt / total if total else 0
            print(f"    {label:34} {dt:9.2f}s  {pct:5.1f}%")
        print(f"    {'(sum)':34} {total:9.2f}s")
