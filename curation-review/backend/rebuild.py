"""Run the full graph rebuild (`pipeline.atlas.build --full`) as a background subprocess, with progress.

CPU-only, makes NO LLM calls / spends NO credits — safe to trigger from the UI. splits.json edits
(and name_variants) are baked into the graph's section boundaries at build time, so a curated split
only takes effect after this runs. On a SUCCESSFUL build the reuse-layer caches are dropped, so every
subsequent request serves the freshly-rebuilt registry / splits.resolved / gpkg — the app updates all
items with no restart. The build goes to `<build>_next` and is PROMOTED by a second, explicit step
(`promote`, POST /api/rebuild/promote): the served atlas is never rebuilt in place.

Progress is derived from the per-stage `[label: Ns]` lines pipeline.atlas.build prints (see build.py `_tick`).
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import threading

import time
from collections import deque

import reuse
from pipeline.common.curated import CURATED, GENERATED, SOURCE

# The stages build.py prints a "[label: Ns]" completion line for, IN ORDER — drives the progress bar.
# (Border runs automatically under --full; area/mu/registry/write always run.)
_EXPECTED_STAGES = [
    "load fids + lakes",
    "blk-chains + graph + geometry",
    "border",
    "curated splits",
    "blanket area splits (cut only)",
    "build_registry",
    "add_mu_sets",
    "write artifacts + gpkg",
]
_TICK_RE = re.compile(r"\[(?P<label>[^\]]+?):\s*(?P<sec>[\d.]+)s\]")
# Rebuilds BESIDE the directory reuse.py serves, never into it: the promoted atlas's handle table
# is what the shipped bundle, index, export and tiles key sections by, and the build is not
# deterministic, so building into it would change those ids under the artifacts that ship
# (`pipeline.common.vintage.promoted_atlas`; `pipeline.atlas.build` refuses it). The candidate is
# `<build>_next`; `promote()` (POST /api/rebuild/promote) makes it the served build by rename
# after `pipeline.tools.build_parity` has reported what changed.
NEXT_BUILD = GENERATED.build(f"{GENERATED.atlas.default_build}_next")
_CMD = [sys.executable, "-m", "pipeline.atlas.build", "--full",
        "--out", str(NEXT_BUILD), "--splits", str(CURATED.waters.splits)]


class _Rebuild:
    """Singleton state for the (at most one) in-flight rebuild."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self.status = "idle"            # idle | running | done | error
        self.started_at = 0.0
        self.finished_at = 0.0
        self.returncode: int | None = None
        self.done_stages: dict[str, float] = {}
        self.current = ""
        self.error = ""
        #: whether the last finished build has been promoted (`promote`): the UI offers the step
        #: while a finished `<build>_next` is waiting, and refreshes its items only after it
        self.promoted = False
        self._log: deque[str] = deque(maxlen=300)

    def start(self) -> dict:
        with self._lock:
            if self.status == "running":
                return self.snapshot()               # already going — no double-start
            self.status = "running"
            self.started_at = time.time()
            self.finished_at = 0.0
            self.returncode = None
            self.done_stages = {}
            self.promoted = False
            self.current = "starting…"
            self.error = ""
            self._log.clear()
            try:
                self._proc = subprocess.Popen(
                    _CMD, cwd=str(reuse._ROOT),
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1,
                    env={**os.environ, "PYTHONPATH": str(reuse._ROOT)},
                )
            except Exception as ex:  # noqa: BLE001 — spawn failed (bad python/path)
                self.status = "error"
                self.error = f"failed to start build: {ex}"
                self.current = "failed"
                self.finished_at = time.time()
                return self.snapshot()
            threading.Thread(target=self._pump, daemon=True).start()
            return self.snapshot()

    def _pump(self) -> None:
        proc = self._proc
        assert proc is not None and proc.stdout is not None
        for raw in proc.stdout:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            self._log.append(line)
            m = _TICK_RE.search(line)
            if m:                                    # a stage completion line
                self.done_stages[m.group("label")] = float(m.group("sec"))
                self.current = m.group("label")
            elif line.strip().endswith(("...", "…")):  # a stage-start activity line
                self.current = line.strip().rstrip("… .")
        proc.wait()
        self.returncode = proc.returncode
        self.finished_at = time.time()
        if proc.returncode == 0:
            self.status = "done"
            self.current = f"done — built {NEXT_BUILD.name}; promote it to serve it"
        else:
            self.status = "error"
            self.error = self.error or f"build exited with code {proc.returncode}"
            self.current = "failed"

    def promote(self) -> dict:
        """Make the finished `<build>_next` the served build (`pipeline.atlas.promote`: parity
        report, rename; the old build is kept as `<build>.prev`), then drop the reuse-layer caches
        so every request serves it. Refused while a rebuild runs or when there is no candidate."""
        from pipeline.atlas.promote import promote as _promote
        with self._lock:
            if self.status == "running":
                return {"ok": False, "error": "a rebuild is running"}
            if not (NEXT_BUILD / "registry.json").exists():
                return {"ok": False, "error": f"no finished build at {NEXT_BUILD}"}
            try:
                got = _promote(NEXT_BUILD, log=self._log.append)
            except SystemExit as ex:
                return {"ok": False, "error": str(ex)}
            try:
                reuse.invalidate_caches()            # serve the promoted artifacts
            except Exception as ex:  # noqa: BLE001
                return {"ok": True, "promoted": got,
                        "error": f"promoted, but cache refresh failed: {ex}"}
            self.promoted = True
            self.current = "promoted — graph refreshed"
            return {"ok": True, "promoted": got}

    def snapshot(self) -> dict:
        now = time.time()
        elapsed = ((self.finished_at or now) - self.started_at) if self.started_at else 0.0
        stages = [{"label": s, "done": s in self.done_stages, "seconds": self.done_stages.get(s)}
                  for s in _EXPECTED_STAGES]
        return {
            "status": self.status,
            "elapsed_s": round(elapsed, 1),
            "current": self.current,
            "stages": stages,
            "n_done": len(self.done_stages),
            "n_total": len(_EXPECTED_STAGES),
            "returncode": self.returncode,
            "error": self.error,
            "promoted": self.promoted,
            "log_tail": list(self._log)[-14:],
        }


MANAGER = _Rebuild()
