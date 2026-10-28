"""T55: THE SLOUGH / CANAL CANDIDATES as a review target — a mark of scope "slough" is held against these.

Two read-only proposal files: the automark (`handoff/PLAN/curation-review/slough-automark.json`, one row per
candidate with the owner's review-page verdicts and the width rule's proposal) and the v2 finder
(`handoff/PLAN/sloughs/v2/candidates_v2.json.gz`, the outline and evidence of each polygon).

PRE-MARKS (owner 2026-10-28): a proposed answer the owner sees and confirms or changes — never a mark.
    narrow      the width rule's auto candidates (<= 44 m wide; the owner called 96% of those stream)
    wide marsh  wider than 44 m but FWA marsh / swamp (the owner: "wide marsh channels are also stream")
An INTERMITTENT polygon (FWA "...-Intermittent") is never pre-marked: it stays on the owner's list; nor is
one with no measured width (the automark read it as 0 m — e.g. Back Channel, a 74 ha marsh).

A slough mark's prints: `meaning` what the candidate is (stream, FWA type, width, area), `binding` its
outline — a regenerated file that redraws the polygon makes the mark stale.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[3]
AUTOMARK = ROOT / "handoff/PLAN/curation-review/slough-automark.json"
FINDER = ROOT / "handoff/PLAN/sloughs/v2/candidates_v2.json.gz"
#: the width rule's cut (slough-automark.md): at or under this the owner called it stream 96% of the time
NARROW_M = 44.0

#: the queue's groups, in review order
GROUPS = ("premark", "owner list", "owner", "default")
GROUP_WORDS = {"premark": "pre-marked stream (confirm)", "owner list": "your list (no pre-mark)",
               "owner": "marked on the review page", "default": "not for review (lake class, ponds)"}


def _paths() -> tuple[Path, Path]:
    return (Path(os.environ.get("CURATION_SLOUGH_AUTOMARK") or AUTOMARK),
            Path(os.environ.get("CURATION_SLOUGH_FINDER") or FINDER))


def _mtime(p: Path) -> float:
    try:
        return p.stat().st_mtime
    except OSError:
        return 0.0


def intermittent(row: dict) -> bool:
    return "intermittent" in str(row.get("fwa") or "").lower()


def premark(row: dict) -> Optional[dict]:
    """The proposed answer for one candidate, or None: {call, basis, why}. Never on an owner-marked,
    intermittent or default row."""
    if row.get("decision") == "owner" or intermittent(row):
        return None
    w = row.get("width_m")
    ws = f"{w:.0f} m wide" if isinstance(w, (int, float)) else "width unknown"
    if not isinstance(w, (int, float)):
        return None                      # no width measured (automark read it as 0 m): the owner's call
    if row.get("decision") == "auto":
        return {"call": "stream", "basis": "narrow",
                "why": f"{ws} (≤ {NARROW_M:.0f} m): you called 117 of 122 such pieces stream"}
    if row.get("decision") == "owner_list" and row.get("wide_wetland"):
        return {"call": "stream", "basis": "wide marsh",
                "why": f"{ws} but FWA {row.get('fwa')}: wide marsh / swamp channels are stream (owner 2026-10-28)"}
    return None


def group(row: dict) -> str:
    if row.get("decision") == "owner":
        return "owner"
    if premark(row):
        return "premark"
    if row.get("decision") in ("owner_list", "auto"):   # auto here = an intermittent one
        return "owner list"
    return "default"


@lru_cache(maxsize=2)
def _load(a: str, f: str, _ma: float, _mf: float) -> dict:
    try:
        doc = json.loads(Path(a).read_text())
    except OSError:
        return {"by_id": {}, "order": [], "finder": {}}
    try:
        fin = {c["id"]: c for c in json.load(gzip.open(f)).get("candidates") or []}
    except OSError:
        fin = {}
    rows = doc.get("rows") or []
    rank = {g: i for i, g in enumerate(GROUPS)}
    # review order: group; within the owner list its own rank; else the narrowest first
    # review order: group; pre-marks narrow before wide marsh; the owner list in its own rank, the
    # intermittent ones after it; then the narrowest first
    def key(r):
        pm = premark(r) or {}
        return (rank[group(r)], pm.get("basis") == "wide marsh", r.get("owner_rank") or 10**6,
                r.get("width_m") or 0, r["id"])
    order = sorted(rows, key=key)
    return {"by_id": {r["id"]: r for r in rows}, "order": [r["id"] for r in order], "finder": fin}


def load() -> dict:
    a, f = _paths()
    return _load(str(a), str(f), _mtime(a), _mtime(f))


def candidate(cid: str) -> Optional[dict]:
    return load()["by_id"].get(cid)


def finder_row(cid: str) -> Optional[dict]:
    return load()["finder"].get(cid)


def _h(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]


def prints(row: dict) -> dict:
    """THE PRINTS of a candidate (`marks.Prints` shape): no book text; one `slough:<id>` record."""
    meaning = {k: row.get(k) for k in ("stream", "name", "fwa", "width_m", "area_ha")}
    fin = finder_row(row["id"]) or {}
    binding = {"outline": fin.get("outline")}
    return {"text": None, "records": [{"key": f"slough:{row['id']}", "meaning": _h(meaning),
                                       "binding": _h(binding)}]}


def prints_of(cid: str) -> Optional[dict]:
    row = candidate(cid)
    return prints(row) if row is not None else None
