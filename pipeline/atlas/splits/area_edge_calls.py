"""THE OWNER'S CALLS ON EDGE WATER THE CUTTER LEAVES OPEN (T1199 review, 2026-11-27): `waters/area_edge_calls.json`.

A boundary river DRAWN wiggling across a closure area's edge — every crossing a graze under the dip — is left open by
the cutter (grazes are no crossings), although the owner's rule is that edge water stays closed. The builder does not
decide it (AGENTS rule 12): the area-cut gate's G6 stops the build and names the stretch, and the owner calls it here.

Each row names a stretch by AREA (its area id) and WATER + MEASURE RANGE (blue line, from_m, to_m), never a section id:

  call "closed"  the stretch is edge water of that area: the cutter treats it as inside (it joins the member run, the
                 floor and the move onto a boundary apply as to any inside), and the gate passes it;
  call "open"    the stretch stays open, accepted: the gate passes it.

Only `accepted` rows are read, each with accepted_by / accepted_on and a note; `proposed` rows are Claude's proposals for
the owner and are read by nothing (a proposal is no acceptance, as in `unplaced_piece_calls.json`). An accepted row that
matches no stretch any more is STALE and stops the build too (the gate reports it), never silently kept.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from pipeline.common import tolerances as _tol

CALLS = ("closed", "open")
#: a row covers a stretch when its range holds the stretch's to this much (`tolerances.SLIVER_M`: two places closer than
#: this are one; a rebuild moves a crossing by millimetres, never by metres)
MATCH_M = _tol.SLIVER_M
_ROW_KEYS = ("area", "blk", "water", "from_m", "to_m", "call", "note")


@dataclass(frozen=True)
class EdgeCall:
    area: str
    blk: str
    water: str
    from_m: float
    to_m: float
    call: str
    note: str

    def covers(self, area: str, blk: str, lo: float, hi: float) -> bool:
        return (self.area == area and self.blk == blk and self.from_m - MATCH_M <= lo
                and hi <= self.to_m + MATCH_M)

    def key(self) -> dict:
        return {"area": self.area, "blk": self.blk, "water": self.water, "from_m": self.from_m, "to_m": self.to_m,
                "call": self.call}


def problems(doc) -> list[str]:
    """Every way the file is malformed (empty: well formed). For `check_curated --waters` and `load`."""
    out = []
    if not isinstance(doc, dict) or not isinstance(doc.get("_about"), str):
        return ["area_edge_calls: the file must be an object with an `_about`"]
    for part in ("accepted", "proposed"):
        rows = doc.get(part)
        if not isinstance(rows, list):
            out.append(f"area_edge_calls: `{part}` must be a list")
            continue
        for i, r in enumerate(rows):
            where = f"area_edge_calls.{part}[{i}]"
            if not isinstance(r, dict):
                out.append(f"{where}: not an object")
                continue
            miss = [k for k in _ROW_KEYS if r.get(k) in (None, "")]
            if miss:
                out.append(f"{where}: missing {', '.join(miss)}")
                continue
            if r["call"] not in CALLS:
                out.append(f"{where}: call must be one of {CALLS}, got {r['call']!r}")
            if not str(r["area"]).startswith("area:"):
                out.append(f"{where}: area must be an area id (area:<kind>:<name>)")
            try:
                if not float(r["from_m"]) < float(r["to_m"]):
                    out.append(f"{where}: from_m must be below to_m")
            except (TypeError, ValueError):
                out.append(f"{where}: from_m / to_m must be numbers")
            if part == "accepted" and (not r.get("accepted_by") or not r.get("accepted_on")):
                out.append(f"{where}: an accepted row needs accepted_by and accepted_on")
    return out


def load(path=None) -> list[EdgeCall]:
    """The ACCEPTED calls (`CURATED.waters.area_edge_calls`; a missing or malformed file raises)."""
    if path is None:
        from pipeline.common.curated import CURATED
        path = CURATED.waters.area_edge_calls
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    bad = problems(doc)
    if bad:
        raise SystemExit(f"{path}: " + "; ".join(bad))
    return [EdgeCall(area=r["area"], blk=str(r["blk"]), water=r["water"], from_m=float(r["from_m"]),
                     to_m=float(r["to_m"]), call=r["call"], note=r["note"]) for r in doc["accepted"]]


def closed_by_area(calls) -> dict[str, dict[str, list[tuple[float, float]]]]:
    """{area id: {blk: [(from_m, to_m)]}} of the accepted `closed` calls: what the cutter treats as inside."""
    out: dict = {}
    for c in calls:
        if c.call == "closed":
            out.setdefault(c.area, {}).setdefault(c.blk, []).append((c.from_m, c.to_m))
    return out
