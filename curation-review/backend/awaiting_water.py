"""T89 AWAITING-WATER entries (owner Q45, 2026-10-23): rules that bind no water because no water of the
printed name is in the printed MU. READ ONLY: `handoff/PLAN/curation-review/t89-awaiting-water.json` is
shown beside the entry (leads, same-name candidates elsewhere, the fix path) so the owner can mark it
-- the mark (POST /api/marks) is the only write; nothing is bound, curated or moved here.

    proposal(entry_id)  the entry's record + a map FeatureCollection of its leads and candidates, or None
    flag(entry_id)      the queue row's compact flag {rules, has_lead}, or None
    doc_meta()          the file's rule / how-to-fix / searched lists
Re-read when the file changes (mtime), no restart.
"""
from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path

import reuse

PATH = Path(os.environ.get("CURATION_T89_AWAITING_WATER")
            or reuse._ROOT / "handoff/PLAN/curation-review/t89-awaiting-water.json")
MAX_CANDIDATES = 8   # the nearest same-name waters are drawn; the full list stays in the panel


@lru_cache(maxsize=4)
def _load(path: str, _mtime: float) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _doc() -> dict:
    try:
        return _load(str(PATH), PATH.stat().st_mtime)
    except (OSError, ValueError):
        return {}


def _find(entry_id: str) -> dict | None:
    return next((p for p in _doc().get("proposals") or []
                 if p.get("kind") == "awaiting_water" and p.get("entry_id") == entry_id), None)


def doc_meta() -> dict:
    d = _doc()
    return {k: d.get(k) for k in ("ticket", "generated", "rule", "how_to_fix", "searched")}


def flag(entry_id: str) -> dict | None:
    p = _find(entry_id)
    return None if p is None else {"rules": p.get("rules"), "has_lead": bool(p.get("lead"))}


def _centre(item_id: str) -> list[float] | None:
    """A lead's place: the middle of its geometry's bounds (the registry holds no coordinates)."""
    try:
        gj = reuse.item_geojson(item_id)
    except Exception:
        return None
    xs: list[float] = []
    ys: list[float] = []

    def walk(c):
        if c and isinstance(c[0], (int, float)):
            xs.append(c[0]); ys.append(c[1])
        else:
            for x in c or []:
                walk(x)
    for f in (gj or {}).get("features") or []:
        walk((f.get("geometry") or {}).get("coordinates"))
    return [round((min(xs) + max(xs)) / 2, 5), round((min(ys) + max(ys)) / 2, 5)] if xs else None


def _pt(coord, label: str, kind: str) -> dict:
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": coord},
            "properties": {"label": label, "kind": kind}}


def leads(p: dict) -> list[dict]:
    """The lead text's places, found: each `wbk:`/`gnis:` item by its geometry, each bare `lon lat`
    pair as written (only when no item was found). [{label, coord, source}] -- a lead that names no place yields none."""
    text = p.get("lead") or ""
    out: list[dict] = []
    for m in re.finditer(r"\b((?:wbk|gnis):\d+)\b", text):
        item = reuse._registry().get(m.group(1))
        c = _centre(m.group(1))
        if c:
            out.append({"label": f"{item.name if item else m.group(1)} ({m.group(1)})",
                        "coord": c, "source": "lead"})
    for m in ([] if out else re.finditer(r"(-1[1-3]\d\.\d+)[ ,]+(\d{2}\.\d+)", text)):
        out.append({"label": "lead (coordinates in the note)", "coord": [float(m.group(1)), float(m.group(2))],
                    "source": "lead"})
    return out


def proposal(entry_id: str) -> dict | None:
    p = _find(entry_id)
    if p is None:
        return None
    found = leads(p)
    cands = [c for c in p.get("same_name_candidates_elsewhere") or [] if c.get("lonlat")]
    feats = [_pt(l["coord"], l["label"], "lead") for l in found]
    feats += [_pt(c["lonlat"], f"{c['name']} · MU {', '.join(c['mus'])} · {c['km_from_printed_mu']:.0f} km away",
                  "candidate") for c in cands[:MAX_CANDIDATES]]
    return {**p, "lead_points": found, "map": {"type": "FeatureCollection", "features": feats},
            **doc_meta()}
