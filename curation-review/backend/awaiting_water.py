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


def _bounds_centre(feats: list[dict]) -> list[float] | None:
    xs: list[float] = []
    ys: list[float] = []

    def walk(c):
        if c and isinstance(c[0], (int, float)):
            xs.append(c[0]); ys.append(c[1])
        else:
            for x in c or []:
                walk(x)
    for f in feats:
        walk((f.get("geometry") or {}).get("coordinates"))
    return [round((min(xs) + max(xs)) / 2, 5), round((min(ys) + max(ys)) / 2, 5)] if xs else None


def _centre(item_id: str) -> list[float] | None:
    """A lead's place: the middle of its geometry's bounds (the registry holds no coordinates). A
    `wbk:` lake with no registry item (an unnamed FWA lake) is read from the lakes layer."""
    try:
        if reuse._registry().get(item_id) is None and item_id.startswith("wbk:"):
            return _bounds_centre(reuse._features("lakes", [item_id[4:]], "waterbody"))
        return _bounds_centre((reuse.item_geojson(item_id) or {}).get("features") or [])
    except Exception:
        return None


@lru_cache(maxsize=32)
def _mu_centre(mu: str) -> tuple | None:
    """Where the printed MU is, for a view when there is no lead: the middle of up to 12 of its lakes
    (ONE lakes-layer read), cached per MU."""
    wbks = [i[4:] for i, it in reuse._registry().items()
            if i.startswith("wbk:") and mu in (it.mus or ()) and it.kind == "lake"][:12]
    if not wbks:
        return None
    try:
        cs = [c for f in reuse._features("lakes", wbks, "waterbody") if (c := _bounds_centre([f]))]
    except Exception:
        return None
    return (round(sum(c[0] for c in cs) / len(cs), 5), round(sum(c[1] for c in cs) / len(cs), 5)) if cs else None


def _pt(coord, label: str, kind: str, fit: bool = False, **more) -> dict:
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": coord},
            "properties": {"label": label, "kind": kind, "fit": fit, **more}}


def leads(p: dict) -> list[dict]:
    """The lead text's places, found: each `wbk:`/`gnis:` item by its geometry, each bare `lon lat`
    pair as written (only when no item was found). A lead stream's LAKES (its boundaries of kind lake,
    e.g. Ruth Redfern Creek -> Deception Lake) follow as candidate lakes. [{label, coord, source}]."""
    text = p.get("lead") or ""
    out: list[dict] = []
    seen: set[str] = set()
    cand_ids = {c.get("item_id") for c in p.get("same_name_candidates_elsewhere") or []}
    for m in re.finditer(r"\b((?:wbk|gnis):\d+)\b", text):
        iid = m.group(1)
        if iid in cand_ids:     # a same-name water elsewhere is listed below, not a lead
            continue
        item = reuse._registry().get(iid)
        c = _centre(iid)
        if c:
            out.append({"label": f"{item.name if item else iid} ({iid})", "coord": c, "source": "lead"})
            seen.add(iid)
        for b in (item.boundaries if item is not None else ()):
            if b.kind == "lake" and b.wbk and f"wbk:{b.wbk}" not in seen:
                bc = _centre(f"wbk:{b.wbk}")
                if bc:
                    nm = b.label if not b.label.startswith("lake ") else "unnamed lake"
                    out.append({"label": f"candidate lake on {item.name}: {nm} (wbk:{b.wbk})",
                                "coord": bc, "source": "lead-lake"})
                    seen.add(f"wbk:{b.wbk}")
    for m in ([] if out else re.finditer(r"(-1[1-3]\d\.\d+)[ ,]+(\d{2}\.\d+)", text)):
        out.append({"label": "lead (coordinates in the note)", "coord": [float(m.group(1)), float(m.group(2))],
                    "source": "lead"})
    return out


def proposal(entry_id: str) -> dict | None:
    p = _find(entry_id)
    if p is None:
        return None
    found = leads(p)
    feats = [_pt(l["coord"], l["label"], "lead", fit=True) for l in found]
    mu = _mu_centre(p["printed_mu"])
    mu = list(mu) if mu else None
    if mu:
        feats.append(_pt(mu, f"printed MU {p['printed_mu']} (approx. centre)", "mu", fit=not found))
    cands = []
    for c in p.get("same_name_candidates_elsewhere") or []:
        if not c.get("lonlat"):
            continue
        words = (f"NOT matched — same name elsewhere ({c['km_from_printed_mu']:.0f} km, "
                 f"MU {', '.join(c['mus'])}), shown for reference")
        cands.append({**c, "words": words})
        if len(feats) < 40:
            feats.append(_pt(c["lonlat"], f"NOT matched — {c['name']} elsewhere ({c['km_from_printed_mu']:.0f} km, "
                             f"MU {', '.join(c['mus'])})", "elsewhere"))
    return {**p, "same_name_candidates_elsewhere": cands, "lead_points": found, "printed_mu_centre": mu,
            "map": {"type": "FeatureCollection", "features": feats}, **doc_meta()}
