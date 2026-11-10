"""THE CLOSED AREAS OF A DFO ROW'S ENTRY B, AS REVIEWED (T573): one mark per area (scope `closed_area`), written through
to a curated file of its own.

    items()            every closed area the stored DFO rows name: target -> {water_id, key, name, kind, item, words,
                       radius_m?, live} — `dfo_salmon.closures.derive`, keyed per item (`<water_id>#<piece key>`)
    prints_of(target)  what a mark on an area is made on: DFO's words for it, from ANY stored wording (owner 2026-11-10:
                       an item only an older wording names is curated too, "in case they go back to old wording").
                       None when no stored row names the item at all. Liveness is NOT in the prints: an item that
                       goes off the live page and comes back keeps its mark (no re-curation)
    build(decided)     the pieces the owner CONFIRMED (verified marks in force), as `closures.PieceGeometry` records
    document(rows)     the curated file's text: `data/curated/dfo_salmon/closed_areas.json`

The file is its own: no record of the places files is touched, so no existing mark moves. A piece whose item DFO
dropped STAYS in the file (the geometry is kept, to come back if the words return); the reader (`closures.attach`)
marks it not live. Proposals (`<generated>/review/dfo_closed_area_geometry.json`) stay proposals until he confirms.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, Optional

from pipeline.regs.dfo_salmon import closures as K

ABOUT = ("THE CLOSED AREAS THE OWNER CONFIRMED (T573): one record per closed area of a DFO row's entry B — a stretch "
         "between two cuts or a circle (centre + radius), `pipeline.regs.dfo_salmon.closures.PieceGeometry`. Written only "
         "by the review marks (scope closed_area, `python -m pipeline.regs.review.apply closed_areas`). A record whose "
         "item DFO no longer names stays here and is read as not live. Nothing in the atlas or the feed reads this yet.")


def path() -> Path:
    """beside the places files (moved with the whole tree by `CURATED_ROOT`)"""
    from pipeline.common.curated import CURATED
    return Path(CURATED.dfo_salmon.places).parent / "closed_areas.json"


def target_of(water_id: str, key: str) -> str:
    return f"{water_id}#{key}"


def items(locations=None) -> dict[str, dict]:
    """every closed area the stored rows name, by mark target; `live`: a row on DFO's page now names it"""
    from pipeline.regs.review import dfo as D
    locs = locations if locations is not None else [l for _s, l, _w in D.locations(D.default_dir()).values()]
    out: dict[str, dict] = {}
    for e in K.derive(locs).values():
        live = (e.get("status") or "active") == "active"
        for a in e["areas"]:
            t = target_of(e["water_id"], a["key"])
            row = out.setdefault(t, {**a, "water_id": e["water_id"], "water": e.get("water"), "live": False,
                                     "rows": []})
            row["live"] = row["live"] or live
            row["rows"].append(e["derived_from"])
    return out


def prints_of(target: str, locations=None) -> Optional[dict]:
    """the prints a closed-area mark holds: DFO's words for the item, on its water — None when no stored row names it.
    Live or not is not part of them (a mark outlives the item leaving and returning to DFO's page)."""
    it = items(locations).get(target)
    if it is None:
        return None
    return {"text": None, "records": [{"key": f"closed_area:{target}", "meaning": it["item"],
                                       "binding": f"{it['water_id']} · {it['kind']}"
                                                  + (f" · {it['radius_m']:g} m" if it.get("radius_m") else "")}]}


def build(decided) -> list[dict]:
    """the confirmed pieces: every `closed_area` mark in force that is verified, its placement validated — in target
    order. A wrong / deferred mark confirms nothing (its area keeps what the file had only through an earlier verified
    mark, which it superseded: it leaves the file)."""
    rows = []
    for m in sorted(decided.scope("closed_area"), key=lambda m: m.target):
        if getattr(m.verdict, "value", m.verdict) != "verified" or not m.piece:
            continue
        g = K.PieceGeometry.model_validate(m.piece).model_dump(mode="json", exclude_none=True)
        rows.append({**g, "confirmed_by": m.reviewer, "confirmed_on": m.date})
    return rows


def document(rows: list[Mapping]) -> str:
    return json.dumps({"_about": ABOUT, "pieces": list(rows)}, indent=1, ensure_ascii=False) + "\n"


def confirmed() -> list[dict]:
    """the file's pieces as placements (`PieceGeometry` fields only), [] when there is no file yet"""
    p = path()
    if not p.exists():
        return []
    doc = json.loads(p.read_text(encoding="utf-8"))
    keep = set(K.PieceGeometry.model_fields)
    return [{k: v for k, v in r.items() if k in keep} for r in doc.get("pieces") or []]
