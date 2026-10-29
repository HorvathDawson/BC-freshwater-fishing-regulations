"""DFO PLACE EDGES THAT ARE NOT SPLITS YET (T228, owner 2026-10-28: "sections are bounded by extents (named splits);
salmon DFO places' extents are splits too; every boundary is a named split").

The bundle build cuts a water where the DFO salmon changes only at a named split. Where the salmon changes at a
point with no split, it does not cut and does not name the point. It records the point as a DATA GAP
(`salmon_edge_gap`): a DFO place edge that has not been placed as a split. This module turns those rows, plus the
places whose placement covers a water their own words exclude ("not including Atnarko River"), into items on the
review's DFO queue (`handoff/PLAN/curation-review/dfo-suggestions.json`, `suggestions[location_id]`). Each item says
what to place. Where the edge is a polygon line (a management-unit group's edge crossing the river), it carries a
pre-filled NEW SPLIT at the crossing, named after the area. Nothing here writes splits.json or the curated files.

    .venv/bin/python -m pipeline.regs.dfo_salmon.edge_gaps [--bundle B] [--out dfo-suggestions.json]
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "handoff/PLAN/curation-review/dfo-suggestions.json"
_NOT_INCLUDING = re.compile(r"not including (?:the )?([A-Z][\w' .-]+?(?: River| Creek| Lake))", re.I)
_MU_GROUP = re.compile(r"area:mu_group:management_units_(\d+)_(\d+)_to_(\d+)_(\d+)$")


def _words(loc) -> str:
    from pipeline.deliver.salmon import _label
    return _label(loc)


def mu_group_crossing(area_id: str, item_name_gnis: Optional[int], near: tuple, gpkg: str) -> Optional[dict]:
    """Where the river (`GNIS_ID`) crosses the edge of an MU group (`area:mu_group:management_units_5_6_to_5_11`), the
    crossing nearest `near` (lon, lat), as {coord, name}. None when the area is not an MU group or nothing crosses."""
    m = _MU_GROUP.match(area_id)
    if not m or item_name_gnis is None:
        return None
    import pyogrio
    from pyproj import Transformer
    from shapely.geometry import Point
    from shapely.ops import unary_union
    r, a, r2, b = (int(x) for x in m.groups())
    mus = [f"{r}-{k}" for k in range(a, b + 1)] if r == r2 else []
    if not mus:
        return None
    w = pyogrio.read_dataframe(gpkg, layer="wmu", where="WILDLIFE_MGMT_UNIT_ID IN (%s)" % ",".join(f"'{x}'" for x in mus))
    s = pyogrio.read_dataframe(gpkg, layer="streams", where=f"GNIS_ID = {int(item_name_gnis)}")
    if w.empty or s.empty:
        return None
    edge = unary_union(list(w.geometry)).boundary
    x = unary_union(list(s.geometry)).intersection(edge)
    pts = [p for p in getattr(x, "geoms", [x]) if not p.is_empty]
    if not pts:
        return None
    to_bc = Transformer.from_crs(4326, 3005, always_xy=True)
    to_ll = Transformer.from_crs(3005, 4326, always_xy=True)
    here = Point(*to_bc.transform(*near))
    best = min(pts, key=lambda p: p.distance(here)).centroid
    lon, lat = to_ll.transform(best.x, best.y)
    return {"coord": [round(lon, 6), round(lat, 6)],
            "name": f"Management Units {r}-{a} to {r}-{b} boundary"}


def items(bundle: Path, gpkg: Optional[str] = None) -> List[dict]:
    """The queue items: one per salmon edge gap (`salmon_edge_gap`), and one per DFO place whose placement covers a water
    its own words exclude. Each says the water, where, the two DFO places that differ, their words and the cause."""
    from pipeline.deliver.salmon import read_bundle
    from pipeline.regs.dfo_salmon import entries as E
    from pipeline.regs.dfo_salmon.fetch import ALL_SLUGS, PAGES
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    P, secs = read_bundle(db)
    chain_of = dict(secs)
    locs, water_items = {}, {}
    for slug in ALL_SLUGS:
        if PAGES[slug].is_stub:
            continue
        ef = E.load(slug, E.ENTRIES_DIR)
        for loc in ef.locations:
            locs[loc.location_id] = loc
            w = ef.water(loc.water_id)
            water_items[loc.location_id] = list(getattr(w, "item_ids", None) or [])
    if gpkg is None:
        from project_config import get_config
        gpkg = str(get_config().fwa_data_gpkg)
    out: List[dict] = []
    for o, d, u, lon, lat, cd, cu in db.execute(
            "SELECT ord, down_sid, up_sid, lon, lat, down_chain, up_chain FROM salmon_edge_gap ORDER BY ord"):
        iid, name = db.execute("SELECT item_id, name FROM item WHERE ord = ?", (o,)).fetchone()
        a, b = P["chains"][cd]["l"], P["chains"][cu]["l"]
        only = [p for p in a if p not in b] + [p for p in b if p not in a]
        lid = only[0] if only else None
        loc = locs.get(lid)
        area = next((e.area_id for e in (loc.binding.extents if loc else []) if getattr(e.op, "value", e.op) == "within"),
                    None)
        cross = mu_group_crossing(area, int(iid.split(":")[1]) if iid.startswith("gnis:") else None, (lon, lat), gpkg) \
            if area and Path(gpkg).exists() else None
        # an area-bound place keeps its extent (`within` its area): what the edge needs is a NEW SPLIT at the crossing,
        # proposed here in the marks' NewSplit shape (the extent editor has no point to hang it on)
        split = {"ref": "sg1", "anchor": "coord", "name": cross["name"], "coord": cross["coord"], "on_item": iid,
                 "quote": _words(loc) if loc else "", "confidence": "medium"} if cross else None
        out.append({
            "location_id": lid, "water": name, "text": _words(loc) if loc else "",
            "state": "edge_gap", "why": "edge_not_placed", "outcome": "new_split" if cross else "place_edge",
            "confidence": "none", "items": [iid], "status": "active",          # no extent fix: the split is the work
            "edge_gap": {"water": name, "item_id": iid, "at": [lon, lat], "down_sid": d, "up_sid": u,
                         "places": {"below": [[p, _words(locs[p]) if p in locs else ""] for p in a],
                                    "above": [[p, _words(locs[p]) if p in locs else ""] for p in b]},
                         "differ": [[p, _words(locs[p]) if p in locs else ""] for p in only],
                         "cause": (f"area-bound: the {area} edge crosses the water ({cross['name']})" if cross
                                   else f"the place {lid} covers only part of the water"),
                         **({"new_split": split} if split else {})},
            "reason": (f"{name}: the salmon changes at {lon},{lat} where no split stands (a DFO place's edge not placed "
                       f"as a split) — not cut until placed"),
            })
    # a place whose own words exclude a water its placement covers ("Bella Coola River (not including Atnarko River)")
    name_of = dict(db.execute("SELECT item_id, name FROM item"))
    ord_of = dict(db.execute("SELECT item_id, ord FROM item"))
    for lid, loc in sorted(locs.items()):
        for m in _NOT_INCLUDING.finditer(_words(loc)):
            excl = m.group(1).strip()
            hit = [i for i, n in name_of.items() if n.lower() == excl.lower()]
            for iid in hit:
                sids = [s for (s,) in db.execute("SELECT sid FROM item_section WHERE ord = ?", (ord_of[iid],))]
                ci = [i for i, c in enumerate(P["chains"]) if lid in c["l"]]
                n = sum(1 for s in sids if chain_of.get(s, 0) in ci)
                if not n:
                    continue
                out.append({
                    "location_id": lid, "water": name_of.get(iid), "text": _words(loc),
                    "state": "edge_gap", "why": "covers_excluded_water", "outcome": "extent_fix", "matched": "whole",
                    "confidence": "high", "items": [iid], "status": "active",
                    "edge_gap": {"water": name_of.get(iid), "item_id": iid, "sections": n, "of": len(sids),
                                 "cause": f"the place's tributary walk covers {excl}, which its own words exclude"},
                    "reason": (f"{_words(loc)} covers {n} of {len(sids)} sections of {excl} ({iid}) — add {iid} to "
                               f"its tributary_excludes"),
                    # the place's own water, its tributaries, the excluded one named (the editor has no exclude field:
                    # the words say what to add)
                    "suggestion": {"op": "whole", "points": [], "items": water_items.get(lid, []), "tributaries": True, "new_splits": [],
                                   "words": f"the whole water and its tributaries, NOT {excl}: add {iid} to "
                                            f"tributary_excludes"}})
    return out


def fold(doc: dict, rows: List[dict]) -> dict:
    """The queue file with `rows` folded in: each location's suggestion row gains `edge_gaps` (a new row is made for a
    location with none). Idempotent: the previous `edge_gaps` are replaced."""
    s = doc.setdefault("suggestions", {})
    for r in s.values():
        r.pop("edge_gaps", None)
    for k in [k for k, r in s.items() if r.get("state") == "edge_gap"]:
        del s[k]
    for r in rows:
        key = r["location_id"] or f"edge:{r['items'][0]}"
        if key in s:
            s[key].setdefault("edge_gaps", []).append(r["edge_gap"])
        else:
            s[key] = {**r, "edge_gaps": [r["edge_gap"]]}
    doc["edge_gaps"] = len(rows)
    return doc


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    from pipeline.common.curated import GENERATED
    ap.add_argument("--bundle", default=str(Path(GENERATED.bundle) / "bundle.sqlite"))
    ap.add_argument("--out", default=str(OUT))
    a = ap.parse_args(argv)
    rows = items(Path(a.bundle))
    p = Path(a.out)
    doc = fold(json.loads(p.read_text()) if p.exists() else {}, rows)
    p.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    for r in rows:
        print(f"{r['water']}: {r['edge_gap']['cause']} -> {r['outcome']} ({r['location_id']})")
    print(f"{len(rows)} edge item(s) on the DFO queue ({p})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
