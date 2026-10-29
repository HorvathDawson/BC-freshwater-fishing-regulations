"""THE DFO CASCADE VIEW (T229): which DFO salmon places apply on each section of one watershed, and which one
answers per species and day — the release's own chains, recomputed from the same three inputs, read only.

    data/curated/regulations/entries/dfo_salmon/region-*.json   the places (precedence, water, words)
    <generated>/regs/dfo_salmon/typed/region-*.json             their rules, by fingerprint
    <reach run>/dfo_salmon.jsonl.gz                             each place's sections (what the bundle reads)

THE CHAIN of a section is every drawn DFO place covering it, NARROWEST FIRST by the bundle's own order
(`pipeline.deliver.salmon.chain_order`: precedence desc — a water 3, a tidal-area default 2, a section default or
closure 1, the region default 0 — then, within a tier, the place covering fewer sections, then location id). Per species and day the FIRST place of the chain with a
rule for that species that day answers (`salmon.verdict_on`, called one place at a time, which is the same
walk). A place with rules that is NOT DRAWN yet sits on every section of its water as `undrawn`: the app paints
those sections `check` (partial coverage).

Nothing here is a second implementation of a verdict: `verdict_on` / `gear_on` are the bundle's.
"""
from __future__ import annotations

import gzip
import json
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

import reuse
from pipeline.regs.review import dfo as D

#: the watersheds the view offers: a frame is one DFO place whose sections ARE the watershed
FRAMES = {
    "skeena": {"label": "Skeena watershed (DFO Region 6, section B)", "region": "6", "frame": "6:b:section"},
    "nass": {"label": "Nass watershed (DFO Region 6, section C)", "region": "6", "frame": "6:c:section"},
}
TIERS = {3: "named water", 2: "tidal-area default", 1: "section default", 0: "region default"}


def _reach_file() -> Path:
    return Path(reuse.REACH_DIR) / "dfo_salmon.jsonl.gz"


def _stamp() -> tuple:
    ps = [_reach_file(), *sorted(Path(D.default_dir()).glob("region-*.json")),
          *sorted(Path(D.default_typed_dir()).glob("region-*.json"))]
    return tuple((str(p), p.stat().st_mtime_ns) for p in ps if p.exists())


@lru_cache(maxsize=2)
def _placed(_key: tuple) -> dict:
    """location_id -> its reach row (`mapped`, `sections`)."""
    with gzip.open(_reach_file(), "rt", encoding="utf-8") as fh:
        return {r["location_id"]: r for r in map(json.loads, fh)}


def _label(loc: dict) -> str:
    area = ((loc.get("source_text") or {}).get("specific_area") or "").strip()
    w = loc.get("water") or ""
    return f"{w} — {area}" if w and area else (w or area or loc["location_id"])


@lru_cache(maxsize=4)
def _model(name: str, _key: tuple) -> dict:
    """Everything but geometry: places, chains (with their year of winners) and section -> chain."""
    from pipeline.deliver import salmon as S

    cfg = FRAMES[name]
    placed = _placed(_key)
    locs = {lid: v for lid, v in D.locations().items() if v[0] == cfg["region"]}
    frame = set((placed.get(cfg["frame"]) or {}).get("sections") or ())
    rules = {lid: D.rule_rows(loc)[1] for lid, (_s, loc, _w) in locs.items()}
    rows = {lid: D.rule_rows(loc)[0] for lid, (_s, loc, _w) in locs.items()}

    covering: dict[str, list[str]] = defaultdict(list)
    for lid in locs:
        r = placed.get(lid)
        if r and r.get("mapped"):
            for s in r["sections"]:
                if s in frame:
                    covering[s].append(lid)
    # an undrawn place with rules today: every section of its water's items (the bundle's PARTIAL COVERAGE)
    reg = reuse._registry()
    undrawn_on: dict[str, set] = defaultdict(set)
    for lid, (_s, loc, water) in locs.items():
        r = placed.get(lid)
        if (r and r.get("mapped")) or not rules[lid] or not water:
            continue
        for it in water.get("item_ids") or ():
            item = reg.get(it)
            for s in (getattr(item, "section_ids", None) or ()):
                if s in frame:
                    undrawn_on[s].add(lid)

    # the bundle's own sort key (`salmon.chain_order`): tier, then the more specific place (fewer sections), then id
    order = S.chain_order({lid: loc.get("precedence", 0) for lid, (_s, loc, _w) in locs.items()},
                          S.extent_sizes(placed.values()))
    chain_ix: dict[tuple, int] = {}
    chains: list[dict] = []
    sec_chain: dict[str, int] = {}
    for s in frame:
        key = (tuple(sorted(set(covering.get(s, ())), key=order)), tuple(sorted(undrawn_on.get(s, ()))))
        if key not in chain_ix:
            chain_ix[key] = len(chains)
            chains.append({"places": list(key[0]), "undrawn": list(key[1]), "n": 0})
        chains[chain_ix[key]]["n"] += 1
        sec_chain[s] = chain_ix[key]

    used = sorted({l for c in chains for l in c["places"] + c["undrawn"]}, key=order)
    pix = {l: i for i, l in enumerate(used)}
    n_frame = max(len(frame), 1)
    places = []
    for l in used:
        _s, loc, water = locs[l]
        r = placed.get(l) or {}
        n_in = sum(c["n"] for c in chains if l in c["places"])
        places.append({
            "id": l, "label": _label(loc), "water": loc.get("water") or "", "section": loc.get("section"),
            "precedence": loc.get("precedence", 0), "tier": TIERS.get(loc.get("precedence", 0), "?"),
            "kind": loc.get("kind"), "status": loc.get("status"), "drawn": bool(r.get("mapped")),
            "n_sections": len(r.get("sections") or ()), "n_in_frame": n_in, "share": round(n_in / n_frame, 4),
            "tributaries": (loc.get("binding") or {}).get("tributaries"),
            "rows": rows[l], "has_rules": bool(rules[l]),
        })

    # the year per chain: per species, runs of [day, winning place (index into `places`, -1 none), status, words]
    for c in chains:
        seq = c["places"]
        per = {}
        for sp in S.SPECIES:
            runs, prev = [], None
            for d, md in enumerate(S.DAYS):
                w, st, t = -1, "closed", S.NOT_LISTED["t"]
                for l in seq:
                    v = S.verdict_on([rules[l]], sp, md)
                    if v:
                        w, st, t = pix[l], v["s"], v["t"]
                        break
                cur = (w, st, t)
                if cur != prev:
                    runs.append([d, w, st, t])
                    prev = cur
            per[sp] = runs
        gear, prev = [], None
        for d, md in enumerate(S.DAYS):
            w, g = -1, ()
            for l in seq:
                g = S.gear_on([rules[l]], md)
                if g:
                    w = pix[l]
                    break
            cur = (w, tuple(g))
            if cur != prev:
                gear.append([d, w, list(g)])
                prev = cur
        c["year"] = per
        c["gear"] = gear
        c["partial"] = bool(c["undrawn"])
        c["places"] = [pix[l] for l in c["places"]]
        c["undrawn"] = [pix[l] for l in c["undrawn"]]
    return {"places": places, "chains": chains, "sec_chain": sec_chain, "n_frame": len(frame)}


#: a lake at least this big (ha) is drawn as its FWA outline; a smaller one as the line through it
LAKE_MIN_HA = 25.0


def _lake_polys(wbks: set) -> dict:
    """wbk -> outline (EPSG:3005), ONE scan of the FWA lakes table (it has no index on the key, so a
    lookup per lake — `reuse._fwa_lake_poly` — is a full scan each: 17,529 of them in the Skeena)."""
    import sqlite3
    from shapely import wkb as _wkb
    from shapely.ops import unary_union
    src = reuse.SOURCE / "bc_fisheries_data.gpkg"
    out: dict = defaultdict(list)
    if not src.exists():
        return {}
    db = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    try:
        for k, blob in db.execute("SELECT WATERBODY_KEY, geom FROM lakes WHERE AREA_HA >= ?", (LAKE_MIN_HA,)):
            if blob and str(k) in wbks:
                env = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get((blob[3] >> 1) & 0x07, 0)
                g = _wkb.loads(bytes(blob[8 + env:]))
                if g is not None and not g.is_empty:
                    out[str(k)].append(g)
    finally:
        db.close()
    return {k: v[0] if len(v) == 1 else unary_union(v) for k, v in out.items()}


@lru_cache(maxsize=8)
def _geojson(name: str, _key: tuple, min_order: int, tol: float) -> dict:
    """One feature per chain: its stream sections merged + simplified, lakes as outline + fill."""
    import shapely
    from shapely.geometry import MultiLineString, mapping
    from shapely.ops import linemerge, transform

    m = _model(name, _key)
    nodes = reuse._graph().nodes
    geoms = reuse._geoms()
    lines: dict[int, list] = defaultdict(list)
    lakes: dict[int, list] = defaultdict(list)
    polys = _lake_polys({s.split(":", 1)[1] for s in m["sec_chain"] if s.startswith("lake:")})
    for s, ci in m["sec_chain"].items():
        if s.startswith("lake:"):
            g = polys.get(s.split(":", 1)[1])
            if g is not None:
                lakes[ci].append(g)
                continue
            g = geoms.get(s)                       # a small lake: the line through it (all streams only)
            if min_order <= 1 and g is not None and not g.is_empty and g.geom_type in ("LineString", "MultiLineString"):
                lines[ci].extend(getattr(g, "geoms", [g]))
            continue
        n = nodes.get(s)
        o = getattr(n, "stream_order", None) if n else None
        if min_order > 1 and (o or 0) < min_order:
            continue
        g = geoms.get(s)
        if g is None or g.is_empty:
            continue
        lines[ci].extend(getattr(g, "geoms", [g]))
    to_ll = reuse._to_lonlat()
    feats = []
    for ci in sorted(set(lines) | set(lakes)):
        if lines.get(ci):
            g = linemerge(MultiLineString(lines[ci])).simplify(tol, preserve_topology=False)
            if not g.is_empty:
                feats.append({"type": "Feature", "properties": {"chain": ci, "kind": "line"},
                              "geometry": mapping(transform(to_ll, g))})
        for p in lakes.get(ci, ()):
            if p.geom_type in ("Polygon", "MultiPolygon"):
                ps = p.simplify(tol / 2, preserve_topology=True)
                feats.append({"type": "Feature", "properties": {"chain": ci, "kind": "lake"},
                              "geometry": mapping(transform(to_ll, ps))})
                feats.append({"type": "Feature", "properties": {"chain": ci, "kind": "line", "lake": True},
                              "geometry": mapping(transform(to_ll, shapely.boundary(ps)))})
            else:
                feats.append({"type": "Feature", "properties": {"chain": ci, "kind": "line", "lake": True},
                              "geometry": mapping(transform(to_ll, p.simplify(tol)))})
    return {"type": "FeatureCollection", "features": feats}


def cascade(name: str, min_order: int = 3, tol: float = 30.0) -> dict:
    if name not in FRAMES:
        raise KeyError(name)
    key = _stamp()
    m = _model(name, key)
    return reuse._json_safe({
        "frame": {"id": name, **FRAMES[name]}, "frames": {k: v["label"] for k, v in FRAMES.items()},
        "reach_file": str(_reach_file()), "n_sections": m["n_frame"], "min_order": min_order,
        "places": m["places"], "chains": m["chains"],
        "geojson": _geojson(name, key, int(min_order), float(tol)),
    })
