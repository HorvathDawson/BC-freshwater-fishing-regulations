"""THE DFO REVIEW MODE's reads (T164): every scraped DFO salmon extent, on the map, with its splits.

    queue(...)            one row per DFO location (water, the notice's words, rules, extent, mark)
    progress()            marked / to do overall, per region, and how many still need a cut
    detail(id)            one location: the notice's words verbatim, its scraped rows, its curated
                          extent, the cut-points on its water (labelled map points), anchor hints,
                          the mark in force
    reach(id)             what its extent binds NOW (the builder's own `build_reach`), as geometry
    check_anchor(id, n)   a proposed NEW split (`marks.NewSplit`) resolved on the map — validated
                          server-side, cheap: a snap to the water, a tributary's mouth, a lake's edge
    extent_preview(id, f) a proposed extent (`marks.ExtentFix`) resolved, read only
    mark_for(body, ...)   the `Mark` a DFO mark payload makes (anchors validated, resolved points
                          stamped) — `reuse.post_mark` commits it, the ONE write

READ ONLY. The curated geography (`data/curated/regulations/entries/dfo_salmon/`) and splits.json
are never written here: a new split is a PROPOSAL in the mark, and T185 turns marks into reviewed
changes (AGENTS 66/67). The rules are the re-scrape's (`<generated>/regs/dfo_salmon/typed/`).
"""
from __future__ import annotations

import json
import os
import re
from collections import Counter
from functools import lru_cache
from pathlib import Path

import reuse
from pipeline.regs.review import dfo as D

#: the DFO entry files and typed feed the app reads; `CURATION_DFO_DIR` / `CURATION_DFO_TYPED` point
#: them at copies (tests)
DFO_DIR = Path(os.environ["CURATION_DFO_DIR"]) if os.environ.get("CURATION_DFO_DIR") else None
DFO_TYPED = Path(os.environ["CURATION_DFO_TYPED"]) if os.environ.get("CURATION_DFO_TYPED") else None

#: THE SUGGESTED EXTENTS (`pipeline.regs.dfo_salmon.suggest`, a frozen, reviewed file): shown on each
#: location, never applied — "accept suggestion" only pre-fills the editor. `CURATION_DFO_SUGGESTIONS`
#: points it at a copy (tests).
SUGGESTIONS_PATH = (Path(os.environ["CURATION_DFO_SUGGESTIONS"]) if os.environ.get("CURATION_DFO_SUGGESTIONS")
                    else reuse._ROOT / "handoff/PLAN/curation-review/dfo-suggestions.json")

#: how far (m) a map point may sit from the water it cuts — further is a click on another water
SNAP_MAX_M = 500.0
#: how far (m) a tributary's mouth may sit from the water — further, it is not its tributary
MOUTH_MAX_M = 300.0
#: how close (m) a section end must be to a lake's outline to be its inlet / outlet
LAKE_TOUCH_M = 40.0
#: the queue filters
NEEDS = ("needs_cut", "bound", "unbound", "no_water")
VERIFIES = ("unmarked", "verified", "wrong", "unsure", "deferred", "stale", "todo")

TRIB_RE = re.compile(r"\b([A-Z][A-Za-z'\-]*(?:\s+[A-Z][A-Za-z'\-]*)*\s+(?:River|Creek|Slough))\b")
LAKE_RE = re.compile(r"\b([A-Z][A-Za-z'\-]*(?:\s+[A-Z][A-Za-z'\-]*)*\s+Lake)\b")


def _locations():
    return D.locations(DFO_DIR)


def _states() -> dict:
    """location_id -> the DFO mark in force on it."""
    from pipeline.regs.review import marks as M
    return {m.target: m for m in M.latest(M.load(reuse.MARKS_PATH)).values() if m.scope == "dfo"}


def _proposals() -> list[dict]:
    """T210: THE PROPOSED SPLITS of EVERY mark (superseded ones too), deduped by anchor
    (`marks.proposals`): one per distinct cut, the first proposal's name kept, with the marks that
    proposed it. Read only — the store is never edited."""
    from pipeline.regs.review import marks as M
    return M.proposals(M.load(reuse.MARKS_PATH))


def _proposal_view(p: dict) -> dict:
    n = p["split"]
    return {"id": p["id"], "anchor": n.anchor, "name": n.name, "item_id": n.item_id,
            "split_id": n.split_id, "on_item": n.on_item, "coord": list(n.coord) if n.coord else None,
            "blk": n.blk, "measure": n.measure, "side": n.side, "offset_m": n.offset_m,
            "quote": n.quote, "marks": p["marks"], "n_marks": len(p["marks"])}


def _on_river(p: dict, items: list[str], blks: set[str]) -> bool:
    n = p["split"]
    return bool(n.on_item in items or (n.blk and str(n.blk) in blks))


def proposed_on(items: list[str]) -> list[dict]:
    """The proposed splits (deduped) that cut this river — its item(s), or a blk of their line
    sections — whichever location or entry on it is open."""
    blks = {str(n.blk) for _nid, n, _g in _line_sections(items)} if items else set()
    return [_proposal_view(p) for p in _proposals() if _on_river(p, items, blks)]


@lru_cache(maxsize=2)
def _suggestions_at(path: str, _mtime: float) -> dict:
    try:
        return json.loads(Path(path).read_text()).get("suggestions") or {}
    except (OSError, ValueError):
        return {}


def suggestion(location_id: str) -> dict | None:
    """The SUGGESTED EXTENT for this location (`suggest.suggest_one`'s row: op, points, new splits,
    confidence, reason), or None. Read only; the mark stays the only write."""
    p = SUGGESTIONS_PATH
    try:
        mt = p.stat().st_mtime
    except OSError:
        return None
    row = _suggestions_at(str(p), mt).get(location_id)
    return row if row and row.get("suggestion") else None


def _now(loc, water):
    from pipeline.regs.review.marks import Prints
    return Prints.model_validate(D.prints(loc, water, DFO_TYPED))


def _verify(m, loc, water) -> dict:
    from pipeline.regs.review import marks as M
    if m is None:
        return {"status": "unmarked", "verdict": None, "reasons": []}
    return M.status(m, _now(loc, water))


def _row(slug: str, loc: dict, water: dict | None, m) -> dict:
    st = loc.get("source_text") or {}
    need = D.need(loc)
    rows, rules = D.rule_rows(loc, DFO_TYPED)
    v = _verify(m, loc, water)
    if not water or not water.get("item_ids"):
        state = "no_water"
    elif need["bound"]:
        state = "bound"
    elif need["missing"]:
        state = "needs_cut"
    else:
        state = "unbound"
    return {"id": loc["location_id"], "region": slug, "kind": loc.get("kind"),
            "water": loc.get("water") or "", "water_id": loc.get("water_id"),
            "items": list((water or {}).get("item_ids") or []),
            "section": loc.get("section"), "status": loc.get("status") or "active",
            "text": st.get("specific_area") or "", "op": st.get("op"),
            "anchors": list(st.get("anchor_types") or []),
            "n_rows": len(rows), "n_rules": len(rules), "need": need, "state": state,
            "duplicate_of": loc.get("duplicate_of"),
            "verify": v["status"], "reasons": v["reasons"],
            "suggested": (suggestion(loc["location_id"]) or {}).get("confidence"),
            "new_splits": len(m.new_splits) if m is not None else 0}


def _rows() -> list[dict]:
    states = _states()
    out = [_row(slug, loc, water, states.get(lid)) for lid, (slug, loc, water) in _locations().items()]
    # the review order: the live page first, then what is missing a cut, then region and water
    rank = {"needs_cut": 0, "unbound": 1, "no_water": 2, "bound": 3}
    out.sort(key=lambda r: (r["status"] != "active", rank[r["state"]], reuse._region_key(r["region"]),
                            r["water"], r["id"]))
    return out


def queue(region: str | None = None, status: str | None = None, need: str | None = None,
          verify: str | None = None, q: str | None = None) -> list[dict]:
    """THE DFO QUEUE: every scraped location, filtered. `status` active | dormant; `need` one of
    `NEEDS`; `verify` one of `VERIFIES` (todo = unmarked or stale); `q` a substring of the water or
    the notice's words."""
    rows = _rows()
    if region:
        rows = [r for r in rows if r["region"] == region]
    if status:
        rows = [r for r in rows if r["status"] == status]
    if need:
        rows = [r for r in rows if r["state"] == need]
    if verify:
        rows = [r for r in rows if (r["verify"] in ("unmarked", "stale") if verify == "todo"
                                    else r["verify"] == verify)]
    if q and q.strip():
        want = q.strip().casefold()
        rows = [r for r in rows if want in r["water"].casefold() or want in r["text"].casefold()
                or want in r["id"].casefold()]
    return rows


def progress() -> dict:
    """Marked / total overall and per region, with the counts by mark, by extent state and live /
    dormant — the DFO queue's progress bar."""
    rows = _rows()

    def count(rs):
        done = sum(1 for r in rs if r["verify"] not in ("unmarked", "stale"))
        return {"total": len(rs), "done": done, "by_verify": dict(Counter(r["verify"] for r in rs)),
                "by_state": dict(Counter(r["state"] for r in rs)),
                "new_splits": sum(r["new_splits"] for r in rs)}   # carried by marks (overlaps)
    regions = sorted({r["region"] for r in rows}, key=lambda s: (int(re.sub(r"\D", "", s) or 0), s))
    allc = {**count(rows), "new_splits": len(_proposals())}      # T210: distinct cuts, not copies
    return {"all": allc, "active": count([r for r in rows if r["status"] == "active"]),
            "regions": {s: count([r for r in rows if r["region"] == s]) for s in regions}}


# --------------------------------------------------------------------------- the detail

def _item_ref(iid: str) -> dict:
    it = reuse._registry().get(iid)
    return {"id": iid, "name": it.name if it else iid, "kind": it.kind if it else None,
            "mus": list(it.mus)[:4] if it else [], "n_sections": len(it.section_ids) if it else 0}


def _split_feature(sid: str, *, rules: list[str], to_verify: bool, item_id: str | None) -> dict | None:
    """One labelled split point (the `EntrySplits` feature shape the map draws)."""
    meta, records, bounds = reuse._splits_meta(), reuse._split_records(), reuse._boundary_index()
    water, sp = records.get(sid, ("", {}))
    on_item, b = bounds.get(sid, (item_id, None))
    if not water and on_item in reuse._registry():
        water = reuse._registry()[on_item].name
    label = ((meta.get(sid) or {}).get("label") or sp.get("label")
             or (getattr(b, "label", None) if b is not None else None) or sid)
    geom = None
    if sid in meta:
        f = reuse._geo(reuse._split_point(sid), "named_split", {})
        geom = f["geometry"] if f else None
    elif b is not None and getattr(b, "wbk", None):
        lake = reuse._features("lakes", [str(b.wbk)], "split")
        pt = reuse._rep_point(lake[0].get("geometry")) if lake else None
        geom = {"type": "Point", "coordinates": pt} if pt else None
    props = {"split_id": sid, "label": label, "water": water,
             "name": f"{water}: {label}" if water else label, "rules": rules, "parts": [],
             "to_verify": to_verify, "grade": None, "item_id": on_item}
    if geom is None:
        return {"unplaced": {**props, "why": "the served atlas has no point for it"}}
    return {"type": "Feature", "geometry": geom, "properties": props}


def _splits_fc(loc: dict, water: dict | None) -> dict:
    """Every cut on the location's water (the items' boundaries) and every split its extent names,
    labelled; the extent's own are `rules: ["extent"]`."""
    named = {s for e in (loc.get("binding") or {}).get("extents") or [] for s in e.get("splits") or []}
    named |= {s for e in (loc.get("binding") or {}).get("tributary_excludes") or []
              for s in e.get("splits") or []}
    on_water: dict[str, str] = {}
    for iid in (water or {}).get("item_ids") or []:
        it = reuse._registry().get(iid)
        for b in (it.boundaries if it else ()):
            on_water.setdefault(b.id, iid)
    feats, unplaced = [], []
    for sid in list(dict.fromkeys([*named, *on_water])):
        f = _split_feature(sid, rules=["extent"] if sid in named else [], to_verify=False,
                           item_id=on_water.get(sid))
        if f is None:
            continue
        if "unplaced" in f:
            unplaced.append(f["unplaced"])
        else:
            feats.append(f)
    return {"type": "FeatureCollection", "features": feats, "unplaced": unplaced}


def _hints(loc: dict, water: dict | None) -> list[dict]:
    """ANCHOR HINTS from the notice's words: each named river / creek / lake, with the registry
    items of that name (a tributary of this water first). A hint, never a binding: the reviewer
    picks."""
    from pipeline.common.names import key as name_key
    text = (loc.get("source_text") or {}).get("specific_area") or ""
    own = set((water or {}).get("item_ids") or [])
    out: list[dict] = []
    seen: set[str] = set()
    for rx, kind, anchor in ((TRIB_RE, "stream", "confluence"), (LAKE_RE, "lake", "lake_outlet")):
        for mm in rx.finditer(text):
            name = mm.group(1)
            if name in seen or name_key(name) == name_key(loc.get("water") or ""):
                continue
            seen.add(name)
            hits = [i for i in reuse.search_items(name, kind=kind, limit=8)
                    if name_key(i["name"]) == name_key(name) and i["id"] not in own]
            out.append({"name": name, "anchor": anchor, "items": hits[:4]})
    for a in (loc.get("source_text") or {}).get("anchor_types") or []:
        word = {"bridge": "bridge", "dam_or_hatchery": "dam", "boundary_sign": "sign",
                "road": "bridge", "point": "coord", "place_name": "coord"}.get(a)
        if word and not any(h["anchor"] == word for h in out):
            out.append({"name": a.replace("_", " "), "anchor": word, "items": []})
    return out


def detail(location_id: str) -> dict | None:
    got = _locations().get(location_id)
    if got is None:
        return None
    slug, loc, water = got
    st = loc.get("source_text") or {}
    rows, rules = D.rule_rows(loc, DFO_TYPED)
    labels = []
    try:
        from pipeline.regs.parsing.catalogue import CatalogueRule, label
        for r in rules:
            try:
                labels.append({"rule_id": r.get("rule_id"), "label": label(CatalogueRule(**r)),
                               "verbatim": r.get("verbatim") or ""})
            except Exception:  # noqa: BLE001 — a rule the catalogue model refuses still shows
                labels.append({"rule_id": r.get("rule_id"), "label": r.get("verbatim") or "",
                               "verbatim": r.get("verbatim") or ""})
    except Exception:  # noqa: BLE001
        labels = [{"rule_id": r.get("rule_id"), "label": r.get("verbatim") or "",
                   "verbatim": r.get("verbatim") or ""} for r in rules]
    m = _states().get(location_id)
    v = _verify(m, loc, water)
    # the other locations on this water: the notice's other reaches, to read the river as a whole
    siblings = [{"id": lid, "text": (l.get("source_text") or {}).get("specific_area") or "",
                 "status": l.get("status") or "active", "section": l.get("section")}
                for lid, (_s, l, _w) in _locations().items()
                if l.get("water_id") and l.get("water_id") == loc.get("water_id") and lid != location_id]
    return reuse._json_safe({
        "row": _row(slug, loc, water, m), "location": loc, "water": water,
        "items": [_item_ref(i) for i in (water or {}).get("item_ids") or []],
        "page_url": D.page_url(slug), "verbatim": {"waters": st.get("waters") or loc.get("water"),
                                                   "specific_area": st.get("specific_area") or ""},
        "rows": rows, "rules": labels, "need": D.need(loc),
        "splits": _splits_fc(loc, water),
        "proposed": proposed_on(list((water or {}).get("item_ids") or [])), "hints": _hints(loc, water), "siblings": siblings,
        "suggestion": suggestion(location_id),
        "mark": ({**json.loads(m.model_dump_json()), **v} if m is not None else None),
    })


# --------------------------------------------------------------------------- reach (read)

def _reach_input(loc: dict, water: dict | None, extents: list[dict] | None = None,
                 tributaries: bool | None = None):
    """`entries.to_reach_input` on the location (optionally with other extents): the builder's
    own shape."""
    import copy
    from dataclasses import fields
    from pipeline.regs.dfo_salmon import entries as E
    loc = copy.deepcopy(loc)
    if extents is not None:
        loc.setdefault("binding", {})["extents"] = extents
    if tributaries is not None:
        loc.setdefault("binding", {})["tributaries"] = tributaries
    el = E.EntryLocation.from_dict(loc)
    wb = None
    if water:
        names = {f.name for f in fields(E.WaterBinding)}
        wb = E.WaterBinding(**{k: v for k, v in water.items() if k in names})
    entry, rules = E.to_reach_input(el, [{}], wb)
    return entry, rules[0]


def _bind(entry: dict, rule: dict, limit: int) -> dict:
    from pipeline.atlas.reach.build import build_reach
    if not rule["extents"]:
        return {"resolvable": False, "why": "no extent curated yet: nothing binds", "n_sections": 0}
    binding, _d = build_reach(entry, rule, reuse._registry(), reuse._graph())
    ids = sorted(binding.sections)
    return reuse._json_safe({
        "resolvable": True, "why": None, "n_sections": len(ids),
        "outcomes": {rule["rule_id"]: binding.outcome.value}, "truncated": len(ids) > limit,
        "geojson": {"type": "FeatureCollection",
                    "features": reuse._sections_geojson(ids[:limit], "preview")}})


def reach(location_id: str, limit: int = 6000) -> dict | None:
    """WHAT THE LOCATION'S EXTENT BINDS NOW — `build_reach` on the curated binding."""
    got = _locations().get(location_id)
    if got is None:
        return None
    _slug, loc, water = got
    entry, rule = _reach_input(loc, water)
    return _bind(entry, rule, limit)


def extent_preview(location_id: str, fix, limit: int = 6000) -> dict | None:
    """A PROPOSED EXTENT (`marks.ExtentFix`, scope "entry") resolved on a copy, read only. A NEW
    split (`new`, or a T191 new point) is not a split yet: no preview, and the answer says so."""
    got = _locations().get(location_id)
    if got is None:
        return None
    _slug, loc, water = got
    new = [p for p in fix.points if p.kind != "split"]
    if new:
        return {"resolvable": False, "n_sections": None, "why": "a NEW split is not a split yet: "
                "its point is checked on the map (check anchor); the extent resolves once T185 "
                "builds it"}
    if fix.op in ("rest", "steelhead_waters", "within"):
        return {"resolvable": False, "n_sections": None,
                "why": f"op {fix.op} is not previewed on a DFO location"}
    ex: dict = {"op": fix.op, "splits": [p.split_id for p in fix.points]}
    if len(fix.items) == 1:
        ex["item_id"] = fix.items[0]
    elif fix.items:
        ex["item_ids"] = list(fix.items)
    try:
        entry, rule = _reach_input(loc, water, [ex], fix.tributaries)
    except Exception as e:  # noqa: BLE001 — the Extent model refuses a malformed one
        return {"resolvable": False, "n_sections": None, "why": f"not an extent: {e}"}
    return _bind(entry, rule, limit)


# --------------------------------------------------------------------------- anchors (read)

@lru_cache(maxsize=1)
def _to_albers():
    from pyproj import Transformer
    return Transformer.from_crs(4326, 3005, always_xy=True).transform


def _lonlat(pt) -> list[float]:
    x, y = reuse._to_lonlat()(pt.x, pt.y)
    return [round(x, 6), round(y, 6)]


def _line_sections(item_ids) -> list[tuple[str, object, object]]:
    """(node_id, graph node, geometry in EPSG:3005) for every LINE section of these items."""
    reg, graph, geoms = reuse._registry(), reuse._graph(), reuse._geoms()
    out = []
    for iid in item_ids:
        it = reg.get(iid)
        for nid in (it.section_ids if it else ()):
            if str(nid).startswith("lake:"):
                continue
            n, g = graph.nodes.get(nid), geoms.get(nid)
            if n is None or g is None or g.is_empty or getattr(n, "blk", None) is None:
                continue
            out.append((nid, n, g))
    return out


def _snap(pt, item_ids) -> dict | None:
    """The nearest point of these items' line sections to `pt` (EPSG:3005): node, blk, measure,
    distance, the snapped point."""
    best = None
    for nid, n, g in _line_sections(item_ids):
        d = g.distance(pt)
        if best is None or d < best[0]:
            best = (d, nid, n, g)
    if best is None:
        return None
    d, nid, n, g = best
    along = g.project(pt)
    down = getattr(n, "down_m", 0.0) or 0.0
    return {"node": nid, "blk": str(n.blk), "measure": round(down + along, 1),
            "distance_m": round(d, 1), "point": g.interpolate(along)}


def _point_at(blk: str, measure: float):
    for down, up, nid in reuse._blk_index().get(str(blk), []):
        if down <= measure <= up:
            g = reuse._geoms().get(nid)
            if g is None or g.is_empty:
                return None
            return g.interpolate(min(max(measure - down, 0.0), g.length))
    return None


def _mouth(item_id: str):
    """A stream's mouth: the downstream end of its lowest section (EPSG:3005), or None."""
    secs = _line_sections([item_id])
    if not secs:
        return None
    nid, n, g = min(secs, key=lambda s: (getattr(s[1], "down_m", 0.0) or 0.0))
    from shapely.geometry import Point
    return Point(g.coords[0]) if g.geom_type == "LineString" else g.interpolate(0)


def _lake_poly(item_id: str):
    if not item_id.startswith("wbk:"):
        it = reuse._registry().get(item_id)
        wbk = next((r.split(":", 1)[1] for r in (it.ref_ids if it else ()) if r.startswith("wbk:")), None)
    else:
        wbk = item_id.split(":", 1)[1]
    if not wbk:
        return None
    return reuse._fwa_lake_poly(wbk) or reuse._wbk_polys().get(wbk) or reuse._geoms().get(f"lake:{wbk}")


def _find_item(name: str | None, item_id: str | None, kind: str) -> str | None:
    if item_id and item_id in reuse._registry():
        return item_id
    if not (name or "").strip():
        return None
    from pipeline.common.names import key as name_key
    hits = [h for h in reuse.search_items(name, kind=kind, limit=10)
            if name_key(h["name"]) == name_key(name)]
    return hits[0]["id"] if len(hits) == 1 else None


def check_anchor(location_id: str, ns) -> dict:
    """A PROPOSED NEW SPLIT (`marks.NewSplit`) resolved, READ ONLY — validated server-side and
    cheap: {ok, why, coord (lon, lat), blk, measure, on_item, distance_m, words}. `ok: false` with
    why when the anchor does not land on the water (a click on another water, a stream that is not
    its tributary, a lake it does not touch, an unknown split). KeyError: no such location."""
    from shapely.geometry import Point
    got = _locations().get(location_id)
    if got is None:
        raise KeyError(location_id)
    _slug, loc, water = got
    items = [ns.on_item] if ns.on_item else list((water or {}).get("item_ids") or [])
    if not items:
        return {"ok": False, "why": "this location's water is not matched to the registry yet"}
    if any(i not in reuse._registry() for i in items):
        return {"ok": False, "why": f"no registry item {items}"}
    wname = ", ".join(_item_ref(i)["name"] for i in items)

    def done(pt, words: str, extra: dict | None = None) -> dict:
        s = _snap(pt, items)
        if s is None:
            return {"ok": False, "why": f"{wname} has no line section to cut"}
        out = {"ok": True, "why": None, "blk": s["blk"], "measure": s["measure"],
               "on_item": items[0] if len(items) == 1 else _item_of(s["node"], items),
               "distance_m": s["distance_m"], "words": words, **(extra or {})}
        p = s["point"]
        if ns.offset_m:
            q = _point_at(s["blk"], s["measure"] + ns.offset_m)
            if q is None:
                return {"ok": False, "why": f"{abs(ns.offset_m):.0f} m "
                        f"{'above' if ns.offset_m > 0 else 'below'} it runs off the water's line"}
            out["measure"] = round(s["measure"] + ns.offset_m, 1)
            out["words"] += f", {abs(ns.offset_m):.0f} m {'above' if ns.offset_m > 0 else 'below'}"
            p = q
        out["coord"] = _lonlat(p)
        return out

    if ns.anchor in ("bridge", "dam", "sign", "coord"):
        pt = Point(*_to_albers()(*ns.coord))
        s = _snap(pt, items)
        if s is None:
            return {"ok": False, "why": f"{wname} has no line section to cut"}
        if s["distance_m"] > SNAP_MAX_M:
            return {"ok": False, "why": f"the point is {s['distance_m']:.0f} m from {wname}: click on "
                    "the water (or pick the water it is on)", "distance_m": s["distance_m"]}
        return done(pt, f"{ns.anchor} on {wname}, snapped {s['distance_m']:.0f} m")
    if ns.anchor == "confluence":
        trib = _find_item(ns.name, ns.item_id, "stream")
        if trib is None:
            return {"ok": False, "why": f"no single stream named {ns.name or ns.item_id!r}: pick it "
                    "from the list"}
        mouth = _mouth(trib)
        if mouth is None:
            return {"ok": False, "why": f"{trib} has no line section"}
        s = _snap(mouth, items)
        if s is None or s["distance_m"] > MOUTH_MAX_M:
            return {"ok": False, "why": f"{_item_ref(trib)['name']}'s mouth is "
                    f"{(s or {}).get('distance_m', 0):.0f} m from {wname}: not its tributary"}
        return done(mouth, f"confluence of {_item_ref(trib)['name']} ({trib})", {"item_id": trib})
    if ns.anchor in ("lake_inlet", "lake_outlet"):
        lake = _find_item(ns.name, ns.item_id, "lake")
        if lake is None:
            return {"ok": False, "why": f"no single lake named {ns.name or ns.item_id!r}: pick it "
                    "from the list"}
        poly = _lake_poly(lake)
        if poly is None:
            return {"ok": False, "why": f"no outline for {lake}"}
        ends = []
        for nid, n, g in _line_sections(items):
            if g.distance(poly) > LAKE_TOUCH_M:
                continue
            down, up = getattr(n, "down_m", 0.0) or 0.0, getattr(n, "up_m", 0.0) or 0.0
            for m_, p in ((down, g.interpolate(0)), (up, g.interpolate(g.length))):
                if p.distance(poly) <= LAKE_TOUCH_M:
                    ends.append((m_, p, str(n.blk)))
        if not ends:
            return {"ok": False, "why": f"{wname} does not touch {_item_ref(lake)['name']}"}
        m_, p, blk = (max if ns.anchor == "lake_inlet" else min)(ends, key=lambda e: e[0])
        words = f"{'inlet' if ns.anchor == 'lake_inlet' else 'outlet'} of {_item_ref(lake)['name']}"
        if len({round(e[0]) for e in ends}) < 2:
            words += " (the water touches the lake once: inlet and outlet are the same point)"
        return done(p, words, {"item_id": lake})
    if ns.anchor == "split":
        sid = ns.split_id
        pt = reuse._split_point(sid) if sid in reuse._splits_meta() else None
        if pt is None:
            return {"ok": False, "why": f"no split {sid!r} placed in the served atlas"}
        return done(pt, f"the existing split {sid}", {"split_id": sid})
    return {"ok": False, "why": f"unknown anchor {ns.anchor}"}


def _item_of(nid: str, items) -> str | None:
    reg = reuse._registry()
    return next((i for i in items if nid in set(reg[i].section_ids)), items[0] if items else None)


# --------------------------------------------------------------------------- the mark

def mark_for(body: dict, *, date: str, pdf_sha256: str):
    """THE `Mark` A DFO PAYLOAD MAKES: prints of the location NOW, each new split's anchor checked
    (`check_anchor`; refused, ValueError, when it does not land) and its resolved point stamped.
    T210: a new split that names a cut already proposed (any mark, `marks.proposals`) REUSES that
    proposal's id (the mark's own ref and its extent fix's points are renamed to it); one whose ref
    is taken by another cut gets a free ref. Returns (mark, prints now, reused: [{ref, id, name}]).
    KeyError: no such location. Writes nothing — `reuse.post_mark` commits the mark."""
    from pipeline.regs.review import marks as M
    got = _locations().get(str(body.get("target") or ""))
    if got is None:
        raise KeyError(body.get("target"))
    _slug, loc, water = got
    now = D.prints(loc, water, DFO_TYPED)
    stamped: list[dict] = []
    for raw in body.get("new_splits") or []:
        ns = M.NewSplit.model_validate(raw)
        res = check_anchor(loc["location_id"], ns)
        if not res.get("ok"):
            raise ValueError(f"new split {ns.ref}: {res.get('why')}")
        upd = {"blk": res["blk"], "measure": res["measure"], "on_item": res.get("on_item"),
               "coord": tuple(res["coord"])}
        if res.get("item_id") and not ns.item_id:
            upd["item_id"] = res["item_id"]
        stamped.append(ns.model_copy(update=upd).model_dump(mode="json"))
    stamped, rename, reused = _dedupe_against_store(stamped)
    fixes = [{**f, "points": [{**p, "split_id": rename.get(p.get("split_id"), p.get("split_id"))}
                              if p.get("kind") == "new" else p for p in f.get("points") or []]}
             for f in body.get("extent_fix") or []]
    data = {**body, "rule_id": None, "new_splits": stamped, "date": date,
            **({"extent_fix": fixes} if fixes else {}),
            "pdf_sha256": pdf_sha256, "pages": [], "prints": now}
    mark = M.Mark.model_validate(data)
    return mark, M.Prints.model_validate(now), reused


def _dedupe_against_store(stamped: list[dict]) -> tuple[list[dict], dict, list[dict]]:
    """The mark's stamped new splits with duplicates folded (T210): a split at an anchor already
    proposed — by an earlier mark, or twice in this one — takes that proposal's id and is said to be
    reused; a ref another cut holds is renamed. -> (splits, {old ref: ref}, reused)."""
    from pipeline.regs.review import marks as M
    known = [(p["id"], p["split"]) for p in _proposals()]
    out: list[dict] = []
    rename: dict[str, str] = {}
    reused: list[dict] = []
    for raw in stamped:
        ns = M.NewSplit.model_validate(raw)
        hit = next(((pid, q) for pid, q in known if M.same_anchor(ns, q)), None)
        if hit is not None:
            pid, q = hit
            rename[ns.ref] = pid
            reused.append({"ref": ns.ref, "id": pid, "name": q.name or q.anchor})
            if any(o["ref"] == pid for o in out):
                continue
            out.append({**raw, "ref": pid})
            continue
        ref, k = ns.ref, 1
        taken = {pid for pid, _ in known} | {o["ref"] for o in out}
        while ref in taken:
            k += 1
            ref = f"{ns.ref}-{k}"
        rename[ns.ref] = ref
        known.append((ref, ns))
        out.append({**raw, "ref": ref})
    return out, rename, reused
