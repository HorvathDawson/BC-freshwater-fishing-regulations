"""T355: A PROPOSED SPLIT'S CUT, PREVIEWED BY THE BUILD'S OWN RESOLVER — read only.

    resolve(ns)                 where a `marks.NewSplit` would cut, before it is saved
    tributaries(on_item)        the streams whose mouth joins the water (a confluence's pick list)
    pick(kind, on_item, ll)     what a map click names: a tributary mouth, a lake, an area polygon

THE OWNER'S PRINCIPLE (memory cascade-resolved-in-python): every cut is resolved in Python by the code
the build runs; the review app's TypeScript draws the answer and nothing else. So a preview is:

  1. the NewSplit as the splits.json record it would land as (`proposals.split_record`, the very
     function T185 lands it with) and that record as a `SplitDef` (`SplitDef.from_dict`: the build's
     validation, so an offset on an area boundary is refused here exactly as the build refuses it);
  2. the water's blue lines as the build makes them (`load_stream_fids` + `build_blk_chains`) from the
     FWA source, for that one water (its bbox, filtered to its own lines + the tributary named);
  3. `anchors.resolve_split_defs` on them — the area polygon by `fwa.get_area_polys`, the border
     cuts by `border.border_split_points` on the B.C. outline, the live blue lines from the served graph.

Not modelled: the added lakes / municipal streams merged at build time, and placement (`place_points`:
a cut a sliver from a standing boundary joins it). The preview says where the RESOLVER puts the cut.
"""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Optional

import reuse

#: the bbox around the water the lines are read in (m): wide enough for a point's `proximity_m`
PAD_M = 2000.0
#: a click names a tributary whose mouth is this close (m), a lake / area it is in
MOUTH_PICK_M = 400.0
#: the area outline drawn on the map is simplified to this (m) — display only
OUTLINE_TOL_M = 25.0


@lru_cache(maxsize=1)
def _fwa():
    from pipeline.atlas.fwa import FWADataAccessor
    from project_config import get_config
    return FWADataAccessor(str(get_config().fwa_data_gpkg))


def _sources():
    """The proposals' sources on the served atlas (the registry that names a tributary's WSC, a lake's
    wbk) — keyed on splits.json's and the registry's stamps, so an edit is read on the next call."""
    return _sources_for(reuse._stamp((reuse.SPLITS_JSON_PATH, reuse.REGISTRY_PATH)))


@lru_cache(maxsize=1)
def _sources_for(_key):
    from pathlib import Path
    from pipeline.regs.review import proposals as P
    src = P.Sources.live(Path(reuse.REGISTRY_PATH).parent)
    src.splits_path = reuse.SPLITS_JSON_PATH
    return src


_DIGITS = re.compile(r"^\d+$")
_WSC = re.compile(r"^\d{3}(-\d{6})*$")


def _wsc_where(wsc: str, descendants: bool) -> str:
    """The FWA rows of a (trimmed) WSC: its own lines (the code padded with -000000), or its subtree."""
    if not _WSC.match(wsc):
        raise ValueError(f"not a watershed code: {wsc!r}")
    if descendants:
        return f"(FWA_WATERSHED_CODE = '{wsc}' OR FWA_WATERSHED_CODE LIKE '{wsc}-%')"
    return f"(FWA_WATERSHED_CODE = '{wsc}' OR FWA_WATERSHED_CODE LIKE '{wsc}-000000%')"


def _where(target: dict, anchor: dict) -> str:
    """The attribute filter for the water's own blue lines (+ the tributary a confluence names)."""
    parts = []
    if target.get("gnis_id"):
        if not _DIGITS.match(target["gnis_id"]):
            raise ValueError(f"not a gnis id: {target['gnis_id']!r}")
        parts.append(f"GNIS_ID = {int(target['gnis_id'])}")
    elif target.get("blk"):
        if not _DIGITS.match(target["blk"]):
            raise ValueError(f"not a blue line key: {target['blk']!r}")
        parts.append(f"BLUE_LINE_KEY = {int(target['blk'])}")
    elif target.get("wsc"):
        parts.append(_wsc_where(target["wsc"], bool(anchor.get("wsc_descendants"))))
    if anchor.get("tributary_blk"):
        if not _DIGITS.match(str(anchor["tributary_blk"])):
            raise ValueError(f"not a blue line key: {anchor['tributary_blk']!r}")
        parts.append(f"BLUE_LINE_KEY = {int(anchor['tributary_blk'])}")
    if anchor.get("tributary_wsc"):
        parts.append(_wsc_where(str(anchor["tributary_wsc"]), False))
    return " OR ".join(parts)


def _bbox(item_ids) -> Optional[tuple]:
    """The served atlas's bounds of these items' sections (EPSG:3005), padded; None when unknown."""
    reg, geoms = reuse._registry(), reuse._geoms()
    bounds = [geoms[s].bounds for i in item_ids if i in reg for s in reg[i].section_ids
              if s in geoms and geoms[s] is not None and not geoms[s].is_empty]
    if not bounds:
        return None
    return (min(b[0] for b in bounds) - PAD_M, min(b[1] for b in bounds) - PAD_M,
            max(b[2] for b in bounds) + PAD_M, max(b[3] for b in bounds) + PAD_M)


@lru_cache(maxsize=16)
def _chains(where: str, bbox: Optional[tuple]):
    """The build's own blue-line chains for these FWA rows (`load_stream_fids` + `build_blk_chains`)."""
    from pipeline.atlas.fwa import get_lake_wbk_kind
    from pipeline.atlas.graph.blk_chains import build_blk_chains, load_stream_fids
    fwa = _fwa()
    fids = load_stream_fids(str(fwa.gpkg_path), bbox=bbox, where=where or None)
    return tuple(build_blk_chains(fids, get_lake_wbk_kind(fwa, bbox)))


@lru_cache(maxsize=1)
def _outline():
    from pipeline.atlas.splits.border import bc_outline
    return bc_outline(_fwa())


@lru_cache(maxsize=1)
def _live_blks() -> frozenset:
    """The blue lines the served graph still holds (a point lands only on those, as at build)."""
    return frozenset(reuse._blk_index())


def _lonlat(chain, measure: float) -> list[float]:
    p = chain.geometry.interpolate(max(0.0, min(measure - chain.mouth_measure, chain.geometry.length)))
    x, y = reuse._to_lonlat()(p.x, p.y)
    return [round(x, 6), round(y, 6)]


def _area_poly(anchor: dict, bbox):
    from pipeline.atlas.fwa import get_area_polys
    spec = (anchor["area_layer"], anchor.get("area_name_field") or "", anchor["area_name"])
    return get_area_polys(_fwa(), [spec], bbox=bbox).get(anchor["area_name"])


def _outline_geojson(poly) -> Optional[dict]:
    if poly is None:
        return None
    from shapely.geometry import mapping
    from shapely.ops import transform
    return {"type": "Feature", "properties": {"kind": "area_outline"},
            "geometry": mapping(transform(reuse._to_lonlat(), poly.boundary.simplify(OUTLINE_TOL_M)))}


def _resolve_record(rec: dict, target: dict, bbox, *, area_poly=None) -> tuple[list, dict]:
    """`resolve_split_defs` on one record: (points, chains by blk). ValueError from the build's own
    validation (`SplitDef.from_dict`) or its same-entry check."""
    from pipeline.atlas.splits.anchors import resolve_split_defs
    from pipeline.common.models import AnchorType
    from pipeline.common.models.splits import SplitDef
    anchor = {k: v for k, v in rec["anchor"].items() if not k.startswith("_")}
    sd = SplitDef.from_dict({"id": rec["id"], "label": rec.get("label", ""), "anchor": anchor, **target})
    chains = list(_chains(_where(target, anchor), bbox))
    border_cuts = None
    if sd.anchor.type == AnchorType.border:
        from pipeline.atlas.splits.border import border_split_points
        border_cuts = {}
        for p in border_split_points(chains, _outline()):
            border_cuts.setdefault(p.blk, []).append(p.route_measure)
    area_polys = {sd.anchor.area_name: area_poly} if area_poly is not None else None
    pts = resolve_split_defs([sd], chains, area_polys=area_polys, live_blks=set(_live_blks()),
                             border_cuts=border_cuts)
    return pts, {c.blk: c for c in chains}


def _why_none(ns, rec: dict, chains: dict, area_poly) -> str:
    a = rec["anchor"]
    if not chains:
        return "the FWA source has no blue line for this water (an added stream is not previewed)"
    if a["type"] == "area_boundary":
        if area_poly is None:
            return f"no polygon named {a['area_name']!r} in {a['area_layer']} ({a.get('area_name_field')}) near this water"
        return f"{a['area_name']} does not cross this water (or only within a sliver of an end)"
    if a["type"] == "border":
        return "this water does not cross the B.C. outline"
    if a["type"] == "confluence":
        return "the tributary's mouth is not on this water (not within the split's proximity)"
    if a["type"] == "lake":
        return "this water does not run through that lake"
    return "the point is not on this water (farther than the split's proximity)"


def resolve(ns) -> dict:
    """WHERE THE BUILD WOULD CUT `ns` (a `marks.NewSplit` with its water, `on_item`): {ok, why,
    record (the splits.json split it would land as), notes, cuts: [{blk, measure, coord, concern}],
    reference: the same cuts without the offset (an offset cut's reference), outline (an area's edge,
    GeoJSON), words}. Read only."""
    from pipeline.regs.review import proposals as P
    if not ns.on_item:
        return {"ok": False, "why": "the split names no water (on_item)"}
    target = P._applies_to(ns.on_item)
    if target is None:
        return {"ok": False, "why": f"the water {ns.on_item!r} is not a gnis / blk / wsc item"}
    try:
        rec, notes = P.split_record(ns, _sources(), sid="preview__cut", provenance="preview")
    except ValueError as ex:
        return {"ok": False, "why": str(ex)}
    items = [ns.on_item] + ([ns.item_id] if ns.item_id and ns.anchor in ("confluence",) else [])
    bbox = _bbox(items)
    a = rec["anchor"]
    area_poly = _area_poly(a, bbox) if a["type"] == "area_boundary" else None
    try:
        pts, chains = _resolve_record(rec, target, bbox, area_poly=area_poly)
    except ValueError as ex:
        return {"ok": False, "why": str(ex), "record": rec}
    out = {"record": rec, "notes": notes, "outline": _outline_geojson(area_poly),
           "cuts": [{"blk": p.blk, "measure": round(p.route_measure, 1), "coord": _lonlat(chains[p.blk], p.route_measure),
                     "concern": p.concern or None} for p in pts if p.blk in chains],
           "reference": []}
    if a.get("offset_m"):
        base = {**rec, "anchor": {k: v for k, v in a.items() if k not in ("offset_m", "offset_dir")}}
        if a["type"] == "lake":            # the end the offset runs from (`resolve_split_defs`' lake rule)
            base["anchor"]["edge"] = "inlet" if a.get("offset_dir") == "upstream" else "outlet"
        try:
            rp, rc = _resolve_record(base, target, bbox, area_poly=area_poly)
            out["reference"] = [{"blk": p.blk, "measure": round(p.route_measure, 1),
                                 "coord": _lonlat(rc[p.blk], p.route_measure)} for p in rp if p.blk in rc]
        except ValueError:
            pass
    if not out["cuts"]:
        return {**out, "ok": False, "why": _why_none(ns, rec, chains, area_poly)}
    n = len(out["cuts"])
    off = (f", {a['offset_m']:.0f} m {a['offset_dir']} of it" if a.get("offset_m") else "")
    return {**out, "ok": True, "why": None,
            "words": f"the build cuts {rec['label']}{off}: {n} cut{'s' if n != 1 else ''}"
                     + "".join(f" · blk {c['blk']} @ {c['measure']:.0f} m" for c in out["cuts"][:4])}


# --------------------------------------------------------------------------- picking (reads)

def _mouth(item_id: str):
    import dfo
    return dfo._mouth(item_id)


def tributaries(on_item: str) -> list[dict]:
    """The streams whose mouth joins this water (the tributary walk's first step, `joining`), each
    with its mouth: {id, name, mouth (lon, lat)} — a confluence split's pick list, by name."""
    reg = reuse._registry()
    it = reg.get(on_item)
    if it is None:
        return []
    from pipeline.atlas.graph.tributaries import joining
    mouths = set(joining(reuse._graph(), it.section_ids))
    out = []
    for iid, item in reg.items():
        if item.kind != "stream" or iid == on_item or not mouths.intersection(item.section_ids):
            continue
        m = _mouth(iid)
        if m is None:
            continue
        x, y = reuse._to_lonlat()(m.x, m.y)
        out.append({"id": iid, "name": item.name, "mouth": [round(x, 6), round(y, 6)]})
    return sorted(out, key=lambda r: (r["name"] or "", r["id"]))


def _albers(ll):
    from shapely.geometry import Point
    import dfo
    return Point(*dfo._to_albers()(*ll))


def pick(kind: str, on_item: Optional[str], ll: tuple[float, float]) -> dict:
    """WHAT A MAP CLICK NAMES for a new split: `tributary` (the water's tributary whose mouth is
    nearest, within `MOUTH_PICK_M`), `lake` (the FWA lake the click is in, as its registry item), or
    `area` (every polygon of `marks.AREA_LAYERS` the click is in, with its name from the layer's field).
    -> {ok, why, hits: [...]}."""
    pt = _albers(ll)
    if kind == "tributary":
        best = None
        for t in tributaries(on_item or ""):
            d = _albers(t["mouth"]).distance(pt)
            if d <= MOUTH_PICK_M and (best is None or d < best[0]):
                best = (d, t)
        if best is None:
            return {"ok": False, "why": f"no tributary of this water has its mouth within {MOUTH_PICK_M:.0f} m "
                    "of the click: click nearer the mouth, or pick it from the list", "hits": []}
        return {"ok": True, "why": None, "hits": [{**best[1], "distance_m": round(best[0], 1)}]}
    box = (pt.x - 1, pt.y - 1, pt.x + 1, pt.y + 1)
    if kind == "lake":
        gdf = _fwa().get_layer("lakes", columns=["WATERBODY_KEY", "GNIS_NAME_1"], bbox=box)
        reg = reuse._registry()
        hits = []
        for wbk, name, g in zip(gdf["WATERBODY_KEY"], gdf["GNIS_NAME_1"], gdf.geometry):
            if g is None or not g.contains(pt):
                continue
            ref = f"wbk:{wbk}"
            iid = next((i for i, it in reg.items() if it.kind != "stream" and ref in it.ref_ids),
                       ref if ref in reg else None)
            hits.append({"id": iid or ref, "name": (reg[iid].name if iid in reg else None) or name or ref,
                         "wbk": str(wbk), "in_registry": iid is not None})
        if not hits:
            return {"ok": False, "why": "the click is in no FWA lake", "hits": []}
        return {"ok": True, "why": None, "hits": hits}
    if kind == "area":
        from pipeline.regs.review.marks import AREA_LAYERS
        fwa, hits = _fwa(), []
        for layer, field in AREA_LAYERS.items():
            if layer not in fwa.layer_names:
                continue
            gdf = fwa.get_layer(layer, columns=[field], bbox=box)
            for name, g in zip(gdf[field], gdf.geometry):
                if g is not None and name and g.contains(pt):
                    hits.append({"layer": layer, "field": field, "name": str(name)})
        if not hits:
            return {"ok": False, "why": f"the click is in no polygon of {', '.join(AREA_LAYERS)}", "hits": []}
        return {"ok": True, "why": None, "hits": hits}
    return {"ok": False, "why": f"pick is tributary | lake | area, not {kind!r}", "hits": []}


# --------------------------------------------------------------------------- area-name autocomplete

#: the most names an autocomplete answers
NAME_HITS = 15


@lru_cache(maxsize=8)
def _layer_names(layer: str) -> tuple:
    """((NAME, name casefolded, (x, y) of its polygon's representative point), …) — one row per
    distinct name of the layer's name field (`marks.AREA_LAYERS`), cached per layer."""
    from pipeline.regs.review.marks import AREA_LAYERS
    field = AREA_LAYERS[layer]
    fwa = _fwa()
    if layer not in fwa.layer_names:
        return ()
    gdf = fwa.get_layer(layer, columns=[field])
    seen: dict = {}
    for name, g in zip(gdf[field], gdf.geometry):
        if not name or g is None or g.is_empty or str(name) in seen:
            continue
        p = g.representative_point()
        seen[str(name)] = (str(name), str(name).casefold(), (p.x, p.y))
    return tuple(seen.values())


def area_names(layer: str, q: str, on_item: Optional[str] = None, limit: int = NAME_HITS) -> list[dict]:
    """THE AREA NAMES matching `q` (case-insensitive, contained) in `layer`, spelled as the layer spells
    them: prefix matches first, then the nearest to the water (`on_item`), then by name. ->
    [{name, layer, field, distance_km}]. ValueError for a layer not in `marks.AREA_LAYERS`."""
    from pipeline.regs.review.marks import AREA_LAYERS
    if layer not in AREA_LAYERS:
        raise ValueError(f"layer is one of {', '.join(AREA_LAYERS)}")
    k = " ".join((q or "").casefold().split())
    box = _bbox([on_item]) if on_item else None
    cx, cy = ((box[0] + box[2]) / 2, (box[1] + box[3]) / 2) if box else (None, None)
    rows = []
    for name, fold, (x, y) in _layer_names(layer):
        if k and k not in fold:
            continue
        d = ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5 if cx is not None else None
        rows.append((not fold.startswith(k), d if d is not None else 0.0, fold, name, d))
    rows.sort(key=lambda r: r[:3])
    return [{"name": r[3], "layer": layer, "field": AREA_LAYERS[layer],
             "distance_km": round(r[4] / 1000, 1) if r[4] is not None else None} for r in rows[:limit]]
