"""T212 follow-up — WHERE THE OWNER EDITED THE DFO TIDAL POLYGON, and which tidal candidates still need him.

    # 1. the edit sites (needs the DFO build gpkg from R2 + the build gpkg's streams; ~3 min, read only)
    .venv/bin/python -m pipeline.tools.audit.tidal_triage edits \
        --dfo-gpkg <DFO_TIDAL_BOUNDARY.gpkg> --gpkg <bc_fisheries_data.gpkg> \
        --out handoff/PLAN/curation-review/tidal
    # 2. the triage (seconds): candidates + edit sites + web findings -> each candidate's class
    .venv/bin/python -m pipeline.tools.audit.tidal_triage triage --out handoff/PLAN/curation-review/tidal

HOW THE EDITS ARE FOUND. The polygon the build reads (`tidal_boundary`) is `step5_simplified` of
https://build.canifishthis.ca/DFO_TIDAL_BOUNDARY.gpkg, built by data/build_tidal_boundary.py from that
file's layer `dfodfooy__dfo_bc_pfma_subareas_chs_v3_gshp`. That source layer IS the owner's edited copy
(all subareas merged into one feature carrying 12-10's attributes, two hand-drawn rows with no
attributes); the unedited DFO original is not kept anywhere (R2 holds one version, the DFO ArcGIS host
gisp.dfo-mpo.gc.ca no longer resolves, the BC WFS does not publish the subareas). So the original
cannot be rebuilt, and the edits are found by a FINGERPRINT instead of a diff:

  * DFO's vertices sit on a 1 cm grid in BC Albers (residual < 0.1 mm). A vertex QGIS adds by hand
    (a reshape/split cut, a moved vertex) lands anywhere. A RUN of off-grid vertices whose
    neighbours (`SURROUND` vertices each side) are on the grid is an EDIT SITE.
  * a row with no DFO attributes is a hand-drawn polygon: every vertex an edit.
  * one stretch of the DFO data is itself off the grid (south Vancouver Island / Gulf Islands, from
    another source): there the fingerprint is blind, and a candidate there is `edited: null`
    ("can't tell"), never "unedited". `blind` in tidal-edits.json is that area.
  * STRAIGHT LINES (owner, T212 follow-up 2): DFO's outline follows the shore vertex by vertex; an
    edge >= STRAIGHT_M with no vertex between that CROSSES an FWA stream is a cut drawn across the
    channel. It sees what the grid cannot: edits inside the off-grid stretch, and cuts made only by
    deleting vertices (both ends then on the grid). Such sites are kind "straight".
  * NOT seen: a deletion that leaves no long edge across a stream, or deleting a whole part.

Each edit site is attributed to the FWA streams within `ATTRIBUTE_M` of it (the build gpkg's
`streams`). A candidate is owner-edited when its blue line is one of them, or its polygon crossing is
within `ATTRIBUTE_M` of a site.

THE CLASSES (owner, T212 follow-up):
  owner-edited      the polygon was edited at this stream and the words do not disagree: accepted
  book agrees       unedited; a located book/DFO/web source agrees with the polygon (within 300 m)
  missed            unedited (or can't tell); a located source DISAGREES — needs the owner
  sanity            edited, yet a located source disagrees — check the edit
  multi-crossing    no source disagrees, but the FWA line crosses the polygon several times (in, out,
                    in again): the boundary is the top of the run joined to the mouth, the gaps between
                    are filled and a separate upstream patch is ignored — shown so the owner sees them
  no claim          unedited, nothing locates a boundary: accepted at the polygon, low priority
Sources that name a boundary but cannot be located (book or web) do not decide; the row says so.

READ ONLY on every input except its own outputs: tidal-edits.json, the `triage` key of each row of
tidal-candidates.json (outside `sources`/`polygon`, so no tidal mark goes stale) and tidal-triage.md.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import shapely
from shapely.geometry import LineString, Point, shape, mapping

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "handoff/PLAN/curation-review/tidal"
SOURCE_LAYER = "dfodfooy__dfo_bc_pfma_subareas_chs_v3_gshp"

#: DFO's vertex grid in BC Albers (m) and how far off it a vertex may sit (float noise) and still be DFO's
GRID_M = 0.01
GRID_TOL_M = 1e-4
#: vertices looked at on each side of an off-grid run
SURROUND = 200
#: an off-grid run is an edit when at least this share of its neighbours is on the grid
SURROUND_ON = 0.6
#: below this share the run is the off-grid DFO stretch: the fingerprint is blind there
BLIND_ON = 0.5
#: the blind area is the off-grid stretch's vertices buffered this far (m)
BLIND_BUFFER_M = 1500.0
#: a stream within this far (m) of an edit site is attributed to it
ATTRIBUTE_M = 150.0
#: a located source within this far (m, along the line) of the polygon's crossing agrees (= AGREE_M)
AGREE_M = 300.0

CLASSES = ("missed", "sanity", "multi-crossing", "owner-edited", "book agrees", "no claim")
#: the classes the Tidal tab shows by default
REVIEW = ("missed", "sanity", "multi-crossing")


# --------------------------------------------------------------------------- geometry helpers

def _to_lonlat():
    from pyproj import Transformer
    return Transformer.from_crs(3005, 4326, always_xy=True).transform


def _to_albers():
    from pyproj import Transformer
    return Transformer.from_crs(4326, 3005, always_xy=True).transform


def off_grid(coords: np.ndarray, grid: float = GRID_M, tol: float = GRID_TOL_M) -> np.ndarray:
    """True per vertex that is NOT on DFO's grid (either axis off by more than `tol`)."""
    c = np.asarray(coords, dtype=float)[:, :2]
    res = np.abs(c / grid - np.round(c / grid)) * grid
    return res.max(axis=1) > tol


def ring_runs(coords: np.ndarray, surround: int = SURROUND) -> list[dict]:
    """The off-grid RUNS of one closed ring (its last vertex repeating the first is dropped): each
    {i, k, on_share, line} — `line` the run with one on-grid neighbour each side, `on_share` the share
    of `surround` vertices each side that are on the grid. Runs wrap around the ring's start."""
    c = np.asarray(coords, dtype=float)[:, :2]
    if len(c) > 1 and np.allclose(c[0], c[-1]):
        c = c[:-1]
    n = len(c)
    o = off_grid(c)
    if not o.any():
        return []
    if o.all():
        return [{"i": 0, "k": n, "on_share": 0.0, "line": np.vstack([c, c[:1]])}]
    start = int(np.argmin(o))              # an on-grid vertex: rotate so no run wraps
    c, o = np.roll(c, -start, axis=0), np.roll(o, -start)
    out, i = [], 0
    while i < n:
        if not o[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and o[j + 1]:
            j += 1
        idx = np.r_[np.arange(i - surround, i), np.arange(j + 1, j + 1 + surround)] % n
        out.append({"i": (i + start) % n, "k": j - i + 1, "on_share": float(1 - o[idx].mean()),
                    "line": c[np.arange(i - 1, j + 2) % n]})
        i = j + 1
    return out


def _rings(g):
    for p in getattr(g, "geoms", [g]):
        if p.is_empty:
            continue
        yield p.exterior
        yield from p.interiors


def find_edits(rows: list[tuple[dict, object]]) -> tuple[list[dict], object]:
    """(edit sites, blind area) from the source layer's rows [(attributes, geometry EPSG:3005)].
    A site: {kind "cut"|"drawn", n_vertices, length_m, line (EPSG:3005 coords)}."""
    sites, blind_pts = [], []
    for attrs, g in rows:
        if g is None or g.is_empty:
            continue
        if _drawn(attrs):
            for ring in _rings(g):
                c = np.asarray(ring.coords)[:, :2]
                sites.append({"kind": "drawn", "n_vertices": len(c) - 1,
                              "length_m": round(ring.length, 1), "line": c})
            continue
        for ring in _rings(g):
            for r in ring_runs(np.asarray(ring.coords)):
                if r["on_share"] >= SURROUND_ON:
                    ln = r["line"]
                    sites.append({"kind": "cut", "n_vertices": r["k"],
                                  "length_m": round(float(LineString(ln).length), 1), "line": ln})
                elif r["on_share"] < BLIND_ON:
                    blind_pts.append(r["line"][1:-1])
    blind = None
    if blind_pts:
        pts = shapely.multipoints(np.vstack(blind_pts)[::5])
        blind = shapely.buffer(pts, BLIND_BUFFER_M, quad_segs=4)
    return sites, blind


#: a polygon edge at least this long (m) with no vertex between is a hand-drawn straight cut (owner,
#: T212 follow-up 2: DFO's own outline follows the shore vertex by vertex, it has no long straight runs)
STRAIGHT_M = 50.0


def long_segments(rows: list[tuple[dict, object]], min_m: float = STRAIGHT_M) -> np.ndarray:
    """Every outline edge of the source rows at least `min_m` long: an (n, 2, 2) array of EPSG:3005
    ends (hand-drawn rows left out: they are edit sites whole)."""
    out = []
    for attrs, g in rows:
        if g is None or g.is_empty or _drawn(attrs):
            continue
        for ring in _rings(g):
            c = np.asarray(ring.coords)[:, :2]
            d = np.hypot(*np.diff(c, axis=0).T)
            k = np.nonzero(d >= min_m)[0]
            if len(k):
                out.append(np.stack([c[k], c[k + 1]], axis=1))
    return np.concatenate(out) if out else np.zeros((0, 2, 2))


def straight_sites(segs: np.ndarray, crosses) -> list[dict]:
    """THE STRAIGHT-LINE FINGERPRINT: each long edge that CROSSES a stream channel (`crosses(line)` ->
    the FWA streams it crosses) is an edit site of kind "straight" — a cut drawn across a river with
    two clicks. It sees edits the 1 cm grid cannot: in DFO's own off-grid stretch, and cuts made only
    by deleting vertices (both ends then sit on the grid)."""
    out = []
    for a, b in segs:
        ln = LineString([a, b])
        hit = crosses(ln)
        if hit:
            out.append({"kind": "straight", "n_vertices": 0, "length_m": round(float(ln.length), 1),
                        "line": np.asarray([a, b]), "crosses": hit})
    return out


def _drawn(attrs: dict) -> bool:
    return all(v is None or (isinstance(v, float) and math.isnan(v)) for v in attrs.values())


# --------------------------------------------------------------------------- attribution

def _gpkg_geom(b: bytes):
    flags = b[3]
    env = (flags >> 1) & 7
    return shapely.from_wkb(bytes(b[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]:]))


def streams_near(con, line: LineString, within: float = ATTRIBUTE_M) -> list[dict]:
    """The FWA stream segments within `within` m of `line` (rtree, then exact): one per blue line,
    the nearest — {blk, name, gnis, wsc, distance_m}."""
    x0, y0, x1, y1 = line.bounds
    x0, y0, x1, y1 = x0 - within, y0 - within, x1 + within, y1 + within
    fids = [i for (i,) in con.execute(
        "select id from rtree_streams_geom where maxx>=? and minx<=? and maxy>=? and miny<=?",
        (x0, x1, y0, y1))]
    best: dict[int, dict] = {}
    for k in range(0, len(fids), 900):
        chunk = fids[k:k + 900]
        for blk, name, gnis, wsc, b in con.execute(
                "select BLUE_LINE_KEY, GNIS_NAME, GNIS_ID, FWA_WATERSHED_CODE, geom from streams "
                f"where fid in ({','.join('?' * len(chunk))})", chunk):
            d = float(line.distance(_gpkg_geom(b)))
            if d <= within and (blk not in best or d < best[blk]["distance_m"]):
                best[blk] = {"blk": str(blk), "name": name, "gnis": gnis, "wsc": wsc,
                             "distance_m": round(d, 1)}
    return sorted(best.values(), key=lambda s: (s["name"] is None, s["distance_m"]))


#: a straight edge must cross the stream this far (m) above its FWA mouth: an edge across the very
#: mouth is DFO's own shoreline meeting a creek, not a cut up a channel
STRAIGHT_ABOVE_MOUTH_M = 75.0


def streams_crossed(con, line: LineString, above_m: float = STRAIGHT_ABOVE_MOUTH_M) -> list[dict]:
    """The FWA blue lines `line` crosses more than `above_m` above their mouth: one per blue line —
    {blk, name, gnis, wsc, distance_m 0, crossing_m (route measure of the crossing)}."""
    x0, y0, x1, y1 = line.bounds
    fids = [i for (i,) in con.execute(
        "select id from rtree_streams_geom where maxx>=? and minx<=? and maxy>=? and miny<=?",
        (x0, x1, y0, y1))]
    best: dict[int, dict] = {}
    for k in range(0, len(fids), 900):
        chunk = fids[k:k + 900]
        for blk, name, gnis, wsc, down, length, b in con.execute(
                "select BLUE_LINE_KEY, GNIS_NAME, GNIS_ID, FWA_WATERSHED_CODE, DOWNSTREAM_ROUTE_MEASURE, "
                f"LENGTH_METRE, geom from streams where fid in ({','.join('?' * len(chunk))})", chunk):
            g = _gpkg_geom(b)
            if not g.intersects(line):
                continue
            x = g.intersection(line)
            p = x if x.geom_type == "Point" else x.representative_point()
            gl = g.length or 1.0
            m = (down or 0.0) + g.project(p) * ((length or gl) / gl)
            if m > above_m and blk not in best:
                best[blk] = {"blk": str(blk), "name": name, "gnis": gnis, "wsc": wsc, "distance_m": 0.0,
                             "crossing_m": round(m, 1)}
    return sorted(best.values(), key=lambda s: (s["name"] is None, s["crossing_m"]))


def edits_doc(dfo_gpkg: Path, gpkg: Path) -> dict:
    import pyogrio
    df = pyogrio.read_dataframe(dfo_gpkg, layer=SOURCE_LAYER)
    if df.crs is None or df.crs.to_epsg() != 3005:
        df = df.to_crs(3005)
    cols = [c for c in df.columns if c != "geometry"]
    rows = [({c: r[c] for c in cols}, r.geometry) for _, r in df.iterrows()]
    sites, blind = find_edits(rows)
    ll = _to_lonlat()
    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    out = []
    try:
        grid_lines = shapely.STRtree([LineString(s["line"]) for s in sites])
        longs = [ab for ab in long_segments(rows)
                 if not len(grid_lines.query(LineString(ab), predicate="dwithin", distance=1.0))]
        sites = sites + straight_sites(np.asarray(longs).reshape(-1, 2, 2),
                                       lambda ln: streams_crossed(con, ln))
        for n, s in enumerate(sites):
            line = LineString(s["line"])
            near = s.get("crosses") or streams_near(con, line)
            lon, lat = ll(*np.asarray(s["line"]).mean(axis=0))
            out.append({"id": f"edit:{n}", "kind": s["kind"], "n_vertices": s["n_vertices"],
                        "length_m": s["length_m"], "at": [round(lon, 6), round(lat, 6)],
                        "streams": near,
                        "line": [[round(v, 6) for v in ll(x, y)] for x, y in s["line"]]})
    finally:
        con.close()
    blind_ll = None
    if blind is not None:
        from shapely.ops import transform
        blind_ll = mapping(shapely.simplify(transform(ll, blind), 0.002))
    return {"_about": "T212 follow-up (pipeline/tools/audit/tidal_triage.py edits): where the owner hand-edited "
                      "the DFO tidal polygon, by the 1 cm vertex-grid fingerprint (the unedited original is "
                      "not kept anywhere). `sites` are edits with the FWA streams within "
                      f"{ATTRIBUTE_M:.0f} m; `blind` is where DFO's own data is off the grid, so edits there "
                      "cannot be seen.",
            "source": f"{dfo_gpkg.name} layer {SOURCE_LAYER}",
            "sites": out, "blind": blind_ll}


# --------------------------------------------------------------------------- the web findings

#: a stream's segments are looked for within this box (m) around its mouth
LINE_BOX_M = 40_000.0


def _line_of(con, c: dict):
    """(segs, geoms) of a candidate's blue line near its mouth, from the build gpkg (the shapes
    `tidal_candidates.locate_marker` takes)."""
    from pipeline.tools.audit import tidal_candidates as TC
    pts = [_albers()(*p) for p in (c["mouth"]["coord"], (c.get("polygon") or {}).get("coord"),
                                   (c.get("proposed") or {}).get("coord")) if p]
    b = LINE_BOX_M
    x0, y0 = min(p[0] for p in pts) - b, min(p[1] for p in pts) - b
    x1, y1 = max(p[0] for p in pts) + b, max(p[1] for p in pts) + b
    fids = [i for (i,) in con.execute(
        "select id from rtree_streams_geom where maxx>=? and minx<=? and maxy>=? and miny<=?",
        (x0, x1, y0, y1))]
    segs = []
    for k in range(0, len(fids), 900):
        chunk = fids[k:k + 900]
        for fid, blk, down, length in con.execute(
                "select fid, BLUE_LINE_KEY, DOWNSTREAM_ROUTE_MEASURE, LENGTH_METRE from streams "
                f"where fid in ({','.join('?' * len(chunk))})", chunk):
            if str(blk) == str(c["blk"]):
                segs.append({"fid": fid, "blk": blk, "down": down or 0.0, "length": length or 0.0})
    return segs, TC._geoms(con, segs)


def locate_web(finding: dict, c: dict, con, basemap) -> dict:
    """Where a web finding puts the boundary on the candidate's line: {measure, coord, located_by}
    (empty when it cannot be placed). A bridge (rail / road, or a sign named by its road) is the OSM
    way crossing the line (+ offset_m upstream); a `measure` the finding states is taken as is; a
    `coord` is snapped to the line."""
    from pipeline.tools.audit import tidal_candidates as TC
    kind = finding.get("kind")
    if kind == "sign" and finding.get("match"):
        kind = "road"
    segs, geoms = _line_of(con, c)
    if not segs:
        return {}
    ll = _to_lonlat()
    if finding.get("line"):
        loc = TC.locate_line(segs, geoms, [_albers()(*e) for e in finding["line"]],
                             float((c.get("mouth") or {}).get("measure") or 0.0))
        if loc:
            pt = TC.point_at(segs, geoms, loc["measure"])
            return {"measure": loc["measure"], "located_by": f"DFO line {' - '.join(finding.get('endpoints') or [])}: "
                    f"{loc['how']}", "coord": [round(v, 6) for v in ll(pt.x, pt.y)] if pt is not None else None}
        return {}
    if finding.get("measure") is not None:
        pt = TC.point_at(segs, geoms, float(finding["measure"]))
        return {"measure": float(finding["measure"]), "located_by": "the measure the source states",
                "coord": [round(v, 6) for v in ll(pt.x, pt.y)] if pt is not None else None}
    if finding.get("coord"):
        m, d = TC.snap(segs, geoms, Point(*_albers()(*finding["coord"])))
        if d <= 1000:
            pt = TC.point_at(segs, geoms, m)
            return {"measure": round(m, 1), "located_by": f"its coordinate, snapped {d:.0f} m to the line",
                    "coord": [round(v, 6) for v in ll(pt.x, pt.y)] if pt is not None else None}
        return {}
    if kind in ("rail", "road") and basemap is not None:
        loc = TC.locate_marker(segs, geoms, basemap, kind, (c.get("polygon") or {}).get("measure"), ll,
                               match=finding.get("match"), pick=finding.get("pick") or "near",
                               offset_m=float(finding.get("offset_m") or 0.0))
        if loc:
            return {"measure": loc["measure"], "coord": loc["coord"],
                    "located_by": f"OSM {kind} crossing '{loc['osm']}' @ {loc['crossing_measure']:.0f} m"
                                  + (f" + {loc['offset_m']:.0f} m" if loc.get("offset_m") else "")}
    return {}


def web_doc(found: list[Path], cands: dict, gpkg: Path, basemap: Path | None) -> dict:
    """tidal-web.json: every web finding (the research agents' files), each located on its line."""
    from pipeline.tools.audit import tidal_candidates as TC
    by_id = {c["id"]: c for c in cands["candidates"]}
    bm = TC.Basemap(basemap) if basemap and basemap.exists() else None
    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    out, nothing, unmatched, context = [], [], [], []
    try:
        for p in found:
            d = json.loads(Path(p).read_text())
            nothing += d.get("searched_nothing_found") or []
            for f in d.get("findings") or []:
                c = by_id.get(f.get("id")) if f.get("id") else _by_name(cands, f)
                if f.get("context"):                  # physical tide, not a licensing boundary: not a source
                    context.append({k: f.get(k) for k in ("water", "marker", "url") if f.get(k)})
                    continue
                if c is None:
                    unmatched.append({k: f.get(k) for k in ("water", "marker", "url") if f.get(k)})
                    continue
                keep = {k: f.get(k) for k in ("id", "water", "marker", "kind", "match", "pick", "offset_m",
                                              "measure", "coord", "line", "endpoints", "endpoint_sources",
                                              "quote", "url", "site", "confidence")
                        if f.get(k) is not None}
                keep["id"] = c["id"]
                loc = locate_web(f, c, con, bm)
                keep["located"] = loc or None
                out.append(keep)
    finally:
        con.close()
    return {"_about": "T212 follow-up: web findings on where tidal water ends (DFO pages and guides, the "
                      "PFMA regulations, forums, news), one per source, each located on the stream's FWA line "
                      "when it can be (`located`). Researched 2026-10-27; pac.dfo-mpo.gc.ca did not resolve, "
                      "so DFO pages were read through web.archive.org / waves-vagues.",
            "findings": out, "searched_nothing_found": nothing,
            "unmatched": unmatched, "context": context}


def _by_name(cands: dict, f: dict) -> dict | None:
    """A finding without a candidate id (a stream the census did not list): the candidate of that
    name whose mouth is within 25 km of the finding's mouth / line, if any."""
    from pipeline.tools.audit import tidal_candidates as TC
    ref = f.get("mouth") or (f.get("line") or [None])[0] or f.get("coord")
    key = TC._name_key(f.get("water"))
    best = None
    for c in cands["candidates"]:
        if TC._name_key(c["water"]) != key or not ref:
            continue
        d = _ground_m(c["mouth"]["coord"], ref)
        if d <= 25_000 and (best is None or d < best[0]):
            best = (d, c)
    return best[1] if best else None


# --------------------------------------------------------------------------- the triage

def _ground_m(a, b) -> float:
    lat = math.radians((a[1] + b[1]) / 2)
    return math.hypot((a[0] - b[0]) * 111_320 * math.cos(lat), (a[1] - b[1]) * 110_540)


def edit_index(edits: dict) -> dict:
    """blk -> the edit sites attributed to it; plus the sites' lon/lat lines and the blind area."""
    by_blk = defaultdict(list)
    for s in edits.get("sites") or []:
        for st in s["streams"]:
            by_blk[st["blk"]].append(s["id"])
    blind = shape(edits["blind"]) if edits.get("blind") else None
    if blind is not None:
        shapely.prepare(blind)
    return {"by_blk": by_blk, "sites": {s["id"]: s for s in edits.get("sites") or []}, "blind": blind}


def edited_state(c: dict, idx: dict) -> tuple[bool | None, list[str]]:
    """(edited, the site ids) for a candidate: True when an edit site is attributed to its blue line or
    sits within ATTRIBUTE_M of its polygon crossing; None (can't tell) when unedited-looking but its
    crossing/mouth is in the blind area; else False."""
    ids = list(idx["by_blk"].get(str(c["blk"]), []))
    pc = (c.get("polygon") or {}).get("coord")
    if pc:
        to_a = _albers()
        p = Point(*to_a(*pc))
        for sid, s in idx["sites"].items():
            if sid in ids or s.get("kind") == "straight":      # a straight cut counts on the streams it crosses
                continue
            if _ground_m(pc, s["at"]) > 50_000:
                continue
            if LineString([to_a(*v) for v in s["line"]]).distance(p) <= ATTRIBUTE_M:
                ids.append(sid)
    if ids:
        return True, sorted(set(ids))
    if idx["blind"] is not None:
        for key in ("polygon", "mouth"):
            pt = (c.get(key) or {}).get("coord")
            if pt and idx["blind"].contains(Point(*pt)):
                return None, []
    return False, []


_ALBERS = None


def _albers():
    global _ALBERS
    if _ALBERS is None:
        _ALBERS = _to_albers()
    return _ALBERS


def web_verdict(w: dict, c: dict) -> bool | None:
    """Does a located web finding agree with the polygon's crossing (or, when the polygon misses the
    stream, with the proposed mouth)? None when the finding is not located."""
    loc = w.get("located") or {}
    pm = (c.get("polygon") or {}).get("measure")
    if loc.get("measure") is not None:
        return abs(float(loc["measure"]) - float(pm if pm is not None else 0.0)) <= AGREE_M
    pt = w.get("coord")
    ref = (c.get("polygon") or {}).get("coord") or (c.get("proposed") or {}).get("coord")
    if not pt or not ref:
        return None
    return _ground_m(pt, ref) <= AGREE_M


def classify(c: dict, edited: bool | None, web: list[dict], grid_blind: bool = False) -> tuple[str, str]:
    """(class, why) — see the module doc. `grid_blind`: the grid test is blind here and only the
    straight-line test looked (it found no cut across the stream)."""
    pol = c.get("polygon") or {}
    multi = bool(pol.get("multi_crossing"))
    book = [s for s in c.get("sources") or [] if s.get("kind") != "tidal_gauge"]
    book_yes = [s for s in book if s.get("agrees") is True]
    book_no = [s for s in book if s.get("agrees") is False]
    web_yes = [w for w in web if w.get("agrees") is True]
    web_no = [w for w in web if w.get("agrees") is False]
    unloc = [s for s in book if s.get("agrees") is None] + [w for w in web if w.get("agrees") is None]
    no = book_no + web_no
    yes = book_yes + web_yes
    said = []
    if book_no:
        said.append(f"book disagrees ({', '.join(s['kind'] for s in book_no)})")
    if web_no:
        said.append(f"web disagrees ({len(web_no)})")
    if yes:
        said.append(f"{'book' if book_yes else 'web'} agrees within {AGREE_M:.0f} m")
    if unloc and not (no or yes):
        said.append(f"{len(unloc)} source(s) name a boundary but are not located")
    edit_s = {True: "polygon edited here", False: "polygon unedited", None: "edit can't be seen here "
              "(DFO data off the grid)"}[edited]
    if multi:
        gaps = ", ".join(f"{a:.0f}-{b:.0f}" for a, b in pol.get("gaps_filled") or [])
        ign = ", ".join(f"{a:.0f}-{b:.0f}" for a, b in pol.get("ignored_pieces") or [])
        said.append(f"crosses the polygon {pol.get('n_cuts')}x at "
                    f"{', '.join(f'{m:.0f}' for m in pol.get('cuts') or [])} m"
                    + (f"; gaps filled {gaps} m" if gaps else "") + (f"; patch ignored {ign} m" if ign else ""))
    if grid_blind and edited is False:
        edit_s = "polygon unedited (grid test blind here; no straight cut across the stream)"
    why = "; ".join([edit_s, *said])
    if no:
        return ("sanity" if edited else "missed"), why
    if multi:
        return "multi-crossing", why
    if edited:
        return "owner-edited", why
    if yes:
        return "book agrees", why
    return "no claim", why


def triage(cands_doc: dict, edits: dict, web_doc: dict | None) -> dict:
    """Writes `triage` into every candidate (in place) and returns the per-class counts."""
    idx = edit_index(edits)
    web_by = defaultdict(list)
    for w in (web_doc or {}).get("findings") or []:
        web_by[w["id"]].append(w)
    counts = Counter()
    used_sites = set()
    for c in cands_doc["candidates"]:
        edited, sids = edited_state(c, idx)
        used_sites.update(sids)
        # the grid is blind here, but the straight-line test is not: no straight cut across this
        # stream means no edit seen (owner, T212 follow-up 2: DFO's own outline has no long straight runs)
        grid_blind = edited is None
        if grid_blind:
            edited = False
        web = []
        for w in web_by.get(c["id"], []):
            web.append({**w, "agrees": web_verdict(w, c)})
        cls, why = classify(c, edited, web, grid_blind)
        c["triage"] = {"class": cls, "edited": edited, "edit_sites": sids, "why": why, "web": web,
                       **({"grid_blind": True} if grid_blind else {})}
        counts[cls] += 1
    cands_doc.setdefault("coverage", {})["by_triage"] = {k: counts.get(k, 0) for k in CLASSES}
    cands_doc["coverage"]["owner_edit_sites"] = len(idx["sites"])
    cands_doc["coverage"]["owner_edit_sites_on_a_candidate"] = len(used_sites)
    return dict(counts)


def summary_md(doc: dict, edits: dict) -> str:
    cs = doc["candidates"]
    by = defaultdict(list)
    for c in cs:
        by[c["triage"]["class"]].append(c)
    sites = edits.get("sites") or []
    used = {s for c in cs for s in c["triage"]["edit_sites"]}
    named_unused = [s for s in sites if s["id"] not in used and any(st["name"] for st in s["streams"])]
    L = ["# T212 follow-up — tidal triage: where the owner edited, and what he missed", "",
         "Generated by `python -m pipeline.tools.audit.tidal_triage` (read only). Each candidate in "
         "`tidal-candidates.json` now carries `triage` (class, edited, edit sites, web findings); the "
         "curation review's **Tidal** tab shows only *missed* + *sanity* + *multi-crossing* by default (\"show "
         "all\" for the rest).",
         "",
         "## How the edits were found",
         "",
         "The unedited DFO original could not be rebuilt: the only copy of the build input "
         "(`DFO_TIDAL_BOUNDARY.gpkg` on R2) holds the owner's edited source layer, the DFO ArcGIS host "
         "no longer resolves, and the BC WFS does not publish the subareas. So edits are found by a "
         "fingerprint, not a diff: DFO's vertices sit on a 1 cm grid in BC Albers; vertices added by hand "
         "do not. Runs of off-grid vertices among on-grid ones are edit sites (plus the 2 hand-drawn rows). "
         "The grid is blind in DFO's own off-grid stretch (south Vancouver Island / Gulf Islands) and to edits "
         "made only by deleting vertices — the straight-line test below covers both.",
         "",
         "Straight-line fingerprint (follow-up 2): DFO's outline follows the shore vertex by vertex, so an "
         "edge of 50 m or more with no vertex between that CROSSES an FWA stream is a cut drawn across the "
         "channel (kind `straight`). It works inside the off-grid stretch and sees cuts made only by deleting "
         "vertices.",
         "",
         f"- Edit sites: **{len(sites)}** ({sum(1 for s in sites if s['kind']=='drawn')} hand-drawn rings, "
         f"{sum(1 for s in sites if s['kind']=='straight')} straight cuts across a stream); "
         f"{len(used)} touch a candidate stream; {len(named_unused)} touch only named streams that are not "
         "candidates (listed at the end).",
         "",
         "## Counts", "",
         "| class | meaning | count |", "|---|---|---|"]
    meaning = {"missed": "book/web disagrees, polygon unedited — **needs the owner**",
               "sanity": "book/web disagrees, polygon edited — check the edit",
               "multi-crossing": "the line crosses the polygon several times; boundary = top of the run "
                                 "joined to the mouth, gaps filled — **owner sees them**",
               "owner-edited": "polygon edited here — accepted (trusted)",
               "book agrees": "unedited; book/web within 300 m — accepted",
               "no claim": "unedited, no located claim — accepted at the polygon, low priority"}
    for k in CLASSES:
        L.append(f"| {k} | {meaning[k]} | {len(by[k])} |")
    L.append(f"| **total** | | {len(cs)} |")
    cant = sum(1 for c in cs if c["triage"]["edited"] is None)
    gb = [c for c in cs if c["triage"].get("grid_blind")]
    L += ["", f"{cant} candidates where no edit can be seen at all; {len(gb)} sit where the grid test is blind "
          "and were judged by the straight-line test alone (no straight cut across the stream: unedited).", ""]

    def _row(c):
        t = c["triage"]
        pm = (c.get("polygon") or {}).get("measure")
        words = []
        for s in c.get("sources") or []:
            if s.get("agrees") is False:
                words.append(f"{s['kind']}: {s['words'][:90]} (@ {s['measure'] if s.get('measure') is not None else '?'} m)")
        for w in t["web"]:
            if w.get("agrees") is False:
                words.append(f"web: {w['marker']} — [{w['site']}]({w['url']})")
        return (f"| {c['water']} (blk {c['blk']}) | {c['polygon'].get('crossing')} @ "
                f"{pm if pm is not None else '—'} | {'; '.join(words)} | {t['why']} |")

    for k, title in (("missed", "Missed — needs the owner"), ("sanity", "Sanity check — edited, but a source disagrees")):
        L += [f"## {title} ({len(by[k])})", "", "| stream | polygon | what disagrees | why |", "|---|---|---|---|"]
        L += [_row(c) for c in by[k]] or ["| — | | | |"]
        L.append("")
    multi = [c for c in cs if (c.get("polygon") or {}).get("multi_crossing")]
    L += [f"## Multi-crossing streams ({len(multi)}; {len(by['multi-crossing'])} in the class, the rest are "
          "missed/sanity first)", "",
          "The polygon is drawn up the river to the top of tidal water; where the FWA line wanders out of it "
          "and back in, the gaps are filled. An upstream piece the polygon does not join to the run within "
          "250 m of the river is a separate patch and ignored.", "",
          "| stream | class | crossings (m above mouth) | boundary (top) | gaps filled | patches ignored |",
          "|---|---|---|---|---|---|"]

    def _iv(v):
        return ", ".join(f"{a:.0f}-{b:.0f}" for a, b in v or []) or "—"
    for c in multi:
        p = c["polygon"]
        L.append(f"| {c['water']} (blk {c['blk']}) | {c['triage']['class']} | "
                 f"{', '.join(f'{m:.0f}' for m in p.get('cuts') or [])}{' …' if p.get('n_cuts', 0) > 12 else ''} | "
                 f"{p.get('measure')} | {_iv(p.get('gaps_filled'))} | {_iv(p.get('ignored_pieces'))} |")
    L.append("")
    sk = [c for c in cs if c["water"] == "Skeena River" or c.get("joins_big_river_tidal_reach_at_m") is not None]
    L += ["## Skeena and the big rivers' tidal tributaries", ""]
    for c in sk:
        if c["water"] != "Skeena River":
            continue
        pr = c["proposed"]
        L.append(f"- **Skeena River** (blk {c['blk']}): proposed {pr['above_mouth_m']:.0f} m above the FWA "
                 f"mouth at {pr['coord'][1]:.5f}, {pr['coord'][0]:.5f} ({pr['basis']}); confidence "
                 f"{c['confidence']}; triage {c['triage']['class']}.")
        for src in c.get("sources") or []:
            L.append(f"  - {src['kind']}: {src['words'][:160]} — {src['source']}"
                     + (f" (@ {src['measure']:.0f} m, {src.get('located_by')})" if src.get("measure") is not None else ""))
        for w in c["triage"]["web"]:
            L.append(f"  - web ({w.get('confidence')}): {w['marker']} — [{w['site'][:90]}]({w['url']})"
                     + (f" (@ {w['located']['measure']:.0f} m)" if w.get("located") else ""))
    tr = [c for c in sk if c["water"] != "Skeena River"]
    L.append(f"- Named tributaries entering the Fraser / Skeena / Nass below that river's tidal top: {len(tr)}"
             + (": " + ", ".join(f"{c['water']} (joins at {c['joins_big_river_tidal_reach_at_m']:.0f} m)" for c in tr)
                if tr else "") + ".")
    L.append("")
    web_n = sum(len(c["triage"]["web"]) for c in cs)
    L += [f"## Web findings ({web_n} on {sum(1 for c in cs if c['triage']['web'])} streams)", "",
          "| stream | marker | agrees? | conf | source |", "|---|---|---|---|---|"]
    for c in cs:
        for w in c["triage"]["web"]:
            ag = {True: "yes", False: "**no**", None: "not located"}[w.get("agrees")]
            L.append(f"| {c['water']} | {w['marker']} | {ag} | {w.get('confidence')} | [{w['site']}]({w['url']}) |")
    L += ["", "## Edit sites on named streams that are not candidates", ""]
    for s in named_unused:
        names = sorted({st["name"] for st in s["streams"] if st["name"]})
        L.append(f"- {s['id']} ({s['kind']}, {s['n_vertices']} vertices, {s['length_m']:.0f} m) at "
                 f"{s['at'][1]:.5f}, {s['at'][0]:.5f}: {', '.join(names)}")
    return "\n".join(L) + "\n"


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("edits")
    e.add_argument("--dfo-gpkg", type=Path, required=True)
    e.add_argument("--gpkg", type=Path, default=ROOT / "data/source/bc_fisheries_data.gpkg")
    e.add_argument("--out", type=Path, default=OUT)
    w = sub.add_parser("web")
    w.add_argument("found", type=Path, nargs="+", help="research findings JSON ({findings, searched_nothing_found})")
    w.add_argument("--gpkg", type=Path, default=ROOT / "data/source/bc_fisheries_data.gpkg")
    w.add_argument("--basemap", type=Path, default=ROOT / "data/source/bc.pmtiles")
    w.add_argument("--out", type=Path, default=OUT)
    t = sub.add_parser("triage")
    t.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args(argv)
    if a.cmd == "web":
        cands = json.loads((a.out / "tidal-candidates.json").read_text())
        doc = web_doc(a.found, cands, a.gpkg, a.basemap)
        (a.out / "tidal-web.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
        n_loc = sum(1 for f in doc["findings"] if f.get("located"))
        print(f"{len(doc['findings'])} findings, {n_loc} located -> {a.out / 'tidal-web.json'}")
        return
    if a.cmd == "edits":
        doc = edits_doc(a.dfo_gpkg, a.gpkg)
        (a.out / "tidal-edits.json").write_text(json.dumps(doc, indent=1) + "\n")
        print(f"{len(doc['sites'])} edit sites -> {a.out / 'tidal-edits.json'}")
        return
    cpath = a.out / "tidal-candidates.json"
    doc = json.loads(cpath.read_text())
    edits = json.loads((a.out / "tidal-edits.json").read_text())
    wpath = a.out / "tidal-web.json"
    web = json.loads(wpath.read_text()) if wpath.exists() else None
    counts = triage(doc, edits, web)
    cpath.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    (a.out / "tidal-triage.md").write_text(summary_md(doc, edits))
    print(counts)


if __name__ == "__main__":
    main()
