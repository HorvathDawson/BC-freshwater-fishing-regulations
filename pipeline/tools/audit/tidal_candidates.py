"""T212 — THE TIDAL BOUNDARY ON EVERY OCEAN-ENTERING STREAM, PROPOSED (research; writes no curated data).

    .venv/bin/python -m pipeline.tools.audit.tidal_candidates \
        --gpkg <bc_fisheries_data.gpkg> --atlas <data/generated/atlas/full> \
        --basemap <data/source/bc.pmtiles> [--osm <overpass json> ...] \
        --out handoff/PLAN/curation-review/tidal

For every stream the owner's edited DFO tidal polygon (the `tidal_boundary` layer of the build gpkg)
crosses, and every NAMED ocean-entering FWA blue line (watershed code 9xx-NNNNNN with nothing below
it, and the Fraser / Skeena / Nass mainstems), find where the line leaves the polygon going
upstream: the CANDIDATE TIDAL POINT (blk + FWA route measure + lon/lat on the FWA line). Then hold it
against what the words say:

  * the synopsis box "Tidal Boundaries on Rivers in Region 2" (printed p.21) and the DFO tidal-area
    pages' "Tidal water boundaries in streams and rivers" (Area 29) and area descriptions;
  * the DFO salmon notices and synopsis rows that name a tidal boundary (curated dfo_salmon entries,
    the catalogue's `regs_verbatim`);
  * curated splits that ARE a tidal limit (Fraser CPR bridge at Mission, Somass Papermill Dam, ...)
    and the tidal level gauges (data/curated/gauges/tidal.json; HYDAT's own: Mission, Port Mann) —
    the tidal limit is above a tidal gauge;
  * OSM bridges (an Overpass dump, `--osm`) to place a marker the words name ("CPR Bridge").

Each candidate: the proposed split (where it should go, label "<water>: mouth (tidal boundary)"),
the polygon's own crossing, every source with whether it agrees, and a confidence. READ ONLY on
every input; the output is a proposal file the curation review shows (`curation-review/backend/
tidal.py`), accepted one by one into a mark — never into splits.json.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import shapely
from shapely import STRtree
from shapely.geometry import LineString, Point

ROOT = Path(__file__).resolve().parents[3]

#: the polygon's crossing within this far (m) of a stream's FWA mouth IS the mouth
AT_MOUTH_M = 75.0
#: a located word-source within this far (m, along the line) of the polygon's crossing agrees
AGREE_M = 300.0
#: the stream's mouth within this far (m) of the polygon still meets it (the polygon edge sits there)
MEETS_M = 60.0
#: the label every proposed tidal split carries (owner, T212): mouths are "mouth (tidal boundary)"
MOUTH_LABEL = "mouth (tidal boundary)"

# --------------------------------------------------------------------------- the words

#: SYNOPSIS 2025-27, printed p.21 (PDF p.23): "Tidal Boundaries on Rivers in Region 2 — Provincial
#: freshwater regulations apply upstream of these boundaries and Federal tidal regulations apply
#: downstream of these boundaries". (name as the FWA spells it, marker words, marker kind)
REGION2_BOX = [
    ("Brunette River", "Burlington Northern Railway Bridge", "rail"),
    ("Campbell River", "Burlington Northern Railway Bridge", "rail"),
    ("Capilano River", "CN Railway Bridge", "rail"),
    ("Coquitlam River", "Mary Hill Bypass Bridge", "road"),
    ("Fraser River", "CPR Bridge at Mission", "rail"),
    ("Kanaka Creek", "CPR Bridge", "rail"),
    ("Nathan Creek", "CN Railway Bridge", "rail"),
    ("Nicomekl River", "Burlington Northern Railway Bridge", "rail"),
    ("Pitt River", "CPR Bridge", "rail"),
    ("Serpentine River", "Burlington Northern Railway Bridge", "rail"),
    ("Seymour River", "CN Railway Bridge", "rail"),
    ("Squamish River", "a line running true west from the tip of the Squamish Dyke (\"Windsurfer Spit\")",
     "line"),
    ("Stave River", "CPR Bridge", "rail"),
    ("West Creek", "CN Railway Bridge", "rail"),
    ("Whonnock Creek", "CP rail line", "rail"),
    ("York Creek", "CP rail line", "rail"),
]
REGION2_SRC = "synopsis 2025-27 p.21 (PDF p.23) box \"Tidal Boundaries on Rivers in Region 2\""
#: the Region 2 box and DFO Area 29 name LOWER MAINLAND streams: only a candidate inside this
#: box (lon_min, lat_min, lon_max, lat_max) takes them — "Campbell River" here is Surrey's, not
#: Vancouver Island's
LOWER_MAINLAND = (-123.40, 48.95, -121.90, 49.82)

#: DFO tidal-area pages (pac.dfo-mpo.gc.ca/fm-gp/rec/tidal-maree/a-s<N>-eng.html, read 2026-10-27):
#: the marker that ENDS tidal water on a stream. `near` (lon, lat) says which stream of that name
#: (within NEAR_KM); `match` picks the crossing (a road name / ref fragment); `pick` "near" (the
#: crossing nearest the polygon's) or "lowest" (the first one up from the mouth); `offset_m` from
#: the crossing, + upstream.
NEAR_KM = 25.0
DFO_AREA_MARKERS = [
    {"water": "Chapman Creek", "words": "Highway 101 Bridge (DFO); the notice: \"tidal water fishing "
     "boundary signs located below the Hwy 101 Bridge\"", "kind": "road", "match": "101",
     "page": "DFO tidal Area 29: Tidal water boundaries in streams and rivers", "near": (-123.73, 49.46)},
    {"water": "Nathan Creek", "words": "CNR Bridge", "kind": "rail", "page": "DFO tidal Area 29",
     "near": (-122.60, 49.17)},
    {"water": "Tlell River", "words": "a white triangular fishing boundary sign approximately 1 km "
     "upstream of the Highway #16 bridge", "kind": "road", "match": "16", "offset_m": 1000.0,
     "page": "DFO tidal Area 2 (Tidal waters of the Tlell River)", "near": (-131.93, 53.57)},
    {"water": "Nimpkish River", "words": "the tidal water boundary signs approximately 100 m upstream "
     "of the Highway 19 bridge", "kind": "road", "match": "19", "offset_m": 100.0,
     "page": "DFO tidal Area 12 (Subarea 12-19, the mouth of Nimpkish River)", "near": (-126.98, 50.56)},
    {"water": "Somass River", "words": "the tidal boundary signs at the top of the rapids at Paper "
     "Mill Dam", "kind": "dam", "page": "DFO tidal Area 23 (Subarea 23-1)", "near": (-124.83, 49.26)},
    {"water": "Nanaimo River", "words": "Subarea 17-14 takes the Nanaimo River from the Cedar Bridge "
     "down (tidal water reaches the Cedar Bridge)", "kind": "road", "match": "cedar",
     "page": "DFO tidal Area 17 (Cedar Bridge)", "near": (-123.86, 49.12)},
    {"water": "Marble River", "words": "\"from the Marble River tidal boundary downstream to ... "
     "Varney Bay\" (not located)", "kind": "unlocated", "page": "DFO tidal Area 27",
     "near": (-127.50, 50.55)},
    {"water": "Salmon River", "words": "Salmon Bay (tidal) runs up to \"the downstream side of the "
     "first bridge upstream on the Salmon River\"", "kind": "road", "pick": "lowest",
     "page": "DFO tidal Area 13 (Subarea 13-34)", "near": (-125.95, 50.39)},
    {"water": "Chemainus River", "words": "\"the tidal waters of the Chemainus River\" (not located)",
     "kind": "unlocated", "page": "DFO tidal Area 17", "near": (-123.70, 48.90)},
]
#: the Region 2 box markers' road-name fragments (a rail marker takes the rail crossing nearest
#: the polygon's: the basemap carries no operator)
REGION2_MATCH = {"Coquitlam River": "mary hill"}

#: CURATED SPLITS THAT ARE (OR NAME) A TIDAL LIMIT — splits.json id -> what it says
CURATED_TIDAL_SPLITS = {
    "fraser_river__the_cpr_bridge_at_mission": "the CPR Bridge at Mission (synopsis p.21; RULINGS K8)",
    "somass_river__tidal_boundary_at_papermill_dam": "tidal boundary at Papermill Dam",
    "tlell_river__tidal_sign_1_5_km_above_hwy_16_bridge": "tidal boundary sign 1.5 km above Hwy 16 "
                                                          "Bridge (synopsis Tlell row)",
    "qualicum_river__hwy_19a_bridge": "Hwy 19A bridge, noted \"at the tidal boundary (m~108 above "
                                      "mouth)\"",
    "campbell_river__cement_block_boundary_sign": "cement block sign, noted \"where the Campbell R "
                                                  "enters the tidal zone\" (LOW CONFIDENCE pin)",
}
#: a curated split that sits a stated distance ABOVE the tidal boundary (book words)
CURATED_ABOVE_TIDAL = {
    "copper_creek__south_bay_main_bridge": (6000.0, "\"second bridge 6 km upstream of tidal boundary\""),
}


# --------------------------------------------------------------------------- geometry helpers

def _gpkg_geom(b: bytes):
    flags = b[3]
    env = (flags >> 1) & 7
    return shapely.from_wkb(bytes(b[8 + {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[env]:]))


def _depth(wsc: str) -> int:
    s = wsc.split("-")
    while s and set(s[-1]) == {"0"}:
        s.pop()
    return len(s)


def _to_lonlat():
    from pyproj import Transformer
    return Transformer.from_crs(3005, 4326, always_xy=True).transform


def _to_albers():
    from pyproj import Transformer
    return Transformer.from_crs(4326, 3005, always_xy=True).transform


def _name_key(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").casefold()).strip()


# --------------------------------------------------------------------------- the census

def load_polygon(con):
    (b,) = con.execute("select geom from tidal_boundary").fetchone()
    return _gpkg_geom(b)


def crossing_fids(con, poly) -> set[int]:
    """Every stream segment the polygon's outline touches (rtree on 40-vertex pieces, then exact)."""
    pieces = []
    for g in getattr(poly, "geoms", [poly]):
        for ring in [g.exterior, *g.interiors]:
            c = np.asarray(ring.coords)
            for i in range(0, len(c) - 1, 40):
                pieces.append(LineString(c[i:i + 41]))
    cur = con.cursor()
    fids: set[int] = set()
    for pc in pieces:
        x0, y0, x1, y1 = pc.bounds
        fids.update(i for (i,) in cur.execute(
            "select id from rtree_streams_geom where maxx>=? and minx<=? and maxy>=? and miny<=?",
            (x0, x1, y0, y1)))
    tree = STRtree(pieces)
    out = set()
    fl = sorted(fids)
    for k in range(0, len(fl), 900):
        chunk = fl[k:k + 900]
        for fid, b in cur.execute(f"select fid, geom from streams where fid in "
                                  f"({','.join('?' * len(chunk))})", chunk):
            if len(tree.query(_gpkg_geom(b), predicate="intersects")):
                out.add(fid)
    return out


def scan(con, cross: set[int]) -> dict[int, list[dict]]:
    """ONE attribute pass: blk -> its segments, for every blue line a crossing segment is on and
    every ocean-entering one (9xx depth 2; Fraser 100 / Skeena 400 / Nass 500 mainstems)."""
    cblk = set()
    for (blk,) in con.execute(f"select BLUE_LINE_KEY from streams where fid in "
                              f"({','.join(str(f) for f in sorted(cross))})"):
        cblk.add(blk)
    out: dict[int, list[dict]] = defaultdict(list)
    for (fid, blk, name, gnis, wsc, down, length, edge) in con.execute(
            "select fid, BLUE_LINE_KEY, GNIS_NAME, GNIS_ID, FWA_WATERSHED_CODE, "
            "DOWNSTREAM_ROUTE_MEASURE, LENGTH_METRE, EDGE_TYPE from streams"):
        if not wsc:
            continue
        keep = blk in cblk
        ocean = False
        if wsc[0] == "9" and _depth(wsc) == 2:
            keep = ocean = True
        elif wsc[:3] in ("100", "400", "500") and wsc[4:10] == "000000" and _depth(wsc) == 1:
            keep = ocean = True
        if keep:
            out[blk].append({"fid": fid, "blk": blk, "name": name, "gnis": gnis, "wsc": wsc,
                             "down": down or 0.0, "length": length or 0.0, "edge": edge,
                             "ocean": ocean})
    return out


def _line_name(segs: list[dict]) -> str | None:
    c = Counter(s["name"] for s in segs if s["name"])
    return c.most_common(1)[0][0] if c else None


def ocean_mainstems(by_blk: dict[int, list[dict]]) -> dict[str, int]:
    """watershed code -> the blk of its MAINSTEM (the longest blue line carrying that code)."""
    by_code: dict[str, list[tuple[float, int]]] = defaultdict(list)
    for blk, segs in by_blk.items():
        if segs and segs[0]["ocean"]:
            by_code[segs[0]["wsc"]].append((sum(s["length"] for s in segs), blk))
    return {code: max(v)[1] for code, v in by_code.items()}


def _geoms(con, segs: list[dict]) -> dict[int, object]:
    fids = [s["fid"] for s in segs]
    out = {}
    for k in range(0, len(fids), 900):
        chunk = fids[k:k + 900]
        for fid, b in con.execute(f"select fid, geom from streams where fid in "
                                  f"({','.join('?' * len(chunk))})", chunk):
            g = _gpkg_geom(b)
            if g.geom_type == "MultiLineString":
                g = shapely.line_merge(g)
                if g.geom_type == "MultiLineString":
                    g = max(g.geoms, key=lambda x: x.length)
            out[fid] = g
    return out


def _measure_at(seg: dict, g, pt) -> float:
    k = (seg["length"] / g.length) if g.length else 1.0
    return seg["down"] + g.project(pt) * k


def analyse(segs: list[dict], geoms: dict, poly) -> dict:
    """WHERE THE POLYGON LEAVES THE LINE: the crossings (measures), the inside intervals and the
    candidate (the UPSTREAM-most point where the line goes from inside the polygon to outside)."""
    segs = sorted(segs, key=lambda s: s["down"])
    bnd = poly.boundary
    cuts: list[float] = []
    pieces: list[tuple[float, float, bool]] = []          # (from m, to m, inside)
    for s in segs:
        g = geoms.get(s["fid"])
        if g is None or g.is_empty:
            continue
        ms = [s["down"], s["down"] + s["length"]]
        x = g.intersection(bnd) if g.intersects(bnd) else None
        if x is not None and not x.is_empty:
            pts = [x] if x.geom_type == "Point" else [
                p for p in getattr(x, "geoms", [x]) if p.geom_type == "Point"] + [
                Point(c) for p in getattr(x, "geoms", [x]) if p.geom_type != "Point"
                for c in p.coords]
            for p in pts:
                m = _measure_at(s, g, p)
                cuts.append(m)
                ms.append(m)
        ms = sorted(set(round(m, 2) for m in ms))
        k = (g.length / s["length"]) if s["length"] else 1.0
        for a, b in zip(ms, ms[1:]):
            if b - a < 0.5:
                continue
            mid = g.interpolate(((a + b) / 2 - s["down"]) * k)
            pieces.append((a, b, bool(shapely.contains_xy(poly, mid.x, mid.y))))
    # merge
    merged: list[list] = []
    for a, b, ins in sorted(pieces):
        if merged and merged[-1][2] == ins and abs(merged[-1][1] - a) < 1.0:
            merged[-1][1] = b
        else:
            merged.append([a, b, ins])
    inside = [(a, b) for a, b, i in merged if i]
    top = max((b for a, b in inside), default=None)
    total_in = sum(b - a for a, b in inside)
    mouth_m = segs[0]["down"] if segs else 0.0
    gaps = sum(b - a for a, b, i in merged if not i and top is not None and b <= top + 0.5)
    return {"cuts": sorted(round(c, 1) for c in cuts), "inside": [(round(a, 1), round(b, 1)) for a, b in inside],
            "top": top, "inside_m": round(total_in, 1), "gap_below_m": round(gaps, 1),
            "mouth_m": mouth_m, "n_inside": len(inside)}


def point_at(segs: list[dict], geoms: dict, m: float):
    for s in sorted(segs, key=lambda s: s["down"]):
        if s["down"] - 0.5 <= m <= s["down"] + s["length"] + 0.5:
            g = geoms.get(s["fid"])
            if g is None:
                return None
            k = (g.length / s["length"]) if s["length"] else 1.0
            return g.interpolate(min(max((m - s["down"]) * k, 0.0), g.length))
    return None


def snap(segs: list[dict], geoms: dict, pt) -> tuple[float, float]:
    """(measure, distance m) of the line's nearest point to `pt` (EPSG:3005)."""
    best = None
    for s in segs:
        g = geoms.get(s["fid"])
        if g is None:
            continue
        d = g.distance(pt)
        if best is None or d < best[0]:
            best = (d, s, g)
    d, s, g = best
    return _measure_at(s, g, pt), d


# --------------------------------------------------------------------------- the word sources

def _dfo_sources() -> dict[str, list[dict]]:
    """name key -> DFO salmon notices whose words name a tidal boundary / tidal water."""
    from pipeline.regs.review import dfo as D
    out: dict[str, list[dict]] = defaultdict(list)
    for lid, (_slug, loc, water) in sorted(D.locations().items()):
        st = loc.get("source_text") or {}
        words = st.get("specific_area") or ""
        if not re.search(r"\btidal\b", words, re.I) or re.search(r"tidal waters? (of )?areas?", words, re.I):
            continue
        out[_name_key(loc.get("water"))].append({
            "kind": "dfo_notice", "source": f"DFO salmon notice {lid}",
            "words": words, "items": list((water or {}).get("item_ids") or []),
            "status": loc.get("status") or "active"})
    return out


def _catalogue_sources() -> dict[str, list[dict]]:
    """name key -> synopsis rows (the catalogue's verbatim) that name a tidal boundary."""
    out: dict[str, list[dict]] = defaultdict(list)
    for f in sorted((ROOT / "data/curated/regulations/entries/catalogue").glob("region-*.json")):
        doc = json.loads(f.read_text())
        for e in doc.get("entries") or []:
            words = " ".join(str(e.get("regs_verbatim") or "").split())
            hits = [m.group(0) for m in re.finditer(r"[^.;\n]*\btidal (boundary|waters? regulations)[^.;\n]*",
                                                     words, re.I)]
            if not hits:
                continue
            name = re.sub(r"\s*\(.*$", "", e.get("name") or "").strip()
            out[_name_key(name)].append({
                "kind": "synopsis_row", "source": f"synopsis row {e['entry_id']} (p."
                f"{','.join(str(p) for p in e.get('source_pages') or [])})",
                "words": " … ".join(dict.fromkeys(h.strip() for h in hits)),
                "mu": e["entry_id"].rsplit("@", 1)[-1] if "@" in e["entry_id"] else None})
    return out


def _osm_bridges(paths: list[Path]) -> list[dict]:
    """OSM ways (Overpass `out geom tags`): railway / highway lines, as EPSG:3005 lines + tags."""
    tr = _to_albers()
    out = []
    for p in paths:
        doc = json.loads(Path(p).read_text())
        for el in doc.get("elements") or []:
            if el.get("type") != "way" or not el.get("geometry"):
                continue
            tags = el.get("tags") or {}
            xs = [tr(c["lon"], c["lat"]) for c in el["geometry"]]
            if len(xs) < 2:
                continue
            kind = "rail" if "railway" in tags else "road" if "highway" in tags else None
            if kind is None:
                continue
            out.append({"id": el["id"], "kind": kind, "tags": tags, "geom": LineString(xs)})
    return out


#: a marker is looked for this far (m) above the mouth / the polygon's crossing
MARKER_REACH_M = 10_000.0


class Basemap:
    """OSM ROADS AND RAIL FROM THE APP'S BASEMAP (`data/source/bc.pmtiles`, Protomaps, the z14
    `roads` layer — OSM replication 2026-03-05), read only: a minimal PMTiles v3 reader, tiles
    fetched for a box and cached. Carries name / ref / bridge, no operator."""
    Z = 14

    def __init__(self, path: Path):
        import struct
        self.f = open(path, "rb")
        h = self.f.read(127)
        if h[:7] != b"PMTiles":
            raise ValueError(f"{path} is not a PMTiles v3 archive")
        (self.root_off, self.root_len, _mo, _ml, self.leaf_off, _ll, self.data_off, _dl) = \
            struct.unpack("<8Q", h[8:72])
        self.ic, self.tc = h[97], h[98]
        self._tiles: dict[tuple[int, int], list[dict]] = {}
        self._tr = _to_albers()
        self._ll = _to_lonlat()

    @staticmethod
    def _varint(b, i):
        s = r = 0
        while True:
            c = b[i]
            i += 1
            r |= (c & 0x7F) << s
            if c < 0x80:
                return r, i
            s += 7

    def _dir(self, raw):
        i = 0
        n, i = self._varint(raw, i)
        cols = [[], [], [], []]
        last = 0
        for _ in range(n):
            v, i = self._varint(raw, i)
            last += v
            cols[0].append(last)
        for c in (1, 2):
            for _ in range(n):
                v, i = self._varint(raw, i)
                cols[c].append(v)
        for k in range(n):
            v, i = self._varint(raw, i)
            cols[3].append(cols[3][-1] + cols[2][k - 1] if v == 0 and k > 0 else v - 1)
        return list(zip(*cols))

    def _read(self, off, ln, comp):
        import gzip
        self.f.seek(off)
        b = self.f.read(ln)
        return gzip.decompress(b) if comp == 2 else b

    @staticmethod
    def _tile_id(z, x, y):
        acc = sum(4 ** t for t in range(z))
        n, d, s = 1 << z, 0, (1 << z) // 2
        while s > 0:
            rx, ry = int(bool(x & s)), int(bool(y & s))
            d += s * s * ((3 * rx) ^ ry)
            if ry == 0:
                if rx == 1:
                    x, y = n - 1 - x, n - 1 - y
                x, y = y, x
            s //= 2
        return acc + d

    def _tile(self, z, x, y):
        tid = self._tile_id(z, x, y)
        off, ln = self.root_off, self.root_len
        for _ in range(4):
            ents = self._dir(self._read(off, ln, self.ic))
            hit = None
            lo, hi = 0, len(ents) - 1
            while lo <= hi:
                m = (lo + hi) // 2
                if ents[m][0] <= tid:
                    hit, lo = ents[m], m + 1
                else:
                    hi = m - 1
            if hit is None:
                return None
            eid, rl, l, o = hit
            if rl == 0:
                off, ln = self.leaf_off + o, l
                continue
            return self._read(self.data_off + o, l, self.tc) if tid < eid + rl else None
        return None

    def _ways_of(self, x, y) -> list[dict]:
        if (x, y) in self._tiles:
            return self._tiles[(x, y)]
        import mapbox_vector_tile as mvt
        z, out = self.Z, []
        raw = self._tile(z, x, y)
        n = 2 ** z
        for f in (mvt.decode(raw).get("roads", {}).get("features", []) if raw else []):
            pr = f["properties"]
            k = pr.get("kind")
            if k not in ("rail", "highway", "major_road", "minor_road"):
                continue
            g = f["geometry"]
            lines = ([g["coordinates"]] if g["type"] == "LineString"
                     else g["coordinates"] if g["type"] == "MultiLineString" else [])
            for ln in lines:
                pts = []
                for px, py in ln:
                    lon = (x + px / 4096) / n * 360 - 180
                    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1 - py / 4096) / n))))
                    pts.append(self._tr(lon, lat))
                if len(pts) < 2:
                    continue
                tags = {("railway" if k == "rail" else "highway"): pr.get("kind_detail") or k}
                tags.update({t: str(pr[t]) for t in ("name", "ref") if pr.get(t)})
                if pr.get("is_bridge"):
                    tags["bridge"] = "yes"
                out.append({"id": f"basemap/{z}/{x}/{y}", "kind": "rail" if k == "rail" else "road",
                            "tags": tags, "geom": LineString(pts)})
        self._tiles[(x, y)] = out
        return out

    def ways(self, env) -> list[dict]:
        """Every road / rail line in the tiles over `env` (an EPSG:3005 geometry)."""
        x0, y0, x1, y1 = env.bounds
        corners = [self._ll(x, y) for x in (x0, x1) for y in (y0, y1)]
        lo0, lo1 = min(c[0] for c in corners), max(c[0] for c in corners)
        la0, la1 = min(c[1] for c in corners), max(c[1] for c in corners)
        n = 2 ** self.Z

        def txy(lon, lat):
            return (int((lon + 180) / 360 * n),
                    int((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n))
        tx0, ty1 = txy(lo0, la0)
        tx1, ty0 = txy(lo1, la1)
        if (tx1 - tx0 + 1) * (ty1 - ty0 + 1) > 400:
            return []
        return [w for x in range(tx0, tx1 + 1) for y in range(ty0, ty1 + 1) for w in self._ways_of(x, y)]


def locate_marker(segs, geoms, osm, kind: str, near_m: float | None, lonlat, *,
                  match: str | None = None, pick: str = "near", offset_m: float = 0.0,
                  mouth_m: float = 0.0) -> dict | None:
    """Where an OSM way of the marker's kind CROSSES the line: the one whose name / ref carries
    `match`, else (`pick` near) the crossing nearest the polygon's candidate or (`pick` lowest) the
    first up from the mouth; then `offset_m` along the line (+ upstream). None when no way crosses."""
    if kind not in ("rail", "road") or not osm:
        return None
    # the line's lower part only: a marker is near the mouth or the polygon's crossing
    reach = max(near_m or 0.0, mouth_m) + MARKER_REACH_M
    line = shapely.union_all([geoms[s["fid"]] for s in segs if s["fid"] in geoms and s["down"] <= reach])
    env = line.envelope.buffer(200)
    ways = osm.ways(env) if isinstance(osm, Basemap) else osm
    hits = []
    for w in ways:
        if w["kind"] != kind or not w["geom"].intersects(env):
            continue
        if kind == "road" and w["tags"].get("highway") in ("footway", "path", "cycleway", "track",
                                                             "service", "steps", "bridleway", "pier"):
            continue
        x = w["geom"].intersection(line)
        if x.is_empty:
            continue
        p = x if x.geom_type == "Point" else x.representative_point()
        m, _d = snap(segs, geoms, p)
        tags = w["tags"]
        label = " ".join(str(tags.get(k)) for k in ("name", "ref") if tags.get(k)) or \
            str(tags.get("railway") or tags.get("highway"))
        hits.append({"measure": round(m, 1), "osm_way": w["id"], "osm": label,
                     "bridge": tags.get("bridge") == "yes", "tags": tags})
    # one crossing per place (a way cut at tile edges crosses twice at one point)
    hits.sort(key=lambda h: (h["measure"], not h["bridge"]))
    uniq: list[dict] = []
    for h in hits:
        if uniq and abs(uniq[-1]["measure"] - h["measure"]) < 15:
            continue
        uniq.append(h)
    if not uniq:
        return None
    named = [h for h in uniq if match and match.casefold() in " ".join(
        str(h["tags"].get(k, "")) for k in ("name", "ref")).casefold()]
    pool = named or uniq
    if pick == "lowest":
        best = min(pool, key=lambda h: h["measure"])
    else:
        ref = near_m if near_m is not None else mouth_m
        best = min(pool, key=lambda h: abs(h["measure"] - ref))
    m = best["measure"] + offset_m
    pt = point_at(segs, geoms, m)
    return {"measure": round(m, 1), "crossing_measure": best["measure"], "osm": best["osm"],
            "named_match": bool(named) if match else None, "offset_m": offset_m or None,
            "crossings": [f"{h['osm']} @ {h['measure']:.0f}" for h in uniq[:8]],
            "coord": [round(v, 6) for v in lonlat(pt.x, pt.y)] if pt is not None else None}


# --------------------------------------------------------------------------- assemble

def _km(a, b) -> float:
    return math.hypot((a[0] - b[0]) * 111.32 * math.cos(math.radians((a[1] + b[1]) / 2)),
                      (a[1] - b[1]) * 111.32)


def _region_ok(lonlat_pt, lm_only: bool) -> bool:
    if not lm_only:
        return True
    x0, y0, x1, y1 = LOWER_MAINLAND
    return x0 <= lonlat_pt[0] <= x1 and y0 <= lonlat_pt[1] <= y1


def build(gpkg: Path, atlas: Path, osm_paths: list[Path], basemap: Path | None = None) -> dict:
    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    poly = load_polygon(con)
    shapely.prepare(poly)
    cross = crossing_fids(con, poly)
    by_blk = scan(con, cross)
    mains = ocean_mainstems(by_blk)
    main_blks = set(mains.values())
    cross_blks = {s["blk"] for segs in by_blk.values() for s in segs if s["fid"] in cross}
    lonlat = _to_lonlat()
    osm = Basemap(basemap) if basemap else _osm_bridges(osm_paths)

    reg = json.loads((atlas / "registry.json").read_text())["items"]
    blk_items: dict[str, list[str]] = defaultdict(list)
    item_of = {}
    for it in reg:
        if it["kind"] != "stream":
            continue
        item_of[it["id"]] = it
        for sid in it["section_ids"]:
            b = sid.split(":")[0]
            if it["id"] not in blk_items[b]:
                blk_items[b].append(it["id"])
    resolved = {s["split_id"]: s for s in json.loads((atlas / "splits.resolved.json").read_text())}
    tidal_gauges = set(json.loads((ROOT / "data/curated/gauges/tidal.json").read_text())["stations"])
    tidal_gauges |= {"08MH024", "08MH126"}             # HYDAT's own remarks (pipeline/gauges/consume/tidal.py)
    dfo_src = _dfo_sources()
    cat_src = _catalogue_sources()

    # which blue lines are candidates: every NAMED one the polygon crosses, and every named
    # ocean-entering mainstem (crossed or not — a polygon that misses one is a finding)
    want = sorted(b for b in (cross_blks | main_blks) if _line_name(by_blk.get(b, [])))
    rows = []
    for blk in want:
        segs = by_blk[blk]
        name = _line_name(segs)
        wsc = segs[0]["wsc"]
        geoms = _geoms(con, segs)
        a = analyse(segs, geoms, poly)
        enters = "ocean" if blk in main_blks else "tidal_water"
        mouth_pt = point_at(segs, geoms, a["mouth_m"])
        mouth_ll = [round(v, 6) for v in lonlat(mouth_pt.x, mouth_pt.y)]
        mouth_gap = round(float(poly.distance(mouth_pt)), 1) if not shapely.contains_xy(
            poly, mouth_pt.x, mouth_pt.y) else 0.0
        if a["top"] is not None:
            poly_m = a["top"]
            crossing = ("at_mouth" if poly_m - a["mouth_m"] <= AT_MOUTH_M else
                        "clean" if a["n_inside"] == 1 and a["gap_below_m"] < 50 else "blobs")
        elif blk in cross_blks or mouth_gap <= MEETS_M:
            poly_m, crossing = a["mouth_m"], "at_mouth"
        else:
            poly_m, crossing = None, "misses"
        poly_pt = point_at(segs, geoms, poly_m) if poly_m is not None else None
        poly_ll = [round(v, 6) for v in lonlat(poly_pt.x, poly_pt.y)] if poly_pt is not None else None
        items = blk_items.get(str(blk), [])
        region_ll = poly_ll or mouth_ll
        key = _name_key(name)

        sources: list[dict] = []
        located: list[dict] = []          # word sources with a measure on this line

        def add(src: dict, measure: float | None = None, how: str | None = None):
            if measure is not None:
                src["measure"] = round(measure, 1)
                src["located_by"] = how
                pt = point_at(segs, geoms, measure)
                if pt is not None:
                    src["coord"] = [round(v, 6) for v in lonlat(pt.x, pt.y)]
                if poly_m is not None:
                    src["polygon_minus_source_m"] = round(poly_m - measure, 1)
                    src["agrees"] = abs(poly_m - measure) <= AGREE_M
                located.append(src)
            sources.append(src)

        # a curated split that IS the tidal limit already places the marker: no OSM guess beside it
        curated_here = any(r and str(r["blk"]) == str(blk)
                           for r in (resolved.get(sid) for sid in CURATED_TIDAL_SPLITS))
        for nm, words, kind in REGION2_BOX:
            if _name_key(nm) == key and _region_ok(region_ll, True):
                loc = None if curated_here else locate_marker(
                    segs, geoms, osm, kind, poly_m, lonlat, match=REGION2_MATCH.get(nm), mouth_m=a["mouth_m"])
                add({"kind": "synopsis_box", "source": REGION2_SRC, "words": f"{nm}: {words}",
                     **({"osm": loc} if loc else {})},
                    loc["measure"] if loc else None, f"OSM {kind} crossing ({loc['osm']})" if loc else None)
        for mk in DFO_AREA_MARKERS:
            if _name_key(mk["water"]) == key and _km(region_ll, mk["near"]) <= NEAR_KM:
                loc = locate_marker(segs, geoms, osm, mk["kind"], poly_m, lonlat, match=mk.get("match"),
                                    pick=mk.get("pick", "near"), offset_m=mk.get("offset_m", 0.0),
                                    mouth_m=a["mouth_m"])
                add({"kind": "dfo_tidal_area", "source": mk["page"], "words": f"{mk['water']}: {mk['words']}",
                     **({"osm": loc} if loc else {})},
                    loc["measure"] if loc else None, f"OSM {mk['kind']} crossing ({loc['osm']})"
                    + (f" + {loc['offset_m']:.0f} m" if loc.get("offset_m") else "") if loc else None)
        for sid, what in CURATED_TIDAL_SPLITS.items():
            r = resolved.get(sid)
            if r and str(r["blk"]) == str(blk):
                add({"kind": "curated_split", "source": f"splits.json {sid}", "words": what},
                    float(r["route_measure"]), "splits.resolved.json route measure")
        for sid, (above, what) in CURATED_ABOVE_TIDAL.items():
            r = resolved.get(sid)
            if r and str(r["blk"]) == str(blk):
                add({"kind": "curated_split_offset", "source": f"splits.json {sid}",
                     "words": f"{what}: the split is {above:.0f} m above it (approximate)"},
                    max(float(r["route_measure"]) - above, a["mouth_m"]), "split measure minus the stated distance")
        if items:
            for lst in dfo_src.values():
                for s in lst:
                    if set(s["items"]) & set(items):
                        sources.append(dict(s))
        else:
            for s in dfo_src.get(key, []):
                sources.append(dict(s))
        zones = {m for i in items for m in item_of[i].get("mus") or []}
        for s in cat_src.get(key, []):
            if not zones or s["mu"] is None or s["mu"] in zones:
                sources.append({k: v for k, v in s.items() if k != "mu"})
        gauges = []
        for sid, r in resolved.items():
            if r.get("source") == "gauge" and str(r["blk"]) == str(blk):
                st = sid.split("__", 1)[1]
                if st in tidal_gauges:
                    gauges.append({"station": st, "label": r.get("label"),
                                   "measure": round(float(r["route_measure"]), 1),
                                   "below_polygon": poly_m is None or float(r["route_measure"]) <= poly_m + 1})
        if gauges:
            sources.append({"kind": "tidal_gauge", "source": "data/curated/gauges/tidal.json + HYDAT remarks",
                            "words": "; ".join(f"{g['station']} {g['label']} at m {g['measure']}" for g in gauges),
                            "gauges": gauges,
                            "agrees": all(g["below_polygon"] for g in gauges)})

        # ---- the proposal
        best_loc = None
        for pref in ("curated_split", "synopsis_box", "dfo_tidal_area", "curated_split_offset"):
            got = [s for s in located if s["kind"] == pref]
            if got:
                best_loc = got[0]
                break
        word_kinds = {s["kind"] for s in sources} - {"tidal_gauge"}
        if best_loc is not None and (poly_m is None or not best_loc.get("agrees")) \
                and best_loc["kind"] != "curated_split_offset":
            prop_m, basis = best_loc["measure"], f"the words ({best_loc['kind']}), located: {best_loc['located_by']}"
        else:
            prop_m = poly_m if poly_m is not None else a["mouth_m"]
            basis = ("the polygon's crossing" if poly_m is not None
                     else "the FWA mouth (the polygon misses this stream)")
        agree = [s for s in located if s.get("agrees")]
        disagree = [s for s in located if s.get("agrees") is False]
        gauge_bad = any(s["kind"] == "tidal_gauge" and not s["agrees"] for s in sources)
        if agree and not disagree and not gauge_bad:
            conf, why = "high", "the polygon's crossing and a located word source agree"
        elif best_loc is not None and best_loc["kind"] in ("curated_split", "synopsis_box", "dfo_tidal_area"):
            conf, why = "medium", "the words name the marker and it is located; the polygon disagrees" \
                if disagree else "the words name the marker and it is located"
        elif crossing == "misses":
            conf, why = "low", f"the polygon misses the stream (mouth {mouth_gap:.0f} m away): mouth assumed"
        elif crossing == "blobs":
            conf, why = "low", (f"the polygon crosses the line {len(a['cuts'])} times ({a['n_inside']} inside "
                                f"pieces, {a['gap_below_m']:.0f} m outside below the top crossing)")
        elif word_kinds:
            conf, why = "medium", "the words name a tidal boundary but do not locate it; the polygon's clean crossing"
        elif gauge_bad:
            conf, why = "low", "a tidal gauge sits above the polygon's crossing"
        elif crossing == "at_mouth":
            conf, why = "medium", "the polygon's edge is at the mouth (the unnamed-stream default agrees)"
        else:
            conf, why = "medium", "one clean crossing of the polygon, no words to check it against"
        if gauge_bad and conf == "high":
            conf = "medium"
        firm = [s for s in located if s["kind"] != "curated_split_offset"]
        spread = (max(s["measure"] for s in firm) - min(s["measure"] for s in firm)) if len(firm) > 1 else 0.0
        words_disagree = spread > AGREE_M
        if words_disagree:
            why += f"; the located words disagree with each other by {spread:.0f} m"
            if conf == "high":
                conf = "medium"
        prop_pt = point_at(segs, geoms, prop_m)
        prop_ll = [round(v, 6) for v in lonlat(prop_pt.x, prop_pt.y)] if prop_pt is not None else mouth_ll
        above_mouth = round(prop_m - a["mouth_m"], 1)
        rows.append({
            "id": f"tidal:{blk}", "water": name, "blk": str(blk), "wsc": re.sub(r"(-000000)+$", "", wsc),
            "gnis_id": next((int(s["gnis"]) for s in segs if s["gnis"]), None),
            "enters": enters, "items": items,
            "item_names": [item_of[i]["name"] for i in items],
            "zones": sorted({m for i in items for m in item_of[i].get("mus") or []}),
            "proposed": {"measure": round(prop_m, 1), "coord": prop_ll, "above_mouth_m": above_mouth,
                         "basis": basis, "split_label": MOUTH_LABEL,
                         "split_name": f"{name}: {MOUTH_LABEL}",
                         "aliases": ([] if above_mouth <= AT_MOUTH_M else ["tidal boundary"])
                         + ([best_loc["words"].split(": ", 1)[-1]] if best_loc else [])},
            "polygon": {"crossing": crossing, "measure": round(poly_m, 1) if poly_m is not None else None,
                        "coord": poly_ll, "cuts": a["cuts"][:12], "n_cuts": len(a["cuts"]),
                        "inside_pieces": a["n_inside"], "outside_below_m": a["gap_below_m"],
                        "mouth_gap_m": mouth_gap},
            "mouth": {"measure": round(a["mouth_m"], 1), "coord": mouth_ll},
            "sources": sources, "confidence": conf, "why": why, "words_disagree": words_disagree,
            "agreement": ("agree" if agree and not disagree else "disagree" if disagree else
                          "text unlocated" if word_kinds else "polygon only"),
        })
    rows.sort(key=lambda r: ({"high": 0, "medium": 1, "low": 2}[r["confidence"]] if r["sources"] else 3,
                             r["enters"] != "ocean", r["water"], r["blk"]))
    named_ocean = {b for b in main_blks if _line_name(by_blk.get(b, []))}
    found = {int(r["blk"]) for r in rows if r["enters"] == "ocean" and r["polygon"]["crossing"] != "misses"}
    coverage = {
        "named_ocean_entering_total": len(named_ocean),
        "named_ocean_entering_found_by_polygon": len(found & named_ocean),
        "named_ocean_entering_missed_by_polygon": len(named_ocean - found),
        "named_tidal_water_streams": sum(1 for r in rows if r["enters"] == "tidal_water"),
        "unnamed_blue_lines_crossed": len({b for b in cross_blks if not _line_name(by_blk.get(b, []))}),
        "by_confidence": dict(Counter(r["confidence"] for r in rows)),
        "by_crossing": dict(Counter(r["polygon"]["crossing"] for r in rows)),
        "by_agreement": dict(Counter(r["agreement"] for r in rows)),
        "with_word_sources": sum(1 for r in rows if any(s["kind"] != "tidal_gauge" for s in r["sources"])),
        "in_registry": sum(1 for r in rows if r["items"]),
    }
    return {"_about": (
        "T212 PROPOSALS, NOT CURATED DATA (pipeline/tools/audit/tidal_candidates.py). One row per named "
        "stream the owner's edited DFO tidal polygon (bc_fisheries_data.gpkg layer tidal_boundary) "
        "crosses, plus every named ocean-entering FWA blue line. `proposed` is where a tidal split would "
        "go (blk + FWA route measure + lon/lat on the FWA line), labelled 'mouth (tidal boundary)'; "
        "`polygon` is the polygon's own crossing; `sources` are the words that name a tidal boundary, "
        "each located on the line when it can be and said to agree (within "
        f"{AGREE_M:.0f} m) or not. The curation review's Tidal mode shows these; accepting one makes a "
        "mark — never a splits.json edit."),
        "coverage": coverage, "candidates": rows}


def summary_md(doc: dict) -> str:
    c = doc["coverage"]
    rows = doc["candidates"]
    out = ["# T212 — tidal boundaries on every ocean-entering stream (proposals)", "",
           "Generated by `python -m pipeline.tools.audit.tidal_candidates` (read only; no curated file "
           "changed). The candidates are in `tidal-candidates.json` and in the curation review's **Tidal** "
           "mode, where accepting one pre-fills a mark (the only write).", "",
           "## Coverage", "",
           f"- Named ocean-entering streams (FWA 9xx mainstems + Fraser/Skeena/Nass): "
           f"**{c['named_ocean_entering_found_by_polygon']} / {c['named_ocean_entering_total']}** met by "
           f"the polygon; {c['named_ocean_entering_missed_by_polygon']} missed (proposed at the FWA mouth, "
           "low confidence).",
           f"- Named streams entering TIDAL WATER that is not the sea (tidal river reaches, estuaries): "
           f"{c['named_tidal_water_streams']}.",
           f"- Unnamed blue lines the polygon crosses (mouth default, not listed): {c['unnamed_blue_lines_crossed']}.",
           f"- Confidence: {c['by_confidence']}.", f"- Polygon crossing: {c['by_crossing']}.",
           f"- Polygon vs words: {c['by_agreement']}; {c['with_word_sources']} streams carry a word source.",
           f"- In the atlas registry (reviewable on the map with an item): {c['in_registry']}.", "",
           "## Streams where words name the tidal boundary", "",
           "| stream | enters | proposed (m above mouth) | polygon | words | agree? | conf |",
           "|---|---|---|---|---|---|---|"]
    for r in rows:
        ws = [s for s in r["sources"] if s["kind"] != "tidal_gauge"]
        if not ws:
            continue
        words = "; ".join(f"{s['kind']}: {s['words'][:90]}" for s in ws[:2])
        out.append(f"| {r['water']} (blk {r['blk']}) | {r['enters']} | {r['proposed']['above_mouth_m']:.0f} | "
                   f"{r['polygon']['crossing']} @ {r['polygon']['measure']} | {words} | {r['agreement']} | "
                   f"{r['confidence']} |")
    out += ["", "## Where the polygon and the words disagree (owner's call)", ""]
    for r in rows:
        located = [s for s in r["sources"] if s.get("measure") is not None]
        bad = [s for s in located if s.get("agrees") is False]
        if not (bad or r.get("words_disagree") or (located and r["polygon"]["measure"] is None)):
            continue
        poly = (f"polygon leaves it at {r['polygon']['measure']:.0f} m" if r["polygon"]["measure"] is not None
                else f"polygon misses it (mouth {r['polygon']['mouth_gap_m']:.0f} m away)")
        said = "; ".join(f"{s['kind']} \"{s['words'][:80]}\" at {s['measure']:.0f} m ({s.get('located_by')})"
                         for s in located)
        out.append(f"- **{r['water']}** (blk {r['blk']}): {poly}; {said}. Proposed: "
                   f"{r['proposed']['measure']:.0f} m from {r['proposed']['basis']}.")
    out += ["", "## Sources read", "",
            f"- {REGION2_SRC}: {len(REGION2_BOX)} Lower Mainland streams, each with its marker.",
            "- DFO tidal-area pages (pac.dfo-mpo.gc.ca/fm-gp/rec/tidal-maree/a-s<N>-eng.html, read 2026-10-27): "
            "Area 29's \"Tidal water boundaries in streams and rivers\" list, and the area descriptions that "
            "bound tidal water on a stream (Areas 2, 12, 13, 17, 23, 27).",
            "- DFO salmon notices (data/curated/regulations/entries/dfo_salmon) and synopsis rows (the "
            "catalogue's regs_verbatim) that say \"tidal boundary\" / \"tidal waters\".",
            "- Curated splits that are a tidal limit (" + ", ".join(CURATED_TIDAL_SPLITS) + ").",
            "- Tidal level gauges: data/curated/gauges/tidal.json + HYDAT's own remarks (08MH024, 08MH126).",
            "- Markers located on OSM roads / rail from the app basemap (data/source/bc.pmtiles, z14; no operator "
            "tags, so a rail marker takes the rail crossing nearest the polygon's).", ""]
    out += ["", "## Low confidence (the polygon blobs or misses)", ""]
    for r in rows:
        if r["confidence"] == "low":
            out.append(f"- {r['water']} (blk {r['blk']}, {r['enters']}): {r['why']}")
    out += ["", "## How a row is decided", "",
            f"- The polygon's candidate is the upstream-most point where the FWA line leaves the polygon "
            f"(within {AT_MOUTH_M:.0f} m of the mouth = the mouth).",
            "- A word source that names a marker is located on the line (a curated split's measure, or an "
            "OSM crossing of the named bridge / rail line). Located and within "
            f"{AGREE_M:.0f} m of the polygon: agree (high). Located and further: the WORDS are proposed and "
            "the disagreement is listed (medium).",
            "- Words that say \"tidal boundary\" without placing it, and streams with no words, take the "
            "polygon's clean crossing (medium); a crossing in several pieces or a polygon that misses the "
            "stream is low.",
            "- A tidal level gauge (data/curated/gauges/tidal.json, HYDAT remarks) above the polygon's "
            "crossing is a disagreement.",
            "- Every proposal is labelled `mouth (tidal boundary)`; one above the FWA mouth also carries "
            "the alias `tidal boundary` and the marker's words.", ""]
    return "\n".join(out)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--gpkg", type=Path, default=ROOT / "data/source/bc_fisheries_data.gpkg")
    ap.add_argument("--atlas", type=Path, default=ROOT / "data/generated/atlas/full")
    ap.add_argument("--osm", type=Path, action="append", default=[], help="Overpass JSON (out geom tags)")
    ap.add_argument("--basemap", type=Path, default=None, help="bc.pmtiles: OSM roads + rail from the app basemap")
    ap.add_argument("--out", type=Path, default=ROOT / "handoff/PLAN/curation-review/tidal")
    a = ap.parse_args(argv)
    doc = build(a.gpkg, a.atlas, a.osm, a.basemap)
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "tidal-candidates.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    (a.out / "tidal-candidates.md").write_text(summary_md(doc))
    print(json.dumps(doc["coverage"], indent=1))


if __name__ == "__main__":
    main()
