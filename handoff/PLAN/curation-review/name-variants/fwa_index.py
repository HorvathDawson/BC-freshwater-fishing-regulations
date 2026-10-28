"""Read-only FWA + registry lookups for the name-variant miner (T224). Nothing is written but a cache.

    blk_table()        BLUE_LINE_KEY -> (gnis_name, total length m, a watershed code)   one scan, cached
    lines_near(geom)   FWA stream fids whose bbox meets `geom`'s bbox (rtree), as (blk, gnis, LineString)
    polys_near(geom)   FWA lakes / wetlands / manmade polygons near `geom`, as (layer, wbk, names, Polygon)
    registry()         blk -> names, wbk -> names, gnis -> names from the built registry (name + variants)
"""
from __future__ import annotations

import json
import os
import pickle
import sqlite3
import struct
from collections import defaultdict
from functools import lru_cache
from pathlib import Path

import shapely
from pyproj import Transformer

ROOT = Path(__file__).resolve().parents[4]
GPKG = ROOT / "data/source/bc_fisheries_data.gpkg"
REGISTRY = ROOT / "data/generated/atlas/full/registry.json"
CACHE = Path(os.environ.get("NV_CACHE", "/tmp/nv-cache"))

TO_ALBERS = Transformer.from_crs(4326, 3005, always_xy=True)
TO_LONLAT = Transformer.from_crs(3005, 4326, always_xy=True)


def to_albers(g):
    return shapely.transform(g, lambda c: __import__("numpy").column_stack(TO_ALBERS.transform(c[:, 0], c[:, 1])))


def lonlat(x, y):
    lon, lat = TO_LONLAT.transform(x, y)
    return round(lon, 5), round(lat, 5)


@lru_cache(maxsize=1)
def _db():
    c = sqlite3.connect(f"file:{GPKG}?mode=ro", uri=True, check_same_thread=False)
    return c


def gpkg_geom(blob: bytes):
    """GeoPackage binary -> shapely: 8-byte header + envelope (size from the flags), then WKB."""
    flags = blob[3]
    env = (0, 32, 48, 48, 64)[(flags >> 1) & 7]
    return shapely.from_wkb(blob[8 + env:])


def blk_table() -> dict:
    CACHE.mkdir(parents=True, exist_ok=True)
    p = CACHE / "blk_table.pkl"
    if p.exists():
        return pickle.loads(p.read_bytes())
    out = {}
    for blk, gnis, ln, wsc in _db().execute(
            "SELECT BLUE_LINE_KEY, max(GNIS_NAME), sum(LENGTH_METRE), min(FWA_WATERSHED_CODE) "
            "FROM streams GROUP BY BLUE_LINE_KEY"):
        out[str(blk)] = (gnis, ln or 0.0, wsc)
    p.write_bytes(pickle.dumps(out))
    return out


def lines_near(g, pad: float = 60.0):
    minx, miny, maxx, maxy = g.bounds
    rows = _db().execute(
        "SELECT s.BLUE_LINE_KEY, s.GNIS_NAME, s.geom, s.EDGE_TYPE FROM rtree_streams_geom r JOIN streams s ON s.fid = r.id "
        "WHERE r.maxx >= ? AND r.minx <= ? AND r.maxy >= ? AND r.miny <= ?",
        (minx - pad, maxx + pad, miny - pad, maxy + pad)).fetchall()
    return [(str(b), n, gpkg_geom(geom), et) for b, n, geom, et in rows]


def polys_near(g, pad: float = 30.0, layers=("lakes", "wetlands", "manmade")):
    minx, miny, maxx, maxy = g.bounds
    out = []
    for lyr in layers:
        for wbk, n1, n2, n3, area, geom in _db().execute(
                f"SELECT s.WATERBODY_KEY, s.GNIS_NAME_1, s.GNIS_NAME_2, s.GNIS_NAME_3, s.AREA_HA, s.geom "
                f"FROM rtree_{lyr}_geom r JOIN {lyr} s ON s.fid = r.id "
                "WHERE r.maxx >= ? AND r.minx <= ? AND r.maxy >= ? AND r.miny <= ?",
                (minx - pad, maxx + pad, miny - pad, maxy + pad)):
            out.append((lyr, str(wbk), [n for n in (n1, n2, n3) if n], area, gpkg_geom(geom)))
    return out


@lru_cache(maxsize=1)
def registry():
    """blk / wbk / gnis -> [(item_id, name, variants)] from the built registry."""
    items = json.loads(REGISTRY.read_text())["items"]
    by_blk, by_wbk, by_gnis = defaultdict(list), defaultdict(list), defaultdict(list)
    for it in items:
        if it["kind"] == "area":
            continue
        rec = (it["id"], it["name"], tuple(it.get("variants") or ()))
        blks, wbks = set(), set()
        for s in it.get("section_ids") or ():
            a, _, b = s.partition(":")
            (wbks.add(b) if a == "lake" else blks.add(a))
        for r in it.get("ref_ids") or ():
            a, _, b = r.partition(":")
            if a == "blk":
                blks.add(b)
            elif a == "wbk":
                wbks.add(b)
            elif a == "gnis":
                by_gnis[b].append(rec)
        for b in blks:
            by_blk[b].append(rec)
        for w in wbks:
            by_wbk[w].append(rec)
    return dict(by_blk), dict(by_wbk), dict(by_gnis)
