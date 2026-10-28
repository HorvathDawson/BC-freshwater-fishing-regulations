"""OSM NAMED WATER from the app's basemap (`data/source/bc.pmtiles`, Protomaps, OSM 2026-03-05), read only.

Every z13 tile over BC: the `water` layer's NAMED lines (river / stream / canal / drain / ditch) clipped to the tile,
and named water POINTS / polygons (lake labels), written as JSON lines {tile, name, kind, geom: [[lon, lat], ...] |
point}. One worker per process (<= 4). Run through scripts/heavy.sh.

    python osm_extract.py OUT.jsonl [--workers 4]
"""
from __future__ import annotations

import gzip
import json
import math
import struct
import sys
from functools import lru_cache
from multiprocessing import Pool
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
PM = ROOT / "data/source/bc.pmtiles"
Z = 13
LINE_KINDS = {"river", "stream", "canal", "drain", "ditch"}


class PM3:
    def __init__(self, path):
        self.f = open(path, "rb")
        h = self.f.read(127)
        (self.root_off, self.root_len, _mo, _ml, self.leaf_off, _ll, self.data_off, _dl) = struct.unpack("<8Q", h[8:72])
        self.ic, self.tc = h[97], h[98]

    def _read(self, off, ln, comp):
        self.f.seek(off)
        b = self.f.read(ln)
        return gzip.decompress(b) if comp == 2 else b

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

    @lru_cache(maxsize=4096)
    def _dir(self, off, ln):
        raw = self._read(off, ln, self.ic)
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

    @staticmethod
    def tile_id(z, x, y):
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

    def tile(self, z, x, y):
        tid = self.tile_id(z, x, y)
        off, ln = self.root_off, self.root_len
        for _ in range(4):
            ents = self._dir(off, ln)
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


_pm = None


def _px2ll(x, y, n, px, py):
    lon = (x + px / 4096) / n * 360 - 180
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1 - py / 4096) / n))))
    return [round(lon, 6), round(lat, 6)]


def work(xy):
    global _pm
    import mapbox_vector_tile as mvt
    from shapely.geometry import box, shape
    if _pm is None:
        _pm = PM3(PM)
    x, y = xy
    raw = _pm.tile(Z, x, y)
    if not raw:
        return []
    n = 2 ** Z
    out = []
    clip = box(0, 0, 4096, 4096)
    for f in mvt.decode(raw).get("water", {}).get("features", []):
        pr = f["properties"]
        name = pr.get("name") or pr.get("name:en")
        if not name:
            continue
        kind = pr.get("kind")
        g = shape(f["geometry"])
        if g.geom_type in ("LineString", "MultiLineString"):
            g = g.intersection(clip)
            parts = [g] if g.geom_type == "LineString" else list(getattr(g, "geoms", []))
            lines = [[_px2ll(x, y, n, px, py) for px, py in p.coords] for p in parts
                     if p.geom_type == "LineString" and len(p.coords) >= 2]
            if lines:
                out.append({"t": f"{x}/{y}", "name": name, "kind": kind, "lines": lines})
        elif g.geom_type == "Point":
            out.append({"t": f"{x}/{y}", "name": name, "kind": kind, "point": _px2ll(x, y, n, g.x, g.y)})
        else:   # a named water polygon: keep its label point (inside the tile)
            p = g.intersection(clip).representative_point() if not g.intersection(clip).is_empty else None
            if p is not None:
                out.append({"t": f"{x}/{y}", "name": name, "kind": kind, "poly_pt": _px2ll(x, y, n, p.x, p.y),
                            "area_px": round(g.intersection(clip).area)})
    return out


def tiles_over_bc():
    from shapely.geometry import shape, box
    from shapely.prepared import prep
    bc = json.loads((ROOT / "data/source/bc_boundary.geojson").read_text())
    import numpy as np
    import shapely
    from pyproj import Transformer
    tr = Transformer.from_crs(3005, 4326, always_xy=True)   # the boundary file is BC Albers
    g = shape(bc["features"][0]["geometry"]).simplify(500)
    geom = shapely.transform(g, lambda c: np.column_stack(tr.transform(c[:, 0], c[:, 1]))).buffer(0.05)
    pg = prep(geom)
    n = 2 ** Z
    minx, miny, maxx, maxy = geom.bounds
    tx = lambda lon: int((lon + 180) / 360 * n)
    ty = lambda lat: int((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n)
    out = []
    for x in range(tx(minx), tx(maxx) + 1):
        for y in range(ty(maxy), ty(miny) + 1):
            lo0 = x / n * 360 - 180
            lo1 = (x + 1) / n * 360 - 180
            la1 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
            la0 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
            if pg.intersects(box(lo0, la0, lo1, la1)):
                out.append((x, y))
    return out


if __name__ == "__main__":
    out = Path(sys.argv[1])
    workers = int(sys.argv[sys.argv.index("--workers") + 1]) if "--workers" in sys.argv else 4
    tiles = tiles_over_bc()
    print(f"{len(tiles)} z{Z} tiles over BC", flush=True)
    k = 0
    with Pool(min(workers, 4)) as pool, out.open("w") as fh:
        for i, rows in enumerate(pool.imap_unordered(work, tiles, chunksize=64)):
            for r in rows:
                fh.write(json.dumps(r) + "\n")
                k += 1
            if i % 20000 == 0:
                print(f"  {i}/{len(tiles)} tiles, {k} rows", flush=True)
    print(f"done: {k} rows", flush=True)
