"""The basemap's z13 `roads` layer, rebuilt from the z14 roads so it keeps every name (T202, owner 2026-10-27).

WHY. The basemap stops at z13 and the camera overzooms that tile to z18. Protomaps' own z13 roads cannot fill that
role. They have every line (same length as z14 for every rural kind, measured on four extents), but planetiler
strips minor-road, path and track NAMES below z14 and merges the unnamed lines. A plain z13 cap would therefore drop
every forest service road, track and trail name: the Ashlu River FSR, A100, Ridge Mountain FSR, Four Lakes Trail.
Those names are how an angler finds the water.

HOW. The z14 `roads` tiles are read from the clipped source archive. Each z14 tile's lines are clipped to that tile,
because its buffer would otherwise duplicate the neighbour's edge. Within each z11 block of z14 tiles, the lines are
merged per identical attribute set and line-merged, written as GeoJSON lines, and tiled at z13 alone by tippecanoe.
The build (`basemap._trim`) tile-joins the result in place of Protomaps' z13 roads. z≤12 roads stay Protomaps' own.

WHAT IS LEFT OUT (all urban; the owner accepted the loss on 2026-10-27):
    URBAN_ONLY  the z14-only kinds: sidewalks, crossings, corridors, subway, living streets, platforms, rest-area
                lanes, miniature rail.
    `oneway`    drawn only as z16+ arrows.
    Never read by the style: `sort_rank`, `ref` (shields read `shield_text`), and `min_zoom` on an unnamed line
    (it is only the label sort key).

Cost: the rebuilt z13 roads are +0–4% over Protomaps' in the country and +34% in Vancouver, where every street keeps
its name. That is about +3 MB province-wide, against about −270 MB for dropping z14.
"""

from __future__ import annotations

import gzip
import json
import math
import struct
from collections import defaultdict
from pathlib import Path

URBAN_ONLY = ("sidewalk", "crossing", "corridor", "living_street", "platform", "services", "rest_area", "subway",
              "miniature")
UNREAD = ("sort_rank", "ref", "oneway")
# Lines are merged within blocks of 4**MERGE_UP z14 tiles (3: one z11 tile, 64 z14 tiles; consecutive in Hilbert
# order, so the block is streamed and memory stays flat). Measured on the rebuilt z13 roads against Protomaps': per z13
# parent (1) +7.6% / +7.1% (Squamish backcountry / Chilcotin), per z12 (2) +5.4% / +7.1%, per z11 (3) +4.0% / +4.7%.
MERGE_UP = 3


# --- a minimal PMTiles v3 reader: the tiles of one zoom, in tile-id (Hilbert) order -------------------------------

def _varint(b: bytes, i: int) -> tuple[int, int]:
    r = s = 0
    while True:
        c = b[i]
        i += 1
        r |= (c & 0x7F) << s
        if c < 0x80:
            return r, i
        s += 7


def _decomp(b: bytes, kind: int) -> bytes:
    if kind == 2:
        return gzip.decompress(b)
    if kind == 1:
        return b
    raise ValueError(f"pmtiles compression {kind} is not gzip or none")


def _directory(b: bytes) -> list[tuple[int, int, int, int]]:
    n, i = _varint(b, 0)
    ids, rl, ln, off = [], [], [], []
    last = 0
    for _ in range(n):
        d, i = _varint(b, i)
        last += d
        ids.append(last)
    for col in (rl, ln):
        for _ in range(n):
            v, i = _varint(b, i)
            col.append(v)
    for k in range(n):
        v, i = _varint(b, i)
        off.append(off[k - 1] + ln[k - 1] if (v == 0 and k > 0) else v - 1)
    return list(zip(ids, rl, ln, off))


def _first_id(z: int) -> int:
    return sum(4 ** k for k in range(z))


def _hilbert_xy(z: int, pos: int) -> tuple[int, int]:
    n = 1 << z
    x = y = 0
    s, t = 1, pos
    while s < n:
        rx = 1 & (t // 2)
        ry = 1 & (t ^ rx)
        if ry == 0:
            if rx == 1:
                x, y = s - 1 - x, s - 1 - y
            x, y = y, x
        x += s * rx
        y += s * ry
        t //= 4
        s *= 2
    return x, y


def tiles_at(archive: Path, z: int):
    """Yield (hilbert position, x, y, raw MVT bytes) for every tile of `archive` at zoom `z`, in Hilbert order (the
    four children of one parent tile are consecutive)."""
    lo, hi = _first_id(z), _first_id(z + 1)
    with open(archive, "rb") as f:
        h = f.read(127)
        if h[:7] != b"PMTiles" or h[7] != 3:
            raise ValueError(f"{archive} is not a PMTiles v3 archive")
        root_o, root_l, _mo, _ml, leaf_o, _ll, data_o = struct.unpack("<7Q", h[8:64])
        icomp, tcomp = h[97], h[98]

        def walk(entries):
            for tid, rl, ln, off in entries:
                if rl == 0:
                    f.seek(leaf_o + off)
                    yield from walk(_directory(_decomp(f.read(ln), icomp)))
                    continue
                if tid + rl <= lo or tid >= hi:
                    continue
                f.seek(data_o + off)
                raw = _decomp(f.read(ln), tcomp)
                for t in range(max(tid, lo), min(tid + rl, hi)):
                    pos = t - lo
                    yield (pos, *_hilbert_xy(z, pos), raw)

        f.seek(root_o)
        yield from walk(_directory(_decomp(f.read(root_l), icomp)))


# --- z14 roads -> merged GeoJSON lines per z13 parent ----------------------------------------------------------------

def props_of(p: dict) -> dict | None:
    """The attributes a rebuilt road keeps, or None for a road left out (URBAN_ONLY)."""
    if p.get("kind_detail") in URBAN_ONLY:
        return None
    q = {k: v for k, v in p.items() if k not in UNREAD}
    if not q.get("name"):
        q.pop("min_zoom", None)
    return q


def _to_lonlat(z: int, x: int, y: int, extent: int):
    """Tile units (y down) -> lon/lat, vectorised for `shapely.transform`."""
    import numpy as np
    n = 1 << z

    def f(c):
        lon = (x + c[:, 0] / extent) / n * 360.0 - 180.0
        lat = np.degrees(np.arctan(np.sinh(np.pi * (1 - 2 * (y + c[:, 1] / extent) / n))))
        return np.round(np.column_stack([lon, lat]), 7)
    return f


def _flush(groups: dict, out) -> int:
    from shapely.geometry import mapping
    from shapely.ops import linemerge, unary_union
    n = 0
    for key, geoms in groups.items():
        u = unary_union(geoms)
        if u.is_empty:
            continue
        m = linemerge(u) if u.geom_type == "MultiLineString" else u
        out.write(json.dumps({"type": "Feature", "properties": json.loads(key), "geometry": mapping(m)},
                             separators=(",", ":")) + "\n")
        n += 1
    groups.clear()
    return n


def roads_geojsonl(archive: Path, out: Path, zoom: int = 14) -> int:
    """Write the `roads` of `archive`'s z`zoom` tiles as GeoJSON lines, merged per attribute set within
    blocks of 4**MERGE_UP tiles (see the module docstring). Returns the number of features written."""
    import mapbox_vector_tile
    import shapely
    from shapely.geometry import box, shape

    written = 0
    parent = None
    groups: dict[str, list] = defaultdict(list)
    with open(out, "w", encoding="utf-8") as o:
        for pos, x, y, raw in tiles_at(archive, zoom):
            if pos >> (2 * MERGE_UP) != parent:
                written += _flush(groups, o)
                parent = pos >> (2 * MERGE_UP)
            layer = mapbox_vector_tile.decode(raw, default_options={"y_coord_down": True}).get("roads")
            if not layer:
                continue
            extent = layer.get("extent", 4096)
            clip = box(0, 0, extent, extent)
            to_ll = _to_lonlat(zoom, x, y, extent)
            for f in layer["features"]:
                p = props_of(f.get("properties") or {})
                if p is None or "Line" not in f["geometry"]["type"]:
                    continue
                # only the part inside its own tile: the buffer is the neighbour's, and would be counted twice
                g = shape(f["geometry"]).intersection(clip)
                lines = [p_ for p_ in getattr(g, "geoms", [g]) if "Line" in p_.geom_type and not p_.is_empty]
                if lines:
                    groups[json.dumps(p, sort_keys=True)].extend(shapely.transform(ln, to_ll) for ln in lines)
        written += _flush(groups, o)
    return written

