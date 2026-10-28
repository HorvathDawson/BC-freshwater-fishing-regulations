"""The basemap's fills on a coarser coordinate grid (T202, owner 2026-10-27: "ship the bigger grid coarsening").

A Protomaps tile is 4096 units across. Our basemap's fills and coastlines do not need that: the app draws them
under its own water, and the camera overzooms the z13 tile to z18. So the last step of the build re-encodes three
layers onto a coarser grid. `extent` is divided by 2**shift, each vertex is rounded to the new grid, repeated
vertices are dropped, and so are the rings and lines that collapse.

    landuse  shift 2 -> 1024 units: ≤1.6 m at z13 at 49°N, 8 px at z18
    water    shift 1 -> 2048: ≤0.8 m (polygons, river lines, sea labels)
    earth    shift 1 -> 2048: ≤0.8 m (the coastline)

Roads, places and boundaries are copied byte for byte: roads keep 4096, so a trail is never stair-stepped.

Measured 2026-10-27 (handoff/PLAN/tiles/T202-basemap-audit.md):
- About −15% of the archive.
- Shots at z10/12/14/16, light and dark, at Vancouver, Squamish backcountry, Chilcotin and Lac Le Jeune: at most
  0.19% of pixels differ (one-pixel shifts on river and coast edges). Nothing visible by eye.

To undo: `GRID_SHIFT = {}` (or remove a layer) and rebuild.

Encoding notes (MVT 2.1):
- A polygon ring keeps its winding.
- A hole whose outer ring collapsed goes with it, so no orphan hole is ever drawn as land.
- A ring whose winding flips or whose area goes to zero after rounding is dropped.
- A feature with no geometry left is dropped.
Every other field of a feature (id, tags, type) and every other field of a layer (name, keys, values, version) is
copied unchanged.
"""

from __future__ import annotations

import gzip
import json
import sqlite3
import subprocess
import zlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

GRID_SHIFT = {"landuse": 2, "water": 1, "earth": 1}


# --- protobuf -------------------------------------------------------------------------------------------------------

def _varint(b: bytes, i: int) -> tuple[int, int]:
    r = s = 0
    while True:
        c = b[i]
        i += 1
        r |= (c & 0x7F) << s
        if c < 0x80:
            return r, i
        s += 7


def _enc(v: int) -> bytes:
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _fields(b: bytes):
    """(field number, wire type, value, the field's raw bytes)."""
    i, n = 0, len(b)
    while i < n:
        s = i
        key, i = _varint(b, i)
        f, w = key >> 3, key & 7
        if w == 2:
            ln, i = _varint(b, i)
            v = b[i:i + ln]
            i += ln
        elif w == 0:
            v, i = _varint(b, i)
        elif w == 5:
            v = b[i:i + 4]
            i += 4
        elif w == 1:
            v = b[i:i + 8]
            i += 8
        else:
            raise ValueError(f"unsupported wire type {w}")
        yield f, w, v, b[s:i]


def _bytes_field(no: int, payload: bytes) -> bytes:
    return _enc((no << 3) | 2) + _enc(len(payload)) + payload


def _uint_field(no: int, v: int) -> bytes:
    return _enc(no << 3) + _enc(v)


def _packed(b: bytes) -> list[int]:
    out, i = [], 0
    while i < len(b):
        v, i = _varint(b, i)
        out.append(v)
    return out


def _unzz(v: int) -> int:
    return (v >> 1) ^ -(v & 1)


def _zz(v: int) -> int:
    return (v << 1) ^ (v >> 63)


# --- geometry -------------------------------------------------------------------------------------------------------

def _parts(cmds: list[int]) -> list[list[tuple[int, int]]]:
    """Absolute-coordinate parts: each MoveTo starts one (a point, a line or a ring); ClosePath is implied by type."""
    x = y = 0
    i = 0
    parts: list[list[tuple[int, int]]] = []
    while i < len(cmds):
        cid, cnt = cmds[i] & 7, cmds[i] >> 3
        i += 1
        if cid == 7:
            continue
        for _ in range(cnt):
            x += _unzz(cmds[i])
            y += _unzz(cmds[i + 1])
            i += 2
            if cid == 1:
                parts.append([(x, y)])
            else:
                parts[-1].append((x, y))
    return parts


def _area2(ring: list[tuple[int, int]]) -> int:
    """Twice the signed area (the MVT spec's surveyor's formula; positive = exterior)."""
    return sum(x1 * y2 - x2 * y1 for (x1, y1), (x2, y2) in zip(ring, ring[1:] + ring[:1]))


def quantize(cmds: list[int], geom_type: int, shift: int) -> list[int]:
    """`cmds` (an MVT geometry command list) on a grid 2**shift coarser. [] when nothing is left."""
    half = 1 << (shift - 1)
    q = lambda p: ((p[0] + half) >> shift, (p[1] + half) >> shift)  # noqa: E731 — round half up, negatives too
    out: list[list[tuple[int, int]]] = []
    if geom_type == 1:
        out = [[q(p) for p in part] for part in _parts(cmds)]
    elif geom_type == 2:
        for part in _parts(cmds):
            line: list[tuple[int, int]] = []
            for p in map(q, part):
                if not line or line[-1] != p:
                    line.append(p)
            if len(line) >= 2:
                out.append(line)
    elif geom_type == 3:
        keep_holes = False
        for ring in _parts(cmds):
            a0 = _area2(ring)
            r: list[tuple[int, int]] = []
            for p in map(q, ring):
                if not r or r[-1] != p:
                    r.append(p)
            while len(r) > 1 and r[-1] == r[0]:
                r.pop()
            a1 = _area2(r) if len(r) >= 3 else 0
            ok = a1 != 0 and (a1 > 0) == (a0 > 0)
            if a0 > 0:                                  # an exterior ring: its holes follow it
                keep_holes = ok
                if ok:
                    out.append(r)
            elif ok and keep_holes:
                out.append(r)
    else:
        return cmds
    enc: list[int] = []
    cx = cy = 0
    for part in out:
        if geom_type == 1:
            enc.append(1 | (len(part) << 3))
            for x, y in part:
                enc += [_zz(x - cx), _zz(y - cy)]
                cx, cy = x, y
            continue
        (x, y), rest = part[0], part[1:]
        enc += [1 | (1 << 3), _zz(x - cx), _zz(y - cy)]
        cx, cy = x, y
        enc.append(2 | (len(rest) << 3))
        for x, y in rest:
            enc += [_zz(x - cx), _zz(y - cy)]
            cx, cy = x, y
        if geom_type == 3:
            enc.append(7 | (1 << 3))
    return enc


# --- tiles ----------------------------------------------------------------------------------------------------------

def _feature(fb: bytes, shift: int) -> bytes | None:
    geom_type, cmds, other = 0, [], []
    for f, _w, v, raw in _fields(fb):
        if f == 3:
            geom_type = v
            other.append(raw)
        elif f == 4:
            cmds = _packed(v)
        else:
            other.append(raw)
    q = quantize(cmds, geom_type, shift)
    if not q:
        return None
    return b"".join(other) + _bytes_field(4, b"".join(_enc(c) for c in q))


def _layer(lb: bytes, shifts: dict) -> bytes:
    name = next((v.decode() for f, _w, v, _r in _fields(lb) if f == 1), "")
    shift = shifts.get(name, 0)
    if not shift:
        return lb
    extent = next((v for f, _w, v, _r in _fields(lb) if f == 5), 4096)
    out = []
    for f, _w, v, raw in _fields(lb):
        if f == 2:
            nf = _feature(v, shift)
            if nf is not None:
                out.append(_bytes_field(2, nf))
        elif f == 5:
            continue
        else:
            out.append(raw)
    out.append(_uint_field(5, extent >> shift))
    return b"".join(out)


def requantize_tile(raw: bytes, shifts: dict = GRID_SHIFT) -> bytes:
    """One MVT tile (uncompressed) with the layers in `shifts` on their coarser grid; the rest copied as they are."""
    return b"".join(_bytes_field(3, _layer(v, shifts)) if f == 3 else r for f, _w, v, r in _fields(raw))


def _work(blob: bytes) -> bytes:
    raw = gzip.decompress(blob) if blob[:2] == b"\x1f\x8b" else blob
    c = zlib.compressobj(6, zlib.DEFLATED, 31)
    return c.compress(requantize_tile(raw)) + c.flush()


def requantize_archive(src: Path, out: Path, tmp: Path, workers: int = 4,
                       name: str = "Protomaps Basemap (British Columbia)") -> None:
    """`src` -> `out` with every tile re-encoded (`requantize_tile`), gzip-6, through an MBTiles and `pmtiles convert`.
    The metadata is carried over, with a clean name and description (tile-join leaves temp paths in them)."""
    from pipeline.deliver.tiles.basemap_roads import all_tiles, header_and_metadata
    hdr, meta = header_and_metadata(src)
    mb = tmp / "grid.mbtiles"
    mb.unlink(missing_ok=True)
    db = sqlite3.connect(mb)
    db.execute("CREATE TABLE metadata (name TEXT, value TEXT)")
    db.execute("CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)")
    rows = {"name": name, "format": "pbf", "type": "baselayer",
            "description": "OpenStreetMap via Protomaps, clipped to BC and trimmed to what the app draws (T85, T202)",
            "attribution": meta.get("attribution", ""), "version": "2",
            "minzoom": str(hdr["minzoom"]), "maxzoom": str(hdr["maxzoom"]),
            "bounds": ",".join(map(str, hdr["bounds"])), "center": ",".join(map(str, hdr["center"])),
            "json": json.dumps({k: meta[k] for k in ("vector_layers", "tilestats") if k in meta})}
    db.executemany("INSERT INTO metadata VALUES (?, ?)", rows.items())
    batch: list[tuple] = []

    def flush(pool):
        for (z, x, y), data in zip([b[0] for b in batch], pool.map(_work, [b[1] for b in batch], chunksize=64)):
            db.execute("INSERT INTO tiles VALUES (?, ?, ?, ?)", (z, x, (1 << z) - 1 - y, data))
        batch.clear()

    with ProcessPoolExecutor(max_workers=workers) as pool:
        for z, x, y, blob in all_tiles(src):
            batch.append(((z, x, y), blob))
            if len(batch) >= 4096:
                flush(pool)
        flush(pool)
    db.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row)")
    db.commit()
    db.close()
    r = subprocess.run(["pmtiles", "convert", str(mb), str(out)], capture_output=True, text=True)
    mb.unlink(missing_ok=True)
    if r.returncode != 0:
        raise RuntimeError(f"pmtiles convert failed ({r.returncode})\n{r.stderr[-3000:]}")
