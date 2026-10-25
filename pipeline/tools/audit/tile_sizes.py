"""Bytes per zoom and per layer in a PMTiles v3 archive of gzip MVT tiles — read-only, one process, low memory.

    python -m pipeline.tools.audit.tile_sizes data/generated/tiles/atlas.pmtiles
    python -m pipeline.tools.audit.tile_sizes data/generated/tiles/basemap.pmtiles --json /tmp/basemap.json

The S2 size table's numbers (handoff/PLAN/app-data/S2-tile-size-easy-wins.md, T64/T85): each UNIQUE tile content is
counted once (the archive's own dedupe of repeated tiles), and its compressed size is shared among its layers in
proportion to their uncompressed bytes. ~1 minute for the 548 MB atlas.
"""
from __future__ import annotations

import argparse
import collections
import gzip
import json
import struct
import sys
from pathlib import Path


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
    raise SystemExit(f"tile_sizes: compression {kind} is not gzip or none")


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


def _zoom(tid: int) -> int:
    z = acc = 0
    while tid >= acc + 4 ** z:
        acc += 4 ** z
        z += 1
    return z


def _skip(b: bytes, i: int, wire: int) -> int:
    if wire == 2:
        n, i = _varint(b, i)
        return i + n
    if wire == 0:
        return _varint(b, i)[1]
    return i + (4 if wire == 5 else 8)


def _layers(tile: bytes) -> dict[str, int]:
    """{layer name: its bytes in the tile} (MVT: a tile is repeated field 3 = Layer, whose field 1 is its name)."""
    out: dict[str, int] = {}
    i = 0
    while i < len(tile):
        key, i = _varint(tile, i)
        if key >> 3 == 3 and key & 7 == 2:
            n, i = _varint(tile, i)
            lb, j, name = tile[i:i + n], 0, "?"
            while j < len(lb):
                k2, j = _varint(lb, j)
                if k2 >> 3 == 1 and k2 & 7 == 2:
                    m, j = _varint(lb, j)
                    name = lb[j:j + m].decode()
                    break
                j = _skip(lb, j, k2 & 7)
            out[name] = out.get(name, 0) + n
            i += n
        else:
            i = _skip(tile, i, key & 7)
    return out


def scan(path: Path) -> dict:
    with open(path, "rb") as f:
        h = f.read(127)
        if h[:7] != b"PMTiles" or h[7] != 3:
            raise SystemExit(f"tile_sizes: {path} is not a PMTiles v3 archive")
        root_o, root_l, _mo, _ml, leaf_o, _ll, data_o = struct.unpack("<7Q", h[8:64])
        icomp, tcomp = h[97], h[98]
        tiles: list[tuple[int, int, int]] = []

        def walk(entries):
            for tid, rl, ln, off in entries:
                if rl == 0:
                    f.seek(leaf_o + off)
                    walk(_directory(_decomp(f.read(ln), icomp)))
                else:
                    tiles.append((_zoom(tid), off, ln))
        f.seek(root_o)
        walk(_directory(_decomp(f.read(root_l), icomp)))
        seen: set[int] = set()
        by: dict[int, collections.Counter] = collections.defaultdict(collections.Counter)
        size, count = collections.Counter(), collections.Counter()
        for z, off, ln in sorted(tiles, key=lambda t: t[1]):
            if off in seen:
                continue
            seen.add(off)
            f.seek(data_o + off)
            ls = _layers(_decomp(f.read(ln), tcomp))
            tot = sum(ls.values()) or 1
            for name, b in ls.items():
                by[z][name] += ln * b / tot
            size[z] += ln
            count[z] += 1
        return {"file_bytes": f.seek(0, 2), "tile_bytes": sum(size.values()),
                "zoom": {z: {"bytes": size[z], "tiles": count[z],
                             "layers": {k: round(v) for k, v in by[z].most_common()}} for z in sorted(size)}}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("archive", type=Path)
    ap.add_argument("--json", type=Path, default=None, help="also write the full table here")
    a = ap.parse_args(argv)
    res = scan(a.archive)
    if a.json:
        a.json.write_text(json.dumps(res, indent=1) + "\n")
    print(f"{a.archive}: {res['file_bytes'] / 1e6:.1f} MB file, {res['tile_bytes'] / 1e6:.1f} MB of tiles")
    lay: collections.Counter = collections.Counter()
    for z, v in res["zoom"].items():
        print(f"  z{z:<2} {v['bytes'] / 1e6:8.1f} MB {v['tiles']:>9,} tiles")
        lay.update(v["layers"])
    for k, v in lay.most_common():
        print(f"  {k:<20} {v / 1e6:8.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
