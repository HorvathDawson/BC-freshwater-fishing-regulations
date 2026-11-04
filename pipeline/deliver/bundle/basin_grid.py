"""(T376) THE WATERSHED GROUPS BY NAME AND BY PLACE: which group a point is in, and what the group is called.

The Conditions map colours a group by `basin_id` (the FWA WATERSHED_GROUP_CODE, `pipeline/deliver/tiles/basins.py`)
and `basin_member` says which stations speak for it. Neither says what the group is CALLED, nor which group a point on
the map is in — and the app needs both: a forecast-day list for the group the map is centred on, or the group a water
the reader opened lies in (T377). Three bundle tables, copied whole into the release's `conditions` file:

  basin        (ix INTEGER PRIMARY KEY, basin_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL)
               every group of the layer (246), ix 1.. in basin_id order. `name` is WATERSHED_GROUP_NAME AS SOURCED
               ("Chilliwack River"), never reworded — a word around it is the app's (core words), not data.
  basin_frame  (w, s, dlon, dlat, cols, rows): the grid, one row, in degrees
  basin_grid   (row INTEGER PRIMARY KEY, b BLOB NOT NULL): row r covers latitudes [s + r·dlat, s + (r+1)·dlat), west
               to east from w; `b` is unsigned LEB128 varints, pairs (run length in cells, ix), the lengths summing to
               `cols`; ix 0 is no group (the sea, outside B.C.). A cell is the group holding its CENTRE.

WHY A GRID AND NOT THE TILES' BASIN LAYER. The layer is in the tiles only at z4–z7 (`BASIN_HANDOVER_Z - 1`) and the app
draws it only while the Conditions field is on, below the handover — a map centred on a river at z10 has no basin
feature loaded to ask, on the web or the phone. The groups' own polygons are 494k vertices simplified; this grid is
~30 kB gzipped (measured 2026-11-04 on the live layer: 22,199 runs, 55 kB raw, 29.5 kB gz).

WHY THIS CELL (1/32° × 1/64°, ~2.1 × 1.7 km at 52° N). A point within about a kilometre of a divide may be answered with
the group next door. For the map's centre that is the honest grain (a reader's "where I am looking"); the finer 1/64° ×
1/128° grid cost 66 kB gz for half the error.
"""

from __future__ import annotations

import sqlite3
from typing import Dict, List, Sequence, Tuple

#: The grid: B.C.'s extent (-139.06..-114.05 E, 48.30..60.00 N on the live layer) with a margin.
FRAME: Dict[str, float] = {"w": -139.25, "s": 48.25, "dlon": 1 / 32, "dlat": 1 / 64, "cols": 810, "rows": 754}


def _uv(out: bytearray, v: int) -> None:
    if v < 0:
        raise ValueError(f"basin_grid: {v} is not an unsigned varint")
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v)


def encode_row(cells: Sequence[int]) -> bytes:
    """One grid row as (run length, ix) pairs."""
    out = bytearray()
    i, n = 0, len(cells)
    while i < n:
        j = i
        while j < n and cells[j] == cells[i]:
            j += 1
        _uv(out, j - i)
        _uv(out, int(cells[i]))
        i = j
    return bytes(out)


def decode_row(b: bytes, cols: int) -> List[int]:
    """`encode_row` undone; refuses a row whose runs do not cover `cols` exactly."""
    out: List[int] = []
    i = 0
    vals: List[int] = []
    while i < len(b):
        v, shift = 0, 0
        while True:
            x = b[i]
            i += 1
            v |= (x & 0x7F) << shift
            shift += 7
            if x < 0x80:
                break
        vals.append(v)
    if len(vals) % 2:
        raise ValueError("basin_grid: a row ends inside a run")
    for k in range(0, len(vals), 2):
        out += [vals[k + 1]] * vals[k]
    if len(out) != cols:
        raise ValueError(f"basin_grid: a row covers {len(out)} cells, not {cols}")
    return out


def names(frame) -> List[Tuple[int, str, str]]:
    """[(ix, basin_id, name)] — ix 1.. in basin_id order. A group with no name, or one id named twice, is refused."""
    by: Dict[str, str] = {}
    for bid, name in zip(frame["basin_id"], frame["WATERSHED_GROUP_NAME"]):
        if name is None or not str(name).strip():
            raise ValueError(f"basin: watershed group {bid} has no WATERSHED_GROUP_NAME")
        if by.setdefault(str(bid), str(name)) != str(name):
            raise ValueError(f"basin: watershed group {bid} has two names")
    return [(k + 1, bid, by[bid]) for k, bid in enumerate(sorted(by))]


def cells(frame, ix: Dict[str, int], f: Dict[str, float] = FRAME):
    """The grid as a (rows, cols) int array: each cell the ix of the group holding its centre, 0 for none. `frame` is
    `tiles.basins.groups()` (BC Albers); the centres are projected to it, so the polygons are never reprojected."""
    import numpy as np
    import shapely
    from pyproj import Transformer

    rows, cols = int(f["rows"]), int(f["cols"])
    lon = f["w"] + (np.arange(cols) + 0.5) * f["dlon"]
    lat = f["s"] + (np.arange(rows) + 0.5) * f["dlat"]
    X, Y = np.meshgrid(lon, lat)
    crs = frame.crs.to_epsg() if frame.crs else 3005
    x, y = Transformer.from_crs(4326, crs, always_xy=True).transform(X.ravel(), Y.ravel())
    geoms = shapely.make_valid(np.asarray(frame.geometry.values))
    ids = np.array([ix[str(b)] for b in frame["basin_id"]])
    tree = shapely.STRtree(geoms)
    pi, gi = tree.query(shapely.points(x, y), predicate="intersects")
    out = np.zeros(rows * cols, dtype=np.int32)
    # a centre on a shared border touches both: the lower ix, so a rebuild writes the same grid
    order = np.lexsort((ids[gi], pi))
    first = np.unique(pi[order], return_index=True)[1]
    out[pi[order][first]] = ids[gi[order]][first]
    return out.reshape(rows, cols)


def write(db: sqlite3.Connection, frame, cov=None, f: Dict[str, float] = FRAME) -> int:
    """Fill `basin`, `basin_frame` and `basin_grid` from the groups (`tiles.basins.groups()`); returns the run count."""
    rows = names(frame)
    db.executemany("INSERT INTO basin VALUES (?,?,?)", rows)
    db.execute("INSERT INTO basin_frame VALUES (?,?,?,?,?,?)",
               (f["w"], f["s"], f["dlon"], f["dlat"], int(f["cols"]), int(f["rows"])))
    grid = cells(frame, {bid: i for i, bid, _n in rows}, f)
    blobs = [(r, encode_row(grid[r].tolist())) for r in range(grid.shape[0])]
    db.executemany("INSERT INTO basin_grid VALUES (?,?)", blobs)
    runs = int(sum((grid[:, 1:] != grid[:, :-1]).sum(axis=1) + 1))
    if cov is not None:
        cov.filled("basin", len(rows))
        cov.filled("basin_grid", len(blobs))
    named = {int(v) for v in grid.ravel()} - {0}
    if len(named) != len(rows):
        # a group no cell centre falls in cannot be found by a point: say so (tiny coastal groups are the risk)
        missing = sorted(bid for i, bid, _n in rows if i not in named)
        print(f"     basin_grid: {len(missing)} group(s) hold no cell centre: {', '.join(missing)}")
    print(f"     basin_grid: {len(rows)} groups named, {grid.shape[0]} x {grid.shape[1]} cells, {runs:,} runs")
    return runs
