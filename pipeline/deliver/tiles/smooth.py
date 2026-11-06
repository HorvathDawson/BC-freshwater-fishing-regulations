"""(T419) Stream lines smoothed for ONE zoom: the river's course, not its meanders.

WHY NOT A BIGGER TOLERANCE
==========================

Douglas-Peucker (tippecanoe's simplifier) keeps the point FURTHEST from the chord, so on a tightly meandering
lowland river (the Langley Salmon River) every meander's apex survives and the line draws as a sawtooth; raising the
tolerance (5 -> 10, b40de633d) took points off but left the apexes, and took real shape off the gentle streams with
them (owner on staging r-4230bc0c1dd5: "the bending oxbows dont become a straight line ... other streams got over
simplified"). Visvalingam (`--visvalingam`) was measured too and was no better.

WHAT THIS DOES
==============

Each stream line drawn at zoom `z` is low-pass filtered at that zoom's scale: a Gaussian over arc length, sigma
`SMOOTH_PX` screen pixels, applied `SMOOTH_PASSES` times at sigma/sqrt(passes) (the same total blur on a straight-ish
line; MORE on a tight meander, whose arc is long for the ground it covers, because each pass re-measures the
shortened arc). A meander narrower than a few pixels collapses onto the course; a bend wider than that keeps its
shape. Then the smoothed line is thinned (`KEEP_PX`) before tippecanoe sees it.

NOTHING THE NETWORK RELIES ON MOVES. Every node shared between two stream features (a confluence, a section break,
a tributary mouth on another line's interior) and every line end is PINNED: the line is cut there, each piece is
smoothed with its ends fixed (odd reflection at the ends), and the pinned coordinate is written back byte for byte,
so `--no-simplification-of-shared-nodes` still finds the same node in both features. Feature ids, properties and
the feature set are untouched — only interior vertices change, and only below the top zoom.

MEASURED (T419, Fraser Valley clip: 4,577 stream features, -122.75..-121.50 x 48.98..49.36; z10, against the
release before T419 at S=5 and the staging build at S=10; shots and numbers in T419-shots/):
    stream vertices z10   3,903 (S5)   3,093 (S10)   1,770 (this, S5)
    tile bytes      z10  15.9 KB      14.4 KB       11.9 KB
    turning per 100 px of the Salmon River (sawtooth): 1,241 -> 812 -> 244 degrees
    95th-percentile distance from the true line, gentle streams: ~0.8 / ~1.5 / ~1.5 px — smoothed, not chorded.
"""

from __future__ import annotations

import math

import numpy as np
import orjson
import shapely

#: The blur, in screen pixels of the zoom being built (512-px tiles). 4 px collapses the Salmon River's oxbows at
#: z9-z11 and keeps Norrish Creek's bends (k = 1, 2, 3, 4, 6 measured; 6 strays 2+ px from gentle streams).
SMOOTH_PX = 4.0
#: Passes of the blur at SMOOTH_PX / sqrt(passes) each (3 measured against 1: tight meanders flatten further, gentle
#: lines move the same).
SMOOTH_PASSES = 3
#: After smoothing, points closer than this to the line (Douglas-Peucker, px) are dropped: the resampled line is
#: dense and tippecanoe would only throw them away later.
KEEP_PX = 0.25

_R = 6378137.0
_Q = 1e6  # the layer files' coordinate precision (6 decimals)


def metres_per_px(z: int, lat: float) -> float:
    return 2 * math.pi * _R * math.cos(math.radians(lat)) / (512 * 2 ** z)


def _parts(geom: dict) -> list:
    t = geom.get("type")
    if t == "LineString":
        return [geom["coordinates"]]
    if t == "MultiLineString":
        return geom["coordinates"]
    return []


def _key(arr: np.ndarray) -> np.ndarray:
    q = np.rint(arr * _Q).astype(np.int64)
    return q[:, 0] * 1_000_000_000 + q[:, 1]  # lon*1e6 in [-1.8e8, 0], lat*1e6 in [0, 9e7]: one int64, no overlap


def pinned(features: list[dict]) -> set[int]:
    """Keys of every vertex two stream features share, and every line end."""
    keys, ends = [], []
    for f in features:
        for part in _parts(f["geometry"]):
            if len(part) < 2:
                continue
            k = _key(np.asarray(part, dtype=float))
            keys.append(np.unique(k))
            ends += [int(k[0]), int(k[-1])]
    if not keys:
        return set()
    u, n = np.unique(np.concatenate(keys), return_counts=True)
    return set(u[n >= 2].tolist()) | set(ends)


def _blur(xy: np.ndarray, sigma: float) -> np.ndarray:
    """One Gaussian pass over arc length with both ends fixed. Pieces shorter than 1.5 sigma are left as they are."""
    seg = np.hypot(*np.diff(xy, axis=0).T)
    length = float(seg.sum())
    if len(xy) < 3 or length < 1.5 * sigma:
        return xy
    n = int(math.ceil(length / (sigma / 4))) + 1
    s = np.concatenate([[0.0], np.cumsum(seg)])
    t = np.linspace(0.0, length, n)
    rs = np.column_stack([np.interp(t, s, xy[:, 0]), np.interp(t, s, xy[:, 1])])
    step = length / (n - 1)
    m = min(int(3 * sigma / step) + 1, n - 1)
    pad = np.vstack([2 * rs[0] - rs[m:0:-1], rs, 2 * rs[-1] - rs[-2:-m - 2:-1]])
    w = np.exp(-0.5 * (np.arange(-m, m + 1) * step / sigma) ** 2)
    w /= w.sum()
    out = np.column_stack([np.convolve(pad[:, 0], w, "valid"), np.convolve(pad[:, 1], w, "valid")])
    out[0], out[-1] = xy[0], xy[-1]
    return out


def _smooth_piece(xy: np.ndarray, sigma: float, keep: float) -> np.ndarray:
    for _ in range(SMOOTH_PASSES):
        xy = _blur(xy, sigma / math.sqrt(SMOOTH_PASSES))
    if keep > 0 and len(xy) > 2:
        xy = shapely.get_coordinates(shapely.simplify(shapely.linestrings(xy), keep, preserve_topology=False))
    return xy


def smooth_line(part: list, pins: set[int], z: int) -> list:
    """One line (lon/lat pairs) smoothed for zoom `z`, cut and fixed at every pinned vertex."""
    if len(part) < 3:
        return part
    a = np.asarray(part, dtype=float)
    lon0, lat0 = float(a[:, 0].mean()), float(a[:, 1].mean())
    kx = math.radians(1) * _R * math.cos(math.radians(lat0))
    ky = math.radians(1) * _R
    xy = np.column_stack([(a[:, 0] - lon0) * kx, (a[:, 1] - lat0) * ky])
    px = metres_per_px(z, lat0)
    keys = _key(a)
    cut = [0] + [i for i in range(1, len(part) - 1) if int(keys[i]) in pins] + [len(part) - 1]
    out = [part[0]]
    for i, j in zip(cut, cut[1:]):
        p = _smooth_piece(xy[i:j + 1], SMOOTH_PX * px, KEEP_PX * px)
        out += [[round(float(x) / kx + lon0, 6), round(float(y) / ky + lat0, 6)] for x, y in p[1:-1]]
        out.append(part[j])
    return out


def smooth_file(path, z: int) -> int:
    """Rewrite a layer file (one tippecanoe pass's stream input) with every line smoothed for zoom `z`. Returns the
    vertex count written."""
    feats = [orjson.loads(ln) for ln in path.read_bytes().splitlines() if ln.strip()]
    pins = pinned(feats)
    n = 0
    with path.open("wb") as fo:
        for f in feats:
            g = f["geometry"]
            if g.get("type") == "LineString":
                g["coordinates"] = smooth_line(g["coordinates"], pins, z)
                n += len(g["coordinates"])
            elif g.get("type") == "MultiLineString":
                g["coordinates"] = [smooth_line(p, pins, z) for p in g["coordinates"]]
                n += sum(map(len, g["coordinates"]))
            fo.write(orjson.dumps(f) + b"\n")
    return n
