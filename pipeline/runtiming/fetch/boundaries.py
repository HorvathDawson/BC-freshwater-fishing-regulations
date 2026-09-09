"""Fetch the conservation-unit polygons from the Explorer's own vector tiles.

WHY TILES RATHER THAN THE FEDERAL SHAPEFILES. Two reasons, and the second is the one that
decides it:

  1. The tile layer is keyed by the SAME cuid as the curves, so the join is exact. DFO's
     open-data boundaries key on `FULL_CU_IN` and their names encode run and age codes
     (`LOWER FRASER RIVER_SU_1.3`) that the Explorer's display names drop, so joining them
     needs polygon overlap and lands 377 of 427 cleanly.
  2. DFO has NO steelhead conservation units — steelhead is managed by the Province — so
     the federal set cannot answer for 36 of our 463 units at all.

The federal boundaries are still worth carrying as a crosswalk for provenance; they are
just not the geometry this index is built on.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

TILE = ("https://www.salmonexplorer.ca/tileserver/tileserver.php"
        "?/conservation-units/{z}/{x}/{y}.pbf")
#: BC plus the Yukon and Transboundary regions, which reach to 69.5 N.
BBOX = (-142.0, 47.0, -112.0, 70.0)
ZOOM = 8                     # the layer's own maxzoom is 10; 8 is ~200 m of detail
UA = {"User-Agent": "Mozilla/5.0 (compatible; bc-fishing-regs/1.0)"}


def deg2tile(lon: float, lat: float, z: int) -> tuple[int, int]:
    n = 2 ** z
    r = math.radians(lat)
    return (int((lon + 180.0) / 360.0 * n),
            int((1.0 - math.asinh(math.tan(r)) / math.pi) / 2.0 * n))


def tile_bounds(x: int, y: int, z: int) -> tuple[float, float, float, float]:
    n = 2 ** z
    lon = lambda xx: xx / n * 360.0 - 180.0
    lat = lambda yy: math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * yy / n))))
    return lon(x), lat(y + 1), lon(x + 1), lat(y)


def get_tile(z: int, x: int, y: int, cache: Path) -> bytes | None:
    p = cache / f"{z}_{x}_{y}.pbf"
    if p.exists():
        return p.read_bytes() or None
    req = urllib.request.Request(TILE.format(z=z, x=x, y=y), headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            b = r.read()
    except urllib.error.HTTPError:
        b = b""
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b)
    return b or None


def main(out: Path, zoom: int = ZOOM) -> None:
    """Download every tile covering BC. Decoding happens in `generate`."""
    cache = out / "cu_tiles"
    x0, y0 = deg2tile(BBOX[0], BBOX[3], zoom)
    x1, y1 = deg2tile(BBOX[2], BBOX[1], zoom)
    coords = [(x, y) for x in range(x0, x1 + 1) for y in range(y0, y1 + 1)]
    with ThreadPoolExecutor(max_workers=8) as ex:
        got = list(ex.map(lambda c: get_tile(zoom, c[0], c[1], cache) is not None, coords))
    print(f"z{zoom}: {len(coords)} tiles requested, {sum(got)} carried data -> {cache}")


if __name__ == "__main__":
    from pipeline.common.curated import SOURCE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=SOURCE / "runtiming")
    ap.add_argument("--zoom", type=int, default=ZOOM)
    a = ap.parse_args()
    main(a.out, a.zoom)
