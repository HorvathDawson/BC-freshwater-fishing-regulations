"""Cranberry River canyon profile: elevation + side relief downstream of the DFO falls point.

Read-only analysis. Run from the repo root:  PYTHONPATH=. python handoff/PLAN/curation-review/cranberry-canyon/profile.py
Line: FWA stream BLUE_LINE_KEY 360885423 (Cranberry River) from data/source/bc_fisheries_data.gpkg.
DEM: AWS Terrarium tiles via pipeline.atlas.waters.added_streams.dem (cached in ~/.cache/terrarium).
"""
from __future__ import annotations

import csv
import json
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
from pyproj import Transformer
from shapely.geometry import LineString, Point

from pipeline.atlas.waters.added_streams.dem import ElevationSampler

OUT = Path(__file__).parent
BLK = 360885423
FALLS = (-128.68426, 55.56407)
STEP = 25.0            # m between stations
ABOVE = 2000.0         # m of context upstream of the falls
XS_HALF = 300.0        # cross-section half width (m)
XS_STEP = 10.0
ZOOM = 14

to_alb = Transformer.from_crs(4326, 3005, always_xy=True)
to_ll = Transformer.from_crs(3005, 4326, always_xy=True)


class Bilinear(ElevationSampler):
    """Same tiles/cache as the repo sampler, bilinear instead of nearest pixel."""

    def elev(self, lon: float, lat: float) -> float:
        fx, fy = self._frac_xy(lon, lat)
        px, py = fx * 256 - 0.5, fy * 256 - 0.5
        x0, y0 = math.floor(px), math.floor(py)
        ax, ay = px - x0, py - y0
        v = 0.0
        for dx, dy, w in ((0, 0, (1 - ax) * (1 - ay)), (1, 0, ax * (1 - ay)), (0, 1, (1 - ax) * ay), (1, 1, ax * ay)):
            gx, gy = x0 + dx, y0 + dy
            img = self._tile(gx // 256, gy // 256)
            r, g, b = img.getpixel((gx % 256, gy % 256))
            v += w * (r * 256 + g + b / 256.0 - 32768.0)
        return v


def mainstem() -> tuple[LineString, gpd.GeoDataFrame]:
    s = gpd.read_file("data/source/bc_fisheries_data.gpkg", layer="streams", where=f"BLUE_LINE_KEY={BLK}")
    s = s.sort_values("DOWNSTREAM_ROUTE_MEASURE")
    coords: list = []
    for g in s.geometry:
        g = g.geoms[0] if g.geom_type.startswith("Multi") else g
        cs = list(g.coords)
        if coords and math.dist(coords[-1][:2], cs[-1][:2]) < math.dist(coords[-1][:2], cs[0][:2]):
            cs = cs[::-1]
        coords.extend(cs if not coords else cs[1:])
    return LineString(coords), s   # 3D, ordered mouth -> upstream


def main() -> None:
    line, segs = mainstem()
    line2d = LineString([c[:2] for c in line.coords])
    fx, fy = to_alb.transform(*FALLS)
    m_falls = line2d.project(Point(fx, fy))
    snap = line2d.interpolate(m_falls)
    snap_ll = to_ll.transform(snap.x, snap.y)
    dem = Bilinear(zoom=ZOOM)

    rows = []
    d = -ABOVE
    while m_falls - d >= 0:
        m = m_falls - d
        p = line2d.interpolate(m)
        a, b = line2d.interpolate(max(0, m - 30)), line2d.interpolate(min(line2d.length, m + 30))
        tx, ty = b.x - a.x, b.y - a.y
        n = math.hypot(tx, ty) or 1.0
        nx, ny = -ty / n, tx / n          # left of upstream direction
        offs = np.arange(-XS_HALF, XS_HALF + 0.1, XS_STEP)
        prof = np.array([dem.elev(*to_ll.transform(p.x + o * nx, p.y + o * ny)) for o in offs])
        ch = float(prof[np.abs(offs) <= 30].min())            # channel = lowest DEM within 30 m of the line
        fwa_z = line.interpolate(m).z if line.has_z else float("nan")

        def side(lim: float, sgn: int) -> float:
            sel = (np.abs(offs) <= lim) & (np.sign(offs) == sgn)
            return float(prof[sel].max() - ch)

        def width(h: float) -> float:   # valley width at h m above channel
            c = len(offs) // 2
            l = r = c
            while l > 0 and prof[l] - ch < h: l -= 1
            while r < len(offs) - 1 and prof[r] - ch < h: r += 1
            return float(offs[r] - offs[l])

        lon, lat = to_ll.transform(p.x, p.y)
        rows.append(dict(
            d_below_falls=round(d, 1), measure=round(m, 1), lon=round(lon, 6), lat=round(lat, 6),
            ch_dem=round(ch, 1), fwa_z=round(fwa_z, 1),
            rel100=round(min(side(100, 1), side(100, -1)), 1), rel200=round(min(side(200, 1), side(200, -1)), 1),
            rel100_max=round(max(side(100, 1), side(100, -1)), 1),
            w20=width(20), w40=width(40)))
        d += STEP

    with open(OUT / "profile.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)
    json.dump({"falls_snap": snap_ll, "m_falls": m_falls, "snap_dist_m": Point(fx, fy).distance(snap)},
              open(OUT / "snap.json", "w"), indent=1)
    print("falls measure", m_falls, "snap", snap_ll, "stations", len(rows))


if __name__ == "__main__":
    main()
