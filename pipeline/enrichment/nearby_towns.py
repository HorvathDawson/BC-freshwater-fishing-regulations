"""Nearest-town lookup for the web-app search index.

Every named waterbody in the tier-0 search index is tagged with the handful of
towns closest to it, so a user searching for a town surfaces the lakes and
rivers beside it.  Towns come from an OSM populated-places gazetteer
(``data/bc_places.json``, produced by ``data/fetch_data.py``).

The index projects town coordinates to BC Albers (EPSG:3005) so the KD-tree
operates in metres — Euclidean nearest-neighbour then equals geographic
nearest, which is not true in raw lon/lat degrees at BC latitudes.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional

import numpy as np
from pyproj import Transformer
from scipy.spatial import cKDTree

logger = logging.getLogger(__name__)

# WGS84 lon/lat -> BC Albers metres.
_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)


def load_places(path: Path) -> List[dict]:
    """Load the OSM places gazetteer, dropping malformed rows.

    Returns an empty list (with a warning) if the file is missing so the
    enrichment step degrades gracefully rather than crashing the build.
    """
    path = Path(path)
    if not path.exists():
        logger.warning("Places gazetteer not found: %s (nearby towns disabled)", path)
        return []
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    places = [
        p
        for p in data
        if p.get("name") and p.get("lat") is not None and p.get("lon") is not None
    ]
    if len(places) != len(data):
        logger.warning(
            "Dropped %d malformed place row(s) from %s", len(data) - len(places), path
        )
    return places


class NearbyTownsIndex:
    """KD-tree over populated places for fast nearest-town queries."""

    def __init__(self, places: List[dict]):
        self._names: List[str] = [p["name"] for p in places]
        if places:
            lons = np.array([p["lon"] for p in places], dtype="float64")
            lats = np.array([p["lat"] for p in places], dtype="float64")
            xs, ys = _TO_ALBERS.transform(lons, lats)
            self._xy = np.column_stack([xs, ys])
            self._tree = cKDTree(self._xy)
        else:
            self._tree = None

    def __len__(self) -> int:
        return len(self._names)

    @classmethod
    def from_file(cls, path: Path) -> "NearbyTownsIndex":
        return cls(load_places(path))

    def nearest(self, lon: float, lat: float, n: int = 10) -> List[str]:
        """Return up to ``n`` unique town names closest to (lon, lat), nearest first."""
        if self._tree is None or n <= 0:
            return []
        x, y = _TO_ALBERS.transform(lon, lat)
        # Over-fetch so we can drop duplicate town names and still return n.
        k = min(len(self._names), max(n * 3, n))
        _, idxs = self._tree.query([x, y], k=k)
        idxs = np.atleast_1d(idxs)

        out: List[str] = []
        seen: set[str] = set()
        for raw_i in idxs:
            i = int(raw_i)
            # cKDTree pads missing neighbours with index == len when k > n_points.
            if i >= len(self._names):
                continue
            name = self._names[i]
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(name)
            if len(out) >= n:
                break
        return out

    def within_radius(
        self,
        points_xy,
        radius_m: float = 15000.0,
        max_towns: Optional[int] = None,
    ) -> List[str]:
        """Towns within ``radius_m`` of *any* of the given points, nearest first.

        This is the reverse of :meth:`nearest`: instead of taking the single
        centre of a feature, it takes every representative point of the feature
        (e.g. the centroid of each stream segment) and returns the union of
        towns near any of them.  A town's rank is its *minimum* distance to any
        of the points, so a long river is tagged with every town it passes,
        ordered by how close it actually gets.

        Args:
            points_xy: array-like of shape (M, 2) in EPSG:3005 metres.
            radius_m:  search radius around each point (default 15 km).
            max_towns: optional cap on returned names (payload safety); the
                       nearest are kept.  ``None`` returns all within radius.

        Returns:
            Unique town names ordered by ascending minimum distance.
        """
        if self._tree is None:
            return []
        pts = np.asarray(points_xy, dtype="float64")
        if pts.ndim != 2 or pts.shape[0] == 0:
            return []

        # For each point, town indices within radius (object array of lists).
        neighbours = self._tree.query_ball_point(pts, r=radius_m)

        best: dict[int, float] = {}
        for p, town_idxs in zip(pts, neighbours):
            if not town_idxs:
                continue
            cand = np.asarray(town_idxs, dtype="int64")
            dx = self._xy[cand, 0] - p[0]
            dy = self._xy[cand, 1] - p[1]
            dists = np.hypot(dx, dy)
            for ti, dist in zip(cand.tolist(), dists.tolist()):
                prev = best.get(ti)
                if prev is None or dist < prev:
                    best[ti] = dist

        ordered = sorted(best.items(), key=lambda kv: kv[1])
        out: List[str] = []
        seen: set[str] = set()
        for ti, _dist in ordered:
            name = self._names[ti]
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(name)
            if max_towns is not None and len(out) >= max_towns:
                break
        return out
