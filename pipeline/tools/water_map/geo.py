"""Lines and polygons for the map (T853), from the tile build's own layers (`layers/*.geojsonl`) — the geometry the
app draws (owner 2026-10-20: geometry from tiles only).

  stream.geojsonl   one feature per stream section, `section_id` = the release's section handle
  lake.geojsonl     lake polygons by `section_id` (a `name` is OPTIONAL — never select a lake by its name)
  tidal_water       the sea, for the coast
  basin             FWA watershed groups (`basin_id`), for the watershed outline

One pass per file with a cheap first-coordinate box test before any JSON is parsed (2.5 GB of streams in ~2 s).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from shapely.geometry import box, shape
from shapely.geometry.base import BaseGeometry

Box = Tuple[float, float, float, float]       # west, south, east, north

_FIRST = re.compile(rb'"coordinates":\[+(-?[\d.]+),(-?[\d.]+)')
_SID = re.compile(rb'"section_id":(\d+)')
_ORD = re.compile(rb'"ord":(\d+)')
_AREA = re.compile(rb'"area_m2":(\d+)')


@dataclass
class Geo:
    sections: Dict[int, BaseGeometry] = field(default_factory=dict)     # the system's own sections
    streams: List[Tuple[int, BaseGeometry]] = field(default_factory=list)   # context: (Strahler order, line)
    lakes: List[Tuple[int, BaseGeometry]] = field(default_factory=list)     # context lakes (section id, polygon)
    sea: List[BaseGeometry] = field(default_factory=list)
    basins: List[Tuple[str, BaseGeometry]] = field(default_factory=list)


def _in(x: float, y: float, b: Box) -> bool:
    return b[0] <= x <= b[2] and b[1] <= y <= b[3]


def read(layers: Path, sids: Set[int], bbox: Box, *, min_order: int = 3, min_lake_m2: int = 200_000,
         margin: float = 0.6) -> Geo:
    """The system's sections (`sids`, any place in the province) and the context inside `bbox` widened by `margin`
    degrees for the first-coordinate test (a feature's first vertex can lie outside a box its line crosses)."""
    layers = Path(layers)
    g = Geo()
    wide = (bbox[0] - margin, bbox[1] - margin, bbox[2] + margin, bbox[3] + margin)
    clip = box(*bbox)
    want = {str(s).encode() for s in sids}
    with open(layers / "stream.geojsonl", "rb") as fh:
        for line in fh:
            tail = line[-240:]
            m = _SID.search(tail)
            sid_b = m.group(1) if m else None
            own = sid_b in want
            if not own:
                f = _FIRST.search(line, 0, 200)
                if not f or not _in(float(f.group(1)), float(f.group(2)), wide):
                    continue
                o = _ORD.search(tail)
                if not o or int(o.group(1)) < min_order:
                    continue
            d = json.loads(line)
            geom = shape(d["geometry"])
            if own:
                sid = int(sid_b)
                prev = g.sections.get(sid)
                g.sections[sid] = geom if prev is None else prev.union(geom)
            elif geom.intersects(clip):
                g.streams.append((int(d["properties"].get("ord") or 0), geom))
    with open(layers / "lake.geojsonl", "rb") as fh:
        for line in fh:
            tail = line[-300:]
            m = _SID.search(tail)
            sid_b = m.group(1) if m else None
            own = sid_b in want
            if not own:
                f = _FIRST.search(line, 0, 200)
                if not f or not _in(float(f.group(1)), float(f.group(2)), wide):
                    continue
                a = _AREA.search(tail)
                if not a or int(a.group(1)) < min_lake_m2:
                    continue
            d = json.loads(line)
            geom = shape(d["geometry"])
            if own:
                g.sections[int(sid_b)] = geom
            elif geom.intersects(clip):
                g.lakes.append((int(d["properties"]["section_id"]), geom))
    for name, dest in (("tidal_water", g.sea), ("basin", g.basins)):
        p = layers / f"{name}.geojsonl"
        if not p.is_file():
            continue
        with open(p, "rb") as fh:
            for line in fh:
                d = json.loads(line)
                geom = shape(d["geometry"])
                if geom.intersects(clip):
                    if name == "basin":
                        dest.append((d["properties"].get("basin_id"), geom))
                    else:
                        dest.append(geom)
    return g


def part_boxes(rel, ords: Iterable[int]) -> Dict[Tuple[int, int], Box]:
    """Each part's box from the release's `geo` file (`part_geo`, millionths of a degree)."""
    con = rel.db("geo")
    out = {}
    for o in sorted(set(ords)):
        for _, pix, w, s, e, n in con.execute("SELECT ord, part_ix, w, s, e, n FROM part_geo WHERE ord = ?", (o,)):
            if None not in (w, s, e, n):
                out[(o, pix)] = (w / 1e6, s / 1e6, e / 1e6, n / 1e6)
    return out


def union_box(boxes: Iterable[Box]) -> Optional[Box]:
    bs = list(boxes)
    if not bs:
        return None
    return (min(b[0] for b in bs), min(b[1] for b in bs), max(b[2] for b in bs), max(b[3] for b in bs))
