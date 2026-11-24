"""(T1024) A STREAM'S POLYGON SAYS WHEN ITS LINE IS DRAWN — checked on the written layer files.

A stream's own polygon (a slough, a river's wide reach: `lake` / `wetland` feature with `water` = "stream") is lit, when
its water is selected, by the casing of its ROUTE — the line `export_streams` writes into the `stream` layer under the
polygon's section id. Where there is no such line the app's selection ring has to go round the polygon instead, or the
selection lights nothing. The tiles did not say which: 22 stream polygons never get a line (20 Okanagan River Oxbows
pieces, Adskwatim Creek, Taylor Slough: no sidecar line) and 1,992 are drawn 1-5 zooms before their line (the line's
zoom comes from magnitude, the polygon's from area; Gun Creek polygon z8, line z13).

So each stream polygon carries `lz`, the first zoom its line is drawn at, and none when it never gets one — or when its
line is a STUB, shorter than `STUB_RATIO` of the polygon's long side (Okanagan River Oxbows 1281106: a 67 m route in a
57,000 m2 loop; 239 polygons on the live atlas, 2026-11-23): lighting the polygon with its section seams beats lighting a stub
(`export_waterbodies`); the style rings it below `lz` and always without one (app `paint.ts` `ringPaint`). This module is
the proof, read from the files the archive is built from — never from the dict `lz` was written from:

  every stream polygon either carries `lz` equal to the first zoom of its section's line in the stream layer, that line
  not a stub; or carries no `lz` and has no line, or only a stub. There is no third case. (The stub test here is on the
  written WGS84 geometry in local metres, the export's on BC Albers: `STUB_SLACK` either side of the ratio absorbs that.)

`check` raises naming every offender's kind and a few section ids; the tile build runs it after the export
(`tiles/__main__.py`), before tippecanoe.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np

from shapely.geometry import shape
from shapely.ops import transform

#: (T1024) a route shorter than this share of its polygon's long side (its minimum rotated rectangle's) is a stub
STUB_RATIO = 0.3
#: the check's allowance either side of STUB_RATIO: it measures the written WGS84 geometry, the export BC Albers
STUB_SLACK = 0.05


def long_side(poly) -> float:
    """The long side of `poly`'s minimum rotated rectangle (0 for a point)."""
    # GEOS's oriented envelope divides by zero on an edge parallel to an axis and still returns the right rectangle
    # (checked: a 500 x 200 box gives 500); numpy's warning about it is noise, not a failure
    with np.errstate(divide="ignore", invalid="ignore"):
        r = poly.minimum_rotated_rectangle
    if r.geom_type == "Polygon":
        c = list(r.exterior.coords)
        return max(math.dist(c[i], c[i + 1]) for i in range(len(c) - 1))
    return r.length


def is_stub(route_length: float, poly, ratio: float = STUB_RATIO) -> bool:
    """Does a route this long fail to stand for `poly` (the same units)? A polygon with no extent has no stub."""
    side = long_side(poly)
    return side > 0 and route_length < ratio * side


def _metres(g):
    """WGS84 -> local metres (equirectangular at the geometry's own latitude: features are small)."""
    k = math.cos(math.radians(g.centroid.y))
    return transform(lambda x, y, z=None: (x * 111_320 * k, y * 110_540), g)

#: the layer files a stream polygon is drawn from, and the line files drawn INTO the `stream` tile layer
POLYGON_FILES = ("lake.geojsonl", "wetland.geojsonl")
LINE_FILES = ("stream.geojsonl", "lake_link.geojsonl")


def _tail(line: str) -> dict:
    """`properties` + `tippecanoe` of one feature line, without parsing its geometry (the stream file is gigabytes;
    `_writer` writes geometry first, then properties, then tippecanoe)."""
    i = line.rfind('"properties":')
    if i < 0:
        raise ValueError(f"not a feature line written by tiles.export._writer: {line[:120]}")
    return json.loads("{" + line[i:])


def stream_polygons(layer_dir: Path) -> tuple[dict[int, dict], list[int]]:
    """({section id: its properties} for every polygon whose `water` is "stream", [section ids of every OTHER polygon
    that carries `lz`]) — `lz` is a stream polygon's alone (the app's ring reads `lz` and no water kind)."""
    out: dict[int, dict] = {}
    standing_lz: list[int] = []
    for name in POLYGON_FILES:
        with (layer_dir / name).open() as fh:
            for line in fh:
                if '"water":"stream"' not in line and '"lz":' not in line:
                    continue
                props = _tail(line)["properties"]
                if props.get("water") == "stream":
                    out[props["section_id"]] = {**props, "_geom": json.loads(line)["geometry"]}
                elif "lz" in props:
                    standing_lz.append(props["section_id"])
    return out, sorted(standing_lz)


def line_zooms(layer_dir: Path, wanted: set[int]) -> dict[int, tuple[int, float]]:
    """{section id: (the FIRST zoom any line of it is drawn at in the stream layer, their length in metres)}, for the
    ids in `wanted`."""
    out: dict[int, tuple[int, float]] = {}
    for name in LINE_FILES:
        with (layer_dir / name).open() as fh:
            for line in fh:
                t = _tail(line)
                sid = t["properties"].get("section_id")
                if sid in wanted:
                    z = t["tippecanoe"]["minzoom"]
                    n = _metres(shape(json.loads(line)["geometry"])).length
                    z0, n0 = out.get(sid, (z, 0.0))
                    out[sid] = (min(z, z0), n0 + n)
    return out


def offenders(layer_dir: Path) -> tuple[int, dict[str, list[int]]]:
    """(how many stream polygons, the polygons that break the rule by how: `lz_without_line`, `lz_not_the_line_zoom`,
    `lz_on_a_stub`, `line_without_lz` (a line that is no stub), `lz_on_standing_water`)."""
    polys, standing_lz = stream_polygons(layer_dir)
    lines = line_zooms(layer_dir, set(polys))
    bad: dict[str, list[int]] = {"lz_without_line": [], "lz_not_the_line_zoom": [], "lz_on_a_stub": [],
                                 "line_without_lz": [], "lz_on_standing_water": standing_lz}
    for sid in sorted(polys):
        lz = polys[sid].get("lz")
        if sid in lines:
            poly = _metres(shape(polys[sid]["_geom"]))
            zoom, length = lines[sid]
        if lz is None:
            if sid in lines and not is_stub(length, poly, STUB_RATIO + STUB_SLACK):
                bad["line_without_lz"].append(sid)
        elif sid not in lines:
            bad["lz_without_line"].append(sid)
        elif zoom != lz:
            bad["lz_not_the_line_zoom"].append(sid)
        elif is_stub(length, poly, STUB_RATIO - STUB_SLACK):
            bad["lz_on_a_stub"].append(sid)
    return len(polys), {k: v for k, v in bad.items() if v}


def check(layer_dir: Path) -> int:
    """Raise unless every stream polygon's `lz` tells the truth about its line; return how many stream polygons."""
    n, bad = offenders(layer_dir)
    if bad:
        raise SystemExit("polygons whose `lz` does not match the stream layer (T1024): " + "; ".join(
            f"{k} {len(v):,} (e.g. {v[:5]})" for k, v in sorted(bad.items()))
            + " — `export_waterbodies` writes `lz` from `export_streams`' route zooms; rerun the whole water export")
    return n
