"""Conservation-unit polygons x stream sections -> the frozen `section -> runs` index.

Stream geometry is already keyed by `section_id`, the same key `section_gauge` uses, so the
result drops into the bundle beside it.

A section joins every CU whose polygon contains its MIDPOINT. Midpoint rather than a line
clip because CU boundaries are whole watersheds and sections are short (mean 1-4 km): the
midpoint is in the same watershed as the section except at a boundary, and it costs one
point-in-polygon instead of a clip per pair. The overlap fraction is NOT computed, and that
is a deliberate limit recorded here rather than in someone's memory — see `relation` below.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import time
from collections import Counter, defaultdict
from pathlib import Path

import mapbox_vector_tile as mvt
import orjson
from shapely import make_valid
from shapely.geometry import Point, mapping, shape
from shapely.ops import unary_union
from shapely.prepared import prep
from shapely.strtree import STRtree

from pipeline.runtiming.runs import read_runs

SIMPLIFY = 0.001      # ~70 m, on top of the ~200 m the z8 tiles already carry


def polygons_from_tiles(cache: Path, zoom: int = 8) -> dict[int, object]:
    """Stitch the cached vector tiles into one polygon per cuid.

    The tile layer carries NO properties — the MVT feature *id* is the cuid, which is how
    the Explorer's own map identifies a unit on click. Tiles clip geometry at their edges,
    so the pieces are collected per id and dissolved.
    """
    pieces: dict[int, list] = defaultdict(list)
    for p in sorted(cache.glob(f"{zoom}_*.pbf")):
        b = p.read_bytes()
        if not b:
            continue
        _, x, y = p.stem.split("_")
        x, y = int(x), int(y)
        t = mvt.decode(gzip.decompress(b) if b[:2] == b"\x1f\x8b" else b)
        layer = t.get("conservation-units")
        if not layer:
            continue
        ext = layer["extent"]
        n = 2 ** zoom
        w, e = x / n * 360.0 - 180.0, (x + 1) / n * 360.0 - 180.0
        lat = lambda yy: math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * yy / n))))
        s, nn = lat(y + 1), lat(y)
        to_ll = lambda pt: (w + pt[0] / ext * (e - w), s + pt[1] / ext * (nn - s))
        for f in layer["features"]:
            cid, g = f.get("id"), f["geometry"]
            depth = {"Polygon": 2, "MultiPolygon": 3}.get(g["type"])
            if cid is None or not depth:
                continue
            conv = lambda c, d: [conv(i, d - 1) for i in c] if d > 1 else [to_ll(q) for q in c]
            geo = make_valid(shape({"type": g["type"], "coordinates": conv(g["coordinates"], depth)}))
            if not geo.is_empty:
                pieces[cid].append(geo)
    return {cid: unary_union(gs).buffer(0) for cid, gs in pieces.items()}


def main(streams: Path, tiles: Path, runs_json: Path, out: Path, zoom: int = 8) -> None:
    runs = read_runs(runs_json)
    polys = polygons_from_tiles(tiles, zoom)
    print(f"{len(polys)} CU polygons stitched from tiles", flush=True)
    missing = set(runs) - set(polys)
    if missing:
        print(f"  WARNING {len(missing)} units have no polygon: {sorted(missing)[:8]}")

    ids = sorted(polys)
    simp = [polys[c].simplify(SIMPLIFY, preserve_topology=False).buffer(0) for c in ids]
    pre = [prep(g) for g in simp]
    tree = STRtree(simp)

    # the polygons, saved for anything that wants to draw or re-score the join
    Path(out / "cu_polygons.geojson").write_text(json.dumps({
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "id": c, "properties": {"cuid": c},
                      "geometry": mapping(polys[c])} for c in ids]}))

    per: dict[str, list[int]] = {}
    n = 0
    t0 = time.time()
    with open(streams, "rb") as f:
        for line in f:
            d = orjson.loads(line)
            g = d["geometry"]
            if g["type"] != "LineString":
                continue
            c = g["coordinates"]
            pt = Point(c[len(c) // 2][:2])
            hit = [ids[i] for i in tree.query(pt) if pre[i].contains(pt)]
            if hit:
                per[d["properties"]["section_id"]] = hit
            n += 1
            if n % 200_000 == 0:
                print(f"  {n:,} sections  {time.time()-t0:.0f}s  {len(per):,} matched", flush=True)

    with open(out / "section_cu.csv", "w") as fh:
        fh.write("section_id,cuid,relation\n")
        for sid, cus in per.items():
            for c in cus:
                # WHAT THE JOIN ACTUALLY KNOWS, recorded per row. `section_gauge` froze a
                # trust score with no statement of what produced it, so representativeness
                # became policy nobody could re-derive. This column is that mistake's fix:
                # today every row says `midpoint`, and a later overlap-weighted pass can
                # add its own kind without a schema change or a guess about provenance.
                fh.write(f"{sid},{c},midpoint\n")

    depth = Counter(len(v) for v in per.values())
    sp = Counter(len({runs[c].species for c in v if c in runs}) for v in per.values())
    report = {"stream_sections": n, "sections_in_a_cu": len(per),
              "rows": sum(len(v) for v in per.values()),
              "cu_depth": dict(depth), "species_depth": dict(sp),
              "polygons": len(polys), "zoom": zoom, "relation": "midpoint"}
    Path(out / "index_report.json").write_text(json.dumps(report, indent=1))
    print(f"{n:,} sections read, {len(per):,} inside a CU, "
          f"{sum(len(v) for v in per.values()):,} rows")


if __name__ == "__main__":
    from pipeline.common.curated import GENERATED, SOURCE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--streams", type=Path,
                    default=GENERATED.tiles / "layers" / "stream.geojsonl")
    ap.add_argument("--tiles", type=Path, default=SOURCE / "runtiming" / "cu_tiles")
    ap.add_argument("--runs", type=Path, default=GENERATED.runtiming / "runs.json")
    ap.add_argument("--out", type=Path, default=GENERATED.runtiming)
    ap.add_argument("--zoom", type=int, default=8)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    main(a.streams, a.tiles, a.runs, a.out, a.zoom)
