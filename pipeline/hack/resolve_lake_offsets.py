"""One-off: resolve the 5 'from <Lake> downstream <N> to fishing boundary signs' offset points.

For each job: find the target stream (by GNIS_NAME), find the lake (by GNIS_NAME_1 across
lakes/manmade/wetlands), locate the lake OUTLET on the stream (down-most stream∩lake crossing),
then walk offset_m downstream along the merged mouth->source line. Prints a proposal per job
(lon/lat + the BLK) for human review. Does NOT write — authoring happens after eyeballing.

Run:  PYTHONPATH="$PWD" .venv/bin/python -m pipeline.hack.resolve_lake_offsets
"""

from __future__ import annotations

from operator import itemgetter

from shapely.geometry import Point
from shapely.ops import linemerge, unary_union
from pyproj import Transformer

from data.data_extractor import FWADataAccessor
from pipeline.common.curated import CURATED, SOURCE

_GPKG = str(SOURCE / "bc_fisheries_data.gpkg")
_TO_4326 = Transformer.from_crs(3005, 4326, always_xy=True)

# (locator_id, stream GNIS_NAME, lake GNIS_NAME_1, offset_m downstream)
JOBS = [
    ("forsyth-creek-3adc4a", "Forsyth Creek", "Connor Lake", 3000),
    ("jewel-creek-b23cd8", "Jewel Creek", "Jewel Lake", 1500),
    ("yakoun-river-eace82", "Yakoun River", "Yakoun Lake", 13000),
    ("zymoetz-copper-river-f226e7", "Zymoetz River", "McDonell Lake", 3000),
    ("ruby-creek-ad1f2c", "Ruby Creek", "Ruby Lake", 100),
]


def _merged_line(fids):
    """Merge a BLK's fids into ONE mouth->source LineString (ordered by DOWNSTREAM_ROUTE_MEASURE)."""
    fids = sorted(fids, key=itemgetter("m"))
    merged = linemerge(unary_union([f["geom"] for f in fids]))
    if merged.geom_type != "LineString":
        # fall back to the longest part
        merged = max(merged.geoms, key=lambda g: g.length)
    # orient mouth->source: the fid with the smallest measure holds the mouth end
    return merged


def main() -> None:
    fwa = FWADataAccessor(_GPKG)
    stream_names = sorted({j[1] for j in JOBS})
    lake_names = sorted({j[2] for j in JOBS})

    streams = fwa.get_features_by_attribute(
        "streams", "GNIS_NAME", stream_names)
    lakes = None
    for layer in ("lakes", "manmade", "wetlands"):
        try:
            part = fwa.get_features_by_attribute(layer, "GNIS_NAME_1", lake_names)
        except Exception:
            continue
        if part is None or part.empty:
            continue
        part = part.copy()
        part["_layer"] = layer
        lakes = part if lakes is None else __import__("geopandas").GeoDataFrame(
            __import__("pandas").concat([lakes, part], ignore_index=True), crs=part.crs)

    print(f"loaded {len(streams)} stream fids, {0 if lakes is None else len(lakes)} lake polys\n")

    for lid, sname, lname, off in JOBS:
        srows = streams[streams["GNIS_NAME"] == sname]
        lrows = lakes[lakes["GNIS_NAME_1"] == lname] if lakes is not None else None
        if srows.empty or lrows is None or lrows.empty:
            print(f"✗ {lid}: stream={len(srows)} lake={0 if lrows is None else len(lrows)} — MISSING (manual)")
            continue

        # Disambiguate name collisions: pick the (stream BLK, lake poly) pair that are CLOSEST.
        best = None  # (dist, blk, line, lake_poly)
        for blk, grp in srows.groupby("BLUE_LINE_KEY"):
            fids = [{"geom": r.geometry, "m": float(r.DOWNSTREAM_ROUTE_MEASURE or 0)}
                    for r in grp.itertuples()]
            line = _merged_line(fids)
            for lr in lrows.itertuples():
                lp = lr.geometry
                d = line.distance(lp)
                if best is None or d < best[0]:
                    best = (d, blk, line, lp)
        if best is None:
            print(f"✗ {lid}: no candidate pair")
            continue
        dist, blk, line, lake_poly = best

        # outlet = down-most stream∩lake-boundary crossing; else nearest stream point to the lake
        inter = line.intersection(lake_poly.boundary)
        pts = [inter] if inter.geom_type == "Point" else list(getattr(inter, "geoms", []))
        pts = [p for p in pts if p.geom_type == "Point"]
        if pts:
            outlet_d = min(line.project(p) for p in pts)
            how = "boundary-crossing"
        else:
            from shapely.ops import nearest_points
            np_line, _ = nearest_points(line, lake_poly)
            outlet_d = line.project(np_line)
            how = f"nearest-point (gap {dist:.0f} m)"
        target_d = max(0.0, outlet_d - off)            # walk downstream (toward mouth)
        pt = line.interpolate(target_d)
        lon, lat = _TO_4326.transform(pt.x, pt.y)
        outlet_pt = line.interpolate(outlet_d)
        olon, olat = _TO_4326.transform(outlet_pt.x, outlet_pt.y)
        clamp = " (CLAMPED at mouth — offset > reach below outlet)" if outlet_d - off < 0 else ""
        print(f"✓ {lid}  blk={blk}")
        print(f"    {lname} outlet @ [{olon:.5f}, {olat:.5f}] via {how} (d={outlet_d:.0f} m)")
        print(f"    signs = {off} m downstream -> [{round(lon,5)}, {round(lat,5)}]{clamp}\n")


if __name__ == "__main__":
    main()
