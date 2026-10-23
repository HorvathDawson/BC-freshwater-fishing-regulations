"""The province as a FIELD — one polygon per watershed group, for the zooms where a river
is a hairline.

WHY A SHAPE AND NOT A GLOW. The first answer to "what does the map say at z5" was a soft
disc around each gauge, and it was wrong three ways at once: a disc paints land the gauge
says nothing about, two overlapping discs blend into a colour that is not on the scale, and
a blurred edge is a lighter shade — which on a sequential ramp is a different number. A
percentile is a claim about a CATCHMENT. A catchment is a shape.

WHY THE GROUPS AND NOT THE NAMED WATERSHEDS. The second answer used
`FWA_NAMED_WATERSHEDS_POLY`, and that layer answers a different question: it is 11,580
basins covering 2.68 MILLION km2 of a 945,000 km2 province, because a named watershed
contains its tributaries' watersheds. Drawn as they come they stack four deep and the
largest wins; filtered to the small ones they leave the province full of holes. Both
failures were visible on screen — busy where they overlapped, blank where they did not.

`FWA_WATERSHED_GROUPS_POLY` is the Province's own complete cover: 246 polygons, 948,072 km2,
ZERO overlap — which is British Columbia, once. It is also the unit the rest of the FWA is
keyed on, so a stream already carries its `WATERSHED_GROUP_CODE` and a gauge's group is a
lookup rather than a point-in-polygon.
"""

from __future__ import annotations

from pathlib import Path

from pipeline.deliver.tiles.layers import BY_NAME

#: Simplification, in metres, and it is DERIVED rather than chosen by eye.
#:
#: At most one pixel at the deepest zoom the field is DRAWN at (app/tools/handover.test.ts
#: holds it to a pixel at the handover). Chosen when the handover was z10, where one pixel
#: is ~100 m at 49 N; it was 500 m, and overzoomed to z10 every 500 m run was a 5 px straight
#: edge (review 2026-10-22). The handover is z8 since T130 (one pixel ~400 m), so this could
#: rise toward that in a tile build to save bytes; it is not raised here, untested.
#: Simplified as one coverage, 100 m is 494k vertices of the source's 5.46M.
SIMPLIFY_M = 100.0

#: The layer in the FWA gpkg. See `data/fetch_data.py` for where it comes from.
LAYER = "watershed_groups"

#: The column that joins a group to the streams and lakes inside it.
KEY = "WATERSHED_GROUP_CODE"


def groups(gpkg: str):
    """The watershed groups, in BC Albers, with a `basin_id`. Shared with the bundle build."""
    import geopandas as gpd

    gdf = gpd.read_file(gpkg, layer=LAYER, engine="pyogrio")
    if gdf.crs and gdf.crs.to_epsg() != 3005:
        gdf = gdf.to_crs(3005)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    # The CODE rather than the id: it is short, stable, human-readable in a tile inspector,
    # and it is what the stream and lake tables carry.
    gdf["basin_id"] = gdf[KEY].astype(str)
    return gdf


def simplify_cover(geoms, tolerance_m: float) -> list:
    """Simplify the basins AS ONE COVERAGE: each shared border is simplified once and both neighbours keep it.

    One polygon at a time, two neighbours each cut their common border differently, and the field opened 3,552 slivers
    and 3,694 overlaps up to ~700 m across (measured on the shipped layer, review 2026-10-22) — white and black wedges a
    valley long once the field was overzoomed to z10. `coverage_simplify` (GEOS >= 3.12) keeps the cover a cover.
    The groups are the Province's complete, non-overlapping cover, so they are a valid coverage as delivered; a polygon
    that is not (one invalid ring in the source) is made valid first.
    """
    import shapely

    arr = shapely.make_valid(shapely.from_wkb(shapely.to_wkb(list(geoms))))
    return list(shapely.coverage_simplify(arr, tolerance_m, simplify_boundary=True))


def export_basins(gpkg: str, out_dir: Path) -> dict:
    """Write the basin layer. A missing groups layer is a skip, never a failure."""
    import fiona
    from pyproj import Transformer

    from pipeline.deliver.tiles.export import _to4326, _writer

    if LAYER not in set(fiona.listlayers(gpkg)):
        print(f"  basin            <- no {LAYER} layer, skipping")
        return {"basin": 0}

    spec = BY_NAME["basin"]
    write, close = _writer(out_dir, spec)
    tf = Transformer.from_crs(3005, 4326, always_xy=True)
    gdf = groups(gpkg)
    for row, g in zip(gdf.itertuples(), simplify_cover(gdf.geometry.values, SIMPLIFY_M)):
        if g is None or g.is_empty:
            continue
        write(_to4326(g, tf), {"basin_id": row.basin_id}, spec.minzoom)
    n = close()
    print(f"  basin            <- {LAYER} {n:>6,}  (simplified {SIMPLIFY_M:g} m, as one coverage)")
    return {"basin": n}
